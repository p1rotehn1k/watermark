import os
import io
import re
import math
import asyncio
import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

import discord
from discord import app_commands
from discord.ext import commands, tasks
import aiohttp
from PIL import Image, ImageDraw, ImageFont
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# Загрузка переменных окружения
# ─────────────────────────────────────────────
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN = os.getenv("DISCORD_TOKEN")


def _to_int(value: str):
    value = (value or "").strip()
    return int(value) if value.isdigit() else None

def _parse_ids(raw: str) -> list[int]:
    if not raw:
        return []
    result = []
    for part in raw.split(","):
        part = part.strip()
        if part.isdigit():
            result.append(int(part))
    return result


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_color(name: str, default=(255, 255, 255)):
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        parts = [int(x) for x in raw.split(",")]
        if len(parts) == 3:
            return tuple(parts)
    except ValueError:
        pass
    return default


def parse_tag_map(raw: str) -> dict:
    mapping = {}
    if not raw:
        return mapping
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        tag, cid = pair.split(":", 1)
        tag = tag.strip().lower()
        cid = cid.strip()
        if tag and cid.isdigit():
            mapping[tag] = int(cid)
    return mapping


def _resolve_db_path() -> str:
    override = os.getenv("DB_PATH", "").strip()
    if override:
        p = Path(override)
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)
    if Path("/app/data").exists():
        return "/app/data/stats.db"
    return os.path.join(BASE_DIR, "stats.db")


DB_PATH = _resolve_db_path()

# ─── Вотермарк ───
SOURCE_CHANNEL_ID = _to_int(os.getenv("SOURCE_CHANNEL_ID", ""))
TAG_MAP = parse_tag_map(os.getenv("TAG_MAP", ""))
OWNER_IDS = _parse_ids(os.getenv("OWNER_USER_ID", ""))
OWNER_USER_ID = OWNER_IDS[0] if OWNER_IDS else None   # первый — для обратной совместимости
GUILD_ID = _to_int(os.getenv("GUILD_ID", ""))

WATERMARK_LINES = os.getenv(
    "WATERMARK_LINES",
    "YouTube: IGROOOCK\\nTwitch: IGROOOCK",
).replace("\\n", "\n").split("\n")

WATERMARK_ANGLE = _env_int("WATERMARK_ANGLE", -30)
WATERMARK_OPACITY = _env_int("WATERMARK_OPACITY", 170)
WATERMARK_COLOR = _env_color("WATERMARK_COLOR", (225, 225, 225))
WATERMARK_FONT_SCALE = _env_float("WATERMARK_FONT_SCALE", 0.032)
WATERMARK_STEP_X = _env_int("WATERMARK_STEP_X", 320)
WATERMARK_STEP_Y = _env_int("WATERMARK_STEP_Y", 170)

BADGE_PATH = os.getenv("BADGE_PATH", "badge.png")
BADGE_SCALE = _env_float("BADGE_SCALE", 0.15)
BADGE_PADDING = _env_float("BADGE_PADDING", 0.25)
BADGE_OPACITY = _env_int("BADGE_OPACITY", 130)
BADGE_TEXT = os.getenv("BADGE_TEXT", "")
BADGE_TEXT_COLOR = _env_color("BADGE_TEXT_COLOR", (255, 255, 255))

# ─── Роли ───
JOIN_ROLE_ID = _to_int(os.getenv("JOIN_ROLE_ID", ""))
UNVERIFIED_ROLE_ID = _to_int(os.getenv("UNVERIFIED_ROLE_ID", ""))
MEMBER_ROLE_ID = _to_int(os.getenv("MEMBER_ROLE_ID", ""))

# ─── Каналы ───
VERIFY_CHANNEL_ID = _to_int(os.getenv("VERIFY_CHANNEL_ID", ""))
LOG_CHANNEL_ID = _to_int(os.getenv("LOG_CHANNEL_ID", ""))
TOP_CHANNEL_ID = _to_int(os.getenv("TOP_CHANNEL_ID", ""))

# ─── Тексты ───
HINT_TITLE = os.getenv("HINT_TITLE", "📸 Как опубликовать пост")
HINT_FOOTER = os.getenv("HINT_FOOTER", "Сообщения без тега удаляются автоматически")
HINT_TEXT = os.getenv("HINT_TEXT", "").strip()

VERIFY_TITLE = os.getenv("VERIFY_TITLE", "Верификация")
VERIFY_DESCRIPTION = os.getenv(
    "VERIFY_DESCRIPTION",
    "Нажмите кнопку ниже, чтобы получить доступ к серверу.",
)
VERIFY_BUTTON_LABEL = os.getenv("VERIFY_BUTTON_LABEL", "✅ Верифицироваться")

# ─── Отладка ───
print("=" * 50, flush=True)
print("DB_PATH              =", DB_PATH, flush=True)
print("SOURCE_CHANNEL_ID    =", SOURCE_CHANNEL_ID, flush=True)
print("VERIFY_CHANNEL_ID    =", VERIFY_CHANNEL_ID, flush=True)
print("GUILD_ID             =", GUILD_ID, flush=True)
print("OWNER_IDS            =", OWNER_IDS, flush=True)
print("TAG_MAP:", flush=True)
for tag, cid in TAG_MAP.items():
    print(f"  #{tag} → {cid}", flush=True)
print("=" * 50, flush=True)

if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN не задан")

# ─────────────────────────────────────────────
# Бот
# ─────────────────────────────────────────────
intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

verify_message_id = None
hint_message_id = None
top_hint_message_id = None
bot_ready_done = False

image_executor = ThreadPoolExecutor(max_workers=1)


# ─────────────────────────────────────────────
# БД
# ─────────────────────────────────────────────
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            tag TEXT NOT NULL,
            posted_at TEXT NOT NULL
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON posts(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_posted_at ON posts(posted_at)")
    conn.commit()
    conn.close()
    print(f"[DB] ✅ Инициализирована: {DB_PATH}", flush=True)

    # Проверка: читается ли БД
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM posts")
    cnt = cur.fetchone()[0]
    conn.close()
    print(f"[DB] В базе сейчас постов: {cnt}", flush=True)


def record_post(user_id: int, username: str, tag: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO posts (user_id, username, tag, posted_at) VALUES (?, ?, ?, ?)",
            (user_id, username, tag, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM posts")
        total = cur.fetchone()[0]
        conn.close()

        print(f"[DB] ✅ Пост от {username} → всего {total}", flush=True)
    except Exception as e:
        import traceback
        print(f"[DB] ❌ Ошибка записи: {e}", flush=True)
        traceback.print_exc()


RU_MONTHS = {
    1: "январь", 2: "февраль", 3: "март", 4: "апрель",
    5: "май", 6: "июнь", 7: "июль", 8: "август",
    9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь",
}
RU_MONTHS_GENITIVE = {
    1: "января", 2: "февраля", 3: "марта", 4: "апреля",
    5: "мая", 6: "июня", 7: "июля", 8: "августа",
    9: "сентября", 10: "октября", 11: "ноября", 12: "декабря",
}

def get_top(period: str = "month", limit: int = 5):
    """
    period: 'month' | 'week' | 'all' | 'YYYY-MM'
    Возвращает (rows, start, end, title_suffix)
    """
    now = datetime.now(timezone.utc)

    if period == "week":
        start = now - timedelta(days=7)
        end = now
        title_suffix = "за последние 7 дней"

    elif period == "all":
        start = datetime(2000, 1, 1, tzinfo=timezone.utc)
        end = now
        title_suffix = "за всё время"

    elif re.match(r"^\d{4}-\d{2}$", period or ""):
        year, month = map(int, period.split("-"))
        start = datetime(year, month, 1, tzinfo=timezone.utc)
        if month == 12:
            end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
        title_suffix = f"за {RU_MONTHS[month]} {year}"

    else:  # текущий месяц
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now
        title_suffix = f"за {RU_MONTHS[now.month]} {now.year}"

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT username, COUNT(*) AS cnt
        FROM posts
        WHERE posted_at >= ? AND posted_at < ?
        GROUP BY user_id
        ORDER BY cnt DESC, username ASC
        LIMIT ?
        """,
        (start.isoformat(), end.isoformat(), limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows, start, end, title_suffix


def get_user_stats(user_id: int):
    now = datetime.now(timezone.utc)
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT COUNT(*) FROM posts WHERE user_id = ? AND posted_at >= ?",
        (user_id, month_start.isoformat()),
    )
    month_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM posts WHERE user_id = ?", (user_id,))
    total_count = cur.fetchone()[0]
    cur.execute(
        """
        SELECT user_id, COUNT(*) AS cnt
        FROM posts
        WHERE posted_at >= ?
        GROUP BY user_id
        ORDER BY cnt DESC
        """,
        (month_start.isoformat(),),
    )
    all_rows = cur.fetchall()
    conn.close()

    rank = None
    for i, (uid, _) in enumerate(all_rows, start=1):
        if uid == user_id:
            rank = i
            break

    return month_count, total_count, rank


def _plural_posts(n: int) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return "пост"
    if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14):
        return "поста"
    return "постов"

def build_top_embed(period: str = "month") -> discord.Embed:
    rows, start, end, title_suffix = get_top(period)

    now = datetime.now(timezone.utc)
    title = f"🏆 Топ {title_suffix}"

    if not rows:
        return discord.Embed(
            title=title,
            description="Пока нет постов за этот период.",
            color=discord.Color.gold(),
        )

    medals = {0: "🥇", 1: "🥈", 2: "🥉"}
    lines = []
    for i, (username, cnt) in enumerate(rows):
        prefix = medals.get(i, f"`#{i + 1:>2}`")
        lines.append(f"{prefix}  **{username}** — {cnt} {_plural_posts(cnt)}")

    embed = discord.Embed(
        title=title,
        description="\n".join(lines),
        color=discord.Color.gold(),
    )
    date_str = f"{now.day} {RU_MONTHS_GENITIVE[now.month]} {now.year}"
    embed.set_footer(text=f"Обновлено {date_str}, {now.strftime('%H:%M')} UTC")
    return embed

# ─── Автодополнение для /top ───
async def top_period_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    now = datetime.now(timezone.utc)
    current_lower = (current or "").lower().strip()

    # Базовые варианты
    choices: list[app_commands.Choice] = [
        app_commands.Choice(name="📅 Текущий месяц", value="month"),
        app_commands.Choice(name="📅 Последние 7 дней", value="week"),
        app_commands.Choice(name="📅 Всё время", value="all"),
    ]

    # Последние 12 месяцев (текущий + 11 назад)
    for i in range(12):
        month = now.month - i
        year = now.year
        while month <= 0:
            month += 12
            year -= 1
        value = f"{year}-{month:02d}"
        label = f"📆 {RU_MONTHS[month].capitalize()} {year}"
        if i == 0:
            label += " (текущий)"
        choices.append(app_commands.Choice(name=label, value=value))

    if not current_lower:
        return choices[:25]

    filtered = [
        c for c in choices
        if current_lower in c.value.lower() or current_lower in c.name.lower()
    ]
    return filtered[:25]


# ─────────────────────────────────────────────
# Шрифты и вотермарка
# ─────────────────────────────────────────────
def load_font(size: int):
    fonts = [
        os.path.join(BASE_DIR, "arialbd.ttf"),
        os.path.join(BASE_DIR, "arial.ttf"),
        os.path.join(BASE_DIR, "fonts", "arialbd.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "C:\\Windows\\Fonts\\arialbd.ttf",
    ]
    for path in fonts:
        try:
            return ImageFont.truetype(path, size)
        except (IOError, OSError):
            continue
    print("[FONT] ⚠️ TTF не найден, использую load_default()", flush=True)
    return ImageFont.load_default()


def _make_diagonal_tile(width: int, height: int) -> Image.Image:
    font_size = max(14, int(min(width, height) * WATERMARK_FONT_SCALE))
    font = load_font(font_size)

    dummy = Image.new("RGBA", (10, 10))
    dummy_draw = ImageDraw.Draw(dummy)

    line_widths = []
    line_heights = []
    for line in WATERMARK_LINES:
        bbox = dummy_draw.textbbox((0, 0), line, font=font)
        line_widths.append(bbox[2] - bbox[0])
        line_heights.append(bbox[3] - bbox[1])

    line_height = max(line_heights) if line_heights else font_size
    line_spacing = int(line_height * 1.25)
    block_w = max(line_widths) if line_widths else 100
    block_h = line_spacing * len(WATERMARK_LINES)

    tile_w = max(WATERMARK_STEP_X, block_w + 40)
    tile_h = max(WATERMARK_STEP_Y, block_h + 40)

    tile = Image.new("RGBA", (tile_w, tile_h), (0, 0, 0, 0))
    tile_draw = ImageDraw.Draw(tile)
    main_color = (*WATERMARK_COLOR, WATERMARK_OPACITY)

    text_x = (tile_w - block_w) // 2
    text_y = (tile_h - block_h) // 2

    for k, line in enumerate(WATERMARK_LINES):
        lw = line_widths[k]
        lx = text_x + (block_w - lw) // 2
        ly = text_y + k * line_spacing
        tile_draw.text((lx, ly), line, font=font, fill=main_color)

    tile = tile.rotate(WATERMARK_ANGLE, resample=Image.BICUBIC, expand=True)
    tw, th = tile.size

    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    for y in range(-th, height + th, th):
        for x in range(-tw, width + tw, tw):
            canvas.paste(tile, (x, y), tile)

    return canvas


def _paste_badge(base: Image.Image) -> None:
    if not BADGE_PATH:
        return
    badge_path = BADGE_PATH if os.path.isabs(BADGE_PATH) else os.path.join(BASE_DIR, BADGE_PATH)
    if not os.path.exists(badge_path):
        print(f"[BADGE] Файл не найден: {badge_path}", flush=True)
        return

    badge = Image.open(badge_path).convert("RGBA")
    width, height = base.size
    badge_size = int(width * BADGE_SCALE)
    badge = badge.resize((badge_size, badge_size), Image.LANCZOS)

    alpha = badge.split()[3]
    alpha = alpha.point(lambda p: int(p * (BADGE_OPACITY / 255)))
    badge.putalpha(alpha)

    pad = int(badge_size * BADGE_PADDING)
    x = pad
    y = height - badge_size - pad

    font = None
    if BADGE_TEXT:
        font_size = max(14, int(badge_size * 0.18))
        font = load_font(font_size)
        dummy = Image.new("RGBA", (10, 10))
        bbox = ImageDraw.Draw(dummy).textbbox((0, 0), BADGE_TEXT, font=font)
        text_h = (bbox[3] - bbox[1]) + int(badge_size * 0.1)
        y -= text_h

    layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    layer.paste(badge, (x, y), badge)
    base.alpha_composite(layer)

    if BADGE_TEXT and font:
        draw = ImageDraw.Draw(base)
        bbox = draw.textbbox((0, 0), BADGE_TEXT, font=font)
        text_w = bbox[2] - bbox[0]
        text_x = x + (badge_size - text_w) // 2
        text_y = y + badge_size + int(badge_size * 0.05)
        draw.text(
            (text_x, text_y),
            BADGE_TEXT,
            font=font,
            fill=(*BADGE_TEXT_COLOR, BADGE_OPACITY),
        )


def add_watermark(image_bytes: bytes, username: str = "", date_str: str = "") -> io.BytesIO:
    base = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    width, height = base.size

    tile = _make_diagonal_tile(width, height)
    base = Image.alpha_composite(base, tile)

    _paste_badge(base)

    output = io.BytesIO()
    base.save(output, format="PNG")
    output.seek(0)
    return output


# ─────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────
async def get_target_channel(channel_id: int):
    channel = bot.get_channel(channel_id)
    if channel is not None:
        return channel
    try:
        return await bot.fetch_channel(channel_id)
    except (discord.NotFound, discord.Forbidden):
        return None


def find_tag(text: str, message: discord.Message):
    if text:
        for word in re.findall(r"#([^\s#<>]+)", text):
            key = word.strip().lower().strip(".,!?;:()[]{}")
            if key in TAG_MAP:
                return key
    for channel in message.channel_mentions:
        name = (channel.name or "").lower().strip()
        if name in TAG_MAP:
            return name
    return None


def strip_tag(text: str, tag: str) -> str:
    if not text:
        return text
    pattern = re.compile(rf"#{re.escape(tag)}\b", re.IGNORECASE)
    cleaned = pattern.sub("", text)
    cleaned = re.sub(r"<#\d+>", "", cleaned)
    lines = [ln.rstrip() for ln in cleaned.split("\n")]
    return "\n".join(lines).strip()

async def safe_pin(msg: discord.Message):
    try:
        if msg.pinned:
            return
        await msg.pin(reason="Подсказка")
        await asyncio.sleep(1.5)
    except discord.HTTPException as e:
        if e.status == 429:
            print(f"[PIN] Rate limit, жду 5 секунд...", flush=True)
            await asyncio.sleep(5)
            try:
                await msg.pin(reason="Подсказка (retry)")
            except Exception as e2:
                print(f"[PIN] Повторная ошибка: {e2}", flush=True)
        else:
            print(f"[PIN] HTTP ошибка: {e}", flush=True)
    except Exception as e:
        print(f"[PIN] {e}", flush=True)

async def delete_message_safe(message: discord.Message):
    try:
        await message.delete()
    except discord.Forbidden:
        print(f"[DELETE] ❌ Нет права Manage Messages", flush=True)
    except discord.NotFound:
        pass
    except Exception as e:
        print(f"[DELETE] Ошибка: {e}", flush=True)


async def notify_channel_safe(channel, text: str = None, embed: discord.Embed = None, delete_after: int = 10):
    try:
        if embed is not None:
            await channel.send(embed=embed, delete_after=delete_after)
        elif text:
            await channel.send(text, delete_after=delete_after)
    except Exception as e:
        print(f"[NOTIFY] {e}", flush=True)


async def process_and_publish(
    *,
    source_channel,
    target_id: int,
    author,
    attachments: list,
    text: str,
    tag: str = "",
) -> bool:
    target_channel = await get_target_channel(target_id)
    if target_channel is None:
        msg = f"❌ Не могу найти канал для тега `#{tag}` (ID {target_id})."
        await notify_channel_safe(source_channel, text=msg)
        return False

    date_str = datetime.now().strftime("%d.%m.%Y")
    files = []
    loop = asyncio.get_running_loop()

    for idx, att in enumerate(attachments, start=1):
        try:
            image_bytes = await att.read()
        except Exception as e:
            print(f"Ошибка чтения {att.filename}: {e}", flush=True)
            continue
        try:
            watermarked = await loop.run_in_executor(
                image_executor,
                add_watermark,
                image_bytes,
                author.display_name,
                date_str,
            )
            files.append(discord.File(fp=watermarked, filename=f"rf4_{idx}.png"))
        except Exception as e:
            import traceback
            print(f"❌ Ошибка обработки {att.filename}: {e}", flush=True)
            traceback.print_exc()

    if not files:
        await notify_channel_safe(source_channel, text="❌ Не удалось обработать изображения.")
        return False

    MAX_FILES = 10
    published = False
    for i in range(0, len(files), MAX_FILES):
        chunk = files[i:i + MAX_FILES]
        content = text if i == 0 else None
        try:
            await target_channel.send(content=content, files=chunk)
            published = True
        except Exception as e:
            print(f"Ошибка отправки: {e}", flush=True)

    # ⬇⬇⬇ ЗАПИСЬ В БД
    if published:
        record_post(author.id, author.display_name, tag)
    else:
        print(f"[PUBLISH] ❌ Не опубликован (тег #{tag})", flush=True)

    return published


# ─────────────────────────────────────────────
# Кнопка верификации
# ─────────────────────────────────────────────
class VerifyView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label=VERIFY_BUTTON_LABEL,
        style=discord.ButtonStyle.green,
        custom_id="verify_button",
    )
    async def verify(self, interaction: discord.Interaction, button: discord.ui.Button):
        guild = interaction.guild
        member = interaction.user

        if not MEMBER_ROLE_ID:
            await interaction.response.send_message("❌ Роль Member не настроена.", ephemeral=True)
            return

        member_role = guild.get_role(MEMBER_ROLE_ID)
        if member_role is None:
            await interaction.response.send_message("❌ Роль не найдена.", ephemeral=True)
            return

        if member_role in member.roles:
            await interaction.response.send_message("ℹ️ Вы уже верифицированы.", ephemeral=True)
            return

        try:
            if UNVERIFIED_ROLE_ID:
                unverified = guild.get_role(UNVERIFIED_ROLE_ID)
                if unverified and unverified in member.roles:
                    await member.remove_roles(unverified, reason="Прошёл верификацию")
            await member.add_roles(member_role, reason="Прошёл верификацию")
            await interaction.response.send_message(
                "✅ Готово! Добро пожаловать на сервер.", ephemeral=True
            )
            if LOG_CHANNEL_ID:
                log = bot.get_channel(LOG_CHANNEL_ID)
                if log:
                    try:
                        await log.send(f"✅ {member.mention} прошёл верификацию.")
                    except Exception:
                        pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ У бота нет прав на выдачу роли.", ephemeral=True
            )


# ─────────────────────────────────────────────
# Слэш-команды
# ─────────────────────────────────────────────
@bot.tree.command(name="top", description="Топ публикующих (только для владельца)")
@app_commands.describe(period="Период: месяц, неделя или всё время")
@app_commands.autocomplete(period=top_period_autocomplete)
async def top_command(interaction: discord.Interaction, period: str = "month"):
    if not OWNER_IDS:
        await interaction.response.send_message(
            "❌ Владелец не настроен (OWNER_USER_ID пустой).",
            ephemeral=True,
        )
        return

    if interaction.user.id not in OWNER_IDS:
        await interaction.response.send_message(
            "❌ Эта команда доступна только владельцу.",
            ephemeral=True,
        )
        return

    # Подтверждаем сразу
    await interaction.response.defer(ephemeral=True)

    period = (period or "month").strip()
    if not (period in ("month", "week", "all") or re.match(r"^\d{4}-\d{2}$", period)):
        period = "month"

    try:
        embed = build_top_embed(period)
    except Exception as e:
        print(f"[TOP] Ошибка БД: {e}", flush=True)
        await interaction.followup.send("❌ Ошибка при чтении базы.", ephemeral=True)
        return

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="mystats", description="Ваша статистика публикаций")
async def mystats_command(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    try:
        month_count, total_count, rank = get_user_stats(interaction.user.id)
    except Exception as e:
        print(f"[MYSTATS] Ошибка БД: {e}", flush=True)
        await interaction.followup.send("❌ Ошибка при чтении базы.", ephemeral=True)
        return

    now = datetime.now(timezone.utc)
    month_name = RU_MONTHS[now.month]

    embed = discord.Embed(
        title=f"📊 {interaction.user.display_name}",
        color=discord.Color.blue(),
    )
    embed.add_field(name=f"За {month_name}", value=f"**{month_count}** постов", inline=True)
    embed.add_field(name="Всего", value=f"**{total_count}** постов", inline=True)
    if rank:
        embed.add_field(name="Место за месяц", value=f"**#{rank}**", inline=True)

    await interaction.followup.send(embed=embed, ephemeral=True)


@bot.tree.command(name="testdb", description="Показать всё содержимое БД (только для владельца)")
async def testdb_command(interaction: discord.Interaction):
    if not OWNER_IDS or interaction.user.id not in OWNER_IDS:
        await interaction.response.send_message("❌ Только для владельца.", ephemeral=True)
        return

    # Подтверждаем сразу
    await interaction.response.defer(ephemeral=True)

    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM posts")
        total = cur.fetchone()[0]

        cur.execute("SELECT COUNT(DISTINCT user_id) FROM posts")
        users = cur.fetchone()[0]

        cur.execute("""
            SELECT username, COUNT(*) AS cnt
            FROM posts
            GROUP BY user_id
            ORDER BY cnt DESC
            LIMIT 10
        """)
        top_rows = cur.fetchall()
        conn.close()

        text = f"Всего постов: **{total}**\n"
        text += f"Уникальных пользователей: **{users}**\n\n"
        if top_rows:
            text += "**Топ-10 по всей базе:**\n"
            for i, (uname, cnt) in enumerate(top_rows, start=1):
                text += f"`#{i}` **{uname}** — {cnt}\n"
        else:
            text += "_База пуста._"

        if len(text) > 1900:
            text = text[:1900] + "..."

        await interaction.followup.send(text, ephemeral=True)
    except Exception as e:
        print(f"[TESTDB] Ошибка БД: {e}", flush=True)
        await interaction.followup.send(f"❌ Ошибка БД: {e}", ephemeral=True)


# ─────────────────────────────────────────────
# Обслуживание канала верификации + подсказка
# ─────────────────────────────────────────────
async def find_button_message(channel: discord.TextChannel):
    try:
        async for msg in channel.history(limit=50):
            if msg.author == bot.user and msg.components:
                return msg
    except Exception as e:
        print(f"[VERIFY] {e}", flush=True)
    return None


async def publish_verify_button(channel: discord.TextChannel):
    embed = discord.Embed(
        title=VERIFY_TITLE,
        description=VERIFY_DESCRIPTION,
        color=discord.Color.green(),
    )
    embed.add_field(
        name="Что делать?",
        value=f"Нажмите кнопку **{VERIFY_BUTTON_LABEL}** ниже.",
        inline=False,
    )
    try:
        msg = await channel.send(embed=embed, view=VerifyView())
        return msg
    except Exception as e:
        print(f"[VERIFY] {e}", flush=True)
        return None


async def purge_verify_channel(keep_id=None):
    if not VERIFY_CHANNEL_ID:
        return
    channel = bot.get_channel(VERIFY_CHANNEL_ID)
    if channel is None:
        return
    try:
        async for msg in channel.history(limit=200):
            if keep_id and msg.id == keep_id:
                continue
            try:
                await msg.delete()
            except discord.Forbidden:
                return
            except Exception:
                pass
    except Exception as e:
        print(f"[PURGE verify] {e}", flush=True)


async def find_hint_message(channel: discord.TextChannel):
    try:
        async for msg in channel.history(limit=50):
            if msg.author == bot.user and msg.embeds:
                for emb in msg.embeds:
                    if emb.title == HINT_TITLE:
                        return msg
    except Exception as e:
        print(f"[HINT] {e}", flush=True)
    return None

async def find_top_hint(channel: discord.TextChannel):
    try:
        async for msg in channel.history(limit=50):
            if msg.author == bot.user and msg.embeds:
                for emb in msg.embeds:
                    if emb.title == "🏆 Топ":
                        return msg
    except Exception as e:
        print(f"[TOP-HINT] {e}", flush=True)
    return None


def build_top_hint_embed() -> discord.Embed:
    return discord.Embed(
        title="🏆 Топ",
        description=(
            "📊 `/mystats` — Ваша статистика публикаций\n"
            "📊 `/top` — Выводит первые 5 мест(Уберу ее с подсказки т.к. команда будет только у тебя)\n"
            "📊 `/testdb` — Команда для отладки (Видишь только ты и я, тоже не будет в подсказке)"
        ),
        color=discord.Color.gold(),
    )


async def publish_top_hint(channel: discord.TextChannel):
    try:
        msg = await channel.send(embed=build_top_hint_embed())
        try:
            await msg.pin(reason="Подсказка по командам")
        except Exception:
            pass
        print("✅ Подсказка топа опубликована.", flush=True)
        return msg
    except Exception as e:
        print(f"[TOP-HINT] {e}", flush=True)
        return None

def build_hint_embed() -> discord.Embed:
    if HINT_TEXT:
        body = HINT_TEXT
    else:
        tags_line = " · ".join(f"`#{t}`" for t in TAG_MAP.keys()) or "—"
        body = (
            f"**1.** Прикрепите одну или несколько картинок\n"
            f"**2.** В тексте укажите **тег** из списка:\n"
            f"{tags_line}\n"
            f"**3.** Отправьте — бот опубликует пост в нужный канал\n\n"
            f"**Пример:**\n"
            f"```\n# ахтуба\nТочка 84:108\nклипса 17\nНа что было поймано\nВаши скрины до 5 шт\n```"
        )
    embed = discord.Embed(title=HINT_TITLE, description=body, color=discord.Color.blue())
    if HINT_FOOTER:
        embed.set_footer(text=HINT_FOOTER)
    return embed


async def publish_hint(channel: discord.TextChannel):
    try:
        msg = await channel.send(embed=build_hint_embed())
        try:
            await msg.pin(reason="Инструкция")
        except Exception:
            pass
        return msg
    except Exception as e:
        print(f"[HINT] {e}", flush=True)
        return None


async def purge_source_channel(keep_ids=None):
    if not SOURCE_CHANNEL_ID:
        return
    channel = bot.get_channel(SOURCE_CHANNEL_ID)
    if channel is None:
        return
    keep_ids = keep_ids or set()
    try:
        async for msg in channel.history(limit=300):
            if msg.id in keep_ids or msg.pinned:
                continue
            if msg.author == bot.user and msg.embeds:
                continue
            try:
                await msg.delete()
            except discord.Forbidden:
                return
            except Exception:
                pass
    except Exception as e:
        print(f"[PURGE source] {e}", flush=True)


# ─────────────────────────────────────────────
# Автоочистка
# ─────────────────────────────────────────────
@tasks.loop(minutes=5)
async def auto_clean():
    global hint_message_id, top_hint_message_id, verify_message_id

    # ─── Source-канал ───
    if SOURCE_CHANNEL_ID:
        src = bot.get_channel(SOURCE_CHANNEL_ID)
        if src:
            exists = False
            if hint_message_id:
                try:
                    await src.fetch_message(hint_message_id)
                    exists = True
                except (discord.NotFound, discord.Forbidden):
                    exists = False

            if not exists:
                print("[AUTO] Подсказка в source-канале пропала — восстанавливаю.", flush=True)
                msg = await publish_hint(src)
                if msg:
                    hint_message_id = msg.id

            # Чистим мусор
            if hint_message_id:
                await purge_source_channel(keep_ids={hint_message_id})
            else:
                await purge_source_channel()

    # ─── Канал топа ───
    if TOP_CHANNEL_ID:
        top_ch = bot.get_channel(TOP_CHANNEL_ID)
        if top_ch:
            exists = False
            if top_hint_message_id:
                try:
                    await top_ch.fetch_message(top_hint_message_id)
                    exists = True
                except (discord.NotFound, discord.Forbidden):
                    exists = False

            if not exists:
                print("[AUTO] Подсказка в канале топа пропала — восстанавливаю.", flush=True)
                msg = await publish_top_hint(top_ch)
                if msg:
                    top_hint_message_id = msg.id

            # Чистим мусор
            keep = {top_hint_message_id} if top_hint_message_id else set()
            try:
                async for msg in top_ch.history(limit=100):
                    if msg.id in keep or msg.pinned:
                        continue
                    try:
                        await msg.delete()
                    except discord.Forbidden:
                        break
                    except Exception:
                        pass
            except Exception as e:
                print(f"[PURGE top] {e}", flush=True)

    # ─── Канал верификации ───
    if VERIFY_CHANNEL_ID:
        ch = bot.get_channel(VERIFY_CHANNEL_ID)
        if ch:
            exists = False
            if verify_message_id:
                try:
                    await ch.fetch_message(verify_message_id)
                    exists = True
                except (discord.NotFound, discord.Forbidden):
                    exists = False

            if not exists:
                print("[AUTO] Кнопка верификации пропала — восстанавливаю.", flush=True)
                msg = await publish_verify_button(ch)
                if msg:
                    verify_message_id = msg.id

            await purge_verify_channel(keep_id=verify_message_id)


@auto_clean.before_loop
async def before_auto_clean():
    await bot.wait_until_ready()


# ─────────────────────────────────────────────
# События
# ─────────────────────────────────────────────
@bot.event
async def on_ready():
    global verify_message_id, hint_message_id, top_hint_message_id, bot_ready_done

    # Защита от повторного запуска при реконнекте
    if bot_ready_done:
        print("[READY] Повторный реконнект — инициализацию пропускаю.", flush=True)
        return
    bot_ready_done = True

    init_db()
    print(f"Бот {bot.user} готов к работе!", flush=True)

    bot.add_view(VerifyView())

    # ─── Синхронизация слэш-команд ───
    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            bot.tree.clear_commands(guild=guild)
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
            print(f"Синхронизировано {len(synced)} слэш-команд на сервере {GUILD_ID}.", flush=True)
            for cmd in synced:
                print(f"  /{cmd.name} — {cmd.description}", flush=True)
        else:
            synced = await bot.tree.sync()
            print(f"Синхронизировано {len(synced)} слэш-команд глобально.", flush=True)
    except Exception as e:
        print(f"Ошибка синхронизации: {e}", flush=True)

    await asyncio.sleep(2)  # пауза, чтобы не долбить Discord

    # ─── Подсказка в source-канале ───
    if SOURCE_CHANNEL_ID:
        src = bot.get_channel(SOURCE_CHANNEL_ID)
        if src:
            existing = await find_hint_message(src)
            if existing:
                hint_message_id = existing.id
                await safe_pin(existing)
            else:
                msg = await publish_hint(src)
                if msg:
                    hint_message_id = msg.id

    await asyncio.sleep(2)

    # ─── Подсказка в канале топа ───
    if TOP_CHANNEL_ID:
        top_ch = bot.get_channel(TOP_CHANNEL_ID)
        if top_ch:
            existing = await find_top_hint(top_ch)
            if existing:
                top_hint_message_id = existing.id
                await safe_pin(existing)
            else:
                msg = await publish_top_hint(top_ch)
                if msg:
                    top_hint_message_id = msg.id
        else:
            print(f"❌ Канал топа {TOP_CHANNEL_ID} не найден.", flush=True)

    await asyncio.sleep(2)

    # ─── Верификация ───
    if VERIFY_CHANNEL_ID:
        channel = bot.get_channel(VERIFY_CHANNEL_ID)
        if channel:
            existing = await find_button_message(channel)
            if existing:
                verify_message_id = existing.id
            else:
                msg = await publish_verify_button(channel)
                if msg:
                    verify_message_id = msg.id
            await purge_verify_channel(keep_id=verify_message_id)

    # ─── Автоочистка ───
    if not auto_clean.is_running():
        auto_clean.start()
        print("Автоочистка запущена.", flush=True)


@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild
    if JOIN_ROLE_ID:
        role = guild.get_role(JOIN_ROLE_ID)
        if role:
            try:
                await member.add_roles(role, reason="Автовыдача")
            except discord.Forbidden:
                pass
    if UNVERIFIED_ROLE_ID:
        role = guild.get_role(UNVERIFIED_ROLE_ID)
        if role and role not in member.roles:
            try:
                await member.add_roles(role, reason="Новый участник")
            except discord.Forbidden:
                pass

    try:
        verify_channel = f"<#{VERIFY_CHANNEL_ID}>" if VERIFY_CHANNEL_ID else "#верификация"
        source_channel = f"<#{SOURCE_CHANNEL_ID}>" if SOURCE_CHANNEL_ID else "#публикации"
        tags_line = " · ".join(f"`#{t}`" for t in TAG_MAP.keys()) or "—"
        embed = discord.Embed(
            title=f"👋 Добро пожаловать, {member.display_name}!",
            description=(
                f"**1. Пройдите верификацию:** {verify_channel}\n"
                f"**2. Публикуйте посты:** {source_channel}\n\n"
                f"**Теги:** {tags_line}"
            ),
            color=discord.Color.green(),
        )
        await member.send(embed=embed)
    except Exception:
        pass

    await purge_verify_channel(keep_id=verify_message_id)
    if hint_message_id:
        await purge_source_channel(keep_ids={hint_message_id})
    else:
        await purge_source_channel()


@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if SOURCE_CHANNEL_ID and message.channel.id == SOURCE_CHANNEL_ID:
        # Логируем только факт обработки, без текста
        # print(f"[MSG] {message.author} | вложений: {len(message.attachments)}", flush=True)

        if hint_message_id and message.id == hint_message_id:
            return

        tag = find_tag(message.content, message)

        if tag is None:
            print(f"[MSG] ⚠️ Тег не найден от {message.author}", flush=True)
            await delete_message_safe(message)
            tags_line = "\n".join(f"• `#{t}` → <#{cid}>" for t, cid in TAG_MAP.items())
            embed = discord.Embed(
                title="❌ Тег не найден",
                description=f"Укажите **один** из тегов:\n\n{tags_line}",
                color=discord.Color.orange(),
            )
            await notify_channel_safe(message.channel, embed=embed)
            return

        image_attachments = [
            a for a in message.attachments
            if a.content_type and a.content_type.startswith("image/")
        ]

        if not image_attachments:
            print(f"[MSG] ⚠️ Нет картинок от {message.author}", flush=True)
            await delete_message_safe(message)
            first_tag = list(TAG_MAP.keys())[0] if TAG_MAP else "тег"
            embed = discord.Embed(
                title="❌ Нет картинок",
                description=f"Пример: `#{first_tag}` + файл.",
                color=discord.Color.orange(),
            )
            await notify_channel_safe(message.channel, embed=embed)
            return

        post_text = strip_tag(message.content, tag) or f"Улов от {message.author.display_name}"

        ok = await process_and_publish(
            source_channel=message.channel,
            target_id=TAG_MAP[tag],
            author=message.author,
            attachments=image_attachments,
            text=post_text,
            tag=tag,
        )

        await delete_message_safe(message)

        if ok:
            await notify_channel_safe(
                message.channel,
                text=f"✅ {message.author.mention}, пост опубликован в <#{TAG_MAP[tag]}>.",
            )
        else:
            await notify_channel_safe(
                message.channel,
                text=f"❌ {message.author.mention}, не удалось опубликовать.",
            )

    await bot.process_commands(message)


# ─────────────────────────────────────────────
# Запуск
# ─────────────────────────────────────────────
if __name__ == "__main__":
    bot.run(TOKEN)