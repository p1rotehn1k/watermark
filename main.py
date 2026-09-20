import os
import io
import re
import math
import sqlite3
import asyncio
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


# ─── Путь к БД (Bothost → /app/data/) ───
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
OWNER_USER_ID = _to_int(os.getenv("OWNER_USER_ID", ""))
GUILD_ID = _to_int(os.getenv("GUILD_ID", ""))

WATERMARK_LINES = os.getenv(
    "WATERMARK_LINES",
    "YouTube: NiOoooN\\nTrovo: NiOoooN\\nTwitch: NT_NiOoooN",
).replace("\\n", "\n").split("\n")

WATERMARK_ANGLE = _env_int("WATERMARK_ANGLE", -30)
WATERMARK_OPACITY = _env_int("WATERMARK_OPACITY", 60)
WATERMARK_COLOR = _env_color("WATERMARK_COLOR", (255, 255, 255))
WATERMARK_FONT_SCALE = _env_float("WATERMARK_FONT_SCALE", 0.05)
WATERMARK_STEP_X = _env_int("WATERMARK_STEP_X", 600)
WATERMARK_STEP_Y = _env_int("WATERMARK_STEP_Y", 200)

BADGE_PATH = os.getenv("BADGE_PATH", "badge.png")
BADGE_SCALE = _env_float("BADGE_SCALE", 0.12)
BADGE_PADDING = _env_float("BADGE_PADDING", 0.2)
BADGE_OPACITY = _env_int("BADGE_OPACITY", 150)
BADGE_TEXT = os.getenv("BADGE_TEXT", "")
BADGE_TEXT_COLOR = _env_color("BADGE_TEXT_COLOR", (255, 255, 255))

# ─── Роли ───
JOIN_ROLE_ID = _to_int(os.getenv("JOIN_ROLE_ID", ""))
UNVERIFIED_ROLE_ID = _to_int(os.getenv("UNVERIFIED_ROLE_ID", ""))
MEMBER_ROLE_ID = _to_int(os.getenv("MEMBER_ROLE_ID", ""))

# ─── Каналы ───
VERIFY_CHANNEL_ID = _to_int(os.getenv("VERIFY_CHANNEL_ID", ""))
LOG_CHANNEL_ID = _to_int(os.getenv("LOG_CHANNEL_ID", ""))
LEADERBOARD_CHANNEL_ID = _to_int(os.getenv("LEADERBOARD_CHANNEL_ID", ""))

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
print("=" * 50)
print("DB_PATH              =", DB_PATH)
print("SOURCE_CHANNEL_ID    =", SOURCE_CHANNEL_ID)
print("VERIFY_CHANNEL_ID    =", VERIFY_CHANNEL_ID)
print("GUILD_ID             =", GUILD_ID)
print("WATERMARK_FONT_SCALE =", WATERMARK_FONT_SCALE)
print("WATERMARK_STEP_X/Y   =", WATERMARK_STEP_X, "/", WATERMARK_STEP_Y)
print("WATERMARK_LINES      =", WATERMARK_LINES)
print("BADGE_SCALE          =", BADGE_SCALE)
print("TAG_MAP:")
for tag, cid in TAG_MAP.items():
    print(f"  #{tag} → {cid}")
print("=" * 50)

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

# Один поток для Pillow — снижает пиковое потребление памяти
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
    print(f"[DB] Инициализирована: {DB_PATH}")


def record_post(user_id: int, username: str, tag: str):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO posts (user_id, username, tag, posted_at) VALUES (?, ?, ?, ?)",
            (user_id, username, tag, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Ошибка записи: {e}")


def get_top(period: str = "month", limit: int = 15):
    now = datetime.now(timezone.utc)
    if period == "week":
        start = now - timedelta(days=7)
    elif period == "all":
        start = datetime(2000, 1, 1, tzinfo=timezone.utc)
    else:
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT username, COUNT(*) AS cnt
        FROM posts
        WHERE posted_at >= ?
        GROUP BY user_id
        ORDER BY cnt DESC, username ASC
        LIMIT ?
        """,
        (start.isoformat(), limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows, start


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
    rows, start = get_top(period)

    now = datetime.now(timezone.utc)
    if period == "month":
        title = f"🏆 Топ за {now.strftime('%B %Y')}"
    elif period == "week":
        title = "🏆 Топ за последние 7 дней"
    else:
        title = "🏆 Топ за всё время"

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
    embed.set_footer(text=f"Обновлено {now.strftime('%d.%m.%Y %H:%M')} UTC")
    return embed


# ─────────────────────────────────────────────
# Шрифты
# ─────────────────────────────────────────────
def load_font(size: int):
    fonts = [
        os.path.join(BASE_DIR, "arialbd.ttf"),
        os.path.join(BASE_DIR, "arial.ttf"),
        os.path.join(BASE_DIR, "fonts", "arialbd.ttf"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/msttcorefonts/Arial_Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "arialbd.ttf",
        "arial.ttf",
        "Arial.ttf",
        "DejaVuSans-Bold.ttf",
        "DejaVuSans.ttf",
        "C:\\Windows\\Fonts\\arialbd.ttf",
        "C:\\Windows\\Fonts\\arial.ttf",
    ]
    for path in fonts:
        try:
            f = ImageFont.truetype(path, size)
            print(f"[FONT] ✅ Загружен: {path} @ {size}px")
            return f
        except (IOError, OSError):
            continue

    print("[FONT] ⚠️ Ни один TTF-шрифт не найден, использую load_default() — размер игнорируется")
    return ImageFont.load_default()


# ─────────────────────────────────────────────
# Диагональный тайл (оптимизированная версия)
# ─────────────────────────────────────────────
def _make_diagonal_tile(width: int, height: int) -> Image.Image:
    """
    Создаёт холст с диагональным повторяющимся текстом.
    Использует маленький тайл и замащивает им картинку — экономит память.
    """
    font_size = max(14, int(min(width, height) * WATERMARK_FONT_SCALE))
    print(f"[WATERMARK] font_size={font_size}px (scale={WATERMARK_FONT_SCALE}, img={width}x{height})")
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
    line_spacing = int(line_height * 1.3)
    block_w = max(line_widths) if line_widths else 100
    block_h = line_spacing * len(WATERMARK_LINES)

    # Размер тайла
    tile_w = max(WATERMARK_STEP_X, block_w + 60)
    tile_h = max(WATERMARK_STEP_Y, block_h + 60)

    # Рисуем один тайл
    tile = Image.new("RGBA", (tile_w, tile_h), (0, 0, 0, 0))
    tile_draw = ImageDraw.Draw(tile)
    color = (*WATERMARK_COLOR, WATERMARK_OPACITY)

    text_x = (tile_w - block_w) // 2
    text_y = (tile_h - block_h) // 2
    for k, line in enumerate(WATERMARK_LINES):
        lw = line_widths[k]
        lx = text_x + (block_w - lw) // 2
        tile_draw.text(
            (lx, text_y + k * line_spacing),
            line,
            font=font,
            fill=color,
        )

    # Поворачиваем тайл
    tile = tile.rotate(WATERMARK_ANGLE, resample=Image.BICUBIC, expand=True)
    tw, th = tile.size

    # Замащиваем картинку тайлом
    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    for y in range(-th, height + th, th):
        for x in range(-tw, width + tw, tw):
            canvas.paste(tile, (x, y), tile)

    return canvas


# ─────────────────────────────────────────────
# Бейдж
# ─────────────────────────────────────────────
def _paste_badge(base: Image.Image) -> None:
    if not BADGE_PATH:
        return

    badge_path = BADGE_PATH if os.path.isabs(BADGE_PATH) else os.path.join(BASE_DIR, BADGE_PATH)
    if not os.path.exists(badge_path):
        print(f"[BADGE] Логотип не найден: {badge_path}")
        return

    try:
        badge = Image.open(badge_path).convert("RGBA")
    except Exception as e:
        print(f"[BADGE] Не удалось прочитать логотип: {e}")
        return

    width, height = base.size
    badge_size = int(width * BADGE_SCALE)
    badge = badge.resize((badge_size, badge_size), Image.LANCZOS)

    alpha = badge.split()[3]
    alpha = alpha.point(lambda p: int(p * (BADGE_OPACITY / 255)))
    badge.putalpha(alpha)

    pad = int(badge_size * BADGE_PADDING)

    x = pad
    y = height - badge_size - pad

    text_height = 0
    font = None
    if BADGE_TEXT:
        font_size = max(14, int(badge_size * 0.18))
        font = load_font(font_size)
        dummy = Image.new("RGBA", (10, 10))
        bbox = ImageDraw.Draw(dummy).textbbox((0, 0), BADGE_TEXT, font=font)
        text_height = (bbox[3] - bbox[1]) + int(badge_size * 0.1)
        y -= text_height

    layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    layer.paste(badge, (x, y), badge)
    base.alpha_composite(layer)

    if BADGE_TEXT and font:
        draw = ImageDraw.Draw(base)
        bbox = draw.textbbox((0, 0), BADGE_TEXT, font=font)
        text_w = bbox[2] - bbox[0]
        text_x = x + (badge_size - text_w) // 2
        text_y = y + badge_size + int(badge_size * 0.05)

        text_alpha = BADGE_OPACITY

        outline = (0, 0, 0, int(text_alpha * 0.85))
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                if dx == 0 and dy == 0:
                    continue
                draw.text(
                    (text_x + dx, text_y + dy),
                    BADGE_TEXT,
                    font=font,
                    fill=outline,
                )
        draw.text(
            (text_x, text_y),
            BADGE_TEXT,
            font=font,
            fill=(*BADGE_TEXT_COLOR, text_alpha),
        )


# ─────────────────────────────────────────────
# Главная функция вотермарки
# ─────────────────────────────────────────────
def add_watermark(image_bytes: bytes, username: str = "", date_str: str = "") -> io.BytesIO:
    base = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    width, height = base.size

    try:
        tile = _make_diagonal_tile(width, height)
        base = Image.alpha_composite(base, tile)
    except Exception as e:
        print(f"[WATERMARK] Ошибка диагонального тайла: {e}")

    try:
        _paste_badge(base)
    except Exception as e:
        print(f"[WATERMARK] Ошибка бейджа: {e}")

    output = io.BytesIO()
    base.save(output, format="PNG")
    output.seek(0)
    return output


# ─────────────────────────────────────────────
# Утилиты
# ─────────────────────────────────────────────
async def download_image(url: str):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            return await resp.read()


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


async def delete_message_safe(message: discord.Message):
    try:
        await message.delete()
    except discord.Forbidden:
        print(f"[DELETE] ❌ Нет права Manage Messages в канале {message.channel.id}")
    except discord.NotFound:
        pass
    except Exception as e:
        print(f"[DELETE] Ошибка: {e}")


async def dm_user_safe(user: discord.User, text: str = None, embed: discord.Embed = None):
    try:
        if embed is not None:
            await user.send(embed=embed)
        elif text:
            await user.send(text)
    except discord.Forbidden:
        pass
    except Exception as e:
        print(f"[DM] Ошибка: {e}")


async def process_and_publish(
    *,
    source_channel,
    target_id: int,
    author,
    attachments: list,
    text: str,
    reply_ephemeral: bool = False,
    interaction: discord.Interaction = None,
    tag: str = "",
) -> bool:
    target_channel = await get_target_channel(target_id)
    if target_channel is None:
        msg = f"❌ Не могу найти канал для тега `#{tag}` (ID {target_id})."
        if interaction:
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await dm_user_safe(author, msg)
        return False

    date_str = datetime.now().strftime("%d.%m.%Y")
    files = []
    loop = asyncio.get_event_loop()

    for idx, att in enumerate(attachments, start=1):
        try:
            image_bytes = await att.read()
        except Exception as e:
            print(f"Ошибка чтения {att.filename}: {e}")
            continue
        try:
            # Пул на 1 воркер — картинки обрабатываются по очереди
            watermarked = await loop.run_in_executor(
                image_executor,
                add_watermark,
                image_bytes,
                author.display_name,
                date_str,
            )
            files.append(discord.File(fp=watermarked, filename=f"rf4_{idx}.png"))
        except Exception as e:
            print(f"Ошибка обработки {att.filename}: {e}")

    if not files:
        msg = "❌ Не удалось обработать изображения."
        if interaction:
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await dm_user_safe(author, msg)
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
            print(f"Ошибка отправки: {e}")

    if published:
        record_post(author.id, author.display_name, tag)

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
            await interaction.response.send_message(
                "❌ Роль Member не настроена.", ephemeral=True
            )
            return

        member_role = guild.get_role(MEMBER_ROLE_ID)
        if member_role is None:
            await interaction.response.send_message(
                "❌ Роль не найдена. Сообщите админу.", ephemeral=True
            )
            return

        if member_role in member.roles:
            await interaction.response.send_message(
                "ℹ️ Вы уже верифицированы.", ephemeral=True
            )
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
                        await log.send(
                            f"✅ {member.mention} (`{member}`) прошёл верификацию."
                        )
                    except Exception:
                        pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "❌ У бота нет прав на выдачу роли.", ephemeral=True
            )


# ─────────────────────────────────────────────
# Слэш-команды
# ─────────────────────────────────────────────
async def tag_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    current = (current or "").lower().lstrip("#").strip()
    choices = []
    for tag in TAG_MAP.keys():
        if current in tag:
            choices.append(app_commands.Choice(name=f"#{tag}", value=tag))
    return choices[:25]


@bot.tree.command(name="post", description="Опубликовать пост в канал по тегу")
@app_commands.describe(
    tag="Тег канала",
    image1="Картинка 1",
    image2="Картинка 2",
    image3="Картинка 3",
    image4="Картинка 4",
    text="Текст поста",
)
@app_commands.autocomplete(tag=tag_autocomplete)
async def post_command(
    interaction: discord.Interaction,
    tag: str,
    image1: discord.Attachment,
    image2: discord.Attachment = None,
    image3: discord.Attachment = None,
    image4: discord.Attachment = None,
    text: str = None,
):
    tag_key = tag.strip().lower().lstrip("#")
    if tag_key not in TAG_MAP:
        await interaction.response.send_message(
            f"❌ Неизвестный тег `#{tag}`.", ephemeral=True
        )
        return

    if SOURCE_CHANNEL_ID and interaction.channel_id != SOURCE_CHANNEL_ID:
        await interaction.response.send_message(
            f"❌ Только в <#{SOURCE_CHANNEL_ID}>.", ephemeral=True
        )
        return

    attachments = [a for a in (image1, image2, image3, image4) if a is not None]
    attachments = [a for a in attachments if a.content_type and a.content_type.startswith("image/")]

    if not attachments:
        await interaction.response.send_message(
            "❌ Прикрепите хотя бы одну картинку.", ephemeral=True
        )
        return

    await interaction.response.defer(ephemeral=True)

    post_text = (text or "").strip() or f"Улов от {interaction.user.display_name}"

    ok = await process_and_publish(
        source_channel=interaction.channel,
        target_id=TAG_MAP[tag_key],
        author=interaction.user,
        attachments=attachments,
        text=post_text,
        interaction=interaction,
        tag=tag_key,
    )

    if ok:
        await interaction.followup.send(
            f"✅ Пост опубликован в <#{TAG_MAP[tag_key]}>.",
            ephemeral=True,
        )


@bot.tree.command(name="top", description="Топ публикующих")
@app_commands.describe(period="month / week / all")
async def top_command(interaction: discord.Interaction, period: str = "month"):
    period = (period or "month").lower()
    if period not in ("month", "week", "all"):
        period = "month"
    embed = build_top_embed(period)
    await interaction.response.send_message(embed=embed)


@bot.tree.command(name="mystats", description="Ваша статистика публикаций")
async def mystats_command(interaction: discord.Interaction):
    month_count, total_count, rank = get_user_stats(interaction.user.id)

    embed = discord.Embed(
        title=f"📊 {interaction.user.display_name}",
        color=discord.Color.blue(),
    )
    embed.add_field(name="За месяц", value=f"**{month_count}** постов", inline=True)
    embed.add_field(name="Всего", value=f"**{total_count}** постов", inline=True)
    if rank:
        embed.add_field(name="Место в месяце", value=f"**#{rank}**", inline=True)

    await interaction.response.send_message(embed=embed, ephemeral=True)


# ─────────────────────────────────────────────
# Обслуживание канала верификации
# ─────────────────────────────────────────────
async def find_button_message(channel: discord.TextChannel):
    try:
        async for msg in channel.history(limit=50):
            if msg.author == bot.user and msg.components:
                return msg
    except Exception as e:
        print(f"[VERIFY] Не удалось прочитать историю: {e}")
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
        print("✅ Сообщение с кнопкой опубликовано.")
        return msg
    except Exception as e:
        print(f"[VERIFY] Не удалось отправить сообщение: {e}")
        return None


async def purge_verify_channel(keep_id=None):
    if not VERIFY_CHANNEL_ID:
        return
    channel = bot.get_channel(VERIFY_CHANNEL_ID)
    if channel is None:
        return
    deleted = 0
    try:
        async for msg in channel.history(limit=200):
            if keep_id and msg.id == keep_id:
                continue
            try:
                await msg.delete()
                deleted += 1
            except discord.Forbidden:
                print("[PURGE] ❌ Нет права Manage Messages.")
                return
            except Exception:
                pass
        if deleted:
            print(f"[PURGE verify] Удалено: {deleted}")
    except Exception as e:
        print(f"[PURGE] Ошибка: {e}")


# ─────────────────────────────────────────────
# Подсказка
# ─────────────────────────────────────────────
async def find_hint_message(channel: discord.TextChannel):
    try:
        async for msg in channel.history(limit=50):
            if msg.author == bot.user and msg.embeds:
                for emb in msg.embeds:
                    if emb.title == HINT_TITLE:
                        return msg
    except Exception as e:
        print(f"[HINT] Не удалось прочитать историю: {e}")
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
            f"```\n# ахтуба\nТроф Каспика\n25 кг\n```\n\n"
            f"Или используйте команду `/post` — там теги выбираются из списка."
        )

    embed = discord.Embed(
        title=HINT_TITLE,
        description=body,
        color=discord.Color.blue(),
    )
    if HINT_FOOTER:
        embed.set_footer(text=HINT_FOOTER)
    return embed


async def publish_hint(channel: discord.TextChannel):
    try:
        msg = await channel.send(embed=build_hint_embed())
        try:
            await msg.pin(reason="Инструкция по публикации постов")
        except discord.Forbidden:
            print("[HINT] ❌ Нет права Manage Messages для закрепления.")
        except Exception as e:
            print(f"[HINT] Не удалось закрепить: {e}")
        print("✅ Подсказка опубликована и закреплена.")
        return msg
    except Exception as e:
        print(f"[HINT] Не удалось отправить подсказку: {e}")
        return None


async def purge_source_channel(keep_ids=None):
    if not SOURCE_CHANNEL_ID:
        return
    channel = bot.get_channel(SOURCE_CHANNEL_ID)
    if channel is None:
        return

    keep_ids = keep_ids or set()
    deleted = 0
    try:
        async for msg in channel.history(limit=300):
            if msg.id in keep_ids:
                continue
            if msg.pinned:
                continue
            if msg.author == bot.user and msg.embeds:
                continue
            try:
                await msg.delete()
                deleted += 1
            except discord.Forbidden:
                print("[PURGE] ❌ Нет права Manage Messages.")
                return
            except Exception:
                pass
        if deleted:
            print(f"[PURGE source] Удалено: {deleted}")
    except Exception as e:
        print(f"[PURGE source] Ошибка: {e}")


# ─────────────────────────────────────────────
# Автоочистка + ежедневный топ
# ─────────────────────────────────────────────
@tasks.loop(minutes=5)
async def auto_clean():
    if hint_message_id:
        await purge_source_channel(keep_ids={hint_message_id})
    else:
        await purge_source_channel()
    await purge_verify_channel(keep_id=verify_message_id)


@auto_clean.before_loop
async def before_auto_clean():
    await bot.wait_until_ready()


@tasks.loop(hours=24)
async def daily_top():
    if not LEADERBOARD_CHANNEL_ID:
        return
    channel = bot.get_channel(LEADERBOARD_CHANNEL_ID)
    if channel is None:
        return
    try:
        embed = build_top_embed("month")
        await channel.send(embed=embed)
    except Exception as e:
        print(f"[TOP] Ошибка: {e}")


@daily_top.before_loop
async def before_daily_top():
    await bot.wait_until_ready()


# ─────────────────────────────────────────────
# События
# ─────────────────────────────────────────────
@bot.event
async def on_ready():
    global verify_message_id, hint_message_id

    init_db()

    print(f"Бот {bot.user} готов к работе!")

    bot.add_view(VerifyView())

    # Синхронизация команд
    try:
        if GUILD_ID:
            guild = discord.Object(id=GUILD_ID)
            bot.tree.copy_global_to(guild=guild)
            synced = await bot.tree.sync(guild=guild)
        else:
            synced = await bot.tree.sync()
        print(f"Синхронизировано {len(synced)} слэш-команд.")
    except Exception as e:
        print(f"Ошибка синхронизации команд: {e}")

    # Подсказка
    if SOURCE_CHANNEL_ID:
        src = bot.get_channel(SOURCE_CHANNEL_ID)
        if src:
            existing = await find_hint_message(src)
            if existing:
                hint_message_id = existing.id
                print(f"Подсказка уже есть: {existing.id}")
                if not existing.pinned:
                    try:
                        await existing.pin(reason="Перезакрепление")
                    except Exception:
                        pass
            else:
                msg = await publish_hint(src)
                if msg:
                    hint_message_id = msg.id
        else:
            print(f"❌ Source-канал {SOURCE_CHANNEL_ID} не найден.")

    # Канал верификации
    if VERIFY_CHANNEL_ID:
        channel = bot.get_channel(VERIFY_CHANNEL_ID)
        if channel is None:
            print(f"❌ Канал верификации {VERIFY_CHANNEL_ID} не найден.")
        else:
            existing = await find_button_message(channel)
            if existing:
                verify_message_id = existing.id
                print(f"Кнопка найдена: {existing.id}")
            else:
                msg = await publish_verify_button(channel)
                if msg:
                    verify_message_id = msg.id
            await purge_verify_channel(keep_id=verify_message_id)

    if not auto_clean.is_running():
        auto_clean.start()
        print("Автоочистка запущена (каждые 5 минут).")

    if LEADERBOARD_CHANNEL_ID and not daily_top.is_running():
        daily_top.start()
        print("Ежедневный топ запущен.")


@bot.event
async def on_member_join(member: discord.Member):
    guild = member.guild

    if JOIN_ROLE_ID:
        role = guild.get_role(JOIN_ROLE_ID)
        if role:
            try:
                await member.add_roles(role, reason="Автовыдача при входе")
            except discord.Forbidden:
                print(f"❌ Нет прав на выдачу роли {role.name}")

    if UNVERIFIED_ROLE_ID:
        role = guild.get_role(UNVERIFIED_ROLE_ID)
        if role and role not in member.roles:
            try:
                await member.add_roles(role, reason="Новый участник")
            except discord.Forbidden:
                print(f"❌ Нет прав на выдачу роли {role.name}")

    try:
        verify_channel = f"<#{VERIFY_CHANNEL_ID}>" if VERIFY_CHANNEL_ID else "#верификация"
        source_channel = f"<#{SOURCE_CHANNEL_ID}>" if SOURCE_CHANNEL_ID else "#публикации"
        tags_line = " · ".join(f"`#{t}`" for t in TAG_MAP.keys()) or "—"

        embed = discord.Embed(
            title=f"👋 Добро пожаловать, {member.display_name}!",
            description=(
                f"**1. Пройдите верификацию:** {verify_channel}\n"
                f"**2. Публикуйте посты:** {source_channel}\n\n"
                f"**Как публиковать:** прикрепите картинки и укажите тег:\n{tags_line}"
            ),
            color=discord.Color.green(),
        )
        await member.send(embed=embed)
    except discord.Forbidden:
        pass
    except Exception as e:
        print(f"[DM] Ошибка: {e}")

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
        if hint_message_id and message.id == hint_message_id:
            return

        if OWNER_USER_ID is not None and message.author.id != OWNER_USER_ID:
            await bot.process_commands(message)
            return

        tag = find_tag(message.content, message)

        if tag is None:
            await delete_message_safe(message)
            tags_line = "\n".join(f"• `#{t}` → <#{cid}>" for t, cid in TAG_MAP.items())
            embed = discord.Embed(
                title="❌ Тег не найден",
                description=(
                    "**Как правильно:**\n"
                    "1. Прикрепите картинки\n"
                    "2. Укажите **один** из тегов:\n\n"
                    f"{tags_line}\n\n"
                    "Пример:\n"
                    "```\n# ахтуба\nТроф Каспика\n25 кг\n```"
                ),
                color=discord.Color.orange(),
            )
            await dm_user_safe(message.author, embed=embed)
            return

        image_attachments = [
            a for a in message.attachments
            if a.content_type and a.content_type.startswith("image/")
        ]
        if not image_attachments:
            await delete_message_safe(message)
            first_tag = list(TAG_MAP.keys())[0] if TAG_MAP else "тег"
            embed = discord.Embed(
                title="❌ Нет картинок",
                description=f"Прикрепите картинку. Пример: `#{first_tag}` + файл.",
                color=discord.Color.orange(),
            )
            await dm_user_safe(message.author, embed=embed)
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
            await dm_user_safe(
                message.author,
                f"✅ Ваш пост опубликован в <#{TAG_MAP[tag]}> (тег `#{tag}`).",
            )
        else:
            await dm_user_safe(
                message.author,
                "❌ Не удалось опубликовать. Проверьте права бота в целевом канале.",
            )

    await bot.process_commands(message)


# ─────────────────────────────────────────────
# Запуск
# ─────────────────────────────────────────────
if __name__ == "__main__":
    bot.run(TOKEN)