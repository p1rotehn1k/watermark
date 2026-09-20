from PIL import Image
import io
from main import add_watermark

with open("badge.png", "rb") as f:
    data = f.read()

result = add_watermark(data)
with open("out.png", "wb") as f:
    f.write(result.read())
print("Готово: out.png")