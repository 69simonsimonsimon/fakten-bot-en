"""
Thumbnail Creator — TikTok Bot EN (Facts)
Generates 1080×1920 thumbnails via DALL-E 3 (AI-generated scene matching the fact topic).
Falls back to a gradient placeholder if generation fails.
"""

from __future__ import annotations
import os
import io
import logging
import requests
from pathlib import Path

logger = logging.getLogger("syncin")

_FONT_PATHS_BOLD = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
    "/usr/share/fonts/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
    "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
]
_FONT_PATHS_REGULAR = [
    "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
    "/usr/share/fonts/liberation/LiberationSans-Regular.ttf",
    "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
    "/System/Library/Fonts/Helvetica.ttc",
]

THUMB_W, THUMB_H = 1080, 1920
_ACCENT_COLOR = (0, 140, 255)   # Blue
_BOT_TAG      = "DID YOU KNOW? 🤯"


def _load_font(size: int, bold: bool = True):
    from PIL import ImageFont
    paths = _FONT_PATHS_BOLD if bold else _FONT_PATHS_REGULAR
    for p in paths:
        if os.path.exists(p):
            try:
                return ImageFont.truetype(p, size)
            except Exception:
                pass
    return ImageFont.load_default()


def _openai_key() -> str:
    return os.environ.get("OPENAI_API_KEY", "").strip()


def _build_dalle_prompt(title: str, subreddit: str) -> str:
    """
    Ask GPT-4o-mini to write a cinematographic DALL-E 3 prompt
    that matches the Reddit story title and provokes curiosity.
    """
    key = _openai_key()
    if not key:
        raise ValueError("OPENAI_API_KEY nicht gesetzt")

    import openai
    client = openai.OpenAI(api_key=key)

    context = f"subreddit: r/{subreddit}" if subreddit else "Reddit Drama / AITA"
    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{
            "role": "user",
            "content": (
                f"Create a DALL-E 3 image prompt for a YouTube Shorts thumbnail (9:16 portrait format).\n"
                f"Fact/topic title: \"{title}\"\n"
                f"Context: {context}\n\n"
                f"Requirements:\n"
                f"- Cinematic, photorealistic or hyper-realistic image\n"
                f"- Visually striking, dramatic, surprising or shocking\n"
                f"- Directly represents the topic visually (e.g. historical scene, nature, science)\n"
                f"- NO text, numbers or letters in the image\n"
                f"- High contrast, dramatic lighting, fills the full portrait frame\n"
                f"- Should make viewers stop scrolling and want to learn more\n\n"
                f"Reply with ONLY the English DALL-E prompt, nothing else."
            ),
        }],
        max_tokens=250,
        temperature=0.9,
    )
    return resp.choices[0].message.content.strip().strip('"')


def _generate_dalle_image(dalle_prompt: str) -> bytes:
    """Generate image via DALL-E 3, return raw JPEG bytes."""
    import openai
    key = _openai_key()
    client = openai.OpenAI(api_key=key)

    resp = client.images.generate(
        model="dall-e-3",
        prompt=dalle_prompt,
        size="1024x1792",   # closest portrait to 1080×1920
        quality="hd",
        n=1,
    )
    url = resp.data[0].url
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.content


def _make_fallback_image() -> "PIL.Image.Image":
    """Dark gradient placeholder when AI generation fails."""
    from PIL import Image, ImageDraw
    import numpy as np
    img = Image.new("RGB", (THUMB_W, THUMB_H), (10, 10, 20))
    arr = np.zeros((THUMB_H, THUMB_W, 3), dtype=np.uint8)
    for y in range(THUMB_H):
        v = int(30 * (1 - y / THUMB_H))
        arr[y] = [v, v, v + 10]
    return Image.fromarray(arr)


def _darken_bottom(img: "PIL.Image.Image", strength: float = 0.82) -> "PIL.Image.Image":
    """Apply a dark vignette to the bottom 60% for text readability."""
    import numpy as np
    from PIL import Image
    arr = np.array(img, dtype=np.float32)
    grad_h = int(THUMB_H * 0.60)
    start_y = THUMB_H - grad_h
    for y in range(start_y, THUMB_H):
        t = (y - start_y) / grad_h   # 0..1
        factor = 1.0 - strength * (t ** 0.65)
        arr[y] = arr[y] * factor
    return Image.fromarray(np.clip(arr, 0, 255).astype(np.uint8))


def _wrap_text(text: str, font, max_width: int, draw) -> list[str]:
    words = text.split()
    lines: list[str] = []
    current = ""
    for word in words:
        test = (current + " " + word).strip()
        bbox = draw.textbbox((0, 0), test, font=font)
        if bbox[2] > max_width and current:
            lines.append(current)
            current = word
        else:
            current = test
    if current:
        lines.append(current)
    return lines or [text]


def _render_overlay(img: "PIL.Image.Image", title: str, subreddit: str):
    """Draws branded pill tag + bold title over the image."""
    from PIL import ImageDraw
    draw   = ImageDraw.Draw(img)
    w, h   = img.size
    margin = 55

    font_title = _load_font(82, bold=True)
    font_tag   = _load_font(36, bold=True)

    tag   = _BOT_TAG
    lines = _wrap_text(title, font_title, w - 2 * margin, draw)
    line_h = int(82 * 1.25)

    tag_bb = draw.textbbox((0, 0), tag, font=font_tag)
    tag_tw = tag_bb[2] - tag_bb[0]
    tag_th = tag_bb[3] - tag_bb[1]

    total_h = (tag_th + 20) + len(lines) * line_h
    y = h - total_h - margin - 20

    # Pill tag (centered)
    pad   = 14
    tx    = (w - tag_tw) // 2
    draw.rounded_rectangle(
        [(tx - pad, y - pad // 2), (tx + tag_tw + pad, y + tag_th + pad // 2)],
        radius=tag_th // 2, fill=_ACCENT_COLOR,
    )
    draw.text((tx, y), tag, font=font_tag, fill=(255, 255, 255))
    y += tag_th + pad + 14

    # Title lines (centered, with shadow)
    for line in lines:
        bb  = draw.textbbox((0, 0), line, font=font_title)
        lw  = bb[2] - bb[0]
        x   = (w - lw) // 2
        # Shadow
        for dx, dy in [(3, 3), (-2, 3), (3, -2), (0, 4)]:
            draw.text((x + dx, y + dy), line, font=font_title, fill=(0, 0, 0, 200))
        draw.text((x, y), line, font=font_title, fill=(255, 255, 255))
        y += line_h


def create_thumbnail(
    video_path: str,
    title: str,
    output_dir: str,
    subreddit: str = "",
) -> dict[str, str]:
    """
    Creates one 1080×1920 thumbnail with a gradient background + text overlay.
    Returns {"thumbnail": path_str} or {} on failure.
    """
    output_dir = Path(output_dir)
    stem = Path(video_path).stem
    out_path = output_dir / f"thumb_{stem}.jpg"

    try:
        img = _make_fallback_image()
        img = _darken_bottom(img)
        _render_overlay(img, title, subreddit)
        img.save(str(out_path), "JPEG", quality=95)
        logger.info(f"[thumbnail] Saved: {out_path.name}")
        return {"thumbnail": str(out_path)}

    except Exception as e:
        logger.error(f"[thumbnail] Thumbnail failed: {e}")
        return {}
