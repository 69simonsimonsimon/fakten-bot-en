import os
import random
import textwrap
from pathlib import Path

import certifi
import numpy as np
import requests
from moviepy import (
    AudioFileClip,
    ColorClip,
    CompositeAudioClip,
    CompositeVideoClip,
    ImageClip,
    VideoClip,
    VideoFileClip,
    afx,
    concatenate_videoclips,
    vfx,
)
from PIL import Image, ImageDraw, ImageFont

WIDTH  = 1080
HEIGHT = 1920

def _resolve_font(mac_path: str, linux_candidates: list) -> str:
    if Path(mac_path).exists():
        return mac_path
    for candidate in linux_candidates:
        if Path(candidate).exists():
            return candidate
    try:
        import subprocess
        out = subprocess.check_output(["fc-list", "--format=%{file}\n"], text=True, timeout=5)
        for line in out.splitlines():
            line = line.strip()
            if line and any(n in line for n in ["Liberation", "DejaVu", "Arial", "Helvetica"]):
                return line
    except Exception:
        pass
    return mac_path

BOLD = _resolve_font(
    "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
    [
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans-Bold.ttf",
    ],
)
REGULAR = _resolve_font(
    "/System/Library/Fonts/Supplemental/Arial.ttf",
    [
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/dejavu/DejaVuSans.ttf",
    ],
)

_backgrounds_env = os.environ.get("BACKGROUNDS_DIR", "")
CACHE_DIR = Path(_backgrounds_env) if _backgrounds_env else Path(__file__).parent.parent / "assets" / "backgrounds"
CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _is_valid_video(path: Path) -> bool:
    """Checks if a video file is readable and large enough. Corrupt files are deleted."""
    try:
        if path.stat().st_size < 1_048_576:  # < 1 MB → corrupt/incomplete
            path.unlink(missing_ok=True)
            print(f"   ⚠️  Corrupt background deleted (too small): {path.name}")
            return False
        import subprocess
        # 1) Header check via ffprobe
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=nb_frames", "-of", "csv=p=0", str(path)],
            capture_output=True, timeout=10
        )
        if result.returncode != 0:
            path.unlink(missing_ok=True)
            print(f"   ⚠️  Corrupt background deleted (ffprobe error): {path.name}")
            return False
        # 2) Frame-decode check: actually read a frame from 5s in.
        #    ffprobe only validates the container header — partially corrupt files
        #    (e.g. truncated data mid-file) pass ffprobe but crash MoviePy later.
        result2 = subprocess.run(
            ["ffmpeg", "-ss", "5", "-i", str(path),
             "-frames:v", "1", "-f", "image2pipe", "-vcodec", "mjpeg",
             "-loglevel", "error", "-"],
            capture_output=True, timeout=20,
        )
        if len(result2.stdout) < 1_000:  # a real JPEG frame is always >1 KB
            path.unlink(missing_ok=True)
            print(f"   ⚠️  Corrupt background deleted (frame decode failed): {path.name}")
            return False
        return True
    except Exception:
        return False

MUSIC_DIR = Path(__file__).parent.parent / "assets" / "music"
MUSIC_DIR.mkdir(parents=True, exist_ok=True)

TOPIC_QUERIES: dict[str, list[str]] = {
    "space":       ["galaxy nebula", "planet earth", "stars cosmos", "milky way", "aurora borealis"],
    "science":     ["laboratory experiment", "microscope cells", "chemistry science", "dna molecule", "physics"],
    "nature":      ["waterfall forest", "ocean waves", "mountains sunrise", "tropical jungle", "desert landscape"],
    "animals":     ["wildlife safari", "underwater fish", "birds flying", "wolves forest", "dolphins ocean"],
    "technology":  ["circuit board", "city neon lights", "data server", "robot technology", "drone aerial"],
    "psychology":  ["human mind", "meditation calm", "crowd people", "brain neurons", "emotion faces"],
    "history":     ["ancient ruins", "medieval castle", "old city", "museum art", "historical architecture"],
    "food":        ["cooking kitchen", "fresh vegetables", "street food", "restaurant meal", "baking bread"],
    "geography":   ["aerial city", "mountain aerial", "ocean aerial", "river landscape", "world map"],
    "human body":  ["heart pulse", "running athlete", "yoga stretching", "medical hospital", "fitness workout"],
    "pop culture": ["concert crowd", "social media phone", "gaming setup", "movie cinema", "festival lights"],
}

GRADIENTS = [
    ((10, 10, 35), (30, 20, 80)),
    ((5, 30, 60), (10, 80, 120)),
    ((20, 5, 40), (70, 15, 90)),
]


# ── Background ────────────────────────────────────────────────────────────────

def _gradient_bg(c1, c2) -> np.ndarray:
    img = Image.new("RGB", (WIDTH, HEIGHT))
    draw = ImageDraw.Draw(img)
    for y in range(HEIGHT):
        t = y / HEIGHT
        r = int(c1[0]*(1-t) + c2[0]*t)
        g = int(c1[1]*(1-t) + c2[1]*t)
        b = int(c1[2]*(1-t) + c2[2]*t)
        draw.line([(0, y), (WIDTH, y)], fill=(r, g, b))
    return np.array(img)


def _fetch_pexels_video(query: str, api_key: str, max_videos: int = 5) -> str | None:
    import random
    slug = query.replace(" ", "_")

    cached = [p for p in sorted(CACHE_DIR.glob(f"{slug}_*.mp4")) if _is_valid_video(p)]
    if len(cached) >= max_videos:
        chosen = random.choice(cached)
        return str(chosen)

    try:
        headers = {"Authorization": api_key}
        verify  = certifi.where()
        videos  = []
        for orientation in ["portrait", None]:
            params = {"query": query, "per_page": 20, "size": "large"}
            if orientation:
                params["orientation"] = orientation
            r = requests.get("https://api.pexels.com/videos/search",
                             headers=headers, params=params, timeout=15, verify=verify)
            videos = r.json().get("videos", [])
            if videos:
                break
        if not videos:
            return None

        random.shuffle(videos)
        downloaded = []

        for video in videos:
            if len(downloaded) + len(cached) >= max_videos:
                break
            idx = len(cached) + len(downloaded) + 1
            cache_file = CACHE_DIR / f"{slug}_{idx:02d}.mp4"
            if cache_file.exists():
                continue

            files = sorted(video["video_files"], key=lambda f: f.get("width", 0), reverse=True)
            url   = next((f["link"] for f in files if f.get("width", 0) >= 2160), None) or \
                    next((f["link"] for f in files if f.get("width", 0) >= 1080), files[0]["link"])

            try:
                print(f"   Downloading background video {idx}...")
                dl = requests.get(url,
                                  headers={"User-Agent": "Mozilla/5.0",
                                           "Referer": "https://www.pexels.com/"},
                                  verify=verify, timeout=60, stream=True)
                dl.raise_for_status()
                with open(str(cache_file), "wb") as f:
                    for chunk in dl.iter_content(1024 * 1024):
                        f.write(chunk)
                downloaded.append(cache_file)
            except Exception:
                continue

        all_videos = sorted(CACHE_DIR.glob(f"{slug}_*.mp4"))
        if not all_videos:
            return None
        return str(random.choice(all_videos))

    except Exception as e:
        print(f"   Pexels failed: {e}")
        return None


PER_QUERY = 5


def _fetch_multiple_pexels_videos(query: str, api_key: str, count: int = 2) -> list[str]:
    import random

    queries   = TOPIC_QUERIES.get(query.lower(), [query])
    all_paths: list[str] = []

    for sub_q in queries:
        slug   = sub_q.replace(" ", "_")
        cached = [p for p in sorted(CACHE_DIR.glob(f"{slug}_*.mp4")) if _is_valid_video(p)]
        if len(cached) < PER_QUERY:
            _fetch_pexels_video(sub_q, api_key, max_videos=PER_QUERY)
            cached = [p for p in sorted(CACHE_DIR.glob(f"{slug}_*.mp4")) if _is_valid_video(p)]
        all_paths.extend(str(p) for p in cached)

    slug_old = query.replace(" ", "_")
    for p in sorted(CACHE_DIR.glob(f"{slug_old}_*.mp4")):
        if str(p) not in all_paths:
            all_paths.append(str(p))

    if not all_paths:
        return []

    random.shuffle(all_paths)
    return all_paths[:count]


def _make_background(video_path: str | None, duration: float, gradient_index: int, zoom: bool = False):
    if video_path:
        try:
            clip = VideoFileClip(video_path)
            ratio = WIDTH / HEIGHT
            if clip.w / clip.h > ratio:
                nw = int(clip.h * ratio)
                clip = clip.cropped(x1=(clip.w-nw)//2, x2=(clip.w+nw)//2)
            else:
                nh = int(clip.w / ratio)
                clip = clip.cropped(y1=(clip.h-nh)//2, y2=(clip.h+nh)//2)
            clip = clip.resized((WIDTH, HEIGHT))
            if clip.duration < duration:
                clip = concatenate_videoclips([clip] * (int(duration / clip.duration) + 2))
            clip = clip.subclipped(0, duration)

            # Ken Burns: subtle zoom 1.0 → 1.06 over clip duration
            if zoom:
                def _zoom_frame(get_frame, t):
                    frame  = get_frame(t)
                    scale  = 1.0 + 0.06 * (t / max(duration, 1))
                    new_h  = int(HEIGHT * scale)
                    new_w  = int(WIDTH  * scale)
                    img    = Image.fromarray(frame).resize((new_w, new_h), Image.BILINEAR)
                    off_x  = (new_w - WIDTH)  // 2
                    off_y  = (new_h - HEIGHT) // 2
                    return np.array(img)[off_y:off_y + HEIGHT, off_x:off_x + WIDTH]
                clip = clip.transform(_zoom_frame)

            overlay = ColorClip((WIDTH, HEIGHT), color=(0,0,0)).with_opacity(0.48).with_duration(duration)
            return CompositeVideoClip([clip, overlay])
        except Exception as e:
            print(f"   Video error: {e}, using gradient")
    idx = gradient_index % len(GRADIENTS)
    return ImageClip(_gradient_bg(*GRADIENTS[idx])).with_duration(duration)


def _make_multi_background(video_paths: list[str], duration: float, gradient_index: int):
    if not video_paths:
        idx = gradient_index % len(GRADIENTS)
        return ImageClip(_gradient_bg(*GRADIENTS[idx])).with_duration(duration)

    n        = len(video_paths)
    seg_dur  = duration / n
    segments = [_make_background(path, seg_dur, gradient_index, zoom=True) for path in video_paths]
    return concatenate_videoclips(segments).with_duration(duration)


# ── Header design ─────────────────────────────────────────────────────────────

def _render_header(title: str) -> np.ndarray:
    """
    Renders the header:
    - Clean minimal pill with semi-transparent dark background, 💡 + 'Fun Fact' in white
    - Below it the title in large bold white text with drop shadow, no colored background
    """
    MAX_TITLE_W = WIDTH - 100

    # ── Pill badge: "💡 Fun Fact" ─────────────────────────────────────────────
    pill_text  = "💡 Fun Fact"
    font_pill  = ImageFont.truetype(BOLD, 38)
    pill_tw    = int(font_pill.getlength(pill_text))
    pill_pad_x = 28
    pill_pad_y = 14
    pill_w     = pill_tw + pill_pad_x * 2
    pill_h     = 38 + pill_pad_y * 2  # font_size + vertical padding

    # ── Title: auto-scale then word-wrap ─────────────────────────────────────
    font_size  = 66
    MIN_SIZE   = 36
    font_title = ImageFont.truetype(BOLD, font_size)
    while font_size > MIN_SIZE and font_title.getlength(title) > MAX_TITLE_W:
        font_size -= 3
        font_title = ImageFont.truetype(BOLD, font_size)

    def _wrap(text: str, font, max_w: int) -> list[str]:
        words, lines, cur = text.split(), [], ""
        for w in words:
            probe = (cur + " " + w).strip()
            if font.getlength(probe) <= max_w:
                cur = probe
            else:
                if cur:
                    lines.append(cur)
                cur = w
        if cur:
            lines.append(cur)
        return lines or [text]

    if font_title.getlength(title) > MAX_TITLE_W:
        title_lines = _wrap(title, font_title, MAX_TITLE_W)
    else:
        title_lines = [title]

    line_h     = font_size + 12
    title_h    = len(title_lines) * line_h + 4
    gap        = 20  # space between pill and title

    max_line_w = max(int(font_title.getlength(l)) for l in title_lines)
    total_w    = min(max(pill_w, max_line_w + 60, 700), WIDTH - 20)
    total_h    = pill_h + gap + title_h

    img  = Image.new("RGBA", (total_w, total_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # ── Draw pill ─────────────────────────────────────────────────────────────
    bx = (total_w - pill_w) // 2
    draw.rounded_rectangle(
        [(bx, 0), (bx + pill_w - 1, pill_h - 1)],
        radius=pill_h // 2,
        fill=(20, 20, 20, 170),
    )
    # Pill text centred inside
    tx = bx + (pill_w - pill_tw) // 2
    ty = (pill_h - 38) // 2
    draw.text((tx, ty), pill_text, font=font_pill, fill=(255, 255, 255, 255))

    # ── Title lines (white with drop shadow, no background) ──────────────────
    ty = pill_h + gap
    for line in title_lines:
        tx2 = (total_w - int(font_title.getlength(line))) // 2
        # Drop shadow
        draw.text((tx2 + 2, ty + 2), line, font=font_title, fill=(0, 0, 0, 180))
        draw.text((tx2, ty),          line, font=font_title, fill=(255, 255, 255, 255))
        ty += line_h

    return np.array(img)


# ── PIL-based karaoke rendering ───────────────────────────────────────────────

def _render_karaoke_frame(
    words: list[str],
    highlight_indices: set[int],
    font_size: int = 96,
    max_width: int = 940,
) -> np.ndarray:
    font_bold   = ImageFont.truetype(BOLD, font_size)
    space_w     = font_bold.getlength(" ")

    lines:   list[list[tuple[int, str]]] = []
    cur_line: list[tuple[int, str]]      = []
    cur_w = 0.0

    for idx, word in enumerate(words):
        w = font_bold.getlength(word)
        if cur_line and cur_w + space_w + w > max_width:
            lines.append(cur_line)
            cur_line = [(idx, word)]
            cur_w = w
        else:
            cur_line.append((idx, word))
            cur_w += (space_w if cur_line else 0) + w
    if cur_line:
        lines.append(cur_line)

    line_h   = font_size + 16
    total_h  = len(lines) * line_h
    total_w  = max_width + 80
    pad      = 28

    img  = Image.new("RGBA", (total_w, total_h + pad * 2), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    draw.rounded_rectangle(
        [(0, 0), (total_w - 1, total_h + pad * 2 - 1)],
        radius=24, fill=(0, 0, 0, 165)
    )

    for li, line_words in enumerate(lines):
        line_text_w = sum(font_bold.getlength(w) for _, w in line_words) + space_w * (len(line_words) - 1)
        x = (total_w - line_text_w) / 2
        y = pad + li * line_h

        for idx, word in line_words:
            color = "#FFE600" if idx in highlight_indices else "white"
            draw.text((x + 2, y + 2), word, font=font_bold, fill=(0, 0, 0, 200))
            draw.text((x, y), word, font=font_bold, fill=color)
            x += font_bold.getlength(word) + space_w

    return np.array(img)


def _make_karaoke_clips(
    word_timings: list[dict],
    total_duration: float,
    group_size: int = 4,
) -> list:
    clips = []
    n = len(word_timings)

    for i, wt in enumerate(word_timings):
        group_start_idx = (i // group_size) * group_size
        group_end_idx   = min(group_start_idx + group_size, n)
        group           = word_timings[group_start_idx:group_end_idx]
        group_words     = [w["word"] for w in group]
        highlight_idx   = i - group_start_idx

        t_start = wt["start"]
        t_end   = word_timings[i + 1]["start"] if i + 1 < n else min(wt["end"] + 0.3, total_duration)
        t_end   = min(t_end, total_duration)

        if t_end <= t_start:
            continue

        frame  = _render_karaoke_frame(group_words, {highlight_idx})
        clip_h = frame.shape[0]
        clip_w = frame.shape[1]

        img_clip = (
            ImageClip(frame)
            .with_start(t_start)
            .with_end(t_end)
            .with_position(((WIDTH - clip_w) // 2, int(HEIGHT * 0.62) - clip_h // 2))
        )
        clips.append(img_clip)

    return clips


# ── Branding & UI overlays ────────────────────────────────────────────────────

def _render_watermark() -> np.ndarray:
    text = "@syncinus5"
    font = ImageFont.truetype(BOLD, 26)
    tw   = int(font.getlength(text)) + 22
    th   = 40
    img  = Image.new("RGBA", (tw, th), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle([(0, 0), (tw - 1, th - 1)], radius=8, fill=(0, 0, 0, 110))
    draw.text((11, 9), text, font=font, fill=(0, 0, 0, 130))
    draw.text((10, 8), text, font=font, fill=(255, 255, 255, 185))
    return np.array(img)


def _make_progress_bar(total_dur: float):
    BAR_H = 5
    color = np.array([255, 107, 53], dtype=np.uint8)

    def make_frame(t: float) -> np.ndarray:
        progress = min(t / max(total_dur, 0.001), 1.0)
        bar_w    = max(1, int(WIDTH * progress))
        frame    = np.zeros((BAR_H, WIDTH, 3), dtype=np.uint8)
        frame[:, :bar_w] = color
        return frame

    return VideoClip(make_frame, duration=total_dur).with_position((0, HEIGHT - BAR_H - 2))


# ── Background music ──────────────────────────────────────────────────────────

def _mix_background_music(speech: AudioFileClip, duration: float) -> AudioFileClip:
    tracks = (
        list(MUSIC_DIR.glob("*.mp3"))
        + list(MUSIC_DIR.glob("*.wav"))
        + list(MUSIC_DIR.glob("*.m4a"))
        + list(MUSIC_DIR.glob("*.ogg"))
    )
    if not tracks:
        return speech

    try:
        track_path = random.choice(tracks)
        print(f"   Music: {track_path.name}")
        music = AudioFileClip(str(track_path))

        music = music.with_effects([afx.AudioLoop(duration=duration)])

        music = music.with_effects([
            afx.MultiplyVolume(0.12),
            afx.AudioFadeIn(1.0),
            afx.AudioFadeOut(1.5),
        ])

        return CompositeAudioClip([speech, music])

    except Exception as e:
        print(f"   Music error (skipped): {e}")
        return speech


# ── Hook overlay (first 4s) ───────────────────────────────────────────────────

_TOPIC_EMOJIS = {
    "space":       "🚀", "science":    "🔬", "nature":     "🌿",
    "animals":     "🦁", "technology": "💻", "psychology": "🧠",
    "history":     "🏛️",  "food":       "🍽️",  "geography":  "🌍",
    "human body":  "💪", "pop culture":"🎬",
}

def _render_hook_frame(title: str, topic: str) -> np.ndarray:
    emoji    = _TOPIC_EMOJIS.get(topic.lower(), "🤯")
    font_em  = ImageFont.truetype(BOLD, 110)
    font_txt = ImageFont.truetype(BOLD, 72)
    MAX_W    = WIDTH - 80

    words, lines, cur = title.split(), [], ""
    for w in words:
        probe = (cur + " " + w).strip()
        if font_txt.getlength(probe) <= MAX_W:
            cur = probe
        else:
            if cur: lines.append(cur)
            cur = w
    if cur: lines.append(cur)

    line_h  = 72 + 14
    em_h    = 130
    gap     = 20
    total_h = em_h + gap + len(lines) * line_h + 60
    total_w = WIDTH - 40

    img  = Image.new("RGBA", (total_w, total_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle([(0, 0), (total_w - 1, total_h - 1)], radius=32, fill=(0, 0, 0, 185))

    ew = int(font_em.getlength(emoji))
    draw.text(((total_w - ew) // 2, 20), emoji, font=font_em, fill=(255, 255, 255, 255))

    ty = em_h + gap
    for line in lines:
        lw = int(font_txt.getlength(line))
        tx = (total_w - lw) // 2
        draw.text((tx + 2, ty + 2), line, font=font_txt, fill=(0, 0, 0, 180))
        draw.text((tx,     ty    ), line, font=font_txt, fill=(255, 255, 255, 255))
        ty += line_h

    return np.array(img)


def _make_hook_clip(title: str, topic: str, total_dur: float, hook_dur: float = 4.0):
    frame = _render_hook_frame(title, topic)
    fh, fw = frame.shape[:2]
    pos_x  = (WIDTH  - fw) // 2
    pos_y  = int(HEIGHT * 0.28) - fh // 2

    return (
        ImageClip(frame)
        .with_duration(hook_dur)
        .with_position((pos_x, pos_y))
        .with_effects([vfx.FadeIn(0.2), vfx.FadeOut(0.5)])
    )


# ── Main function ─────────────────────────────────────────────────────────────

def create_video(
    title: str,
    fact: str,
    audio_path: str,
    output_path: str,
    word_timings: list[dict] | None = None,
    sentence_timings: list[tuple] | None = None,
    gradient_index: int = 0,
    topic: str = "nature",
    visual_query: str = "",
) -> str:
    pexels_key   = os.environ.get("PEXELS_API_KEY", "")
    audio        = AudioFileClip(audio_path)
    total_dur    = audio.duration + 0.5

    bg_query    = visual_query if visual_query else topic
    video_paths = _fetch_multiple_pexels_videos(bg_query, pexels_key, count=3) if pexels_key else []
    if not video_paths and visual_query:
        video_paths = _fetch_multiple_pexels_videos(topic, pexels_key, count=3) if pexels_key else []
    n_bg = len(video_paths)
    if n_bg > 1:
        print(f"   Using {n_bg} background videos")
    background = _make_multi_background(video_paths, total_dur, gradient_index)
    clips      = [background]

    header_img = _render_header(title)
    header_h   = header_img.shape[0]
    clips.append(
        ImageClip(header_img)
        .with_duration(total_dur)
        .with_position(("center", 80))
        .with_effects([vfx.FadeIn(0.4)])
    )

    # Hook overlay (first 4 seconds) — strong visual opener
    clips.append(_make_hook_clip(title, topic, total_dur, hook_dur=4.0))

    # Karaoke text (3 words per group for faster rhythm)
    if word_timings:
        title_word_count = len(title.split())
        fact_timings = word_timings[title_word_count + 1:]
        if fact_timings:
            clips.extend(_make_karaoke_clips(fact_timings, total_dur, group_size=3))

    wm_img = _render_watermark()
    wm_h, wm_w = wm_img.shape[:2]
    clips.append(
        ImageClip(wm_img)
        .with_duration(total_dur)
        .with_position((WIDTH - wm_w - 24, HEIGHT - wm_h - 110))
        .with_effects([vfx.FadeIn(0.6)])
    )

    clips.append(_make_progress_bar(total_dur))

    mixed_audio = _mix_background_music(audio, total_dur)

    video = CompositeVideoClip(clips, size=(WIDTH, HEIGHT)).with_duration(total_dur).with_audio(mixed_audio)
    import platform
    _codec = "h264_videotoolbox" if platform.system() == "Darwin" else "libx264"
    _extra = ["-b:v", "4000k", "-pix_fmt", "yuv420p", "-b:a", "192k"] if _codec == "h264_videotoolbox" else ["-preset", "fast", "-crf", "18", "-pix_fmt", "yuv420p", "-b:a", "192k"]
    video.write_videofile(
        output_path, fps=30, codec=_codec, audio_codec="aac", logger=None,
        ffmpeg_params=_extra,
    )
    audio.close()
    if mixed_audio is not audio:
        try:
            mixed_audio.close()
        except Exception:
            pass
    video.close()
    return output_path
