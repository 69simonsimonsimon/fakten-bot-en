#!/usr/bin/env python3
"""
SynCin Facts Bot EN — Local Video Generator
=============================================
Generates a facts video and uploads it to the Bunny queue.
GitHub Actions posts it automatically on schedule.

Usage:
  python run_local.py              # random topic
  python run_local.py science
  python run_local.py space 3      # generate 3 videos
"""

import json
import logging
import os
import random
import sys
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).parent
load_dotenv(ROOT / ".env", override=True)
sys.path.insert(0, str(ROOT / "modules"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("faktbot-en")

OUTPUT_DIR = ROOT / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

TOPICS = [
    "science", "history", "nature", "technology", "space",
    "animals", "psychology", "food", "geography", "human body", "pop culture",
]


def generate_and_queue(topic: str = None) -> bool:
    import certifi
    import requests

    from fact_generator import generate_fact
    from tts import text_to_speech
    from video_creator import create_video

    topic = topic or random.choice(TOPICS)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S%f")[:-3]
    audio_path = OUTPUT_DIR / f"audio_{stamp}.mp3"
    video_path = OUTPUT_DIR / f"video_{stamp}.mp4"

    # 1. Generate fact
    logger.info(f"💡  Generating fact — topic: {topic}")
    fact_data = generate_fact(topic, long=True)
    logger.info(f"    → {fact_data['title']}")

    # 2. TTS + Word Timings
    logger.info("🎙️   Generating voiceover (ElevenLabs)...")
    tts_text = f"{fact_data['title']}. {fact_data['fact']}"
    words = tts_text.split()
    if len(words) > 165:
        tts_text = " ".join(words[:165]) + "."
    _, word_timings = text_to_speech(tts_text, str(audio_path), topic=topic)
    logger.info(f"    → {len(word_timings)} words")

    # 3. Render video
    logger.info("🎞️   Rendering video...")
    create_video(
        title=fact_data["title"],
        fact=fact_data["fact"],
        audio_path=str(audio_path),
        output_path=str(video_path),
        word_timings=word_timings,
        gradient_index=random.randint(0, 4),
        topic=topic,
        visual_query=fact_data.get("visual_query", ""),
    )
    audio_path.unlink(missing_ok=True)

    mb = video_path.stat().st_size / 1024 / 1024
    logger.info(f"    → {video_path.name} ({mb:.1f} MB)")

    # 4. Caption
    description  = fact_data.get("description", fact_data["title"])
    full_caption = description + " " + " ".join(fact_data["hashtags"])

    # 5. Bunny queue upload
    logger.info("☁️   Uploading to Bunny queue...")
    password = os.environ["BUNNY_STORAGE_PASSWORD"]
    zone     = os.environ.get("BUNNY_STORAGE_NAME", "syncin")
    cdn_url  = os.environ.get("BUNNY_CDN_URL", "https://syncin.b-cdn.net")
    hostname = os.environ.get("BUNNY_STORAGE_HOSTNAME", "storage.bunnycdn.com")

    filename = f"fakten_en_{stamp}.mp4"

    with open(str(video_path), "rb") as f:
        r = requests.put(
            f"https://{hostname}/{zone}/queue/{filename}",
            headers={"AccessKey": password, "Content-Type": "video/mp4"},
            data=f, verify=certifi.where(), timeout=300,
        )
    r.raise_for_status()

    meta = {
        "title":   fact_data["title"],
        "caption": full_caption,
        "topic":   topic,
        "cdn_url": f"{cdn_url}/queue/{filename}",
    }
    mr = requests.put(
        f"https://{hostname}/{zone}/queue/{filename.replace('.mp4', '.json')}",
        headers={"AccessKey": password, "Content-Type": "application/json"},
        data=json.dumps(meta, ensure_ascii=False).encode(),
        verify=certifi.where(), timeout=30,
    )
    mr.raise_for_status()

    video_path.unlink(missing_ok=True)
    logger.info(f"✅  Queued: {filename}")
    logger.info(f"    Title: {fact_data['title']}")
    return True


if __name__ == "__main__":
    import concurrent.futures
    topic   = sys.argv[1] if len(sys.argv) > 1 else None
    count   = int(sys.argv[2]) if len(sys.argv) > 2 else 1
    workers = min(count, int(sys.argv[3]) if len(sys.argv) > 3 else 3)

    done = []

    def _task(i):
        if count > 1:
            logger.info(f"\n{'='*50}\nVideo {i+1}/{count}\n{'='*50}")
        if generate_and_queue(topic):
            done.append(1)

    if workers > 1 and count > 1:
        logger.info(f"🚀  Parallel: {count} videos × {workers} workers")
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            list(ex.map(_task, range(count)))
    else:
        for i in range(count):
            _task(i)

    logger.info(f"\n🏁  Done: {len(done)}/{count} videos queued on Bunny")
