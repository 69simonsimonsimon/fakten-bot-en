#!/usr/bin/env python3
"""
TikTok Facts Bot (English) — @syncinUS
---------------------------------------
Automatically creates a facts video and uploads it to TikTok.

Usage:
  python main.py                    # Create + upload one video
  python main.py --topic science    # Choose a specific topic
  python main.py --only-create      # Only create video, no upload
  python main.py --schedule         # Post daily automatically (cron mode)
"""

import argparse
import os
import random
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)

sys.path.insert(0, str(Path(__file__).parent / "modules"))

from fact_generator import generate_fact
from tts import text_to_speech, get_sentence_timings  # noqa: F401
from video_creator import create_video
from tiktok_uploader_zernio import upload_video_zernio as upload_video_browser


TOPICS = [
    "science",
    "history",
    "nature",
    "technology",
    "space",
    "animals",
    "psychology",
    "food",
    "geography",
    "human body",
    "pop culture",
]

OUTPUT_DIR = Path(__file__).parent / "output"


def run_once(topic: str = None, only_create: bool = False, privacy: str = "SELF_ONLY", long: bool = False) -> str:
    topic = topic or random.choice(TOPICS)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    audio_path = OUTPUT_DIR / f"audio_{timestamp}.mp3"
    video_path = OUTPUT_DIR / f"video_{timestamp}.mp4"

    print(f"\n{'='*50}")
    print(f"TikTok Bot (@syncinUS) — {datetime.now().strftime('%m/%d/%Y %I:%M %p')}")
    print(f"Topic: {topic}")
    print(f"{'='*50}\n")

    # 1. Generate fact
    print("1. Generating fact...")
    fact_data = generate_fact(topic, long=long)
    print(f"   Title: {fact_data['title']}")
    print(f"   Fact:  {fact_data['fact'][:80]}...")

    # 2. Text-to-Speech
    print("\n2. Creating voiceover...")
    tts_text = f"{fact_data['title']}. {fact_data['fact']}"
    _, word_timings = text_to_speech(tts_text, str(audio_path), topic=topic)
    print(f"   Audio: {audio_path.name} ({len(word_timings)} words)")

    # 3. Create video
    print("\n3. Creating video...")
    gradient_index = random.randint(0, 4)
    create_video(
        title=fact_data["title"],
        fact=fact_data["fact"],
        audio_path=str(audio_path),
        output_path=str(video_path),
        word_timings=word_timings,
        gradient_index=gradient_index,
        topic=topic,
        visual_query=fact_data.get("visual_query", ""),
    )
    print(f"   Video: {video_path.name}")

    audio_path.unlink(missing_ok=True)

    description  = fact_data.get("description", fact_data["title"])
    full_caption = description + " " + " ".join(fact_data["hashtags"])

    import json as _json
    meta = {
        "title":    fact_data["title"],
        "topic":    topic,
        "caption":  full_caption,
        "uploaded": False,
    }
    Path(str(video_path).replace(".mp4", ".json")).write_text(
        _json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    if only_create:
        print(f"\nVideo saved (no upload): {video_path}")
        print(f"\nTikTok caption:\n{full_caption}")
        return str(video_path)

    # 4. TikTok upload (Browser)
    print("\n4. Uploading to TikTok (Browser)...")
    try:
        success = upload_video_browser(str(video_path), full_caption)
        if success:
            print(f"\nSuccessfully uploaded!")
        else:
            print(f"\nUpload incomplete — video saved locally: {video_path}")
    except Exception as e:
        print(f"\nUpload failed: {e}")
        print(f"Video saved locally: {video_path}")

    return str(video_path)


def run_scheduler(topic: str = None, interval_hours: int = 24, privacy: str = "PUBLIC_TO_EVERYONE"):
    print(f"Scheduler started — posting every {interval_hours} hours")
    print("Press Ctrl+C to stop.\n")

    while True:
        try:
            run_once(topic=topic, privacy=privacy)
        except Exception as e:
            print(f"Error creating/uploading: {e}")

        next_run = datetime.fromtimestamp(time.time() + interval_hours * 3600)
        print(f"\nNext post: {next_run.strftime('%m/%d/%Y %I:%M %p')}")
        print(f"Waiting {interval_hours} hours...\n")
        time.sleep(interval_hours * 3600)


def main():
    parser = argparse.ArgumentParser(description="TikTok Facts Bot (English) — @syncinUS")
    parser.add_argument("--topic", type=str, default=None, help=f"Choose topic: {', '.join(TOPICS)}")
    parser.add_argument("--only-create", action="store_true", help="Only create video, no upload")
    parser.add_argument("--schedule", action="store_true", help="Post every 24h automatically")
    parser.add_argument("--interval", type=int, default=24, help="Hours between posts (default: 24)")
    parser.add_argument(
        "--long", action="store_true", help="Longer video (at least 1 minute)",
    )
    parser.add_argument(
        "--privacy",
        type=str,
        default="SELF_ONLY",
        choices=["SELF_ONLY", "MUTUAL_FOLLOW_FRIENDS", "FOLLOWER_OF_CREATOR", "PUBLIC_TO_EVERYONE"],
        help="TikTok visibility (default: SELF_ONLY for testing)",
    )
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)

    if args.schedule:
        run_scheduler(topic=args.topic, interval_hours=args.interval, privacy=args.privacy)
    else:
        run_once(topic=args.topic, only_create=args.only_create, privacy=args.privacy, long=args.long)


if __name__ == "__main__":
    main()
