"""
syncinUS Dashboard – FastAPI Backend
Starten mit: python dashboard/app.py
Dann im Browser: http://localhost:8000
"""

import json
import logging
import os
import random
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

import uvicorn
from fastapi import Body, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Projekt-Root und Module einbinden
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "modules"))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env", override=True)

IS_RAILWAY = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("RAILWAY_PROJECT_ID"))

# ── Logging ───────────────────────────────────────────────────────────────────
_log_base = Path(os.environ.get("OUTPUT_DIR", str(ROOT / "output")))
LOG_DIR   = _log_base / "logs"
LOG_DIR.mkdir(exist_ok=True, parents=True)
LOG_FILE = LOG_DIR / "bot.log"

_handler = RotatingFileHandler(str(LOG_FILE), maxBytes=1_000_000, backupCount=3, encoding="utf-8")
_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
logger = logging.getLogger("syncin")
logger.setLevel(logging.INFO)
logger.addHandler(_handler)
logger.addHandler(logging.StreamHandler())  # auch auf stdout


# ── Telegram-Benachrichtigungen ───────────────────────────────────────────────
def _tg_credentials():
    token   = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    return token, chat_id

def notify(title: str, message: str):
    """Send Telegram notification. Falls back silently if env vars not set."""
    try:
        import urllib.request as _ur, json as _j
        _token, _chat_id = _tg_credentials()
        if not _token or not _chat_id:
            return
        _body = _j.dumps({
            "chat_id": _chat_id,
            "text": f"<b>{title}</b>\n{message}",
            "parse_mode": "HTML",
        }).encode()
        _req = _ur.Request(
            f"https://api.telegram.org/bot{_token}/sendMessage",
            data=_body, headers={"Content-Type": "application/json"},
        )
        _ur.urlopen(_req, timeout=10)
    except Exception:
        pass

def notify_photo(image_path: str, caption: str):
    """Schickt ein Thumbnail-Foto per Telegram für manuellen YouTube-Upload."""
    try:
        import requests as _req
        _token, _chat_id = _tg_credentials()
        if not _token or not _chat_id:
            return
        from pathlib import Path as _P
        p = _P(image_path)
        if not p.exists():
            return
        with open(p, "rb") as f:
            _req.post(
                f"https://api.telegram.org/bot{_token}/sendPhoto",
                data={"chat_id": _chat_id, "caption": caption[:1024]},
                files={"photo": (p.name, f, "image/jpeg")},
                timeout=30,
            )
    except Exception:
        pass


def _tg_send(text: str):
    """Send a raw Telegram message (no title formatting)."""
    try:
        import urllib.request as _ur, json as _j
        _token   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        _chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        if not _token or not _chat_id:
            return
        _body = _j.dumps({
            "chat_id": _chat_id,
            "text": text,
            "parse_mode": "HTML",
        }).encode()
        _req = _ur.Request(
            f"https://api.telegram.org/bot{_token}/sendMessage",
            data=_body, headers={"Content-Type": "application/json"},
        )
        _ur.urlopen(_req, timeout=10)
    except Exception:
        pass


try:
    from fact_generator import generate_fact
    from tts import text_to_speech
    from video_creator import create_video
    from tiktok_uploader_zernio import upload_video_browser, DuplicateContentError
    from analytics_scraper import fetch_analytics, load_cached
except Exception as _import_err:
    logger.error(f"Import-Fehler beim Start: {_import_err}")
    raise

OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(ROOT / "output")))
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

TOPICS = [
    "science", "history", "nature", "technology", "space",
    "animals", "psychology", "food", "geography", "human body",
    "pop culture",
    "dark history",       # Shocking historical facts
    "crime",              # True crime & unsolved cases
    "conspiracy truth",   # Real conspiracies that turned out to be true
    "money",              # Wealth, inequality, shocking numbers
    "war",                # Wars, surprising facts
    "medicine",           # Shocking medical history
    "survival",           # Extreme survival stories
]

_SENSITIVE_TOPICS = {"dark history", "crime", "conspiracy truth", "money", "war", "medicine"}


def _pick_topic() -> str:
    """
    Picks a topic intelligently:
    - Avoids topics used in the last 14 days
    - Prevents 3 sensitive topics in a row
    """
    from datetime import timedelta
    cutoff = datetime.now() - timedelta(days=14)

    recent: list[str] = []
    for jf in sorted(OUTPUT_DIR.glob("video_*.json"), key=lambda f: f.stat().st_mtime, reverse=True):
        try:
            if datetime.fromtimestamp(jf.stat().st_mtime) < cutoff:
                break
            m = json.loads(jf.read_text(encoding="utf-8"))
            t = m.get("topic", "")
            if t:
                recent.append(t)
        except Exception:
            pass

    used_14d = set(recent)
    last_2   = recent[:2]

    available = [t for t in TOPICS if t not in used_14d]
    if not available:
        available = list(TOPICS)

    if sum(1 for t in last_2 if t in _SENSITIVE_TOPICS) >= 2:
        safe = [t for t in available if t not in _SENSITIVE_TOPICS]
        if safe:
            available = safe
            logger.info("[topic] 2 sensitive topics in a row — picking safe topic")

    chosen = random.choice(available)
    logger.info(f"[topic] Chosen: '{chosen}' | Used last 14d: {sorted(used_14d)}")
    return chosen


# Rotierende Call-to-Actions — täglich anderer CTA für mehr Abwechslung im Feed
_CTAS = [
    "Did you know this? Comment below! 👇",
    "Type YES if this blew your mind! 💬",
    "Follow @syncinUS for daily facts! 🧠",
    "More facts? Follow @syncinUS! 🔥",
    "New facts daily on @syncinUS! ✨",
    "Comment YES or NO — did you know this? 😮",
    "A new fact every day — @syncinUS! 📚",
    "Type MIND BLOWN if this surprised you! 🤯",
    "Tag someone who needs to know this! 👇",
    "I'm still thinking about this one 😳 Follow @syncinUS!",
    "Drop a 🤯 if you had no idea!",
    "I'm telling everyone I know about this 😤 Follow @syncinUS!",
]

app  = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
jobs: dict[str, dict] = {}        # job_id → status-dict
uploads: dict[str, str] = {}      # filename → "running" | "done" | "error" | "duplicate"
batch_jobs: dict[str, dict] = {}  # batch_id → batch-status


@app.on_event("startup")
async def startup_recovery():
    """Upload videos that were generated but not uploaded due to a service restart.
    Also detects slots whose generation was interrupted mid-flight and re-triggers them."""
    def _do_recovery():
        time.sleep(15)
        now = time.time()

        # ── Phase 1: Upload unfertige Videos ─────────────────────────────────
        for mp4 in sorted(OUTPUT_DIR.glob("*.mp4"), key=lambda f: f.stat().st_mtime, reverse=True):
            age_h = (now - mp4.stat().st_mtime) / 3600
            if age_h > 6:
                break
            if age_h < (2 / 60):  # Weniger als 2 Min alt → wird gerade generiert, überspringen
                continue
            meta_file = mp4.with_suffix(".json")
            if not meta_file.exists():
                continue
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                if meta.get("uploaded"):
                    continue
                caption  = meta.get("caption", "")
                filename = mp4.name
                if filename in uploads:
                    continue
                logger.info(f"Startup-Recovery: nicht hochgeladenes Video gefunden: {filename} ({age_h:.1f}h alt)")
                uploads[filename] = "running"
                _run_upload(filename, str(mp4), caption)
            except Exception as e:
                logger.warning(f"Startup-Recovery Fehler für {mp4.name}: {e}")

        # ── Phase 2: Unterbrochene Slot-Generierung erkennen & nachholen ─────
        # Wenn ein Slot feuerte (fired_key gesetzt), aber in den letzten 30 Min
        # kein Video erzeugt wurde, war die Generierung mid-flight unterbrochen.
        try:
            _now_dt    = datetime.now()
            _today     = _now_dt.strftime("%Y-%m-%d")
            _now_epoch = time.time()
            _fired     = _load_fired_keys()
            _cfg       = _load_schedule_cfg()
            _SKIP_JSON = {"used_facts.json", "fired_keys.json", "schedule.json", "used_posts.json"}

            if _cfg.get("enabled"):
                for _slot in _cfg.get("slots", []):
                    _slot_time = _slot.get("time", "")
                    if not _slot_time or ":" not in _slot_time:
                        continue
                    _key = f"{_today}_{_slot_time}"
                    if _key not in _fired:
                        continue  # Slot hat heute noch nicht gefeuert

                    _t_h, _t_m = int(_slot_time.split(":")[0]), int(_slot_time.split(":")[1])
                    _slot_epoch = _now_dt.replace(
                        hour=_t_h, minute=_t_m, second=0, microsecond=0
                    ).timestamp()

                    # Nur Slots der letzten 30 Minuten prüfen
                    if _now_epoch - _slot_epoch > 30 * 60 or _slot_epoch > _now_epoch:
                        continue

                    # Gibt es Output-Dateien die nach dem Slot-Zeitpunkt erstellt wurden?
                    _post_mp4  = [f for f in OUTPUT_DIR.glob("*.mp4")  if f.stat().st_mtime >= _slot_epoch - 30]
                    _post_json = [f for f in OUTPUT_DIR.glob("*.json")
                                  if f.name not in _SKIP_JSON and f.stat().st_mtime >= _slot_epoch - 30]
                    if _post_mp4 or _post_json:
                        continue  # Generierung hat etwas produziert — kein Recovery nötig

                    # Kein Output → Generierung war unterbrochen → neu starten
                    logger.info(f"Startup-Recovery: Slot {_slot_time} wurde mid-generation unterbrochen — starte Neugenerierung")
                    _job_id = str(uuid.uuid4())[:8]
                    jobs[_job_id] = {"status": "running", "progress": 0, "message": "Startet (Slot-Recovery)…", "video": None}
                    threading.Thread(target=_run_scheduled_single, args=(_job_id, _slot), daemon=True).start()
                    break  # Nur einen Slot auf einmal nachholen
        except Exception as _e:
            logger.warning(f"Startup-Recovery Slot-Check Fehler: {_e}")

    threading.Thread(target=_do_recovery, daemon=True).start()


# ── Healthcheck (Railway) ─────────────────────────────────────────────────────

@app.get("/health")
def health():
    mem = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                k, v = line.split(":", 1)
                mem[k.strip()] = v.strip()
    except Exception:
        pass
    return {"status": "ok", "mem_total": mem.get("MemTotal","?"), "mem_available": mem.get("MemAvailable","?"), "mem_free": mem.get("MemFree","?")}


# ── Video-Liste ───────────────────────────────────────────────────────────────

@app.get("/api/videos")
def list_videos():
    videos = []
    for mp4 in sorted(OUTPUT_DIR.glob("*.mp4"), key=lambda f: f.stat().st_mtime, reverse=True):
        meta_file = mp4.with_suffix(".json")
        meta: dict = {}
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        videos.append({
            "filename": mp4.name,
            "size_mb": round(mp4.stat().st_size / 1_048_576, 1),
            "created": datetime.fromtimestamp(mp4.stat().st_mtime).strftime("%d.%m.%Y %H:%M"),
            "title":    meta.get("title", mp4.stem),
            "topic":    meta.get("topic", ""),
            "caption":  meta.get("caption", ""),
            "uploaded": meta.get("uploaded", False),
        })
    return videos


# ── Video generieren ──────────────────────────────────────────────────────────

@app.post("/api/generate")
def start_generate(topic: str = "", long: bool = True):
    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"status": "running", "progress": 0, "message": "Starting...", "video": None}
    t = threading.Thread(target=_run_generation, args=(job_id, topic or None, long), daemon=True)
    t.start()
    return {"job_id": job_id}


def _free_disk_mb() -> float:
    import shutil
    check_path = str(OUTPUT_DIR) if OUTPUT_DIR.exists() else "/"
    return shutil.disk_usage(check_path).free / 1_048_576


def _cleanup_backgrounds_all():
    """Deletes all downloaded background videos after generation."""
    try:
        from video_creator import CACHE_DIR
        deleted, freed = 0, 0.0
        for v in list(CACHE_DIR.glob("*.mp4")):
            try:
                freed += v.stat().st_size / 1_048_576
                v.unlink()
                deleted += 1
            except Exception:
                pass
        if deleted:
            logger.info(f"Backgrounds cleaned: {deleted} videos deleted, {freed:.0f} MB freed")
    except Exception as e:
        logger.warning(f"Background cleanup failed: {e}")


def _cleanup_cache_if_needed(min_free_mb: float = 400):
    """Deletes oldest background videos when disk space is low."""
    try:
        from video_creator import CACHE_DIR
        free = _free_disk_mb()
        if free >= min_free_mb:
            return
        logger.warning(f"Disk almost full ({free:.0f} MB free) — cleaning cache ...")
        videos = sorted(CACHE_DIR.glob("*.mp4"), key=lambda p: p.stat().st_mtime)
        deleted = 0
        for v in videos:
            if _free_disk_mb() >= min_free_mb:
                break
            try:
                size_mb = v.stat().st_size / 1_048_576
                v.unlink()
                deleted += 1
                logger.info(f"   Cache deleted: {v.name} ({size_mb:.0f} MB)")
            except Exception:
                pass
        logger.info(f"Cache cleanup done: {deleted} videos deleted, {_free_disk_mb():.0f} MB free")
    except Exception as e:
        logger.warning(f"Cache cleanup failed: {e}")


def _run_generation(job_id: str, topic: str | None, long: bool):
    import traceback

    def upd(msg: str, pct: int):
        jobs[job_id]["message"]  = msg
        jobs[job_id]["progress"] = pct

    try:
        _cleanup_cache_if_needed()
        topic     = topic or _pick_topic()
        stamp     = datetime.now().strftime("%Y%m%d_%H%M%S")
        audio_path = OUTPUT_DIR / f"audio_{stamp}.mp3"
        video_path = OUTPUT_DIR / f"video_{stamp}.mp4"

        print(f"[GEN] START topic={topic} job={job_id}", flush=True)
        upd("Generating fact…", 10)
        fact_data = generate_fact(topic, long=long)
        print(f"[GEN] Fact OK: {fact_data['title']}", flush=True)

        upd("Creating voiceover…", 30)
        tts_text = f"{fact_data['title']}. {fact_data['fact']}"
        words = tts_text.split()
        if len(words) > 170:
            truncated = " ".join(words[:170])
            # Cut at last complete sentence
            last_end = max(truncated.rfind(". "), truncated.rfind("! "), truncated.rfind("? "))
            tts_text = truncated[:last_end + 1] if last_end > 50 else truncated
        print(f"[GEN] TTS start ({len(tts_text.split())} words)", flush=True)
        _, word_timings = text_to_speech(tts_text, str(audio_path), topic=topic)
        print(f"[GEN] TTS OK ({len(word_timings)} words)", flush=True)

        upd("Creating video…", 55)
        visual_query = fact_data.get("visual_query", "").strip()
        print(f"[GEN] Video start query='{visual_query}'", flush=True)
        create_video(
            title=fact_data["title"],
            fact=fact_data["fact"],
            audio_path=str(audio_path),
            output_path=str(video_path),
            word_timings=word_timings,
            gradient_index=random.randint(0, 4),
            topic=topic,
            visual_query=visual_query,
        )
        audio_path.unlink(missing_ok=True)
        _cleanup_backgrounds_all()
        print(f"[GEN] Video OK: {video_path.name}", flush=True)

        # Metadaten speichern
        description  = fact_data.get("description", fact_data["title"])
        cta          = random.choice(_CTAS)
        full_caption = description + "\n" + cta + "\n" + " ".join(fact_data["hashtags"])

        # Generate thumbnail
        upd("Creating thumbnail…", 88)
        thumb_path = ""
        try:
            from thumbnail_creator import create_thumbnail as _make_thumb
            thumbs = _make_thumb(str(video_path), fact_data["title"], str(OUTPUT_DIR))
            thumb_path = Path(thumbs.get("thumbnail", "")).name
        except Exception as _te:
            logger.warning(f"Thumbnail generation failed: {_te}")

        meta = {
            "title":     fact_data["title"],
            "topic":     topic,
            "caption":   full_caption,
            "uploaded":  False,
            "thumbnail": thumb_path,
        }
        video_path.with_suffix(".json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        upd(f"Done: {video_path.name}", 100)
        jobs[job_id]["status"] = "done"
        jobs[job_id]["video"]  = video_path.name
        logger.info(f"Video erstellt: {video_path.name} (Thema: {topic})")
        notify("syncinUS Bot", f"Video fertig: {fact_data['title'][:50]}")

    except BaseException as e:
        tb = traceback.format_exc()
        print(f"[GEN] ERROR: {e}\n{tb}", flush=True)
        jobs[job_id]["status"]  = "error"
        jobs[job_id]["message"] = f"{type(e).__name__}: {e}"
        logger.error(f"Video-Generierung fehlgeschlagen: {e}\n{tb}")
        raise


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str):
    return jobs.get(job_id, {"status": "not_found"})


# ── Batch-Generierung ─────────────────────────────────────────────────────────

@app.post("/api/generate-batch")
def start_batch(count: int = 3, topic: str = "", long: bool = True):
    batch_id = str(uuid.uuid4())[:8]
    batch_jobs[batch_id] = {
        "status":      "running",
        "total":       count,
        "done":        0,
        "current":     0,
        "current_job": None,
        "videos":      [],
        "message":     "Starting...",
    }
    t = threading.Thread(
        target=_run_batch, args=(batch_id, count, topic or None, long), daemon=True
    )
    t.start()
    return {"batch_id": batch_id}


def _run_batch(batch_id: str, count: int, topic: str | None, long: bool):
    for i in range(count):
        job_id = str(uuid.uuid4())[:8]
        jobs[job_id] = {"status": "running", "progress": 0, "message": "Starting...", "video": None}
        batch_jobs[batch_id]["current_job"] = job_id
        batch_jobs[batch_id]["current"]     = i + 1
        batch_jobs[batch_id]["message"]     = f"Video {i+1} von {count}…"

        _run_generation(job_id, topic, long)

        job = jobs[job_id]
        if job.get("video"):
            batch_jobs[batch_id]["videos"].append(job["video"])
        batch_jobs[batch_id]["done"] = i + 1

    total = len(batch_jobs[batch_id]["videos"])
    batch_jobs[batch_id]["status"]  = "done"
    batch_jobs[batch_id]["message"] = f"Fertig! {total} Video{'s' if total != 1 else ''} erstellt."


@app.get("/api/batch/{batch_id}")
def get_batch(batch_id: str):
    b = batch_jobs.get(batch_id)
    if not b:
        return {"status": "not_found"}
    result = dict(b)
    # Aktuellen Job-Fortschritt mitliefern
    if b.get("current_job"):
        j = jobs.get(b["current_job"], {})
        result["job_progress"] = j.get("progress", 0)
        result["job_message"]  = j.get("message", "")
    return result


# ── TikTok Upload ─────────────────────────────────────────────────────────────

@app.post("/api/upload/{filename}")
def start_upload(filename: str, custom_caption: str = ""):
    video_path = OUTPUT_DIR / filename
    if not video_path.exists():
        return {"error": "Datei nicht gefunden"}

    meta_file = video_path.with_suffix(".json")
    caption   = custom_caption  # manuell eingetragen hat Vorrang
    if not caption and meta_file.exists():
        try:
            caption = json.loads(meta_file.read_text(encoding="utf-8")).get("caption", "")
        except Exception:
            pass

    # Manuelle Beschreibung im JSON speichern für spätere Uploads
    if custom_caption and meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            meta["caption"] = custom_caption
            meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    uploads[filename] = "running"
    t = threading.Thread(
        target=_run_upload, args=(filename, str(video_path), caption), daemon=True
    )
    t.start()
    return {"status": "started"}


def _run_upload(filename: str, video_path: str, caption: str, max_attempts: int = 1):
    meta_file  = Path(video_path).with_suffix(".json")
    video_size = Path(video_path).stat().st_size if Path(video_path).exists() else 0
    size_mb    = round(video_size / 1_048_576, 1)
    title      = filename
    thumb_path = ""
    try:
        if meta_file.exists():
            _m = json.loads(meta_file.read_text(encoding="utf-8"))
            title      = _m.get("title", filename)
            _thumb_name = _m.get("thumbnail", "")
            if _thumb_name:
                _thumb_full = OUTPUT_DIR / _thumb_name
                if _thumb_full.exists():
                    thumb_path = str(_thumb_full)
    except Exception:
        pass

    if video_size < 500_000:
        logger.error(f"Upload abgebrochen: {filename} ist zu klein ({video_size // 1024} KB) — Video-Generierung wahrscheinlich fehlgeschlagen")
        uploads[filename] = "error"
        _append_upload_history(filename, title, "failed", size_mb)
        return

    upload_ok = False

    for attempt in range(1, max_attempts + 1):
        # ── Doppelpost-Schutz: bereits hochgeladen? → sofort abbrechen ──────
        try:
            if meta_file.exists() and json.loads(meta_file.read_text(encoding="utf-8")).get("uploaded"):
                logger.info(f"Upload übersprungen: {filename} bereits hochgeladen (Doppelpost verhindert)")
                uploads[filename] = "done"
                return
        except Exception:
            pass

        logger.info(f"Upload Versuch {attempt}/{max_attempts}: {filename}")
        uploads[filename] = f"running (Versuch {attempt}/{max_attempts})"

        try:
            ok = upload_video_browser(video_path, caption)
        except DuplicateContentError as e:
            logger.error(f"   ✗ Duplikat (409) — neues Video wird generiert: {e}")
            uploads[filename] = "duplicate"
            _append_upload_history(filename, title, "duplicate", size_mb)
            return
        except Exception as e:
            logger.error(f"Upload-Fehler (Versuch {attempt}/{max_attempts}): {e}")
            ok = False

        if ok:
            # Sofort als hochgeladen markieren
            try:
                if meta_file.exists():
                    meta = json.loads(meta_file.read_text(encoding="utf-8"))
                    meta["uploaded"] = True
                    meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as meta_e:
                logger.warning(f"Metadata-Update fehlgeschlagen (Upload war erfolgreich): {meta_e}")
            upload_ok = True
            break   # ← Sofort raus — kein Retry nach erfolgreichem Upload möglich
        else:
            logger.warning(f"Upload fehlgeschlagen (Versuch {attempt}/{max_attempts}): {filename}")
            if attempt < max_attempts:
                uploads[filename] = f"wartet auf Retry ({attempt}/{max_attempts})…"
                time.sleep(60)

    # ── Cleanup außerhalb der Retry-Schleife ─────────────────────────────────
    if upload_ok:
        uploads[filename] = "done"
        _append_upload_history(filename, title, "success", size_mb)
        logger.info(f"Upload erfolgreich: {filename}")
        notify("syncinUS Bot", f"✓ Video hochgeladen: {Path(video_path).stem[:40]}")
        try:
            Path(video_path).unlink(missing_ok=True)
            Path(video_path).with_suffix(".mp3").unlink(missing_ok=True)
            Path(video_path).with_suffix(".json").unlink(missing_ok=True)
            logger.info(f"Video nach Upload gelöscht: {filename}")
        except Exception as e:
            logger.warning(f"Auto-Delete nach Upload fehlgeschlagen: {e}")
        for item in upload_queue:
            if item["filename"] == filename and item["status"] == "uploading":
                item["status"] = "done"
    else:
        uploads[filename] = "error"
        _append_upload_history(filename, title, "failed", size_mb)
        for item in upload_queue:
            if item["filename"] == filename and item["status"] == "uploading":
                item["status"] = "error"
        logger.error(f"Upload endgültig fehlgeschlagen nach {max_attempts} Versuchen: {filename}")
        notify("syncinUS Bot", f"❌ Upload fehlgeschlagen: {Path(video_path).stem[:40]}")


@app.get("/api/upload-status/{filename}")
def upload_status(filename: str):
    return {"status": uploads.get(filename, "idle")}


@app.delete("/api/videos/{filename}")
def delete_video(filename: str):
    video_file = OUTPUT_DIR / filename
    meta_file  = video_file.with_suffix(".json")
    if not video_file.exists():
        return {"error": "Datei nicht gefunden"}
    video_file.unlink()
    if meta_file.exists():
        meta_file.unlink()
    uploads.pop(filename, None)
    return {"status": "deleted"}


@app.get("/api/upload-history")
def get_upload_history():
    if not UPLOAD_HISTORY_FILE.exists():
        return []
    try:
        return json.loads(UPLOAD_HISTORY_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


@app.post("/api/mark-uploaded/{filename}")
def mark_uploaded(filename: str):
    meta_file = (OUTPUT_DIR / filename).with_suffix(".json")
    if meta_file.exists():
        meta = json.loads(meta_file.read_text(encoding="utf-8"))
    else:
        meta = {"title": filename.replace(".mp4",""), "topic": "", "caption": "", "uploaded": False}
    meta["uploaded"] = True
    meta_file.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    uploads[filename] = "done"
    return {"status": "ok"}


# ── Analytics ─────────────────────────────────────────────────────────────────

analytics_job: dict = {"status": "idle", "message": ""}
_analytics_last_refresh: datetime | None = None
_ANALYTICS_AUTO_INTERVAL = 15 * 60  # 15 Minuten

@app.get("/api/analytics")
def get_analytics(refresh: bool = False):
    global _analytics_last_refresh
    if refresh:
        if IS_RAILWAY:
            return {"status": "cloud_mode", "message": "Führe sync_to_railway.py auf deinem Mac aus"}
        analytics_job["status"] = "running"
        analytics_job["message"] = "Opening Creator Center..."
        def _refresh_and_update():
            global _analytics_last_refresh
            _run_analytics()
            if analytics_job["status"] == "done":
                _analytics_last_refresh = datetime.now()
        t = threading.Thread(target=_refresh_and_update, daemon=True)
        t.start()
        return {"status": "started"}
    data = load_cached()
    return {
        "status": "ok",
        "data": data,
        "count": len(data),
        "last_updated": _analytics_last_refresh.isoformat() if _analytics_last_refresh else None,
        "is_railway": IS_RAILWAY,
    }

@app.get("/api/analytics/status")
def analytics_status():
    return analytics_job


@app.get("/api/config")
def get_config():
    """Gibt Umgebungs-Konfiguration zurück (Cloud vs. Lokal)."""
    return {"is_railway": IS_RAILWAY, "output_dir": str(OUTPUT_DIR)}


@app.post("/api/analytics/sync-cache")
def sync_analytics_cache(data: list = Body(...)):
    """Empfängt Analytics-Cache von lokalem Mac und speichert ihn."""
    from analytics_scraper import CACHE_FILE as _CACHE_FILE
    global _analytics_last_refresh
    _CACHE_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    _analytics_last_refresh = datetime.now()
    _append_analytics_history(data)
    logger.info(f"Analytics-Cache synchronisiert: {len(data)} Videos vom lokalen Mac")
    return {"status": "ok", "count": len(data)}

def _run_analytics():
    try:
        analytics_job["message"] = "Reading TikTok stats..."
        data = fetch_analytics()
        analytics_job["status"]  = "done"
        cached = load_cached()
        if data and data is not cached:
            analytics_job["message"] = f"{len(data)} Videos geladen"
            _append_analytics_history(data)
            logger.info(f"Analytics-Snapshot gespeichert: {len(data)} Videos")
        else:
            analytics_job["message"] = f"Gecachte Daten ({len(data)} Videos) — kein Internet"
            logger.warning("Analytics: kein Internet, gecachte Daten gezeigt")
    except Exception as e:
        err = str(e)
        if "ERR_INTERNET_DISCONNECTED" in err or "kein Internet" in err:
            analytics_job["status"]  = "error"
            analytics_job["message"] = "Kein Internet — bitte Verbindung prüfen"
        else:
            analytics_job["status"]  = "error"
            analytics_job["message"] = err
        logger.error(f"Analytics-Fehler: {e}")


def _analytics_auto_refresh_loop():
    """Aktualisiert Analytics automatisch alle 15 Minuten im Hintergrund."""
    global _analytics_last_refresh
    if IS_RAILWAY and not os.environ.get("TIKTOK_COOKIES", "").strip():
        logger.info("Cloud-Modus: Analytics-Auto-Refresh deaktiviert (kein TIKTOK_COOKIES gesetzt)")
        return
    if IS_RAILWAY:
        logger.info("Cloud-Modus: Analytics-Auto-Refresh aktiv (TIKTOK_COOKIES vorhanden)")
    time.sleep(90)  # Kurz warten bis Dashboard bereit ist
    while True:
        try:
            logger.info("Auto-Analytics: starte Hintergrund-Refresh...")
            data = fetch_analytics()
            if data:
                _analytics_last_refresh = datetime.now()
                _append_analytics_history(data)
                logger.info(f"Auto-Analytics: {len(data)} Videos aktualisiert ({_analytics_last_refresh.strftime('%H:%M:%S')})")
        except Exception as e:
            logger.warning(f"Auto-Analytics Fehler: {e}")
        time.sleep(_ANALYTICS_AUTO_INTERVAL)


# ── Upload-Warteschlange ──────────────────────────────────────────────────────

QUEUE_FILE             = OUTPUT_DIR / "upload_queue.json"
SCHEDULE_FILE          = OUTPUT_DIR / "schedule.json"
ANALYTICS_HISTORY_FILE = OUTPUT_DIR / "analytics_history.json"
UPLOAD_HISTORY_FILE    = OUTPUT_DIR / "upload_history.json"


def _append_upload_history(filename: str, title: str, status: str, size_mb: float):
    history = []
    if UPLOAD_HISTORY_FILE.exists():
        try:
            history = json.loads(UPLOAD_HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    history.append({
        "filename": filename,
        "title":    title or filename,
        "time":     datetime.now().strftime("%d.%m. %H:%M"),
        "status":   status,
        "size_mb":  size_mb,
    })
    UPLOAD_HISTORY_FILE.write_text(
        json.dumps(history[-20:], ensure_ascii=False, indent=2), encoding="utf-8"
    )

upload_queue: list[dict] = []   # [{filename, caption, scheduled_time, status}]
_queue_lock = threading.Lock()


def _load_queue():
    global upload_queue
    if QUEUE_FILE.exists():
        try:
            upload_queue = json.loads(QUEUE_FILE.read_text(encoding="utf-8"))
        except Exception:
            upload_queue = []


def _save_queue():
    QUEUE_FILE.write_text(json.dumps(upload_queue, ensure_ascii=False, indent=2), encoding="utf-8")


def _queue_processor():
    """Hintergrund-Thread: prüft minütlich ob ein geplanter Upload fällig ist."""
    while True:
        try:
            now = datetime.now().strftime("%Y-%m-%d %H:%M")
            with _queue_lock:
                for item in upload_queue:
                    if item["status"] == "waiting" and item["scheduled_time"] <= now:
                        item["status"] = "uploading"
                        vp = str(OUTPUT_DIR / item["filename"])
                        t  = threading.Thread(
                            target=_run_upload,
                            args=(item["filename"], vp, item.get("caption", "")),
                            daemon=True,
                        )
                        t.start()
                        logger.info(f"Queue: starte Upload für {item['filename']}")
                _save_queue()
        except Exception as e:
            logger.error(f"Queue-Processor-Fehler: {e}")
        time.sleep(30)


@app.get("/api/queue")
def get_queue():
    return upload_queue


@app.post("/api/queue/add")
def add_to_queue(filename: str, scheduled_time: str, custom_caption: str = ""):
    """Fügt ein Video zur Upload-Warteschlange hinzu. scheduled_time: 'YYYY-MM-DD HH:MM'"""
    video_path = OUTPUT_DIR / filename
    if not video_path.exists():
        return {"error": "Datei nicht gefunden"}
    caption = custom_caption
    if not caption:
        meta_file = video_path.with_suffix(".json")
        if meta_file.exists():
            try:
                caption = json.loads(meta_file.read_text(encoding="utf-8")).get("caption", "")
            except Exception:
                pass
    with _queue_lock:
        # Duplikate vermeiden
        upload_queue[:] = [q for q in upload_queue if q["filename"] != filename]
        upload_queue.append({
            "filename":       filename,
            "caption":        caption,
            "scheduled_time": scheduled_time,
            "status":         "waiting",
        })
        upload_queue.sort(key=lambda x: x["scheduled_time"])
        _save_queue()
    logger.info(f"Queue: {filename} geplant für {scheduled_time}")
    return {"status": "queued"}


@app.delete("/api/queue/{filename}")
def remove_from_queue(filename: str):
    with _queue_lock:
        upload_queue[:] = [q for q in upload_queue if q["filename"] != filename]
        _save_queue()
    return {"status": "removed"}


# ── Auto-Zeitplan ─────────────────────────────────────────────────────────────

class ScheduleSlot(BaseModel):
    time:     str  = "18:00"
    mode:     str  = "new"       # "new" = generieren, "existing" = vorhandenes Video
    topic:    str  = ""
    filename: str  = ""          # nur relevant wenn mode == "existing"
    long:     bool = True        # Zeitplan-Posts immer lang (≥1 min) für bessere Performance

class ScheduleConfig(BaseModel):
    enabled:          bool               = False
    recovery_until:   str | None         = None
    recovery_reason:  str                = ""
    slots:            list[ScheduleSlot] = [ScheduleSlot()]


DEFAULT_SCHEDULE = {
    "enabled":          True,
    "recovery_until":   None,   # ISO-Datum "YYYY-MM-DD" oder None
    "recovery_reason":  "",
    "slots": [
        {"time": "06:00", "mode": "new", "topic": "", "filename": "", "long": True},
        {"time": "11:00", "mode": "new", "topic": "", "filename": "", "long": True},
        {"time": "15:00", "mode": "new", "topic": "", "filename": "", "long": True},
        {"time": "21:00", "mode": "new", "topic": "", "filename": "", "long": True},
    ],
}


def _load_schedule_cfg() -> dict:
    if SCHEDULE_FILE.exists():
        try:
            raw = json.loads(SCHEDULE_FILE.read_text(encoding="utf-8"))
            # Altes Format (time + count) automatisch migrieren
            if "time" in raw and "slots" not in raw:
                slots = [{"time": raw["time"], "topic": raw.get("topic", ""), "long": raw.get("long", False)}]
                raw = {"enabled": raw.get("enabled", False), "slots": slots}
            return {**DEFAULT_SCHEDULE, **raw}
        except Exception:
            pass
    return dict(DEFAULT_SCHEDULE)


def _save_schedule_cfg(cfg: dict):
    SCHEDULE_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def _check_views_drop() -> tuple[bool, str]:
    """
    Prüft ob ein Shadow Ban wahrscheinlich ist.
    Gibt (True, Grund) zurück wenn ein Einbruch erkannt wird.
    """
    try:
        from analytics_scraper import load_cached
        data = load_cached()
        if len(data) < 3:
            return False, ""

        recent = data[:3]
        # Signal 1: letzte 3 Videos alle mit 0 Views
        if all(v.get("views", 0) == 0 for v in recent):
            return True, "Letzte 3 Videos haben 0 Views"

        # Signal 2: starker Views-Einbruch (>85% Rückgang) gegenüber älteren Videos
        if len(data) >= 6:
            recent_avg = sum(v.get("views", 0) for v in data[:3]) / 3
            older_avg  = sum(v.get("views", 0) for v in data[3:6]) / 3
            if older_avg > 50 and recent_avg < older_avg * 0.15:
                return True, f"Views-Einbruch: Ø {recent_avg:.0f} vs. Ø {older_avg:.0f} (>{85}% Rückgang)"

        return False, ""
    except Exception:
        return False, ""


FIRED_KEYS_FILE = OUTPUT_DIR / "fired_keys.json"


def _load_fired_keys() -> set:
    try:
        if FIRED_KEYS_FILE.exists():
            return set(json.loads(FIRED_KEYS_FILE.read_text(encoding="utf-8")))
    except Exception:
        pass
    return set()


def _save_fired_keys(keys: set):
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        pruned = {k for k in keys if today in k or "recovery" in k or "pause" in k}
        FIRED_KEYS_FILE.write_text(json.dumps(list(pruned), ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


def _scheduler_loop():
    """Hintergrund-Thread: prüft alle 30s ob ein Slot feuern soll."""
    fired_keys: set[str] = _load_fired_keys()
    while True:
        try:
            cfg = _load_schedule_cfg()
            if cfg.get("enabled"):
                now      = datetime.now()
                today    = now.strftime("%Y-%m-%d")
                cur_time = now.strftime("%H:%M")

                # ── Recovery-Modus prüfen ──────────────────────────────────
                recovery_until = cfg.get("recovery_until")
                if recovery_until and today <= recovery_until:
                    # Einmal pro Tag loggen
                    day_key = f"recovery_logged_{today}"
                    if day_key not in fired_keys:
                        fired_keys.add(day_key)
                        logger.info(f"Recovery-Modus aktiv bis {recovery_until} — kein Upload heute")
                    time.sleep(30)
                    continue

                # Recovery abgelaufen → automatisch beenden
                if recovery_until and today > recovery_until:
                    cfg["recovery_until"]  = None
                    cfg["recovery_reason"] = ""
                    _save_schedule_cfg(cfg)
                    logger.info("Recovery-Modus beendet — Zeitplan wieder aktiv")
                    notify("syncinUS Bot", "Recovery abgeschlossen — Zeitplan wieder aktiv!")

                # ── Auto-Pause: Views-Drop prüfen ─────────────────────────
                pause_check_key = f"pause_checked_{today}"
                if pause_check_key not in fired_keys:
                    fired_keys.add(pause_check_key)
                    should_pause, reason = _check_views_drop()
                    if should_pause:
                        recovery_days = 7
                        until = (now + __import__('datetime').timedelta(days=recovery_days)).strftime("%Y-%m-%d")
                        cfg["recovery_until"]  = until
                        cfg["recovery_reason"] = f"Auto-Pause: {reason}"
                        _save_schedule_cfg(cfg)
                        logger.warning(f"Auto-Pause aktiviert bis {until}: {reason}")
                        notify("syncinUS Bot", f"⚠️ Auto-Pause: {reason[:60]}")
                        time.sleep(30)
                        continue

                # ── Slots feuern ───────────────────────────────────────────
                fired_keys = {k for k in fired_keys if k.startswith(today) or k.startswith("recovery") or k.startswith("pause")}

                # Slot-Abstands-Warnung (nur einmal pro Tag)
                warn_key = f"{today}_gap_warned"
                if warn_key not in fired_keys:
                    fired_keys.add(warn_key)
                    slot_times = sorted([
                        int(s["time"].split(":")[0]) * 60 + int(s["time"].split(":")[1])
                        for s in cfg.get("slots", [])
                    ])
                    for i in range(1, len(slot_times)):
                        if slot_times[i] - slot_times[i-1] < 6 * 60:
                            logger.warning(f"Zeitplan: Slots liegen weniger als 6h auseinander — Shadow-Ban-Risiko!")
                            break

                for slot in cfg.get("slots", []):
                    target = slot.get("time", "18:00")
                    key    = f"{today}_{target}"
                    # Fire at exact time OR within 4 minutes after (catches restarts that miss exact moment)
                    t_h, t_m = int(target.split(":")[0]), int(target.split(":")[1])
                    slot_min = t_h * 60 + t_m
                    now_min  = now.hour * 60 + now.minute
                    in_window = slot_min <= now_min <= slot_min + 4
                    if in_window and key not in fired_keys:
                        fired_keys.add(key)
                        _save_fired_keys(fired_keys)
                        mode  = slot.get("mode", "new")
                        label = slot.get("filename") if mode == "existing" else (slot.get("topic") or "random")
                        logger.info(f"Zeitplan: Slot um {target} feuert (mode={mode}, {label})")
                        notify("syncinUS Bot", f"Zeitplan: Video um {target}…")
                        job_id = str(uuid.uuid4())[:8]
                        jobs[job_id] = {"status": "running", "progress": 0, "message": "Startet…", "video": None}
                        threading.Thread(
                            target=_run_scheduled_single,
                            args=(job_id, slot),
                            daemon=True,
                        ).start()
        except Exception as e:
            logger.error(f"Scheduler-Fehler: {e}")
        time.sleep(30)


def _run_scheduled_single(job_id: str, slot: dict):
    """Führt einen Zeitplan-Slot aus: entweder neues Video generieren+hochladen
    oder ein vorhandenes Video direkt hochladen."""
    # Random jitter 0-12 min so posts don't always appear at exact same time
    _jitter = random.randint(0, 720)
    if _jitter:
        logger.info(f"Schedule: jitter {_jitter}s")
        time.sleep(_jitter)

    mode     = slot.get("mode", "new")
    filename = slot.get("filename", "")

    if mode == "auto":
        # ── Auto: ältestes vorhandenes Video nehmen, sonst neu generieren ──
        candidates = sorted(
            [f for f in OUTPUT_DIR.glob("*.mp4")],
            key=lambda f: f.stat().st_mtime
        )
        picked = None
        for f in candidates:
            meta_f = f.with_suffix(".json")
            if meta_f.exists():
                try:
                    m = json.loads(meta_f.read_text(encoding="utf-8"))
                    if not m.get("uploaded", False):
                        picked = f
                        break
                except Exception:
                    pass
        if picked:
            filename = picked.name
            mode     = "existing"   # weiter als "existing" behandeln
            logger.info(f"Zeitplan (auto): nehme vorhandenes Video {filename}")
        else:
            mode = "new"            # kein Video vorhanden → neu generieren
            logger.info("Zeitplan (auto): kein vorhandenes Video → generiere neu")

    if mode == "existing" and filename:
        # ── Vorhandenes Video hochladen ────────────────────────────────────
        vp = OUTPUT_DIR / filename
        if not vp.exists():
            logger.error(f"Zeitplan: Datei nicht gefunden: {filename}")
            jobs[job_id]["status"]  = "error"
            jobs[job_id]["message"] = f"Datei nicht gefunden: {filename}"
            return
        meta_f  = vp.with_suffix(".json")
        caption = ""
        title   = ""
        if meta_f.exists():
            try:
                meta    = json.loads(meta_f.read_text(encoding="utf-8"))
                caption = meta.get("caption", "")
                title   = meta.get("title", "")
            except Exception:
                pass
        # Fallback: Caption aus Titel + Standard-Tags generieren wenn leer
        if not caption:
            fallback_cta = random.choice(_CTAS)
            caption = (
                f"{title + ' ' if title else ''}Did you know? 🤯\n"
                f"{fallback_cta}\n"
                f"#fyp #foryou #facts #didyouknow #viral"
            )
            logger.warning(f"Zeitplan: Caption war leer für {filename} — Fallback genutzt")
        jobs[job_id]["status"]  = "done"
        jobs[job_id]["message"] = f"Uploade {filename}…"
        jobs[job_id]["video"]   = filename
        logger.info(f"Zeitplan: uploade vorhandenes Video {filename} | Caption: {caption[:60]}…")
        _run_upload(filename, str(vp), caption)
        notify("syncinUS Bot", f"Zeitplan: Video hochgeladen!")
    else:
        # ── Neues Video generieren + hochladen (mit Auto-Retry) ────────────
        topic      = slot.get("topic") or None
        long       = slot.get("long", True)
        _slot_time = slot.get("time", "?")
        for _attempt in range(3):  # max 3 Versuche
            try:
                _run_generation(job_id, topic, long)
                break  # Erfolg
            except Exception as _e:
                _err = str(_e)
                if "Broken pipe" in _err or "BrokenPipe" in _err or isinstance(_e, OSError):
                    _delay, _reason = 30, "BrokenPipe (OOM)"
                elif "529" in _err or "verload" in _err.lower():
                    _delay, _reason = 90, "Anthropic 529"
                elif "not able to create" in _err or "parse" in _err.lower():
                    _delay, _reason = 5, "Claude-Ablehnung"
                else:
                    _delay, _reason = 30, type(_e).__name__
                if _attempt < 2:
                    logger.warning(f"Zeitplan: Slot {_slot_time} — {_reason}, Retry {_attempt+1}/2 in {_delay}s")
                    notify("syncinUS Bot", f"⚠️ Slot {_slot_time}: {_reason}\nRetry {_attempt+1}/2 in {_delay}s…")
                    time.sleep(_delay)
                    jobs[job_id] = {"status": "running", "progress": 0, "message": f"Retry {_attempt+1}…", "video": None}
                else:
                    logger.error(f"Zeitplan: Slot {_slot_time} endgültig fehlgeschlagen nach 3 Versuchen: {_err}")
                    notify("syncinUS Bot", f"❌ Slot {_slot_time}: Aufgegeben nach 3 Versuchen\n{_reason}: {_err[:60]}")
        job = jobs.get(job_id, {})
        if job.get("video"):
            vp     = str(OUTPUT_DIR / job["video"])
            meta_f = Path(vp).with_suffix(".json")
            caption = ""
            title   = ""
            if meta_f.exists():
                try:
                    meta    = json.loads(meta_f.read_text(encoding="utf-8"))
                    caption = meta.get("caption", "")
                    title   = meta.get("title", "")
                except Exception:
                    pass
            if not caption:
                fallback_cta = random.choice(_CTAS)
                caption = (
                    f"{title + ' ' if title else ''}Did you know? 🤯\n"
                    f"{fallback_cta}\n"
                    f"#fyp #foryou #facts #didyouknow #viral"
                )
                logger.warning(f"Zeitplan: Caption war leer für {job['video']} — Fallback genutzt")
            logger.info(f"Zeitplan: starte Upload für {job['video']} | Caption: {caption[:60]}…")
            _run_upload(job["video"], vp, caption)
            # 409-Duplikat: neues Video generieren und erneut hochladen
            if uploads.get(job["video"]) == "duplicate":
                logger.warning("Zeitplan: 409-Duplikat — regeneriere neues Video...")
                job_id2 = str(uuid.uuid4())[:8]
                jobs[job_id2] = {"status": "running", "progress": 0, "message": "Retry (duplicate)...", "video": None}
                _run_generation(job_id2, topic, long)
                job2 = jobs.get(job_id2, {})
                if job2.get("video"):
                    vp2   = str(OUTPUT_DIR / job2["video"])
                    meta2 = Path(vp2).with_suffix(".json")
                    cap2  = json.loads(meta2.read_text(encoding="utf-8")).get("caption", caption) if meta2.exists() else caption
                    logger.info(f"Zeitplan: Retry-Upload nach Duplikat für {job2['video']}")
                    _run_upload(job2["video"], vp2, cap2)
            notify("syncinUS Bot", f"Zeitplan: Video hochgeladen!")
        else:
            logger.error(f"Zeitplan: Video-Generierung fehlgeschlagen (job {job_id})")


@app.get("/api/videos/unuploaded")
def list_unuploaded():
    """Gibt alle lokal vorhandenen Videos zurück die noch nicht hochgeladen wurden."""
    result = []
    for mp4 in sorted(OUTPUT_DIR.glob("*.mp4"), key=lambda f: f.stat().st_mtime, reverse=True):
        meta_file = mp4.with_suffix(".json")
        meta: dict = {}
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                pass
        if not meta.get("uploaded", False):
            result.append({
                "filename": mp4.name,
                "title":    meta.get("title", mp4.stem),
                "topic":    meta.get("topic", ""),
                "created":  datetime.fromtimestamp(mp4.stat().st_mtime).strftime("%d.%m. %H:%M"),
            })
    return result


@app.get("/api/schedule")
def get_schedule():
    return _load_schedule_cfg()


@app.post("/api/schedule")
def save_schedule(cfg: ScheduleConfig):
    data = cfg.model_dump()
    _save_schedule_cfg(data)
    n     = len(data["slots"])
    times = ", ".join(s["time"] for s in data["slots"])
    status = "aktiviert" if data["enabled"] else "deaktiviert"
    logger.info(f"Zeitplan {status}: {n} Slot(s) um {times}")
    return {"status": "ok", **data}


@app.post("/api/schedule/pause")
def manual_pause(days: int = 7, reason: str = "Manuell pausiert"):
    cfg = _load_schedule_cfg()
    until = (datetime.now() + __import__('datetime').timedelta(days=days)).strftime("%Y-%m-%d")
    cfg["recovery_until"]  = until
    cfg["recovery_reason"] = reason
    _save_schedule_cfg(cfg)
    logger.info(f"Manueller Recovery-Modus: {days} Tage bis {until} — {reason}")
    return {"status": "paused", "recovery_until": until}


@app.post("/api/schedule/resume")
def manual_resume():
    cfg = _load_schedule_cfg()
    cfg["recovery_until"]  = None
    cfg["recovery_reason"] = ""
    _save_schedule_cfg(cfg)
    logger.info("Recovery-Modus manuell beendet")
    return {"status": "resumed"}


# ── Telegram Webhook ──────────────────────────────────────────────────────────

_TG_BOTS = {
    "ef": ("EN Fakten",  "https://tiktok-bot-english-production.up.railway.app"),
    "df": ("DE Fakten",  "https://syncin-bot-production.up.railway.app"),
    "dr": ("DE Reddit",  "https://reddit-story-de-production.up.railway.app"),
    "er": ("EN Reddit",  "https://reddit-story-en-production.up.railway.app"),
}
_TG_HELP = (
    "🤖 <b>Befehle</b>\n\n"
    "<b>Generieren + hochladen:</b>\n"
    "/gen ef — EN Fakten\n"
    "/gen df — DE Fakten\n"
    "/gen dr — DE Reddit\n"
    "/gen er — EN Reddit\n\n"
    "<b>Nächstes ausstehendes Video hochladen:</b>\n"
    "/upload ef · df · dr · er\n\n"
    "<b>Status aller Bots:</b>\n"
    "/status\n\n"
    "<b>Hilfe:</b>\n"
    "/help"
)


def _tg_api(url: str, body: dict | None = None) -> dict:
    import urllib.request as _ur, json as _j
    data = _j.dumps(body or {}).encode() if body is not None else b"{}"
    req  = _ur.Request(url, data=data, headers={"Content-Type": "application/json"})
    return _j.loads(_ur.urlopen(req, timeout=15).read())


@app.post("/telegram-webhook")
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
        msg  = data.get("message") or data.get("edited_message")
        if not msg:
            return {"ok": True}
        text = (msg.get("text") or "").strip()
        if not text:
            return {"ok": True}

        parts = text.lower().split()
        cmd   = parts[0] if parts else ""
        arg   = parts[1] if len(parts) > 1 else ""

        if cmd == "/help":
            _tg_send(_TG_HELP)

        elif cmd == "/gen":
            if arg not in _TG_BOTS:
                _tg_send(f"❓ Unbekannter Bot <code>{arg}</code>. Verfügbar: ef · df · dr · er")
                return {"ok": True}
            name, base_url = _TG_BOTS[arg]
            _tg_send(f"🎬 <b>{name}</b> — Generierung gestartet…")
            def _do_gen(n=name, u=base_url):
                try:
                    r = _tg_api(f"{u}/api/generate", {})
                    job = r.get("job_id", "?")
                    _tg_send(f"⏳ <b>{n}</b> — Job läuft (<code>{job}</code>)\nDu kriegst eine Nachricht wenn fertig.")
                except Exception as e:
                    _tg_send(f"❌ <b>{n}</b> — Fehler: {e}")
            threading.Thread(target=_do_gen, daemon=True).start()

        elif cmd == "/upload":
            if arg not in _TG_BOTS:
                _tg_send(f"❓ Unbekannter Bot <code>{arg}</code>. Verfügbar: ef · df · dr · er")
                return {"ok": True}
            name, base_url = _TG_BOTS[arg]
            def _do_upload(n=name, u=base_url):
                try:
                    import urllib.request as _ur, json as _j
                    vids = _j.loads(_ur.urlopen(f"{u}/api/videos/unuploaded", timeout=10).read())
                    if not vids:
                        _tg_send(f"📭 <b>{n}</b> — Keine ausstehenden Videos")
                        return
                    fn = vids[0]["filename"]
                    _tg_api(f"{u}/api/upload/{fn}", {})
                    _tg_send(f"⬆️ <b>{n}</b> — Upload gestartet: <code>{fn}</code>")
                except Exception as e:
                    _tg_send(f"❌ <b>{n}</b> — Fehler: {e}")
            threading.Thread(target=_do_upload, daemon=True).start()

        elif cmd == "/status":
            def _do_status():
                lines = ["📊 <b>Bot Status</b>\n"]
                for key, (name, base_url) in _TG_BOTS.items():
                    try:
                        import urllib.request as _ur, json as _j
                        vids    = _j.loads(_ur.urlopen(f"{base_url}/api/videos/unuploaded", timeout=8).read())
                        history = _j.loads(_ur.urlopen(f"{base_url}/api/upload-history", timeout=8).read())
                        last    = history[0] if history else None
                        pending = len(vids)
                        last_str = f" · Letzter Upload: {last['filename'][:25]}" if last else ""
                        icon    = "⏳" if pending else "✅"
                        lines.append(f"{icon} <b>{name}</b>{' — ' + str(pending) + ' ausstehend' if pending else ' — alles hochgeladen'}{last_str}")
                    except Exception:
                        lines.append(f"❓ <b>{name}</b> — nicht erreichbar")
                _tg_send("\n".join(lines))
            threading.Thread(target=_do_status, daemon=True).start()

        else:
            _tg_send(_TG_HELP)

        return {"ok": True}
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return {"ok": True}


# ── Beste Posting-Zeiten ─────────────────────────────────────────────────────

@app.get("/api/analytics/best-times")
def get_best_times():
    """
    Analysiert welche Tageszeiten die höchsten Views bringen.
    Matcht lokale JSON-Dateien (Erstellungszeitpunkt als Upload-Proxy)
    mit den Analytics-Daten (Views) und gruppiert nach Stunde.
    """
    from collections import defaultdict

    analytics = load_cached()
    if not analytics:
        return {"error": "Keine Analytics-Daten vorhanden. Bitte zuerst Stats abrufen.", "data": []}

    # Analytics nach bereinigtem Titel indexieren
    analytics_by_title: dict[str, int] = {}
    for video in analytics:
        key = video.get("title", "")[:50].lower().strip()
        if key:
            analytics_by_title[key] = video.get("views", 0)

    hour_views: dict[int, list[int]] = defaultdict(list)

    for jf in OUTPUT_DIR.glob("*.json"):
        try:
            d = json.loads(jf.read_text(encoding="utf-8"))
            title = d.get("title", "").strip()
            if not title or title.startswith("video_"):
                continue

            # Zeitstempel aus Dateiname ableiten (video_YYYYMMDD_HHMMSS.json)
            hour = None
            stem = jf.stem  # z.B. "video_20260415_183045"
            parts = stem.split("_")
            if len(parts) >= 3 and len(parts[2]) == 6:
                try:
                    hour = int(parts[2][:2])
                except Exception:
                    pass
            if hour is None:
                hour = datetime.fromtimestamp(jf.stat().st_mtime).hour

            # Views suchen — fuzzy match über Titel-Anfang
            views = 0
            title_key = title[:40].lower()
            for key, v in analytics_by_title.items():
                # Überschneidung von mindestens 3 Wörtern reicht
                t_words = set(title_key.split())
                k_words = set(key.split())
                if len(t_words & k_words) >= 2:
                    views = v
                    break

            hour_views[hour].append(views)
        except Exception:
            pass

    if not hour_views:
        return {"error": "Nicht genug Daten für Analyse (mind. 1 Video mit Analytics benötigt).", "data": []}

    result = []
    for hour in sorted(hour_views.keys()):
        vlist = hour_views[hour]
        avg   = sum(vlist) / len(vlist)
        result.append({
            "hour":        hour,
            "label":       f"{hour:02d}:00",
            "avg_views":   int(avg),
            "video_count": len(vlist),
        })

    result.sort(key=lambda x: x["avg_views"], reverse=True)
    return {"data": result, "top3": result[:3]}


# ── Analytics-Verlauf ─────────────────────────────────────────────────────────

def _append_analytics_history(data: list[dict]):
    history = []
    if ANALYTICS_HISTORY_FILE.exists():
        try:
            history = json.loads(ANALYTICS_HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    history.append({
        "timestamp":    datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_views":  sum(v.get("views", 0)    for v in data),
        "total_likes":  sum(v.get("likes", 0)    for v in data),
        "total_comments": sum(v.get("comments", 0) for v in data),
        "total_videos": len(data),
    })
    history = history[-90:]   # max 90 Snapshots behalten
    ANALYTICS_HISTORY_FILE.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


@app.get("/api/analytics/history")
def get_analytics_history():
    if ANALYTICS_HISTORY_FILE.exists():
        try:
            return json.loads(ANALYTICS_HISTORY_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


# ── Logs ──────────────────────────────────────────────────────────────────────

@app.get("/api/logs")
def get_logs(lines: int = 80):
    if not LOG_FILE.exists():
        return {"logs": []}
    try:
        all_lines = LOG_FILE.read_text(encoding="utf-8").splitlines()
        return {"logs": all_lines[-lines:]}
    except Exception:
        return {"logs": []}


# ── Background Cache ──────────────────────────────────────────────────────────

cache_job: dict = {"status": "idle", "message": "", "progress": ""}

@app.post("/api/prefetch-cache")
def start_prefetch(count: int = 8):
    if cache_job["status"] == "running":
        return {"status": "already_running"}
    cache_job["status"]   = "running"
    cache_job["message"]  = "Starte Download..."
    cache_job["progress"] = ""
    t = threading.Thread(target=_run_prefetch, args=(count,), daemon=True)
    t.start()
    return {"status": "started"}

@app.get("/api/prefetch-status")
def prefetch_status():
    from video_creator import CACHE_DIR
    total = len(list(CACHE_DIR.glob("*.mp4")))
    return {**cache_job, "total_cached": total}

def _run_prefetch(count: int):
    import os
    from video_creator import _fetch_pexels_video, CACHE_DIR, TOPIC_QUERIES, PER_QUERY

    ALL_TOPICS = ["science","history","nature","technology","space",
                  "animals","psychology","food","geography","human body","pop culture"]
    api_key = os.environ.get("PEXELS_API_KEY", "")

    # Alle Sub-Queries aller Themen einsammeln
    all_queries: list[tuple[str, str]] = []  # (topic_label, sub_query)
    for topic in ALL_TOPICS:
        for sub_q in TOPIC_QUERIES.get(topic, [topic]):
            all_queries.append((topic, sub_q))

    total_q = len(all_queries)
    downloaded = 0

    for i, (topic, sub_q) in enumerate(all_queries):
        slug     = sub_q.replace(" ", "_")
        existing = len(list(CACHE_DIR.glob(f"{slug}_*.mp4")))
        cache_job["message"]  = f"{sub_q} ({i+1}/{total_q})"
        cache_job["progress"] = f"{i}/{total_q}"

        target = max(count, PER_QUERY)
        if existing < target:
            _fetch_pexels_video(sub_q, api_key, max_videos=target)
            new_count = len(list(CACHE_DIR.glob(f"{slug}_*.mp4")))
            downloaded += max(0, new_count - existing)

    total = len(list(CACHE_DIR.glob("*.mp4")))
    cache_job["status"]  = "done"
    cache_job["message"] = f"Fertig — {total} Videos im Cache (+{downloaded} neu)"
    logger.info(f"Prefetch abgeschlossen: {total} Videos total, {downloaded} neu heruntergeladen")


@app.post("/api/cleanup-cache")
def cleanup_cache(keep: int = 20):
    """Deletes all background cache videos except the newest `keep` ones."""
    try:
        from video_creator import CACHE_DIR
        videos = sorted(CACHE_DIR.glob("*.mp4"), key=lambda p: p.stat().st_mtime, reverse=True)
        to_delete = videos[keep:]
        deleted = 0
        freed_mb = 0.0
        for v in to_delete:
            try:
                freed_mb += v.stat().st_size / 1_048_576
                v.unlink()
                deleted += 1
            except Exception:
                pass
        free_now = _free_disk_mb()
        logger.info(f"Manual cache cleanup: {deleted} videos deleted, {freed_mb:.0f} MB freed, {free_now:.0f} MB free")
        return {"deleted": deleted, "freed_mb": round(freed_mb, 1), "free_disk_mb": round(free_now, 1), "remaining": len(videos) - deleted}
    except Exception as e:
        return {"error": str(e)}


# ── Video-Dateien ausliefern ──────────────────────────────────────────────────

@app.get("/videos/{filename}")
def serve_video(filename: str):
    path = OUTPUT_DIR / filename
    if not path.exists():
        return {"error": "not found"}
    return FileResponse(str(path), media_type="video/mp4")


# ── Static Frontend (muss als letztes gemountet werden) ──────────────────────

app.mount("/", StaticFiles(directory=str(Path(__file__).parent / "static"), html=True))


def _auto_fill_cache():
    """
    Wird beim Start einmalig im Hintergrund ausgeführt.
    Füllt nur fehlende Cache-Einträge auf — bereits vorhandene Videos werden
    NICHT neu heruntergeladen. Kein manueller Eingriff nötig.
    Auf Railway: komplett deaktiviert — Videos werden on-demand beim Generieren geladen
    und bleiben dank Volume persistent gespeichert.
    """
    import os

    # Auf Railway: kein Vorabladen — verhindert Download-Sturm beim Start
    if IS_RAILWAY:
        logger.info("Cache-Startup: Railway-Modus — kein Vorabladen (on-demand bei Generierung)")
        return

    try:
        from video_creator import _fetch_pexels_video, CACHE_DIR, TOPIC_QUERIES, PER_QUERY
        api_key = os.environ.get("PEXELS_API_KEY", "")
        if not api_key:
            return

        ALL_TOPICS = ["science","history","nature","technology","space",
                      "animals","psychology","food","geography","human body","pop culture"]

        missing_queries = []
        for topic in ALL_TOPICS:
            for sub_q in TOPIC_QUERIES.get(topic, [topic]):
                slug = sub_q.replace(" ", "_")
                existing = len(list(CACHE_DIR.glob(f"{slug}_*.mp4")))
                if existing < PER_QUERY:
                    missing_queries.append((sub_q, existing))

        if not missing_queries:
            total = len(list(CACHE_DIR.glob("*.mp4")))
            logger.info(f"Cache vollständig ({total} Videos) — kein Nachfüllen nötig")
            return

        total_before = len(list(CACHE_DIR.glob("*.mp4")))
        logger.info(f"Cache-Startup: {len(missing_queries)} Sub-Queries unvollständig — lade nach…")

        for sub_q, existing in missing_queries:
            # Auf Railway: zwischen Downloads kurz pausieren damit Video-Generierung
            # nicht mit dem Cache-Download um Ressourcen konkurriert
            if IS_RAILWAY:
                time.sleep(2)
            try:
                _fetch_pexels_video(sub_q, api_key, max_videos=PER_QUERY)
            except Exception as e:
                logger.warning(f"Cache-Startup: Fehler bei '{sub_q}': {e}")

        total_after = len(list(CACHE_DIR.glob("*.mp4")))
        logger.info(f"Cache-Startup abgeschlossen: {total_after} Videos (+{total_after - total_before} neu)")

    except Exception as e:
        logger.warning(f"Cache-Startup fehlgeschlagen: {e}")


if __name__ == "__main__":
    # Persistente Queue laden
    _load_queue()

    # Hintergrund-Threads starten
    threading.Thread(target=_scheduler_loop,             daemon=True).start()
    threading.Thread(target=_queue_processor,            daemon=True).start()
    threading.Thread(target=_auto_fill_cache,            daemon=True).start()
    threading.Thread(target=_analytics_auto_refresh_loop, daemon=True).start()

    port = int(os.environ.get("PORT", 8000))
    logger.info(f"syncinUS Dashboard gestartet → http://0.0.0.0:{port}")
    print(f"\n  syncinUS Dashboard  →  http://0.0.0.0:{port}\n")
    # Railway braucht host=0.0.0.0 damit der Healthcheck durchkommt
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
