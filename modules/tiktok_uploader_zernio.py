"""
TikTok Uploader via Zernio API
------------------------------
Kein Browser, kein Playwright — einfache HTTP-Requests.
Video → catbox.moe (temporäres Hosting) → Zernio API → TikTok

Benötigt in .env:
    ZERNIO_API_KEY=...
    ZERNIO_TIKTOK_ACCOUNT_ID=...
"""

import json
import logging
import os
import time
from pathlib import Path

import requests

logger = logging.getLogger("syncin")

ZERNIO_BASE = "https://zernio.com/api/v1"


class DuplicateContentError(Exception):
    """Raised when Zernio rejects the post as duplicate content (HTTP 409)."""


def _zernio_headers() -> dict:
    key = os.environ.get("ZERNIO_API_KEY", "").strip()
    if not key:
        raise ValueError("ZERNIO_API_KEY fehlt — bitte in Railway Variables eintragen")
    return {
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }


def _account_id() -> str:
    aid = os.environ.get("ZERNIO_TIKTOK_ACCOUNT_ID", "").strip()
    if not aid:
        raise ValueError("ZERNIO_TIKTOK_ACCOUNT_ID fehlt — bitte in Railway Variables eintragen")
    return aid


def _youtube_account_id() -> str:
    return os.environ.get("ZERNIO_YOUTUBE_ACCOUNT_ID", "").strip()


def _instagram_account_id() -> str:
    return os.environ.get("ZERNIO_INSTAGRAM_ACCOUNT_ID", "").strip()


def _upload_to_host(video_path: str) -> str:
    """
    Lädt das Video zu einem temporären Hoster hoch und gibt die öffentliche URL zurück.
    Probiert mehrere Dienste bis einer klappt (Fallback-Kette).
    """
    size_mb = Path(video_path).stat().st_size / 1_048_576
    logger.info(f"   Video-Upload: {size_mb:.1f} MB ...")

    errors = []

    # 1. Catbox (permanent, anonymous) — Videos bleiben dauerhaft verfügbar
    try:
        with open(video_path, "rb") as f:
            resp = requests.post(
                "https://catbox.moe/user/api.php",
                data={"reqtype": "fileupload"},
                files={"fileToUpload": ("video.mp4", f, "video/mp4")},
                timeout=120,
            )
        if resp.ok and resp.text.strip().startswith("https://"):
            url = resp.text.strip()
            logger.info(f"   Catbox (permanent): {url}")
            return url
        errors.append(f"Catbox HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        errors.append(f"Catbox: {e}")
    logger.warning(f"   Catbox fehlgeschlagen: {errors[-1]}")

    # 2. Litterbox (catbox temp, 72h) — Fallback
    try:
        with open(video_path, "rb") as f:
            resp = requests.post(
                "https://litterbox.catbox.moe/resources/internals/api.php",
                data={"reqtype": "fileupload", "time": "72h"},
                files={"fileToUpload": ("video.mp4", f, "video/mp4")},
                timeout=120,
            )
        if resp.ok and resp.text.strip().startswith("https://"):
            url = resp.text.strip()
            logger.info(f"   Litterbox (72h): {url}")
            return url
        errors.append(f"Litterbox HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        errors.append(f"Litterbox: {e}")
    logger.warning(f"   Litterbox fehlgeschlagen: {errors[-1]}")

    # 3. gofile.io — funktioniert von Server-IPs, kein Ratelimit
    try:
        # Server holen
        srv_resp = requests.get("https://api.gofile.io/servers", timeout=10)
        server = srv_resp.json()["data"]["servers"][0]["name"]
        with open(video_path, "rb") as f:
            resp = requests.post(
                f"https://{server}.gofile.io/contents/uploadfile",
                files={"file": ("video.mp4", f, "video/mp4")},
                timeout=120,
            )
        data = resp.json()
        if data.get("status") == "ok":
            url = data["data"]["downloadPage"]
            # Direktlink konstruieren
            file_id = data["data"]["fileId"]
            direct  = f"https://store1.gofile.io/download/direct/{data['data']['parentFolder']}/{file_id}/video.mp4"
            logger.info(f"   gofile.io: {direct}")
            return direct
        errors.append(f"gofile HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        errors.append(f"gofile.io: {e}")
    logger.warning(f"   gofile.io fehlgeschlagen: {errors[-1]}")

    # 4. 0x0.st — Fallback
    try:
        with open(video_path, "rb") as f:
            resp = requests.post(
                "https://0x0.st",
                files={"file": ("video.mp4", f, "video/mp4")},
                timeout=120,
            )
        if resp.ok and resp.text.strip().startswith("https://"):
            url = resp.text.strip()
            logger.info(f"   0x0.st: {url}")
            return url
        errors.append(f"0x0.st HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        errors.append(f"0x0.st: {e}")
    logger.warning(f"   0x0.st fehlgeschlagen: {errors[-1]}")

    raise RuntimeError(
        f"Alle Hosting-Dienste fehlgeschlagen ({size_mb:.1f} MB): " + " | ".join(errors)
    )


def _upload_image_to_host(image_path: str) -> str:
    """Lädt ein Thumbnail-Bild zu catbox/litterbox hoch und gibt die öffentliche URL zurück."""
    errors = []
    try:
        with open(image_path, "rb") as f:
            resp = requests.post(
                "https://litterbox.catbox.moe/resources/internals/api.php",
                data={"reqtype": "fileupload", "time": "72h"},
                files={"fileToUpload": ("thumbnail.jpg", f, "image/jpeg")},
                timeout=60,
            )
        if resp.ok and resp.text.strip().startswith("https://"):
            url = resp.text.strip()
            logger.info(f"   Thumbnail hochgeladen: {url}")
            return url
        errors.append(f"Litterbox HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        errors.append(f"Litterbox: {e}")

    try:
        with open(image_path, "rb") as f:
            resp = requests.post(
                "https://catbox.moe/user/api.php",
                data={"reqtype": "fileupload"},
                files={"fileToUpload": ("thumbnail.jpg", f, "image/jpeg")},
                timeout=60,
            )
        if resp.ok and resp.text.strip().startswith("https://"):
            url = resp.text.strip()
            logger.info(f"   Thumbnail (catbox): {url}")
            return url
        errors.append(f"Catbox HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        errors.append(f"Catbox: {e}")

    raise RuntimeError(f"Thumbnail-Hosting fehlgeschlagen: " + " | ".join(errors))


def _create_post(video_url: str, caption: str, thumbnail_url: str = "") -> str:
    """
    Erstellt den Post via Zernio auf TikTok + YouTube Shorts (falls Account gesetzt).
    """
    platforms = [{"platform": "tiktok", "accountId": _account_id()}]
    yt_id = _youtube_account_id()
    ig_id = _instagram_account_id()
    if yt_id:
        platforms.append({"platform": "youtube", "accountId": yt_id})
    if ig_id:
        platforms.append({"platform": "instagram", "accountId": ig_id})

    yt_caption = caption if "#Shorts" in caption else caption + " #Shorts"

    platform_settings = {
        "tiktok": {
            "privacy":       "public",
            "allowComments": True,
            "allowDuets":    True,
            "allowStitches": True,
        },
    }
    if yt_id:
        yt_settings = {
            "privacyStatus": "public",
            "category":      "22",
            "madeForKids":   False,
        }
        if thumbnail_url:
            yt_settings["thumbnailUrl"] = thumbnail_url
        platform_settings["youtube"] = yt_settings
    if ig_id:
        ig_settings = {"mediaType": "REELS"}
        if thumbnail_url:
            ig_settings["coverUrl"] = thumbnail_url
        platform_settings["instagram"] = ig_settings

    logger.info(f"   Erstelle Post via Zernio ({', '.join(p['platform'] for p in platforms)}) ...")
    resp = requests.post(
        f"{ZERNIO_BASE}/posts",
        headers=_zernio_headers(),
        json={
            "content":          yt_caption[:4000],
            "platforms":        platforms,
            "mediaItems":       [{"url": video_url, "type": "video"}],
            "publishNow":       True,
            "platformSettings": platform_settings,
        },
        timeout=300,
    )
    if not resp.ok:
        if resp.status_code == 409:
            raise DuplicateContentError(
                f"Zernio API Fehler: HTTP 409 — {resp.text[:300]}"
            )
        raise RuntimeError(
            f"Zernio API Fehler: HTTP {resp.status_code} — {resp.text[:300]}"
        )

    post_id = (resp.json().get("post") or {}).get("_id", "unknown")
    logger.info(f"   Post erstellt (ID: {post_id})")
    return post_id


def _wait_for_publish(post_id: str, max_wait: int = 120) -> bool:
    """
    Wartet bis der Post von Zernio verarbeitet und auf TikTok veröffentlicht wurde.
    Gibt True zurück wenn status=published.
    """
    h = {k: v for k, v in _zernio_headers().items() if k != "Content-Type"}
    logger.info(f"   Warte auf TikTok-Veröffentlichung (max {max_wait}s) ...")
    for i in range(0, max_wait, 10):
        time.sleep(10)
        try:
            resp = requests.get(f"{ZERNIO_BASE}/posts/{post_id}", headers=h, timeout=15)
            if not resp.ok:
                continue
            post      = resp.json().get("post") or {}
            status    = post.get("status", "")
            platforms = post.get("platforms", [])
            p_status  = platforms[0].get("status", "") if platforms else ""
            logger.info(f"   Status nach {i+10}s: post={status}, tiktok={p_status}")
            if status == "published" or p_status == "published":
                return True
            if p_status in ("failed", "error"):
                p = platforms[0]
                err = p.get("error") or p.get("errorMessage") or p.get("message") or json.dumps(p)
                logger.error(f"   TikTok-Plattform-Fehler: {err}")
                return False
        except Exception as e:
            logger.warning(f"   Status-Check Fehler: {e}")
    logger.warning("   Timeout beim Warten auf Veröffentlichung — prüfe TikTok manuell")
    return False


def _mark_uploaded(video_path: str):
    """Schreibt uploaded=True sofort in die Metadaten — verhindert Doppelpost bei Retry."""
    meta = Path(video_path).with_suffix(".json")
    try:
        if meta.exists():
            d = json.loads(meta.read_text(encoding="utf-8"))
            d["uploaded"] = True
            meta.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("   ✓ Metadata: uploaded=True gesetzt")
    except Exception as e:
        logger.warning(f"   Metadata-Update fehlgeschlagen: {e}")


def upload_video_zernio(video_path: str, caption: str, thumbnail_path: str = "") -> bool:
    """
    Hauptfunktion: Lädt Video via Zernio zu TikTok hoch.
    1. Video → temporärer Hoster (public URL)
    2. Zernio API → TikTok post erstellen
    3. Sofort uploaded=True setzen (Doppelpost-Schutz)
    4. Warte auf Bestätigung (Fehler hier verhindern keinen Erfolg mehr)
    """
    # Mindestgröße prüfen — zu kleine Videos sind korrupt und werden von TikTok abgelehnt
    size_mb = Path(video_path).stat().st_size / 1_048_576
    if size_mb < 3.0:
        logger.error(f"   ✗ Video zu klein ({size_mb:.1f} MB < 3.0 MB) — korrupt, Upload abgebrochen")
        return False

    # Phase 1: Video hochladen — darf fehlschlagen, kein Post erstellt
    try:
        video_url = _upload_to_host(video_path)
    except Exception as e:
        logger.error(f"   ✗ Video-Upload fehlgeschlagen: {e}")
        return False

    # Phase 1b: Thumbnail hosten (optional)
    thumb_url = ""
    if thumbnail_path and Path(thumbnail_path).exists():
        try:
            thumb_url = _upload_image_to_host(thumbnail_path)
        except Exception as e:
            logger.warning(f"   Thumbnail-Upload fehlgeschlagen (kein Blocker): {e}")

    # Phase 2: Post erstellen — darf fehlschlagen, noch kein Post erstellt
    try:
        post_id = _create_post(video_url, caption, thumbnail_url=thumb_url)
    except DuplicateContentError:
        raise  # weiterwerfen — _run_upload soll 409 behandeln (Retry mit neuem Video)
    except Exception as e:
        logger.error(f"   ✗ Post-Erstellung fehlgeschlagen: {e}")
        return False

    # Phase 3: Post ist erstellt → SOFORT als hochgeladen markieren
    # Jeder Retry-Versuch danach wird durch den Doppelpost-Schutz in _run_upload geblockt
    _mark_uploaded(video_path)

    # Phase 4: Auf Veröffentlichung warten — Fehler hier ändern nichts mehr am Ergebnis
    try:
        published = _wait_for_publish(post_id)
        if published:
            logger.info("   ✓ TikTok-Video erfolgreich veröffentlicht!")
        else:
            logger.warning("   ⚠️  Post erstellt, Status unklar — prüfe TikTok manuell")
    except Exception as e:
        logger.warning(f"   Status-Check übersprungen (Post wurde erstellt): {e}")

    return True


# ── Bunny Queue Upload (ersetzt direkten Zernio-Upload auf Railway) ──────────────
def _bunny_queue_upload(video_path: str, caption: str, title: str = "", prefix: str = "fakten_de") -> bool:
    """Upload video to Bunny queue for scheduled posting via GitHub Actions poster."""
    import certifi
    import datetime as _dt
    from pathlib import Path as _P

    stamp    = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{stamp}.mp4"

    password = os.environ.get("BUNNY_STORAGE_PASSWORD", "").strip()
    hostname = os.environ.get("BUNNY_STORAGE_HOSTNAME", "storage.bunnycdn.com")
    zone     = os.environ.get("BUNNY_STORAGE_NAME", "syncin")
    cdn_url  = os.environ.get("BUNNY_CDN_URL", "https://syncin.b-cdn.net")

    if not password:
        logger.error("[upload] BUNNY_STORAGE_PASSWORD nicht gesetzt — kein Bunny Upload")
        return False

    logger.info(f"[upload] Bunny Queue: {filename} …")
    try:
        with open(str(video_path), "rb") as f:
            r = requests.put(
                f"https://{hostname}/{zone}/queue/{filename}",
                headers={"AccessKey": password, "Content-Type": "video/mp4"},
                data=f,
                verify=certifi.where(),
                timeout=300,
            )
        r.raise_for_status()

        meta = json.dumps({
            "title":   title or caption.split("\n")[0][:80],
            "caption": caption,
            "cdn_url": f"{cdn_url}/queue/{filename}",
        }, ensure_ascii=False).encode()
        requests.put(
            f"https://{hostname}/{zone}/queue/{filename.replace('.mp4', '.json')}",
            headers={"AccessKey": password, "Content-Type": "application/json"},
            data=meta,
            verify=certifi.where(),
            timeout=30,
        ).raise_for_status()

        logger.info(f"[upload] ✅ Bunny Queue: {filename}")
        return True
    except Exception as e:
        logger.error(f"[upload] Bunny Upload fehlgeschlagen: {e}")
        return False


# Kompatibilitäts-Alias (app.py bleibt unverändert)
def upload_video_browser(video_path: str, caption: str, thumbnail_path: str = "", title: str = "") -> bool:
    return _bunny_queue_upload(video_path, caption, title=title, prefix="fakten_en")
