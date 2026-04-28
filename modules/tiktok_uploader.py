import os
import math
import time
import requests


TIKTOK_API_BASE = "https://open.tiktokapis.com/v2"
CHUNK_SIZE = 10 * 1024 * 1024  # 10 MB per chunk


def _get_headers(access_token: str) -> dict:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=UTF-8",
    }


def _init_upload(access_token: str, file_size: int, chunk_size: int, total_chunks: int) -> dict:
    """Initialize a video upload and get the upload URL."""
    url = f"{TIKTOK_API_BASE}/post/publish/video/init/"
    payload = {
        "post_info": {
            "title": "",  # will be set per-upload
            "privacy_level": "SELF_ONLY",  # safe default; change to PUBLIC_TO_EVERYONE when ready
            "disable_duet": False,
            "disable_comment": False,
            "disable_stitch": False,
        },
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": file_size,
            "chunk_size": chunk_size,
            "total_chunk_count": total_chunks,
        },
    }
    resp = requests.post(url, json=payload, headers=_get_headers(access_token))
    resp.raise_for_status()
    data = resp.json()
    if data.get("error", {}).get("code") != "ok":
        raise RuntimeError(f"TikTok init upload error: {data}")
    return data["data"]


def _upload_chunks(upload_url: str, video_path: str, chunk_size: int, total_chunks: int) -> None:
    """Upload video file in chunks to TikTok's server."""
    file_size = os.path.getsize(video_path)
    with open(video_path, "rb") as f:
        for chunk_index in range(total_chunks):
            chunk_data = f.read(chunk_size)
            start_byte = chunk_index * chunk_size
            end_byte = start_byte + len(chunk_data) - 1

            headers = {
                "Content-Type": "video/mp4",
                "Content-Range": f"bytes {start_byte}-{end_byte}/{file_size}",
                "Content-Length": str(len(chunk_data)),
            }

            resp = requests.put(upload_url, data=chunk_data, headers=headers)
            if resp.status_code not in (200, 201, 206):
                raise RuntimeError(
                    f"Chunk {chunk_index} upload failed: {resp.status_code} {resp.text}"
                )
            print(f"  Chunk {chunk_index + 1}/{total_chunks} hochgeladen")


def _publish_video(access_token: str, publish_id: str, title: str, hashtags: list[str]) -> dict:
    """Publish the uploaded video with title and hashtags."""
    # TikTok title = post caption (max 2200 chars), hashtags appended
    caption = title + " " + " ".join(hashtags)
    caption = caption[:2200]

    url = f"{TIKTOK_API_BASE}/post/publish/video/init/"
    # Note: For direct post, title is set during init. This function re-publishes with caption.
    # The actual publish happens automatically after all chunks are uploaded.
    # We return the publish_id for status polling.
    return {"publish_id": publish_id, "caption": caption}


def check_publish_status(access_token: str, publish_id: str) -> dict:
    """Poll the publish status of an uploaded video."""
    url = f"{TIKTOK_API_BASE}/post/publish/status/fetch/"
    payload = {"publish_id": publish_id}
    resp = requests.post(url, json=payload, headers=_get_headers(access_token))
    resp.raise_for_status()
    return resp.json()


def upload_video(
    access_token: str,
    video_path: str,
    title: str,
    hashtags: list[str],
    privacy: str = "SELF_ONLY",
) -> str:
    """
    Full upload flow:
    1. Init upload
    2. Upload chunks
    3. Wait for processing
    Returns the publish_id.

    privacy options: "SELF_ONLY" | "MUTUAL_FOLLOW_FRIENDS" | "FOLLOWER_OF_CREATOR" | "PUBLIC_TO_EVERYONE"
    """
    file_size = os.path.getsize(video_path)
    total_chunks = math.ceil(file_size / CHUNK_SIZE)
    caption = title + " " + " ".join(hashtags)
    caption = caption[:2200]

    print(f"Starte TikTok Upload: {os.path.basename(video_path)}")
    print(f"  Dateigröße: {file_size / 1024 / 1024:.1f} MB | Chunks: {total_chunks}")

    # 1. Init
    url = f"{TIKTOK_API_BASE}/post/publish/video/init/"
    payload = {
        "post_info": {
            "title": caption,
            "privacy_level": privacy,
            "disable_duet": False,
            "disable_comment": False,
            "disable_stitch": False,
        },
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": file_size,
            "chunk_size": CHUNK_SIZE,
            "total_chunk_count": total_chunks,
        },
    }
    resp = requests.post(url, json=payload, headers=_get_headers(access_token))
    resp.raise_for_status()
    init_data = resp.json()

    if init_data.get("error", {}).get("code") != "ok":
        raise RuntimeError(f"TikTok init error: {init_data}")

    upload_url = init_data["data"]["upload_url"]
    publish_id = init_data["data"]["publish_id"]
    print(f"  Upload initialisiert. Publish ID: {publish_id}")

    # 2. Upload chunks
    _upload_chunks(upload_url, video_path, CHUNK_SIZE, total_chunks)

    # 3. Poll status
    print("  Warte auf Verarbeitung durch TikTok...")
    for _ in range(30):
        time.sleep(5)
        status = check_publish_status(access_token, publish_id)
        status_code = status.get("data", {}).get("status", "UNKNOWN")
        print(f"  Status: {status_code}")
        if status_code in ("PUBLISH_COMPLETE", "SUCCESS"):
            print("  Video erfolgreich veröffentlicht!")
            return publish_id
        if status_code in ("FAILED", "ERROR"):
            raise RuntimeError(f"TikTok Verarbeitung fehlgeschlagen: {status}")

    print("  Timeout beim Warten auf TikTok-Verarbeitung.")
    return publish_id


if __name__ == "__main__":
    # Quick test (needs real credentials)
    token = os.environ.get("TIKTOK_ACCESS_TOKEN", "")
    if not token:
        print("TIKTOK_ACCESS_TOKEN nicht gesetzt.")
    else:
        pid = upload_video(
            access_token=token,
            video_path="/tmp/test_video.mp4",
            title="Test Upload",
            hashtags=["#test", "#bot"],
            privacy="SELF_ONLY",
        )
        print(f"Publish ID: {pid}")
