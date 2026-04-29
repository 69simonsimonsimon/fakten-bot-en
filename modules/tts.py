import asyncio
import os
import re
import tempfile

import edge_tts
import requests as _requests

# ElevenLabs: Rachel — klar, energetisch, gut für englische Facts
_EL_VOICE_ID = os.environ.get("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
_EL_MODEL    = "eleven_multilingual_v2"

OPENAI_VOICE = "onyx"
OPENAI_MODEL = "tts-1"


# ── ElevenLabs TTS ────────────────────────────────────────────────────────────

def _tts_elevenlabs(text: str, audio_path: str, api_key: str) -> list[dict]:
    url  = f"https://api.elevenlabs.io/v1/text-to-speech/{_EL_VOICE_ID}"
    resp = _requests.post(
        url,
        headers={"xi-api-key": api_key, "Content-Type": "application/json"},
        json={
            "text":     text,
            "model_id": _EL_MODEL,
            "voice_settings": {"stability": 0.45, "similarity_boost": 0.80, "style": 0.30, "use_speaker_boost": True},
        },
        timeout=120,
    )
    resp.raise_for_status()
    with open(audio_path, "wb") as f:
        f.write(resp.content)
    from openai import OpenAI
    wc = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    with open(audio_path, "rb") as f:
        tr = wc.audio.transcriptions.create(model="whisper-1", file=f, response_format="verbose_json", timestamp_granularities=["word"])
    return [{"word": w.word.strip(), "start": w.start, "end": w.end} for w in (tr.words or [])]


# ── OpenAI TTS + Whisper word timings ────────────────────────────────────────

def _tts_openai(text: str, audio_path: str, api_key: str) -> list[dict]:
    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    # 1. Generate audio
    response = client.audio.speech.create(
        model=OPENAI_MODEL,
        voice=OPENAI_VOICE,
        input=text,
    )
    with open(audio_path, "wb") as f:
        f.write(response.content)

    # 2. Whisper transcription with word timestamps
    with open(audio_path, "rb") as f:
        transcript = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            response_format="verbose_json",
            timestamp_granularities=["word"],
        )

    word_timings = []
    for w in (transcript.words or []):
        word_timings.append({"word": w.word.strip(), "start": w.start, "end": w.end})

    return word_timings


# ── Edge TTS Fallback ─────────────────────────────────────────────────────────

async def _tts_edge_async(text: str, audio_path: str) -> list[dict]:
    communicate  = edge_tts.Communicate(text, "en-US-GuyNeural", boundary="WordBoundary")
    word_timings = []
    with open(audio_path, "wb") as f:
        async for chunk in communicate.stream():
            if chunk["type"] == "audio":
                f.write(chunk["data"])
            elif chunk["type"] == "WordBoundary":
                start = chunk["offset"] / 1e7
                dur   = chunk["duration"] / 1e7
                word_timings.append({"word": chunk["text"], "start": start, "end": start + dur})
    return word_timings


# ── Public API ────────────────────────────────────────────────────────────────

def text_to_speech(text: str, output_path: str, topic: str = "") -> tuple[str, list[dict]]:
    """
    ElevenLabs (primary) → OpenAI TTS → Edge TTS fallback.
    """
    el_key     = os.environ.get("ELEVENLABS_API_KEY", "").strip()
    openai_key = os.environ.get("OPENAI_API_KEY", "").strip()

    if openai_key:
        try:
            print(f"   OpenAI TTS [{OPENAI_VOICE}] ...")
            timings = _tts_openai(text, output_path, openai_key)
            return output_path, timings
        except Exception as e:
            print(f"   OpenAI TTS error: {e} — falling back to Edge TTS")

    print("   Edge TTS fallback: en-US-GuyNeural")
    timings = asyncio.run(_tts_edge_async(text, output_path))
    return output_path, timings


def get_sentence_timings(fact_text: str, word_timings: list[dict]) -> list[tuple]:
    sentences = re.split(r'(?<=[.!?])\s+', fact_text.strip())
    if not word_timings:
        return [(s, i * 3.0, (i + 1) * 3.0) for i, s in enumerate(sentences)]
    result, word_idx = [], 0
    for sentence in sentences:
        n = len(re.findall(r'\w+', sentence))
        si = min(word_idx, len(word_timings) - 1)
        ei = min(word_idx + n - 1, len(word_timings) - 1)
        result.append((sentence, max(0, word_timings[si]["start"] - 0.1), word_timings[ei]["end"] + 0.2))
        word_idx += n
    return result
