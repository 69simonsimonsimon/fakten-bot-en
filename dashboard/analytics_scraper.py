"""
TikTok Analytics Scraper
Liest echte Video-Stats von TikTok Studio und matcht sie mit lokalen Metadaten.
"""

import asyncio
import json
import re
from pathlib import Path

import os

from playwright.async_api import async_playwright

ANALYTICS_URL = "https://www.tiktok.com/tiktokstudio/content"
OUTPUT_DIR    = Path(os.environ.get("OUTPUT_DIR", str(Path(__file__).parent.parent / "output")))
CACHE_FILE    = OUTPUT_DIR / "analytics_cache.json"


def _get_chrome_cookies() -> list[dict]:
    """
    Liefert TikTok-Cookies für Playwright.
    Reihenfolge:
      1. TIKTOK_COOKIES env-Variable (Railway & lokal als Fallback)
      2. browser_cookie3 aus lokalem Chrome (nur lokal)
    """
    import os

    # 1. Env-Variable (Railway)
    raw = os.environ.get("TIKTOK_COOKIES", "").strip()
    if raw:
        try:
            cookies = json.loads(raw)
            if cookies:
                print(f"   {len(cookies)} Cookies aus TIKTOK_COOKIES env geladen")
                return cookies
        except Exception as e:
            print(f"   ⚠️  TIKTOK_COOKIES konnte nicht geparst werden: {e}")

    # 2. Lokaler Chrome via browser_cookie3
    try:
        import browser_cookie3
        jar = browser_cookie3.chrome(domain_name=".tiktok.com")
        cookies = [
            {
                "name":   c.name,
                "value":  c.value,
                "domain": c.domain if c.domain.startswith(".") else "." + c.domain,
                "path":   c.path or "/",
            }
            for c in jar
        ]
        if cookies:
            print(f"   {len(cookies)} Cookies aus lokalem Chrome geladen")
        return cookies
    except Exception:
        return []


def _load_local_meta() -> dict[str, dict]:
    """Lädt alle lokalen JSON-Metadaten, indexiert nach caption-Wörtern für Matching."""
    meta = {}
    for jf in OUTPUT_DIR.glob("*.json"):
        try:
            d = json.loads(jf.read_text(encoding="utf-8"))
            cap = d.get("caption", "")
            if cap:
                meta[cap[:50].lower()] = d
        except Exception:
            pass
    return meta


def _match_topic(video_text: str, local_meta: dict[str, dict]) -> str:
    """Findet das Thema eines TikTok-Videos anhand des Caption-Texts."""
    text_lower = video_text.lower()
    best_match, best_score = "", 0
    for key, data in local_meta.items():
        score = sum(1 for w in key.split() if w in text_lower)
        if score > best_score:
            best_score = score
            best_match = data.get("topic", "")
    return best_match or "unbekannt"


def _parse_num(text: str) -> int:
    """Parst TikTok-Zahlen: '1.2K' → 1200, '3.5M' → 3500000, '42' → 42."""
    if not text:
        return 0
    text = text.strip().replace(",", ".").replace("\u202f", "")
    try:
        if text.upper().endswith("M"):
            return int(float(text[:-1]) * 1_000_000)
        if text.upper().endswith("K"):
            return int(float(text[:-1]) * 1_000)
        return int(float(text))
    except Exception:
        return 0


async def _scroll_to_load_all(page) -> int:
    """
    Scrollt alle scrollbaren Container der Seite nach unten bis keine neuen
    Videos mehr erscheinen. Probiert window, document.body und alle großen
    overflow-Container — TikTok Studio nutzt einen eigenen Scroll-Div.
    """
    prev_count = -1
    stale_rounds = 0

    scroll_js = """() => {
        // 1. Alle scrollbaren Container auf der Seite finden
        const scrollables = [document.documentElement, document.body];
        document.querySelectorAll('*').forEach(el => {
            const st = window.getComputedStyle(el);
            const overflow = st.overflow + st.overflowY;
            if ((overflow.includes('scroll') || overflow.includes('auto')) &&
                el.scrollHeight > el.clientHeight + 50) {
                scrollables.push(el);
            }
        });

        // 2. Alle bis ganz nach unten scrollen
        scrollables.forEach(el => {
            try { el.scrollTop = el.scrollHeight; } catch(e) {}
        });

        // 3. Letztes Video-Element ins Sichtfeld scrollen
        const videos = document.querySelectorAll('a[href*="/video/"]');
        if (videos.length > 0) {
            videos[videos.length - 1].scrollIntoView({ behavior: 'instant', block: 'end' });
        }

        // 4. Auch window scrollen
        window.scrollTo(0, document.body.scrollHeight);

        return document.querySelectorAll('a[href*="/video/"]').length;
    }"""

    for round_num in range(40):   # max 40 Runden
        count = await page.evaluate(scroll_js)

        if count == prev_count:
            stale_rounds += 1
            if stale_rounds >= 4:   # 4 Runden ohne neues Video → wirklich fertig
                break
        else:
            stale_rounds = 0
            prev_count   = count
            print(f"   {count} Videos geladen…")

        # Längere Pause damit Lazy-Loading nachladen kann
        await page.wait_for_timeout(2200)

        # Ab Runde 5 auch Keyboard-Scroll versuchen (End-Taste)
        if round_num >= 5:
            try:
                await page.keyboard.press("End")
                await page.wait_for_timeout(500)
            except Exception:
                pass

    final = await page.evaluate(
        "() => document.querySelectorAll('a[href*=\"/video/\"]').length"
    )
    print(f"   Scrolling abgeschlossen — {final} Videos gefunden")
    return final


async def _scrape() -> list[dict]:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, slow_mo=150,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        page = await ctx.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        cookies = _get_chrome_cookies()
        if cookies:
            await ctx.add_cookies(cookies)

        print("   Öffne TikTok Studio Content-Seite…")
        await page.goto(ANALYTICS_URL, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(8000)

        if "login" in page.url.lower():
            print("   Bitte einloggen (Browser ist offen)…")
            for _ in range(36):
                await page.wait_for_timeout(5000)
                if "login" not in page.url.lower():
                    print("   Login erkannt.")
                    await page.wait_for_timeout(4000)
                    break
            else:
                await browser.close()
                return []

        # Alle Videos durch Scrollen laden
        await _scroll_to_load_all(page)
        # Kurz warten damit letzte Videos fertig rendern
        await page.wait_for_timeout(2000)

        # ── Daten aus dem DOM extrahieren ─────────────────────────────────────
        raw = await page.evaluate("""() => {
            function parseNum(t) {
                if (!t) return 0;
                t = t.trim().replace(/,/g, '.');
                if (/[0-9]M$/i.test(t)) return Math.round(parseFloat(t)*1e6);
                if (/[0-9]K$/i.test(t)) return Math.round(parseFloat(t)*1e3);
                const n = parseFloat(t);
                return isNaN(n) ? 0 : Math.round(n);
            }

            // Alle Zahlen aus TUXText-Elementen — auch K/M-Formate
            const nums = Array.from(document.querySelectorAll('.TUXText'))
                .map(e => e.innerText.trim())
                .filter(t => /^[\\d.,]+\\s*[KkMm]?$/.test(t))
                .map(parseNum);

            // Video-Links (dedupliziert nach href)
            const seen = new Set();
            const links = [];
            for (const a of document.querySelectorAll('a[href*="/video/"]')) {
                if (!seen.has(a.href)) {
                    seen.add(a.href);
                    links.push({ href: a.href, text: a.innerText.trim().substring(0, 120) });
                }
            }

            // Thumbnails
            const thumbs = Array.from(document.querySelectorAll('img'))
                .map(i => i.src)
                .filter(s => s && (s.includes('/video/') || s.includes('thumbnail') || s.includes('thumb')))
                .slice(0, 100);

            return { nums, links, thumbs };
        }""")

        await browser.close()

    videos    = raw["links"]
    nums      = raw["nums"]
    thumbs    = raw["thumbs"]
    local_meta = _load_local_meta()

    print(f"   DOM: {len(videos)} Video-Links, {len(nums)} Zahlen, {len(thumbs)} Thumbnails")

    # ── Stats zuordnen ────────────────────────────────────────────────────────
    # Je 3 aufeinanderfolgende Zahlen = Views, Likes, Kommentare eines Videos.
    # Falls die Zahl nicht aufgeht, alles auf 0 setzen statt falsche Werte zuzuweisen.
    stats_per_video = 3
    expected_nums   = len(videos) * stats_per_video

    if len(nums) < expected_nums:
        print(f"   ⚠️  Weniger Zahlen ({len(nums)}) als erwartet ({expected_nums}) — "
              f"möglicherweise fehlen Stats für einige Videos")

    result = []
    for i, video in enumerate(videos):
        base     = i * stats_per_video
        views    = nums[base]     if base     < len(nums) else 0
        likes    = nums[base + 1] if base + 1 < len(nums) else 0
        comments = nums[base + 2] if base + 2 < len(nums) else 0
        topic    = _match_topic(video["text"], local_meta)

        result.append({
            "title":     video["text"][:80],
            "href":      video["href"],
            "thumbnail": thumbs[i] if i < len(thumbs) else "",
            "views":     views,
            "likes":     likes,
            "comments":  comments,
            "shares":    0,
            "topic":     topic,
        })

    print(f"   {len(result)} Videos verarbeitet")

    if result:
        CACHE_FILE.write_text(
            json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    return result


def fetch_analytics() -> list[dict]:
    """Startet den Scraper. Bei Netzwerkfehler: bis zu 2 Wiederholungsversuche,
    danach Fallback auf gecachte Daten."""
    last_error = None
    for attempt in range(1, 3):
        try:
            result = asyncio.run(_scrape())
            if result:
                return result
        except Exception as e:
            last_error = e
            msg = str(e)
            if "ERR_INTERNET_DISCONNECTED" in msg or "ERR_NAME_NOT_RESOLVED" in msg or "ERR_CONNECTION" in msg:
                print(f"   ⚠️  Kein Internet (Versuch {attempt}/2) — warte 15s…")
                import time; time.sleep(15)
            else:
                raise  # Andere Fehler sofort weitergeben

    # Fallback: gecachte Daten zurückgeben
    cached = load_cached()
    if cached:
        print(f"   ⚠️  Kein Internet — zeige gecachte Daten ({len(cached)} Videos)")
        return cached

    raise RuntimeError(f"Analytics nicht verfügbar (kein Internet, kein Cache): {last_error}")


def load_cached() -> list[dict]:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []
