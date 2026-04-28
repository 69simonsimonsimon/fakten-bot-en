"""
TikTok Browser Uploader
-----------------------
Nutzt Playwright + Chrome-Cookies für automatischen Upload zu TikTok Studio.
"""

import asyncio
import json
from pathlib import Path

from playwright.async_api import async_playwright

UPLOAD_URL = "https://www.tiktok.com/tiktokstudio/upload"

# Erfolgs-Indikatoren (URL-Fragmente oder Seiten-Texte nach dem Post)
SUCCESS_URLS    = ["content", "manage", "post_success"]
SUCCESS_TEXTS   = ["veröffentlicht", "your video is now live", "video posted",
                   "posted successfully", "ist jetzt live", "erfolgreich"]


def _get_chrome_cookies() -> list[dict]:
    # Load cookies saved by save_cookies.py (for @syncinUS)
    cookie_file = Path(__file__).parent.parent / "tiktok_cookies.json"
    if cookie_file.exists():
        try:
            cookies = json.loads(cookie_file.read_text(encoding="utf-8"))
            print(f"   {len(cookies)} @syncinUS cookies loaded from {cookie_file.name}")
            return cookies
        except Exception as e:
            print(f"   Could not load cookies from file: {e}")
    print("   No cookies found — run save_cookies.py first to log in as @syncinUS")
    return []


async def _scroll_to_top(page):
    """Scrollt die Seite UND alle inneren Container nach oben (Caption ist ganz oben)."""
    await page.evaluate("""() => {
        // Hauptseite
        window.scrollTo(0, 0);
        document.documentElement.scrollTop = 0;
        document.body.scrollTop = 0;
        // Alle scrollbaren inneren Container
        const selectors = [
            '[class*="scroll"]', '[class*="container"]', '[class*="form"]',
            '[class*="panel"]', '[class*="editor"]', '[class*="upload"]',
            '[class*="right"]', '[class*="left"]', '[class*="content"]'
        ];
        selectors.forEach(sel => {
            document.querySelectorAll(sel).forEach(el => {
                try {
                    if (el.scrollHeight > el.clientHeight + 50) {
                        el.scrollTop = 0;
                    }
                } catch(e) {}
            });
        });
    }""")
    await page.wait_for_timeout(600)


async def _dismiss_overlays(page):
    """
    Entfernt TikTok Tutorial-Overlays (react-joyride) die alle Klicks blockieren.
    Diese tauchen NACH dem Video-Upload auf und müssen vor Caption/Sound entfernt werden.
    """
    # Methode 1: 'Verstanden' / 'Got it' Buttons klicken
    for popup_text in ["Verstanden", "Got it", "OK", "Skip", "Weiter", "Next",
                       "Schließen", "Close", "Überspringen", "Later"]:
        try:
            btn = page.locator(f"button:has-text('{popup_text}')")
            if await btn.count() > 0 and await btn.first.is_visible():
                await btn.first.click(force=True)
                print(f"   Popup geschlossen: '{popup_text}'")
                await page.wait_for_timeout(500)
        except Exception:
            pass

    # Methode 2: react-joyride Portal + Overlay via JS entfernen
    removed = await page.evaluate("""() => {
        const removed = [];
        [
            '#react-joyride-portal',
            '[data-test-id="overlay"]',
            '.react-joyride__overlay',
            '.react-joyride__spotlight',
            '[class*="joyride"]',
        ].forEach(sel => {
            document.querySelectorAll(sel).forEach(el => {
                el.remove();
                removed.push(sel);
            });
        });
        return removed;
    }""")
    if removed:
        print(f"   Tutorial-Overlay entfernt via JS: {list(set(removed))}")
        await page.wait_for_timeout(300)

    # Methode 3: Escape (schließt viele Modals/Dialoge)
    await page.keyboard.press("Escape")
    await page.wait_for_timeout(400)


async def _fill_caption(page, caption: str) -> bool:
    """
    Füllt das TikTok-Beschreibungsfeld.
    Scrollt zuerst nach oben (Caption-Feld ist ganz oben),
    dann: Clipboard-Paste (mit erteilter Permission) → keyboard.type → execCommand.
    """
    if not caption:
        print("   Caption ist leer — überspringe")
        return False

    clean = caption[:500]

    # ── Seite nach oben scrollen (Caption-Feld ist ganz oben) ───────────────
    await _scroll_to_top(page)
    await page.wait_for_timeout(800)

    # ── Diagnostik-Screenshot ────────────────────────────────────────────────
    try:
        await page.screenshot(path="/tmp/tiktok_before_caption.png")
        print("   Screenshot: /tmp/tiktok_before_caption.png")
    except Exception:
        pass

    # ── Warte auf DraftEditor (bis zu 10s) ──────────────────────────────────
    try:
        await page.wait_for_selector(".public-DraftEditor-content", timeout=10_000)
    except Exception:
        print("   Warte auf DraftEditor abgelaufen — versuche trotzdem...")

    # ── Feld finden ──────────────────────────────────────────────────────────
    selectors = [
        "[data-e2e='caption_container'] .public-DraftEditor-content",
        "[data-e2e='caption-container'] .public-DraftEditor-content",
        ".caption-wrapper .public-DraftEditor-content",
        ".public-DraftEditor-content",
        "div[contenteditable='true'][class*='editor']",
        "div[contenteditable='true'][class*='caption']",
    ]
    field = None
    for sel in selectors:
        try:
            loc = page.locator(sel).first
            cnt = await loc.count()
            if cnt > 0:
                # Sichtbarkeit nicht zwingend prüfen — scroll_into_view macht es
                field = loc
                print(f"   Caption-Feld gefunden: {sel}")
                break
        except Exception:
            continue

    # Fallback: erstes contenteditable
    if not field:
        try:
            all_ce = page.locator("div[contenteditable='true']")
            for i in range(await all_ce.count()):
                loc = all_ce.nth(i)
                field = loc
                print(f"   Caption-Feld (Fallback): contenteditable #{i}")
                break
        except Exception:
            pass

    if not field:
        print("   ❌ Beschreibungsfeld nicht gefunden — Screenshot: /tmp/tiktok_no_caption_field.png")
        try:
            await page.screenshot(path="/tmp/tiktok_no_caption_field.png")
        except Exception:
            pass
        return False

    try:
        # Overlays nochmal entfernen (sicherheitshalber direkt vor dem Klick)
        await _dismiss_overlays(page)

        # Via JS ins Blickfeld scrollen und fokussieren (umgeht Pointer-Event-Blocker)
        await page.evaluate("""() => {
            const el = document.querySelector("[data-e2e='caption_container'] .public-DraftEditor-content")
                    || document.querySelector(".public-DraftEditor-content")
                    || document.querySelector("div[contenteditable='true']");
            if (el) {
                el.scrollIntoView({ behavior: 'instant', block: 'center' });
                el.focus();
            }
        }""")
        await page.wait_for_timeout(600)

        # Zusätzlich normalen Klick versuchen (mit force=True falls nötig)
        try:
            await field.click(timeout=5000)
        except Exception:
            try:
                await field.click(force=True, timeout=3000)
                print("   Feld mit force=True geklickt")
            except Exception as e:
                print(f"   Klick fehlgeschlagen (weiter mit JS-Focus): {e}")
        await page.wait_for_timeout(400)

        # ── Feld leeren ──────────────────────────────────────────────────────
        await page.keyboard.press("Meta+a")
        await page.wait_for_timeout(150)
        await page.keyboard.press("Delete")
        await page.wait_for_timeout(200)

        # ── Methode 1: Clipboard-Paste (Playwright-Permission erteilt) ────────
        try:
            # Permission wurde beim Context-Start erteilt — sollte klappen
            await page.evaluate(f"async () => {{ await navigator.clipboard.writeText({json.dumps(clean)}); }}")
            await field.click()
            await page.wait_for_timeout(300)
            await page.keyboard.press("Meta+a")
            await page.keyboard.press("Delete")
            await page.wait_for_timeout(150)
            await page.keyboard.press("Meta+v")
            await page.wait_for_timeout(1000)
            filled = (await field.inner_text()).strip()
            if filled and len(filled) > 5:
                print(f"   ✓ Caption via Clipboard-Paste ({len(filled)} Zeichen)")
                return True
            print(f"   Clipboard-Paste: Feld danach leer/kurz ('{filled[:30]}')")
        except Exception as e:
            print(f"   Clipboard-Paste fehlgeschlagen: {e}")

        # ── Methode 2: execCommand insertText (Draft.js-kompatibel) ──────────
        try:
            await field.click()
            await page.wait_for_timeout(300)
            result = await page.evaluate(f"""() => {{
                const el = document.querySelector("[data-e2e='caption_container'] .public-DraftEditor-content")
                        || document.querySelector(".public-DraftEditor-content")
                        || document.querySelector("div[contenteditable='true']");
                if (!el) return 'no_element';
                el.focus();
                // Alles selektieren und löschen
                document.execCommand('selectAll', false, null);
                document.execCommand('delete', false, null);
                // Text einfügen
                const ok = document.execCommand('insertText', false, {json.dumps(clean)});
                return ok ? 'ok' : 'execCommand_false';
            }}""")
            await page.wait_for_timeout(800)
            filled = (await field.inner_text()).strip()
            if filled and len(filled) > 5:
                print(f"   ✓ Caption via execCommand ({len(filled)} Zeichen, result={result})")
                return True
            print(f"   execCommand result={result}, Feld: '{filled[:30]}'")
        except Exception as e:
            print(f"   execCommand fehlgeschlagen: {e}")

        # ── Methode 3: keyboard.type (langsam aber sicher) ───────────────────
        try:
            await field.click()
            await page.keyboard.press("Meta+a")
            await page.keyboard.press("Delete")
            await page.wait_for_timeout(200)
            # Kürzere Version für keyboard.type (Hashtags weglassen)
            short = clean[:200]
            await page.keyboard.type(short, delay=20)
            await page.wait_for_timeout(800)
            filled = (await field.inner_text()).strip()
            if filled and len(filled) > 5:
                print(f"   ✓ Caption via keyboard.type ({len(filled)} Zeichen, gekürzt)")
                return True
        except Exception as e:
            print(f"   keyboard.type fehlgeschlagen: {e}")

        print("   ❌ Alle Caption-Methoden fehlgeschlagen")
        await page.screenshot(path="/tmp/tiktok_caption_fail.png")
        return False

    except Exception as e:
        print(f"   Fehler beim Eintragen der Beschreibung: {e}")
        try:
            await page.screenshot(path="/tmp/tiktok_caption_error.png")
        except Exception:
            pass
        return False


async def _wait_for_post_ready(page) -> object | None:
    """
    Findet den Post-Button und wartet bis er klickbar ist (aria-disabled != 'true').
    Gibt den Locator zurück oder None bei Timeout.
    """
    selectors = [
        "[data-e2e=post_video_button]",
        "button:has-text('Veröffentlichen')",
        "button:has-text('Post')",
        "button:has-text('Posten')",
        "button:has-text('Publish')",
    ]

    btn = None
    for sel in selectors:
        loc = page.locator(sel).first
        if await loc.count() > 0:
            btn = loc
            print(f"   Post-Button gefunden mit: {sel}")
            break

    if not btn:
        print("   Post-Button nicht gefunden.")
        return None

    print("   Warte bis Video verarbeitet ist (max. 4 Minuten)...")
    for i in range(120):   # 120 × 2 s = 4 Minuten
        aria = await btn.get_attribute("aria-disabled")
        disabled_attr = await btn.get_attribute("disabled")
        if aria != "true" and disabled_attr is None:
            print(f"   Video bereit nach {i*2}s.")
            return btn
        if i % 15 == 0 and i > 0:
            print(f"   Noch nicht bereit... ({i*2}s)")
        await page.wait_for_timeout(2000)

    print("   Timeout: Button bleibt gesperrt.")
    return None


async def _check_success(page, url_before: str) -> bool:
    """Prüft ob der Post erfolgreich war (URL-Wechsel oder Erfolgstext)."""
    cur_url = page.url.lower()
    # URL-Wechsel — aber nur wenn es kein Upload-URL mehr ist
    if cur_url != url_before.lower() and "upload" not in cur_url:
        return True
    if any(s in cur_url for s in SUCCESS_URLS):
        return True
    try:
        page_text = (await page.evaluate("() => document.body.innerText")).lower()
        if any(s in page_text for s in SUCCESS_TEXTS):
            return True
    except Exception:
        pass
    return False


async def _add_and_mute_sound(page) -> bool:
    """
    Öffnet den TikTok Sound-Editor (rechtes Panel → 'Sounds' Tab),
    wählt einen Sound aus und stellt ihn auf Volume 0.
    Basierend auf dem TikTok Studio Layout: rechtes Panel mit Tabs
    'Bearbeiten | Sounds | Text'.
    """
    import random

    try:
        # ── 0. Diagnose-Screenshot zu Beginn ────────────────────────────────
        await page.screenshot(path="/tmp/tiktok_before_sound.png")
        print("   Screenshot: /tmp/tiktok_before_sound.png")

        # ── 1. 'Sounds' Tab im rechten Panel klicken ─────────────────────────
        # TikTok Studio zeigt rechts: Bearbeiten | Sounds | Text
        sounds_tab_selectors = [
            # Exakte Tab-Texte (DE + EN)
            "button:has-text('Sounds')",
            "[role='tab']:has-text('Sounds')",
            "div[role='tab']:has-text('Sounds')",
            # Klassen-basiert
            "[class*='tab']:has-text('Sounds')",
            "[class*='SoundTab']",
            "[class*='sound-tab']",
            # Icon + Text
            "div[class*='right'] button:has-text('Sounds')",
            "div[class*='editor'] button:has-text('Sounds')",
            # Musik-Icon-Buttons
            "[data-e2e='music-tab']",
            "[data-e2e='sounds-tab']",
            "[data-e2e='add-music']",
            "[data-e2e='music-icon']",
        ]

        sounds_tab = None
        for sel in sounds_tab_selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible():
                    sounds_tab = loc
                    print(f"   Sounds-Tab gefunden: {sel}")
                    break
            except Exception:
                continue

        if not sounds_tab:
            # JS-Fallback: alle Tabs durchsuchen
            found = await page.evaluate("""() => {
                const tabs = Array.from(document.querySelectorAll('[role="tab"], button, div[class*="tab"]'));
                const soundTab = tabs.find(el =>
                    el.textContent.trim().toLowerCase() === 'sounds' ||
                    el.textContent.trim().toLowerCase() === 'sound' ||
                    el.textContent.trim().toLowerCase() === 'musik'
                );
                if (soundTab) { soundTab.click(); return soundTab.textContent.trim(); }
                return null;
            }""")
            if found:
                print(f"   Sounds-Tab via JS gefunden: '{found}'")
                await page.wait_for_timeout(1500)
                sounds_tab = True  # Dummy für den Check unten
            else:
                await page.screenshot(path="/tmp/tiktok_no_sound_btn.png")
                print("   Sounds-Tab nicht gefunden — Screenshot: /tmp/tiktok_no_sound_btn.png")
                print("   Sound-Feature übersprungen")
                return False

        if sounds_tab is not True:
            await sounds_tab.click()
            await page.wait_for_timeout(2000)

        # ── 1b. Navigation-Dialog abfangen (Sounds öffnet manchmal externen Editor) ─
        for stay_text in ["Abbrechen", "Cancel", "Stay", "Bleiben", "Nein"]:
            try:
                b = page.locator(f"button:has-text('{stay_text}')").first
                if await b.count() > 0 and await b.is_visible():
                    await b.click()
                    print(f"   Navigation-Dialog abgebrochen: '{stay_text}'")
                    await page.wait_for_timeout(500)
                    break
            except Exception:
                pass

        # ── 2. Sound-Liste Screenshot (nach Tab-Klick) ────────────────────────
        await page.screenshot(path="/tmp/tiktok_sounds_panel.png")
        print("   Screenshot: /tmp/tiktok_sounds_panel.png")

        # ── 3. Trending-Tab oder ersten Sound auswählen ───────────────────────
        for tab_text in ["Trending", "Beliebt", "Popular", "Top", "Charts", "Empfohlen"]:
            try:
                tab = page.locator(
                    f"[role='tab']:has-text('{tab_text}'), "
                    f"button:has-text('{tab_text}'), "
                    f"div[class*='tab']:has-text('{tab_text}')"
                ).first
                if await tab.count() > 0 and await tab.is_visible():
                    await tab.click()
                    await page.wait_for_timeout(1200)
                    print(f"   Trending-Tab: '{tab_text}'")
                    break
            except Exception:
                continue

        # ── 4. Ersten verfügbaren Sound auswählen ────────────────────────────
        sound_item_selectors = [
            "[data-e2e='sound-item']",
            "[data-e2e='music-item']",
            "[data-e2e='sound-card']",
            "[class*='soundItem']",
            "[class*='sound-item']",
            "[class*='musicItem']",
            "[class*='music-item']",
            "[class*='SoundCard']",
            "[class*='AudioItem']",
            "li[class*='sound']",
            "li[class*='music']",
        ]
        selected = False
        for sel in sound_item_selectors:
            try:
                items = page.locator(sel)
                count = await items.count()
                if count > 0:
                    pick = random.randint(0, min(4, count - 1))
                    await items.nth(pick).click()
                    await page.wait_for_timeout(1200)
                    print(f"   Sound ausgewählt: #{pick + 1} von {count} (Selektor: {sel})")
                    selected = True
                    break
            except Exception:
                continue

        if not selected:
            clicked = await page.evaluate("""() => {
                const sels = ['[class*=sound]', '[class*=music]', '[class*=audio]'];
                for (const s of sels) {
                    const items = document.querySelectorAll(
                        `${s} li, ${s} [role=listitem], ${s} [class*=item], ${s} [class*=card]`
                    );
                    if (items.length > 0) {
                        const pick = Math.floor(Math.random() * Math.min(5, items.length));
                        items[pick].click();
                        return `clicked item ${pick} of ${items.length}`;
                    }
                }
                return null;
            }""")
            if clicked:
                print(f"   Sound via JS: {clicked}")
                await page.wait_for_timeout(1200)
                selected = True

        if not selected:
            print("   Kein Sound-Item gefunden — überspringe")
            return False

        # ── 5. "Verwenden / Use / Confirm" Button ────────────────────────────
        for use_text in ["Use", "Verwenden", "Select", "Auswählen", "Hinzufügen",
                         "Add", "Confirm", "OK", "Bestätigen"]:
            try:
                btn = page.locator(f"button:has-text('{use_text}')").first
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.click()
                    await page.wait_for_timeout(1000)
                    print(f"   '{use_text}' geklickt")
                    break
            except Exception:
                continue

        # ── 6. Volume auf 0 setzen ────────────────────────────────────────────
        await page.wait_for_timeout(1500)
        volume_set = await page.evaluate("""() => {
            const sliders = Array.from(document.querySelectorAll('input[type="range"]'));
            if (!sliders.length) return null;
            // Letzten Slider nehmen (normalerweise der Sound-Regler)
            const target = sliders[sliders.length - 1];
            const old = target.value;
            const setter = Object.getOwnPropertyDescriptor(
                window.HTMLInputElement.prototype, 'value'
            )?.set;
            if (setter) setter.call(target, '0');
            else target.value = '0';
            target.dispatchEvent(new InputEvent('input',  { bubbles: true }));
            target.dispatchEvent(new Event('change',      { bubbles: true }));
            return { from: old, to: target.value, total_sliders: sliders.length };
        }""")

        if volume_set:
            print(f"   Sound-Volume: {volume_set.get('from','?')} → 0 (stumm, {volume_set.get('total_sliders',0)} Slider)")
        else:
            print("   Volume-Slider nicht gefunden — Sound evtl. hörbar")

        await page.wait_for_timeout(800)
        print("   ✓ Sound hinzugefügt und stumm gestellt")
        return True

    except Exception as e:
        print(f"   Sound-Feature Fehler (übersprungen): {e}")
        try:
            await page.screenshot(path="/tmp/tiktok_sound_error.png")
        except Exception:
            pass
        return False


async def _do_upload(video_path: str, caption: str) -> bool:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=150,
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            permissions=["clipboard-read", "clipboard-write"],
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

        # ── 1. TikTok Studio öffnen ───────────────────────────────────────────
        print("   Öffne TikTok Studio...")
        await page.goto(UPLOAD_URL, wait_until="domcontentloaded", timeout=30_000)
        await page.wait_for_timeout(7_000)

        if "login" in page.url.lower():
            print("   Bitte manuell einloggen (Browser ist offen)...")
            for _ in range(36):
                await page.wait_for_timeout(5000)
                if "login" not in page.url.lower():
                    print("   Login erkannt.")
                    await page.wait_for_timeout(5000)
                    break
            else:
                print("   Login-Timeout.")
                await browser.close()
                return False

        # ── 2. Video hochladen ────────────────────────────────────────────────
        file_input = page.locator("input[type=file]")
        if await file_input.count() == 0:
            print("   Upload-Feld nicht gefunden.")
            await browser.close()
            return False

        print("   Lade Videodatei hoch...")
        await file_input.set_input_files(video_path)
        await page.wait_for_timeout(4_000)

        # Popups/Overlays sofort nach Upload wegklicken
        await _dismiss_overlays(page)

        # ── 3. Warten bis Post-Button aktiv ──────────────────────────────────
        print("   Warte bis Video verarbeitet...")
        post_btn = None
        for i in range(120):
            for sel in ["[data-e2e=post_video_button]",
                        "button:has-text('Veröffentlichen')",
                        "button:has-text('Post')",
                        "button:has-text('Publish')"]:
                loc = page.locator(sel).first
                if await loc.count() > 0:
                    aria = await loc.get_attribute("aria-disabled")
                    if aria != "true":
                        post_btn = loc
                        print(f"   Video bereit nach {i*2}s!")
                        break
            if post_btn:
                break
            # Während Warten auch Popups wegklicken
            await _dismiss_overlays(page)
            await page.wait_for_timeout(2000)

        if not post_btn:
            print("   Post-Button nie gefunden — Abbruch.")
            await browser.close()
            return False

        await page.wait_for_timeout(1500)

        # ── 4. Alle Overlays entfernen (react-joyride blockiert Klicks!) ──────
        print("   Entferne Tutorial-Overlays...")
        await _dismiss_overlays(page)
        await _dismiss_overlays(page)  # Zweimal — manchmal mehrere Schichten

        # ── 5. Zum Caption-Feld scrollen ──────────────────────────────────────
        await _scroll_to_top(page)

        # ── 6. Caption einfügen ───────────────────────────────────────────────
        print(f"   Caption ({len(caption)} Zeichen): {caption[:80]}{'…' if len(caption) > 80 else ''}")
        caption_ok = await _fill_caption(page, caption)
        if not caption_ok:
            print("   ⚠️  Caption konnte nicht eingetragen werden!")
        await page.wait_for_timeout(1000)

        # ── 7. Sound hinzufügen & stumm stellen ──────────────────────────────
        await _add_and_mute_sound(page)
        await page.wait_for_timeout(800)

        # ── 8. Post-Button klicken ────────────────────────────────────────────
        # Nochmal sicherstellen dass Button aktiv ist
        aria = await post_btn.get_attribute("aria-disabled")
        if aria == "true":
            print("   Button gesperrt — warte...")
            for _ in range(30):
                await page.wait_for_timeout(2000)
                aria = await post_btn.get_attribute("aria-disabled")
                if aria != "true":
                    break
            else:
                print("   Button bleibt gesperrt — Abbruch.")
                await browser.close()
                return False

        await post_btn.scroll_into_view_if_needed()
        await page.wait_for_timeout(500)
        url_before = page.url
        print("   Klicke 'Veröffentlichen'...")
        await post_btn.click()

        # ── 9. Bestätigungsdialog abfangen ────────────────────────────────────
        await page.wait_for_timeout(3000)
        for confirm_text in ["Jetzt veröffentlichen", "Publish anyway", "Post anyway",
                             "Trotzdem veröffentlichen", "Continue"]:
            try:
                btn = page.locator(f"button:has-text('{confirm_text}')")
                if await btn.count() > 0 and await btn.first.is_visible():
                    print(f"   Dialog → '{confirm_text}'")
                    await btn.first.click()
                    await page.wait_for_timeout(2000)
                    break
            except Exception:
                pass

        # ── 10. Auf Erfolg warten ─────────────────────────────────────────────
        print("   Warte auf Bestätigung...")
        for i in range(90):
            await page.wait_for_timeout(1000)
            cur = page.url.lower()
            if cur != url_before.lower() and "upload" not in cur:
                print(f"   ✓ Erfolgreich veröffentlicht! URL: {page.url[:70]}")
                await page.wait_for_timeout(3000)
                await browser.close()
                return True

            if i in (5, 15, 30):
                for confirm_text in ["Jetzt veröffentlichen", "Publish anyway", "Post anyway"]:
                    try:
                        btn = page.locator(f"button:has-text('{confirm_text}')")
                        if await btn.count() > 0 and await btn.first.is_visible():
                            print(f"   Späten Dialog ({i}s) → '{confirm_text}'")
                            await btn.first.click()
                            await page.wait_for_timeout(2000)
                            break
                    except Exception:
                        pass

        await page.screenshot(path="/tmp/tiktok_post_fail.png", full_page=False)
        print("   Kein Erfolg nach 90s — Screenshot: /tmp/tiktok_post_fail.png")
        await page.wait_for_timeout(3000)
        await browser.close()
        return False


def upload_video_browser(video_path: str, caption: str) -> bool:
    """Öffnet TikTok Studio im Browser und lädt das Video automatisch hoch."""
    return asyncio.run(_do_upload(video_path, caption))
