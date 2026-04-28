import anthropic
import json
import os
import re
import threading
from pathlib import Path

_generation_lock = threading.Lock()

_CLAUDE_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-5")


def _llm_call(prompt: str, max_tokens: int = 800) -> str:
    """Call Anthropic Claude — falls back to OpenAI GPT-4o-mini if credits exhausted."""
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if anthropic_key:
        try:
            client = anthropic.Anthropic(api_key=anthropic_key)
            msg = client.messages.create(
                model=_CLAUDE_MODEL,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            return msg.content[0].text.strip()
        except anthropic.BadRequestError as e:
            if "credit balance" in str(e).lower():
                import logging
                logging.getLogger("faktbot").warning("[llm] Anthropic credits exhausted — OpenAI fallback")
            else:
                raise

    import openai
    oai_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not oai_key:
        raise RuntimeError("Neither Anthropic nor OpenAI API key available")
    oai = openai.OpenAI(api_key=oai_key)
    resp = oai.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return resp.choices[0].message.content.strip()

# ── Hashtag pools ─────────────────────────────────────────────────────────────

_HASHTAG_CORE = ["#fyp", "#foryou", "#facts"]

_TOPIC_HASHTAGS: dict[str, list[str]] = {
    "science":     ["#science", "#sciencefacts", "#experiment", "#discovery",
                    "#biology", "#chemistry", "#physics", "#stem"],
    "history":     ["#history", "#historyfacts", "#historical", "#ancienthistory",
                    "#archaeology", "#historyoftheworld", "#didyouknow"],
    "space":       ["#space", "#nasa", "#universe", "#planets", "#astronaut",
                    "#astronomy", "#cosmos", "#spacefacts"],
    "technology":  ["#technology", "#tech", "#innovation", "#future", "#ai",
                    "#digital", "#artificialintelligence", "#gadgets"],
    "animals":     ["#animals", "#wildlife", "#nature", "#animalfacts",
                    "#animallover", "#wildanimals", "#animalworld"],
    "psychology":  ["#psychology", "#mentalhealth", "#brain", "#mind",
                    "#psychologyfacts", "#behavior", "#consciousness"],
    "food":        ["#food", "#foodfacts", "#cooking", "#kitchen",
                    "#nutrition", "#foodie", "#cuisine"],
    "geography":   ["#geography", "#world", "#countries", "#travel",
                    "#earthfacts", "#continents", "#worldfacts"],
    "human body":  ["#humanbody", "#health", "#medicine", "#anatomy",
                    "#bodyfacts", "#science", "#biology"],
    "pop culture": ["#popculture", "#trending", "#entertainment", "#culture",
                    "#music", "#movies", "#viral"],
    "nature":      ["#nature", "#earth", "#environment", "#naturalwonders",
                    "#naturefacts", "#ecology", "#outdoors"],
}

_HASHTAG_REACH = [
    "#viral", "#explore", "#foryoupage", "#foryou", "#learnontiktok",
    "#funfact", "#didyouknow", "#interesting", "#mindblown",
    "#amazing", "#knowledge", "#educational", "#factsdaily",
    "#learn", "#mindblowing", "#wow", "#crazy", "#mustwatch", "#tiktokeducation",
]


def _get_base_hashtags(topic: str = "") -> list[str]:
    import random
    t = topic.lower().strip()

    topic_pool = _TOPIC_HASHTAGS.get(t, [])
    if not topic_pool:
        topic_pool = ["#facts", "#education", "#learning", "#knowledge",
                      "#didyouknow", "#factsdaily"]
    random.shuffle(topic_pool)
    topic_tags = topic_pool[:3]

    reach_pool = [r for r in _HASHTAG_REACH if r not in _HASHTAG_CORE and r not in topic_tags]
    random.shuffle(reach_pool)
    reach_tags = reach_pool[:2]

    return ["#fyp", "#facts", "#didyouknow"] + topic_tags + reach_tags


_OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", str(Path(__file__).parent.parent / "output")))
HISTORY_FILE = _OUTPUT_DIR / "fact_history.json"


def _load_history() -> list[dict]:
    entries: dict[str, str] = {}

    if HISTORY_FILE.exists():
        try:
            raw = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
            for item in raw:
                if isinstance(item, str):
                    if item.strip():
                        entries.setdefault(item.strip(), "")
                elif isinstance(item, dict):
                    t = item.get("title", "").strip()
                    if t:
                        entries[t] = item.get("summary", "")
        except Exception:
            pass

    for jf in _OUTPUT_DIR.glob("*.json"):
        if jf.name == "fact_history.json":
            continue
        try:
            d = json.loads(jf.read_text(encoding="utf-8"))
            if not d.get("uploaded", False):
                continue
            title = d.get("title", "").strip()
            if title and not title.startswith("video_") and len(title) >= 8:
                entries.setdefault(title, "")
        except Exception:
            pass

    return [{"title": t, "summary": s} for t, s in entries.items() if t]


def _save_to_history(title: str, summary: str = ""):
    try:
        existing = {e["title"]: e["summary"] for e in _load_history()}
        existing[title] = summary[:150] if summary else ""
        history = [{"title": t, "summary": s} for t, s in existing.items()]
        HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        HISTORY_FILE.write_text(
            json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    except Exception as e:
        print(f"   Warning: could not save history: {e}")


_STOPWORDS = {
    "have", "been", "were", "that", "this", "with", "from", "they", "will",
    "would", "could", "should", "their", "there", "about", "which", "when",
    "more", "than", "some", "also", "into", "just", "like", "over", "even",
    "most", "only", "such", "very", "much", "many", "each", "does", "both",
    "after", "before", "while", "since", "until", "than", "then", "than",
    "because", "through", "between", "during", "without", "within", "around",
    "these", "those", "other", "every", "make", "made", "time", "year",
}


def _keywords(text: str) -> set[str]:
    words = re.findall(r'\b[a-z]{4,}\b', text.lower())
    return {w for w in words if w not in _STOPWORDS}


def _is_too_similar(title: str, fact: str, history: list[dict]) -> tuple[bool, str]:
    new_kw = _keywords(title + " " + fact[:300])
    if not new_kw:
        return False, ""

    for entry in history:
        hist_text = entry["title"] + " " + entry.get("summary", "")
        hist_kw   = _keywords(hist_text)
        if not hist_kw:
            continue
        overlap = len(new_kw & hist_kw) / min(len(new_kw), len(hist_kw))
        if overlap >= 0.45:
            return True, entry["title"]

    return False, ""


def generate_fact(topic: str = "general", long: bool = False) -> dict:
    with _generation_lock:
        return _generate_fact_locked(topic=topic, long=long)


def _generate_fact_locked(topic: str = "general", long: bool = False) -> dict:

    fact_length = (
        "Explain the fact across 7-9 engaging sentences (English). "
        "Cover the background, give examples, use specific numbers, and end with a surprising insight. "
        "IMPORTANT: Exactly 130-145 words — no more, no less. The last sentence must be complete!"
        if long else
        "Explain the fact in 2-3 punchy sentences (English). Surprising and educational."
    )

    used = _load_history()
    avoid_block = ""
    if used:
        avoid_lines = []
        for e in used:
            line = f"- {e['title']}"
            if e.get("summary"):
                line += f"  →  {e['summary'][:100]}"
            avoid_lines.append(line)
        avoid_list = "\n".join(avoid_lines)
        avoid_block = f"""
IMPORTANT – These facts have already been used. Neither the exact title nor any fact covering the same core idea may appear again:
{avoid_list}

These topic areas are therefore LOCKED (no fact about the same key claim, regardless of how it's framed).
Choose a completely different, surprising topic!
"""

    if topic.lower() in ("pop culture", "popculture", "pop-culture"):
        from datetime import date
        today = date.today().strftime("%B %Y")
        prompt = f"""Create a surprising fact about a current pop culture topic for an English TikTok video (@syncinus5).
Today is: {today}
{avoid_block}
Focus on CURRENT topics such as:
- Recent movies, TV shows, or streaming hits (Netflix, Disney+, etc.)
- Current music, artists, albums, or records
- Recent social media trends or viral moments
- Current video games or gaming phenomena
- Current celebrities, their records, or surprising facts
- Recent meme culture or internet phenomena

Choose something that English-speaking viewers aged 16–30 will recognize and find interesting.
Avoid old or outdated topics – the fact should feel relevant TODAY.

Return ONLY valid JSON (no markdown, no extra text):
{{
  "title": "Short, punchy title (max 6 words, English)",
  "fact": "{fact_length}",
  "description": "A short, curiosity-building TikTok caption (1-2 sentences, English, with 1-2 fitting emojis). No more than 100 characters.",
  "hashtags": ["#popculture1", "#popculture2", "#popculture3", "#popculture4"],
  "visual_query": "2-3 English search terms for a fitting stock video (e.g. 'concert crowd lights' or 'phone social media scroll'). Only cinematically feasible subjects — no abstract terms."
}}

Rules:
- Everything in English
- Fact must be 100% true and verifiable
- Title must have a HOOK: either a well-known person/show name + surprising number, OR a counterintuitive statement ("This Netflix show was almost cancelled")
- The fact must START with the most surprising/shocking sentence (the hook comes first!)
- Open with a curiosity gap — something almost nobody knows about this topic
- Use concrete numbers: streams, followers, revenue, records, dates
- Structure: "Most fans don't know that X" or "Before Y became a superstar, Z"
- Description should build curiosity without revealing the fact (teaser style)
- Hashtags: 4 topic-specific pop culture hashtags (e.g. #netflix #taylorswift)"""
    else:
        # Special categories with provocative prompt adjustments
        _provocative_topics = {
            "dark history":    "dark history (e.g. state-sanctioned crimes, forgotten massacres, human experimentation, shocking historical practices — verified facts only)",
            "crime":           "true crime (spectacular cases, unsolved murders, serial killers, wrongful convictions — real facts, not fiction)",
            "conspiracy truth":"real conspiracies that turned out to be TRUE (e.g. MK-Ultra, Watergate, COINTELPRO, Tuskegee experiment — verified, documented facts only)",
            "money":           "money & inequality (shocking stats about wealth, poverty, how corporations avoid taxes, billionaire facts that will make people furious)",
            "war":             "war & military history (shocking or little-known facts about wars, weapons, propaganda, atrocities)",
            "medicine":        "medical history (bizarre historical treatments, shocking malpractice, pharmaceutical scandals — verified facts only)",
            "survival":        "extreme survival stories (true stories about unbelievable survival under impossible conditions)",
        }
        topic_desc = _provocative_topics.get(topic.lower(), topic)

        prompt = f"""Create a fascinating, provocative fact for an English TikTok video (@syncinUS).
Topic: {topic_desc}
{avoid_block}
Goal: The fact should shock, anger, or astound viewers enough that they HAVE to comment ("This can't be real!", "That's insane!", "I had no idea!"). Divisive or outrage-inducing facts go viral.

Return ONLY valid JSON (no markdown, no extra text):
{{
  "title": "Short, punchy title (max 6 words, English)",
  "fact": "{fact_length}",
  "description": "A short, provocative TikTok caption (1-2 sentences, English, with 1-2 emojis). Should trigger outrage or disbelief. No more than 100 characters.",
  "hashtags": ["#topic1", "#topic2", "#topic3", "#topic4"],
  "visual_query": "2-3 English search terms for a fitting stock video (e.g. 'dark archive documents' or 'courtroom drama gavel'). Only cinematically feasible subjects — no abstract terms."
}}

Rules:
- Everything in English
- Fact must be 100% true and verifiable — no speculation
- Title must SHOCK immediately: concrete number + outrageous claim, or a statement that triggers disbelief
- The fact MUST start with the most shocking sentence (hook first!)
- Choose facts that trigger an emotional reaction: outrage, disbelief, shock, anger
- Use concrete numbers, names, dates — nothing vague
- For crime/history topics: show the human dimension (victims, perpetrators, consequences)
- Description should provoke without being clickbait — the fact justifies the reaction
- Hashtags: 4 topic-specific tags (e.g. #truecrime #dark for crime topics)"""

    MAX_ATTEMPTS = 5
    data = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        attempt_prompt = prompt
        if attempt > 1 and data:
            attempt_prompt += (
                f"\n\nNOTE: Your last suggestion '{data.get('title','')}' was "
                f"too similar to an existing fact. "
                f"Choose a COMPLETELY different core topic this time!"
            )

        raw = _llm_call(attempt_prompt, max_tokens=1400 if long else 800)
        # Robust JSON extraction — tolerates markdown blocks and extra text
        if "```" in raw:
            import re as _re
            raw = _re.sub(r'```json\s*', '', raw)
            raw = _re.sub(r'```\s*', '', raw)
        import re as _re
        match = _re.search(r'\{.*\}', raw, _re.DOTALL)
        if match:
            raw = match.group(0)
        try:
            data = json.loads(raw.strip())
        except json.JSONDecodeError as je:
            print(f"   ⚠️  Attempt {attempt}/{MAX_ATTEMPTS}: Invalid JSON — {je} — retrying…")
            data = None
            if attempt < MAX_ATTEMPTS:
                continue
            raise

        too_similar, similar_to = _is_too_similar(
            data.get("title", ""), data.get("fact", ""), used
        )
        if too_similar:
            print(f"   ⚠️  Attempt {attempt}/{MAX_ATTEMPTS}: '{data['title']}' too similar to '{similar_to}' — retrying…")
            if attempt == MAX_ATTEMPTS:
                print("   ⚠️  Max attempts reached, using last suggestion anyway.")
            continue

        print(f"   ✓ Similarity check OK (attempt {attempt}/{MAX_ATTEMPTS})")
        break

    existing_tags = {h.lower() for h in data.get("hashtags", [])}
    for tag in _get_base_hashtags(topic):
        if tag.lower() not in existing_tags:
            data["hashtags"].append(tag)

    summary = data.get("fact", "")[:150].strip()
    _save_to_history(data["title"], summary)
    print(f"   History updated: '{data['title']}' ({len(used)+1} facts saved)")

    return data


if __name__ == "__main__":
    fact = generate_fact("space")
    print(json.dumps(fact, ensure_ascii=False, indent=2))
