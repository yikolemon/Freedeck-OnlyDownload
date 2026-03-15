#!/usr/bin/env python3
"""Enrich Tianyi game catalog CSV with Steam AppID mapping.

This script is intended for maintainers to pre-fill a `steam_appid` column in
the shipped catalog CSV. It uses Steam Store's public storesearch endpoint and
applies conservative matching rules to avoid wrong mappings.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import difflib
import html
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlencode

import aiohttp

STEAM_STORESEARCH = "https://store.steampowered.com/api/storesearch/"

STOPWORDS = {
  "the",
  "of",
  "and",
  "a",
  "an",
  "to",
  "for",
  "in",
  "on",
  "with",
  "at",
  "from",
  "by",
  "or",
}

# Extra disambiguation words we should ignore when building combined search terms.
EXTRA_WORD_BLACKLIST = {
    "digital",
    "deluxe",
    "ultimate",
    "complete",
    "gold",
    "premium",
    "platinum",
    "legendary",
    "special",
    "enhanced",
    "definitive",
    "anniversary",
    "edition",
    "remake",
    "remastered",
    "demo",
    "beta",
    "playtest",
    "soundtrack",
    "ost",
    "pack",
    "bundle",
}

# Terms that usually indicate the item is not the base game.
NON_GAME_TOKENS = {
    "soundtrack",
    "music",
    "ost",
    "demo",
    "dlc",
    "bundle",
    "pack",
    "upgrade",
    "expansion",
    "season",
    "pass",
    "artbook",
    "wallpaper",
    "guide",
    "beta",
    "test",
    "tool",
    "editor",
    "video",
    "server",
}

STEAM_APPDETAILS = "https://store.steampowered.com/api/appdetails"


@dataclass(frozen=True)
class MatchResult:
    appid: int
    name: str
    score: float
    second_score: float
    term: str
    candidates: List[Tuple[int, str, float]]


def _extract_english_candidates(title: str) -> List[str]:
    parts = [p.strip() for p in re.split(r"[／/]+", title or "") if p.strip()]
    candidates: List[str] = []
    for part in parts:
        if re.search(r"[A-Za-z]", part):
            candidates.append(part)
    if candidates:
        # Prefer the last segment: many titles are "中文/English".
        return [candidates[-1]] + [c for c in candidates[:-1] if c != candidates[-1]]
    return []


def _has_cjk(text: str) -> bool:
    return bool(re.search(r"[\u3400-\u9fff]", text or ""))


def _clean_term(term: str, *, strip_brackets: bool = True) -> str:
    value = html.unescape(str(term or "")).strip()
    if not value:
        return ""

    if strip_brackets:
        # Remove bracketed suffixes like "(Gold Edition)".
        value = re.sub(r"\([^)]*\)", " ", value)
        value = re.sub(r"（[^）]*）", " ", value)
        value = re.sub(r"\[[^\]]*\]", " ", value)

    # Common edition noise (keep conservative to avoid removing real title words).
    noise = [
        "legendary edition",
        "supporter edition",
        "digital deluxe edition",
        "deluxe edition",
        "ultimate edition",
        "complete edition",
        "gold edition",
        "premium edition",
        "platinum edition",
        "special edition",
        "game of the year edition",
        "goty edition",
        "collector's edition",
        "definitive edition",
        "tech demo",
        "tech",
        "playtest",
        "beta",
        "demo",
        "remake",
        "remastered",
        "legendary",
        "gold",
        "deluxe",
        "ultimate",
        "complete",
        "edition",
    ]
    lowered = value.lower()
    for word in noise:
        # Replace full word occurrences only for ascii words.
        lowered = re.sub(rf"\b{re.escape(word)}\b", " ", lowered)
    value = lowered

    # Normalize separators/punctuation commonly used in titles/subtitles.
    value = re.sub(r"[\u2013\u2014\u2212\-:：|,，.&]+", " ", value)
    value = " ".join(value.split())
    return value.strip()


def _normalize_for_match(text: str, *, strip_brackets: bool = True) -> str:
    value = _clean_term(text, strip_brackets=strip_brackets).lower()
    # Keep ASCII letters/digits and CJK, collapse everything else into spaces.
    value = re.sub(r"[^0-9a-z\u3400-\u9fff]+", " ", value)
    return " ".join(value.split()).strip()


def _similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    # SequenceMatcher works well for both ASCII and CJK after normalization.
    ratio = difflib.SequenceMatcher(None, a, b).ratio()
    return float(ratio)


def _tokenize(normalized: str) -> List[str]:
    tokens = [t for t in str(normalized or "").split() if t]
    filtered: List[str] = []
    for token in tokens:
        if token in STOPWORDS:
            continue
        filtered.append(token)
    return filtered


def _score_match(
    *,
    normalized_query: str,
    normalized_candidate: str,
    query_is_cjk: bool,
) -> float:
    if not normalized_query or not normalized_candidate:
        return 0.0

    seq = _similarity(normalized_query, normalized_candidate)
    if query_is_cjk:
        return seq

    q_tokens = set(_tokenize(normalized_query))
    c_tokens = set(_tokenize(normalized_candidate))
    if not q_tokens or not c_tokens:
        return seq

    intersection = len(q_tokens & c_tokens)
    coverage = intersection / max(1, len(q_tokens))

    prefix_bonus = 0.04 if normalized_candidate.startswith(normalized_query) else 0.0
    score = 0.70 * float(coverage) + 0.30 * float(seq) + float(prefix_bonus)

    penalty = 0.0
    extras = [t for t in (c_tokens - q_tokens) if t]
    # Mild penalty for additional tokens (helps prefer base-game entries).
    extra_non_numeric = [t for t in extras if not t.isdigit()]
    if extra_non_numeric:
        penalty += min(0.18, 0.035 * len(extra_non_numeric))

    if any(token in NON_GAME_TOKENS for token in extras):
        penalty += 0.25

    extra_numeric = [t for t in extras if t.isdigit()]
    if extra_numeric:
        penalty += min(0.30, 0.12 * len(extra_numeric))

    score = max(0.0, min(1.0, score - penalty))
    return float(score)


async def _steam_get_app_type(
    session: aiohttp.ClientSession,
    *,
    appid: int,
    cache: Dict[int, str],
) -> str:
    target = int(appid or 0)
    if target <= 0:
        return ""
    cached = cache.get(target)
    if cached is not None:
        return cached

    url = f"{STEAM_APPDETAILS}?{urlencode({'appids': str(target), 'cc': 'US', 'l': 'english'})}"
    backoff = 0.4
    for attempt in range(3):
        try:
            async with session.get(url, ssl=False) as resp:
                if int(resp.status) in (429, 500, 502, 503, 504):
                    await asyncio.sleep(backoff)
                    backoff *= 1.8
                    continue
                if int(resp.status) != 200:
                    cache[target] = ""
                    return ""
                payload = await resp.json(content_type=None)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(backoff)
            backoff *= 1.8
            continue

        entry = payload.get(str(target)) if isinstance(payload, dict) else None
        data = entry.get("data") if isinstance(entry, dict) else None
        kind = str(data.get("type") or "").strip().lower() if isinstance(data, dict) else ""
        cache[target] = kind
        return kind

    cache[target] = ""
    return ""


async def _refine_match_to_game_only(
    session: aiohttp.ClientSession,
    match: MatchResult,
    *,
    app_type_cache: Dict[int, str],
) -> MatchResult:
    candidates = match.candidates or []
    if not candidates:
        return match
    top = candidates[:5]
    types = [
        await _steam_get_app_type(session, appid=int(appid), cache=app_type_cache)
        for appid, _name, _score in top
    ]
    game_candidates = [
        cand for cand, kind in zip(top, types) if str(kind or "").lower() == "game"
    ]
    if not game_candidates:
        return match
    game_candidates.sort(key=lambda x: x[2], reverse=True)
    best_appid, best_name, best_score = game_candidates[0]
    second_score = game_candidates[1][2] if len(game_candidates) > 1 else 0.0
    return MatchResult(
        appid=int(best_appid),
        name=str(best_name),
        score=float(best_score),
        second_score=float(second_score),
        term=str(match.term),
        candidates=game_candidates,
    )


def _pick_search_terms(title: str) -> List[str]:
    raw = str(title or "").strip()
    if not raw:
        return []

    terms: List[str] = []
    parts = [p.strip() for p in re.split(r"[／/]+", raw) if p.strip()] or [raw]

    # Build a prioritized term list:
    # 1) each part as-is
    # 2) each part cleaned (often removes edition suffixes and punctuation)
    # Avoid using the full raw "中文/English" combined title as a term, as it
    # tends to reduce search relevance and can crowd out better cleaned terms.
    def push(text: str) -> None:
        if text:
            terms.append(text)

    def base_segment(text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        # Split on common subtitle separators.
        head = re.split(r"[\u2013\u2014\u2212\-:：|]+", value, maxsplit=1)[0].strip()
        return head or value

    for part in parts:
        base = base_segment(part)
        if base and base.lower() != str(part).lower():
            push(base)
        push(part)

        cleaned_base = _clean_term(base)
        if cleaned_base and cleaned_base.lower() not in {str(base).lower(), str(part).lower()}:
            push(cleaned_base)

        cleaned_part = _clean_term(part)
        if cleaned_part and cleaned_part.lower() not in {str(part).lower(), cleaned_base.lower() if cleaned_base else ""}:
            push(cleaned_part)

    # If we have an English base title and additional English hints in other
    # segments, build combined search terms to disambiguate (e.g. "Carmageddon Rogue").
    english_base: str = ""
    for part in parts:
        if re.search(r"[A-Za-z]", part):
            english_base = part

    if english_base:
        base_norm = _normalize_for_match(english_base, strip_brackets=True)
        base_tokens = set(_tokenize(base_norm))
        extras_words: List[str] = []
        for part in parts:
            if part == english_base:
                continue
            for word in re.findall(r"[A-Za-z]{3,}", part):
                low = word.lower()
                if low in STOPWORDS or low in EXTRA_WORD_BLACKLIST:
                    continue
                if low in base_tokens:
                    continue
                if low in (w.lower() for w in extras_words):
                    continue
                extras_words.append(word)
                if len(extras_words) >= 2:
                    break
            if len(extras_words) >= 2:
                break

        if extras_words:
            combined = f"{english_base} {extras_words[0]}"
            push(combined)
            push(_clean_term(combined))
            if len(extras_words) > 1:
                combined2 = f"{english_base} {extras_words[0]} {extras_words[1]}"
                push(combined2)
                push(_clean_term(combined2))

    # Extra fallback: extracted English candidate (rarely needed but cheap).
    for candidate in _extract_english_candidates(raw):
        base = base_segment(candidate)
        push(base)
        push(candidate)

        cleaned_base = _clean_term(base)
        if cleaned_base:
            push(cleaned_base)

        cleaned = _clean_term(candidate)
        if cleaned:
            push(cleaned)

    seen: set[str] = set()
    deduped: List[str] = []
    for term in terms:
        key = " ".join(str(term or "").split()).strip()
        if not key:
            continue
        low = key.lower()
        if low in seen:
            continue
        seen.add(low)
        deduped.append(key)
    return deduped[:8]


async def _steam_storesearch(
    session: aiohttp.ClientSession,
    term: str,
    *,
    cc: str,
    lang: str,
    timeout_s: int = 10,
    max_retries: int = 3,
) -> List[Dict[str, Any]]:
    params = {"term": term, "cc": cc, "l": lang}
    url = f"{STEAM_STORESEARCH}?{urlencode(params)}"
    backoff = 0.6
    for attempt in range(max_retries + 1):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout_s)) as resp:
                status = int(resp.status)
                if status in (429, 500, 502, 503, 504):
                    if attempt < max_retries:
                        await asyncio.sleep(backoff)
                        backoff *= 1.8
                        continue
                    return []
                if status != 200:
                    return []
                payload = await resp.json(content_type=None)
                if not isinstance(payload, dict):
                    return []
                items = payload.get("items")
                if not isinstance(items, list):
                    return []
                return [it for it in items if isinstance(it, dict)]
        except asyncio.CancelledError:
            raise
        except Exception:
            if attempt < max_retries:
                await asyncio.sleep(backoff)
                backoff *= 1.8
                continue
            return []
    return []


def _best_match_for_term(term: str, items: Sequence[Dict[str, Any]]) -> Optional[MatchResult]:
    normalized_query = _normalize_for_match(term, strip_brackets=True)
    if not normalized_query:
        return None
    query_is_cjk = _has_cjk(normalized_query)

    scored: List[Tuple[int, str, float]] = []
    for item in items[:25]:
        try:
            appid = int(item.get("id") or 0)
        except Exception:
            appid = 0
        if appid <= 0:
            continue
        if str(item.get("type") or "").lower() != "app":
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        normalized_candidate = _normalize_for_match(name, strip_brackets=False)
        score = _score_match(
            normalized_query=normalized_query,
            normalized_candidate=normalized_candidate,
            query_is_cjk=query_is_cjk,
        )
        scored.append((appid, name, float(score)))

    if not scored:
        return None

    scored.sort(key=lambda x: x[2], reverse=True)
    best_appid, best_name, best_score = scored[0]
    second_score = scored[1][2] if len(scored) > 1 else 0.0
    return MatchResult(
        appid=best_appid,
        name=best_name,
        score=float(best_score),
        second_score=float(second_score),
        term=term,
        candidates=scored[:5],
    )


async def resolve_steam_appid_for_title(
    session: aiohttp.ClientSession,
    title: str,
    *,
    min_score: float,
    margin: float,
    per_request_delay_s: float,
    cache: Dict[str, List[Dict[str, Any]]],
    app_type_cache: Dict[int, str],
) -> Optional[MatchResult]:
    terms = _pick_search_terms(title)
    if not terms:
        return None

    best: Optional[MatchResult] = None
    for term in terms:
        lang = "schinese" if _has_cjk(term) else "english"
        cc = "CN" if lang == "schinese" else "US"
        if term in cache:
            items = cache[term]
        else:
            items = await _steam_storesearch(session, term, cc=cc, lang=lang)
            cache[term] = items
            if per_request_delay_s > 0:
                await asyncio.sleep(per_request_delay_s)

        match = _best_match_for_term(term, items)
        if not match:
            continue

        if match.score >= min_score and (match.score - match.second_score) < margin and len(match.candidates) > 1:
            match = await _refine_match_to_game_only(session, match, app_type_cache=app_type_cache)

        kind = await _steam_get_app_type(session, appid=int(match.appid), cache=app_type_cache)
        if kind != "game":
            continue

        normalized_query = _normalize_for_match(match.term, strip_brackets=True)
        normalized_name = _normalize_for_match(match.name, strip_brackets=True)
        if match.score >= min_score and (
            (match.score - match.second_score) >= margin
            or (
                normalized_query
                and normalized_query == normalized_name
                and kind == "game"
            )
        ):
            return match

        if not best or match.score > best.score:
            best = match

        # If we already have a very confident match, stop early.
        if match.score >= 0.98:
            break

    if not best:
        return None

    if best.score >= min_score and (best.score - best.second_score) < margin and len(best.candidates) > 1:
        best = await _refine_match_to_game_only(session, best, app_type_cache=app_type_cache)

    kind = await _steam_get_app_type(session, appid=int(best.appid), cache=app_type_cache)
    if kind != "game":
        return None

    normalized_query = _normalize_for_match(best.term, strip_brackets=True)
    normalized_name = _normalize_for_match(best.name, strip_brackets=True)
    if best.score >= min_score and (
        (best.score - best.second_score) >= margin
        or (
            normalized_query
            and normalized_query == normalized_name
            and kind == "game"
        )
    ):
        return best
    return None


def _read_rows(csv_path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        rows: List[Dict[str, str]] = []
        for row in reader:
            if not isinstance(row, dict):
                continue
            rows.append({k: str(v or "") for k, v in row.items()})
    return fieldnames, rows


def _write_rows(csv_path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    tmp_path = csv_path.with_suffix(csv_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    tmp_path.replace(csv_path)


async def async_main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(description="Fill steam_appid column for a catalog CSV.")
    parser.add_argument(
        "--csv",
        default="defaults/tianyi_catalog/freedeck_catalog.csv",
        help="Catalog CSV path to enrich (default: shipped catalog).",
    )
    parser.add_argument("--min-score", type=float, default=0.92, help="Minimum match score to accept.")
    parser.add_argument("--margin", type=float, default=0.05, help="Minimum score gap between top two candidates.")
    parser.add_argument("--delay", type=float, default=0.12, help="Delay between Steam requests (seconds).")
    parser.add_argument(
        "--checkpoint",
        type=int,
        default=200,
        help="Persist CSV after every N processed rows (0 = only at end).",
    )
    parser.add_argument("--limit", type=int, default=0, help="Only process first N rows (0 = all).")
    parser.add_argument(
        "--report",
        default="defaults/tianyi_catalog/steam_appid_report.csv",
        help="Write unresolved items to a report CSV.",
    )
    args = parser.parse_args(list(argv))

    csv_path = Path(args.csv).expanduser().resolve()
    if not csv_path.is_file():
        print(f"ERROR: CSV not found: {csv_path}", file=sys.stderr)
        return 2

    fieldnames, rows = _read_rows(csv_path)
    if "steam_appid" not in fieldnames:
        fieldnames.append("steam_appid")

    total = len(rows)
    limit = int(args.limit or 0)
    if limit > 0:
        total = min(total, limit)

    print(f"Loaded rows: {len(rows)}  Processing: {total}")
    headers = {
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Freedeck-AppID-Enricher/1.0",
        "Referer": "https://store.steampowered.com/",
    }

    unresolved: List[Dict[str, str]] = []
    cache: Dict[str, List[Dict[str, Any]]] = {}
    app_type_cache: Dict[int, str] = {}

    timeout = aiohttp.ClientTimeout(total=12)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        started = time.time()
        resolved = 0
        skipped = 0
        processed = 0
        checkpoint = max(0, int(args.checkpoint or 0))
        for idx in range(total):
            row = rows[idx]
            title = str(row.get("title", "") or "").strip()
            existing = str(row.get("steam_appid", "") or "").strip()
            if existing:
                skipped += 1
                continue
            if not title:
                unresolved.append({"game_id": row.get("game_id", ""), "title": "", "reason": "empty title"})
                continue

            match = await resolve_steam_appid_for_title(
                session,
                title,
                min_score=float(args.min_score),
                margin=float(args.margin),
                per_request_delay_s=float(args.delay),
                cache=cache,
                app_type_cache=app_type_cache,
            )
            if match:
                row["steam_appid"] = str(int(match.appid))
                resolved += 1
            else:
                unresolved.append({"game_id": row.get("game_id", ""), "title": title, "reason": "no confident match"})

            processed += 1
            if checkpoint > 0 and processed % checkpoint == 0:
                _write_rows(csv_path, fieldnames, rows)
                print(f"Checkpoint saved: processed={processed} file={csv_path}", flush=True)

            if (idx + 1) % 25 == 0 or idx + 1 == total:
                elapsed = max(0.001, time.time() - started)
                rate = (idx + 1) / elapsed
                print(
                    f"[{idx + 1}/{total}] resolved={resolved} unresolved={len(unresolved)} skipped={skipped} cache={len(cache)} rate={rate:.2f}/s",
                    flush=True,
                )

    _write_rows(csv_path, fieldnames, rows)
    print(f"Updated CSV written: {csv_path}")

    report_path = Path(args.report).expanduser().resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["game_id", "title", "reason"])
        writer.writeheader()
        for item in unresolved:
            writer.writerow(item)
    print(f"Report written: {report_path} (unresolved={len(unresolved)})")
    return 0


def main() -> None:
    try:
        exit_code = asyncio.run(async_main(sys.argv[1:]))
    except KeyboardInterrupt:
        exit_code = 130
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
