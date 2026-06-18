"""
World Cup prediction-markets parser — Stage 1 (parse + store).

Single-file parser. It discovers active *World Cup match* markets across
ADI Predictstreet, Limitless, Polymarket and Kalshi, normalizes them into one
schema, upserts current state into Supabase (Postgres), writes a snapshot row
per observed market, and marks previously-active-but-now-missing markets as
inactive (never deletes). Google Sheets mirroring is optional/debug-only.

Run locally:   python parser.py
Run on CI:     see .github/workflows/parser.yml

Design rules: fail per-source gracefully, never fabricate fields, keep raw JSON.
"""

from __future__ import annotations

import base64
import json
import hashlib
import logging
import os
import re
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import requests
from dateutil import parser as dtparser  # noqa: F401  (kept for downstream date use)
from dotenv import load_dotenv

# ============================================================================
# === CONFIG =================================================================
# ============================================================================

load_dotenv()

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("wc-parser")


def _env(key: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and not val:
        log.error("Missing required env var: %s", key)
        sys.exit(1)
    return val or ""


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "y", "on")


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


# --- Supabase ---
SUPABASE_URL = _env("SUPABASE_URL", required=True)
SUPABASE_KEY = _env("SUPABASE_KEY", required=True)  # use service_role key (write)
SUPABASE_SCHEMA = _env("SUPABASE_SCHEMA", "public")

# --- Source toggles ---
ENABLE_POLYMARKET = _env_bool("ENABLE_POLYMARKET", True)
ENABLE_KALSHI = _env_bool("ENABLE_KALSHI", True)
ENABLE_ADI = _env_bool("ENABLE_ADI", True)
ENABLE_LIMITLESS = _env_bool("ENABLE_LIMITLESS", True)

# --- Write minimization ---
# Snapshots are written only when a market's volume/status changed since the
# last run, and (by default) only for markets that belong to a cross-platform
# group — that's the only data a "current volumes" dashboard could ever chart.
# Set ENABLE_SNAPSHOTS=false to skip the history table entirely.
ENABLE_SNAPSHOTS = _env_bool("ENABLE_SNAPSHOTS", True)
SNAPSHOT_GROUPS_ONLY = _env_bool("SNAPSHOT_GROUPS_ONLY", True)

# --- Google Sheets (optional mirror) ---
GOOGLE_SHEETS_ENABLED = _env_bool("GOOGLE_SHEETS_ENABLED", False)
GOOGLE_SHEET_ID = _env("GOOGLE_SHEET_ID")
GOOGLE_SERVICE_ACCOUNT_JSON_BASE64 = _env("GOOGLE_SERVICE_ACCOUNT_JSON_BASE64")

# --- Parser tuning ---
REQUEST_TIMEOUT = _env_int("REQUEST_TIMEOUT", 30)
WORLD_CUP_KEYWORDS = [
    k.strip().lower()
    for k in _env("WORLD_CUP_KEYWORDS", "world cup,fifa world cup").split(",")
    if k.strip()
]

# --- Source endpoints (all documented public bases) ---
GAMMA_API_URL = _env("GAMMA_API_URL", "https://gamma-api.polymarket.com")
KALSHI_API_URL = _env("KALSHI_API_URL", "https://api.elections.kalshi.com/trade-api/v2")
ADI_API_URL = _env("ADI_API_URL", "https://core-api.adipredictstreet.com")
LMTS_API_URL = _env("LMTS_API_URL", "https://api.limitless.exchange")
# --- Source-specific discovery hints (optional, improve precision) ---
# Polymarket: numeric tag id(s) to query directly via /events?tag_id=<id>.
# This is the reliable discovery path (the Gamma "FIFA World Cup" tag is 102232)
# and avoids the deep-offset keyword scan that 422s past ~offset 2000.
POLY_TAG_IDS = [s.strip() for s in _env("POLY_TAG_IDS").split(",") if s.strip()]
# Polymarket: comma-separated tag slugs to query directly, e.g. "world-cup".
POLY_TAG_SLUGS = [s.strip() for s in _env("POLY_TAG_SLUGS").split(",") if s.strip()]
POLY_MAX_PAGES = _env_int("POLY_MAX_PAGES", 50)  # page cap (any discovery mode)
# Kalshi: comma-separated series tickers for World Cup, e.g. "KXWORLDCUP".
KALSHI_SERIES_TICKERS = [
    s.strip() for s in _env("KALSHI_SERIES_TICKERS").split(",") if s.strip()
]
KALSHI_MAX_PAGES = _env_int("KALSHI_MAX_PAGES", 50)
# ADI: matches feed is football-fixture oriented (stages group..final). When
# true, every READY match is treated as World Cup. Keep true while the only
# active tournament on ADI is the World Cup; set false to require a keyword/tag.
ADI_ASSUME_WORLD_CUP = _env_bool("ADI_ASSUME_WORLD_CUP", True)
ADI_TAG = _env("ADI_TAG")  # optional single tag slug to filter /api/matches
# Limitless: primary discovery is the deterministic /markets/active browse
# (optionally scoped to the World Cup category via LMTS_CATEGORY_ID). Semantic
# /markets/search is kept as a fallback. Tune queries / similarity floor.
LMTS_SEARCH_QUERIES = [
    q.strip() for q in
    _env("LMTS_SEARCH_QUERIES", "FIFA World Cup,World Cup soccer").split(",")
    if q.strip()
]
LMTS_SIMILARITY = _env_float("LMTS_SIMILARITY", 0.4)
LMTS_MAX_PAGES = _env_int("LMTS_MAX_PAGES", 10)
LMTS_CATEGORY_ID = _env("LMTS_CATEGORY_ID")  # опц. числовой id категории WC (точность)

PLATFORM_POLY = "polymarket"
PLATFORM_KALSHI = "kalshi"
PLATFORM_ADI = "adi"
PLATFORM_LMTS = "limitless"

NOW = lambda: datetime.now(timezone.utc).isoformat()  # noqa: E731

# ============================================================================
# === HTTP HELPERS ===========================================================
# ============================================================================

_session = requests.Session()
_session.headers.update({"User-Agent": "wc-parser/1.0 (+github-actions)"})


def http_get_json(url: str, params: Optional[dict] = None, retries: int = 3) -> Any:
    """GET with small backoff. Raises on final failure (callers catch)."""
    last_exc: Optional[Exception] = None
    for attempt in range(retries):
        try:
            r = _session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:  # noqa: BLE001 — uniform backoff for net/HTTP
            last_exc = e
            status = getattr(getattr(e, "response", None), "status_code", None)
            # Don't retry clear client errors except 429.
            if status and 400 <= status < 500 and status != 429:
                raise
            if attempt < retries - 1:
                time.sleep(1.0 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


# ============================================================================
# === NORMALIZATION HELPERS ==================================================
# ============================================================================

# Match "Team A vs Team B" written several ways.
_VS_SPLIT = re.compile(r"\s+(?:vs\.?|v\.?|—|–|@)\s+", re.IGNORECASE)


def _clean_team(s: str) -> str:
    """Strip category suffixes Polymarket appends, e.g. 'Haiti - Player Props'."""
    for sep in (" - ", " – ", " — ", " | ", ": ", " ("):
        i = s.find(sep)
        if i != -1:
            s = s[:i]
    return s.strip(" -–—|:")


def parse_teams(title: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Best-effort extraction of (team_a, team_b) from a match-style title."""
    if not title:
        return None, None
    # Cut off anything after a colon/parenthesis (often "... : To win").
    core = re.split(r"[:(]", title, maxsplit=1)[0].strip()
    parts = _VS_SPLIT.split(core)
    if len(parts) == 2:
        a, b = _clean_team(parts[0]), _clean_team(parts[1])
        if a and b and len(a) < 40 and len(b) < 40:
            return a, b
    return None, None
    
def parse_lmts_teams(title: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Limitless titles often look like:
    'World Cup, Uruguay vs Cape Verde Islands, Jun 21, 2026'
    """
    if not title:
        return None, None

    t = title.strip()

    m = re.search(
        r"^(?:world cup,\s*)?(.+?)\s+vs\.?\s+(.+?)(?:,\s+[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4})?$",
        t,
        re.IGNORECASE,
    )

    if m:
        return _clean_team(m.group(1)), _clean_team(m.group(2))

    return parse_teams(title)


def text_is_world_cup(*chunks: Optional[str]) -> bool:
    blob = " ".join(c for c in chunks if c).lower()
    return any(kw in blob for kw in WORLD_CUP_KEYWORDS)


def to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        f = float(x)
        return f
    except (TypeError, ValueError):
        return None


def fifa_code(x: Any) -> Optional[str]:
    """ADI teamA/participantA may be a str or an object — pull a label."""
    if x is None:
        return None
    if isinstance(x, str):
        return x or None
    if isinstance(x, dict):
        for k in ("code", "shortName", "short_name", "name", "label", "slug"):
            v = x.get(k)
            if isinstance(v, str) and v:
                return v
    return None


def make_record(**kw: Any) -> dict:
    """
    Build a normalized record with all schema keys present (missing → None).
    `raw` is the source payload (stored as jsonb).
    """
    raw = kw.get("raw_json")
    return {
        "platform": kw["platform"],
        "platform_market_id": str(kw["platform_market_id"]),
        "platform_event_id": (str(kw["platform_event_id"])
                              if kw.get("platform_event_id") is not None else None),
        "competition_slug": kw.get("competition_slug"),
        "competition_name": kw.get("competition_name"),
        "event_name": kw.get("event_name"),
        "match_name": kw.get("match_name"),
        "team_a": kw.get("team_a"),
        "team_b": kw.get("team_b"),
        "market_title": kw.get("market_title"),
        "market_type": kw.get("market_type"),
        "market_url": kw.get("market_url"),
        "event_start_time": kw.get("event_start_time"),
        "market_status": kw.get("market_status"),
        "is_active": bool(kw.get("is_active", True)),
        "volume_total": to_float(kw.get("volume_total")),
        "volume_24h": to_float(kw.get("volume_24h")),
        "volume_currency": kw.get("volume_currency"),
        "raw_updated_at": kw.get("raw_updated_at"),
        "raw_json": raw if isinstance(raw, (dict, list)) else None,
    }


# ============================================================================
# === SOURCE: POLYMARKET (Gamma) =============================================
# ============================================================================

def _poly_market_url(event_slug: Optional[str]) -> Optional[str]:
    return f"https://polymarket.com/event/{event_slug}" if event_slug else None


def _poly_normalize_event(ev: dict) -> list[dict]:
    """Turn one Gamma event (with nested markets) into normalized records."""
    out: list[dict] = []
    ev_id = ev.get("id")
    ev_slug = ev.get("slug")
    ev_title = ev.get("title") or ev.get("name")
    ev_start = ev.get("startDate") or ev.get("start_date")
    team_a, team_b = parse_teams(ev_title)

    for m in ev.get("markets") or []:
        mid = m.get("id")
        if mid is None:
            continue
        m_title = m.get("question") or m.get("title")
        closed = bool(m.get("closed"))
        m_team_a, m_team_b = parse_teams(m_title)
        out.append(make_record(
            platform=PLATFORM_POLY,
            platform_market_id=mid,
            platform_event_id=ev_id,
            competition_slug=ev_slug,
            competition_name=ev_title,
            event_name=ev_title,
            match_name=m_title or ev_title,
            team_a=m_team_a or team_a,
            team_b=m_team_b or team_b,
            market_title=m_title,
            market_type=m.get("marketType"),  # raw; canon derived from title later
            market_url=_poly_market_url(ev_slug),
            event_start_time=ev_start,
            market_status="closed" if closed else "open",
            is_active=not closed,
            volume_total=to_float(m.get("volumeNum") or m.get("volume")),
            volume_24h=to_float(m.get("volume24hr") or m.get("volume24hrClob")),
            volume_currency="USD",
            raw_updated_at=m.get("updatedAt") or ev.get("updatedAt"),
            raw_json=m,
        ))
    return out


def _coerce_events(payload: Any) -> list[dict]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        return payload.get("data") or payload.get("events") or []
    return []


def _poly_paginate(base_params: dict) -> list[dict]:
    """
    Page /events with the given filter params. Resilient: a client error on a
    deep page (Gamma 422s past a high offset) stops pagination and returns what
    we have so far instead of raising and dropping the whole source.
    """
    out: list[dict] = []
    offset = 0
    for _ in range(POLY_MAX_PAGES):
        params = {**base_params, "limit": 100, "offset": offset}
        try:
            payload = http_get_json(f"{GAMMA_API_URL}/events", params=params)
        except requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status and 400 <= status < 500:
                log.warning("polymarket: stop paging at offset %d (HTTP %s)",
                            offset, status)
                break
            raise
        page = _coerce_events(payload)
        if not page:
            break
        out.extend(page)
        if len(page) < 100:
            break
        offset += 100
    return out


def fetch_polymarket() -> list[dict]:
    """
    Discover World Cup events on Gamma and normalize their markets.

    Discovery modes, in priority order:
      1. POLY_TAG_IDS  → /events?tag_id=<id>      (reliable; WC tag is 102232)
      2. POLY_TAG_SLUGS→ /events?tag_slug=<slug>  (precise)
      3. fallback      → page /events and keep title/slug keyword matches
    All modes share resilient pagination (a deep-page 4xx ends paging, it does
    not fail the source). closed=false + active=true keeps live markets only.
    """
    events: list[dict] = []

    if POLY_TAG_IDS:
        for tid in POLY_TAG_IDS:
            events.extend(_poly_paginate(
                {"closed": "false", "active": "true", "tag_id": tid}))
        log.info("polymarket: %d events via tag ids %s", len(events), POLY_TAG_IDS)
    elif POLY_TAG_SLUGS:
        for slug in POLY_TAG_SLUGS:
            events.extend(_poly_paginate(
                {"closed": "false", "active": "true", "tag_slug": slug}))
        log.info("polymarket: %d events via tag slugs %s", len(events), POLY_TAG_SLUGS)
    else:
        page_all = _poly_paginate(
            {"closed": "false", "active": "true",
             "order": "startDate", "ascending": "false"})
        events = [ev for ev in page_all
                  if text_is_world_cup(ev.get("title"), ev.get("slug"))]
        log.info("polymarket: %d World Cup events of %d scanned (keyword)",
                 len(events), len(page_all))
        # For precision/cost set POLY_TAG_IDS=102232 (the Gamma FIFA World Cup
        # tag) so discovery skips the full-catalog scan entirely.

    records: list[dict] = []
    seen: set[str] = set()
    for ev in events:
        for rec in _poly_normalize_event(ev):
            if rec["platform_market_id"] in seen:
                continue
            seen.add(rec["platform_market_id"])
            records.append(rec)
    return records


# ============================================================================
# === SOURCE: KALSHI =========================================================
# ============================================================================

def _kalshi_market_url(series_ticker: Optional[str],
                       event_ticker: Optional[str] = None) -> Optional[str]:
    # Веб-URL Kalshi: серия первой, в нижнем регистре, напр.
    #   https://kalshi.com/markets/kxmenworldcup/.../kxmenworldcup-26
    # Человекочитаемого slug в API нет — ведём на страницу серии (резолвится).
    if series_ticker:
        return f"https://kalshi.com/markets/{series_ticker.lower()}"
    if event_ticker:  # серия = префикс event-тикера до первого '-'
        return f"https://kalshi.com/markets/{event_ticker.split('-')[0].lower()}"
    return None


def _kalshi_status_active(status: Optional[str]) -> bool:
    return (status or "").lower() in ("open", "active", "unopened")


def _kalshi_normalize_event(ev: dict) -> list[dict]:
    out: list[dict] = []
    event_ticker = ev.get("event_ticker")
    series_ticker = ev.get("series_ticker")
    ev_title = ev.get("title") or ev.get("sub_title")
    team_a, team_b = parse_teams(ev_title)

    for m in ev.get("markets") or []:
        ticker = m.get("ticker")
        if not ticker:
            continue
        status = m.get("status")
        m_title = (m.get("title") or m.get("yes_sub_title")
                   or m.get("subtitle") or ev_title)
        out.append(make_record(
            platform=PLATFORM_KALSHI,
            platform_market_id=ticker,
            platform_event_id=event_ticker,
            competition_slug=(series_ticker or "").lower() or None,
            competition_name=ev_title,
            event_name=ev_title,
            match_name=ev_title,
            team_a=team_a,
            team_b=team_b,
            market_title=m_title,
            market_type=m.get("market_type"),
            market_url=_kalshi_market_url(series_ticker, event_ticker),
            event_start_time=m.get("open_time") or ev.get("open_time"),
            market_status=status,
            is_active=_kalshi_status_active(status),
            # NOTE: Kalshi volume is contract count, NOT USD — currency reflects that.
            volume_total=to_float(m.get("volume_fp") or m.get("volume")),
            volume_24h=to_float(m.get("volume_24h_fp") or m.get("volume_24h")),
            volume_currency="contracts",
            raw_updated_at=m.get("updated_time"),
            raw_json=m,
        ))
    return out


def _kalshi_iter_events(params: dict) -> list[dict]:
    """Cursor-paginate Kalshi /events with nested markets."""
    events: list[dict] = []
    cursor: Optional[str] = None
    for _ in range(KALSHI_MAX_PAGES):
        p = dict(params)
        p["with_nested_markets"] = "true"
        p["limit"] = 200
        if cursor:
            p["cursor"] = cursor
        payload = http_get_json(f"{KALSHI_API_URL}/events", params=p)
        if not isinstance(payload, dict):
            break
        page = payload.get("events") or []
        events.extend(page)
        cursor = payload.get("cursor")
        if not cursor or not page:
            break
    return events


def fetch_kalshi() -> list[dict]:
    """
    Discover World Cup markets on Kalshi.

    Modes:
      1. If KALSHI_SERIES_TICKERS set → /events?series_ticker=<t> (precise).
      2. Else → /events?status=open paged, keep events matching keywords.
    """
    events: list[dict] = []
    if KALSHI_SERIES_TICKERS:
        for t in KALSHI_SERIES_TICKERS:
            events.extend(_kalshi_iter_events({"series_ticker": t}))
        log.info("kalshi: %d events via series %s", len(events), KALSHI_SERIES_TICKERS)
    else:
        all_open = _kalshi_iter_events({"status": "open"})
        events = [
            ev for ev in all_open
            if text_is_world_cup(ev.get("title"), ev.get("sub_title"),
                                 ev.get("series_ticker"))
        ]
        log.info("kalshi: %d World Cup events of %d open (keyword scan)",
                 len(events), len(all_open))
        # TODO: for precision/cost, set KALSHI_SERIES_TICKERS to the World Cup
        # series ticker(s) (find via the market URL on kalshi.com or /series).

    records: list[dict] = []
    seen: set[str] = set()
    for ev in events:
        for rec in _kalshi_normalize_event(ev):
            if rec["platform_market_id"] in seen:
                continue
            seen.add(rec["platform_market_id"])
            records.append(rec)
    return records


# ============================================================================
# === SOURCE: ADI PREDICTSTREET ==============================================
# ============================================================================

def _adi_market_url(match_slug: Optional[str], event_slug: Optional[str] = None,
                    market_slug: Optional[str] = None) -> Optional[str]:
    # Фронт ADI: страница матча — adipredictstreet.com/match/{match_slug}
    # напр. /match/fifwc-cze-rsa-2026-06-18
    slug = match_slug or event_slug or market_slug
    return f"https://adipredictstreet.com/match/{slug}" if slug else None


def _adi_canon_type(tags: list) -> Optional[str]:
    """Derive canonical market type from ADI tags (e.g. MONEYLINE)."""
    blob = " ".join(
        ((t.get("name") or "") + " " + (t.get("slug") or "")).lower()
        for t in (tags or []) if isinstance(t, dict)
    )
    if any(w in blob for w in ("moneyline", "1x2", "match winner",
                               "match-winner", "match result")):
        return "match_winner"
    if "correct" in blob and "score" in blob:
        return "correct_score"
    if any(w in blob for w in ("total", "over/under", "over-under", "over_under")):
        return "total_goals"
    if "btts" in blob or "both teams" in blob:
        return "btts"
    if "scorer" in blob or "goalscorer" in blob:
        return "scorer"
    return None


def _adi_orient_score(git: Optional[str], home: Optional[str],
                      away: Optional[str]) -> Optional[str]:
    """ADI groupItemTitle like '3-0' is home-first; orient to sorted-team order."""
    if not git:
        return None
    m = re.search(r"(\d{1,2})\s*[-:]\s*(\d{1,2})", git)
    if not m:
        return None
    x, y = m.group(1), m.group(2)
    ca, cb = canon_team(home), canon_team(away)
    teams = sorted(v for v in (ca, cb) if v)
    if len(teams) == 2 and ca == teams[1]:
        x, y = y, x
    return f"{x}-{y}"


def _adi_selection(ctype: Optional[str], git: Optional[str],
                   home: Optional[str], away: Optional[str]) -> Optional[str]:
    """Canonical selection from ADI groupItemTitle (the outcome label)."""
    if not ctype:
        return None
    g = (git or "").strip()
    if ctype == "match_winner":
        if not g:
            return None
        if g.lower() in ("draw", "tie"):
            return "draw"
        return canon_team(g)
    if ctype == "correct_score":
        return _adi_orient_score(g, home, away)
    if ctype == "total_goals":
        mm = re.search(r"(over|under)\D*(\d+(?:\.\d+)?)", g.lower())
        return f"{mm.group(1)}_{mm.group(2)}" if mm else None
    if ctype == "btts":
        if g.lower() in ("yes", "y"):
            return "yes"
        if g.lower() in ("no", "n"):
            return "no"
    return None


def _adi_match_is_world_cup(match: dict) -> bool:
    if ADI_ASSUME_WORLD_CUP:
        return True
    tag_text = " ".join(
        (t or {}).get("name", "") + " " + (t or {}).get("slug", "")
        for t in (match.get("tags") or [])
    )
    child_titles = " ".join((e or {}).get("title", "") for e in (match.get("events") or []))
    return text_is_world_cup(match.get("title"), tag_text, child_titles)


def _adi_normalize_match(match: dict) -> list[dict]:
    out: list[dict] = []
    match_title = match.get("title")
    match_slug = match.get("slug")
    match_start = match.get("startTime")
    pa = fifa_code(match.get("participantA"))
    pb = fifa_code(match.get("participantB"))
    if not (pa and pb):
        ta, tb = parse_teams(match_title)
        pa, pb = pa or ta, pb or tb
    comp_name = None
    for t in (match.get("tags") or []):
        if isinstance(t, dict) and t.get("name"):
            comp_name = t["name"]
            break

    for ev in match.get("events") or []:
        ev_id = ev.get("id")
        ev_slug = ev.get("slug")
        ev_title = ev.get("title")
        ev_start = ev.get("eventStartTime") or match_start
        ev_team_a = fifa_code(ev.get("teamA")) or pa
        ev_team_b = fifa_code(ev.get("teamB")) or pb
        ev_vol_total = ev.get("totalVolume")
        ev_vol_24h = ev.get("volume24h")
        for m in ev.get("markets") or []:
            mid = m.get("id") or m.get("slug")
            if mid is None:
                continue
            status = m.get("status")
            tags = m.get("tags") or []
            ctype = _adi_canon_type(tags)
            csel = _adi_selection(ctype, m.get("groupItemTitle"),
                                  ev_team_a, ev_team_b)
            # Raw market type = the non-competition tag name (e.g. "MONEYLINE").
            type_tag = next(
                (t.get("name") for t in tags
                 if isinstance(t, dict) and t.get("name")
                 and t.get("slug") != "fifa-wc-2026"),
                None,
            )
            rec = make_record(
                platform=PLATFORM_ADI,
                platform_market_id=mid,
                platform_event_id=ev_id,
                competition_slug=match_slug,
                competition_name=comp_name,
                event_name=ev_title,
                match_name=match_title,
                team_a=ev_team_a,
                team_b=ev_team_b,
                market_title=m.get("question") or m.get("title") or ev_title,
                market_type=type_tag,
                market_url=_adi_market_url(match_slug, ev_slug, m.get("slug")),
                event_start_time=m.get("kickoff") or ev_start,
                market_status=status,
                is_active=(status or "").upper() in ("OPEN", "PRE_MARKET", "PAUSED"),
                volume_total=to_float(m.get("totalVolume") or ev_vol_total),
                volume_24h=to_float(m.get("volume24h") or ev_vol_24h),
                volume_currency="USDC",
                raw_updated_at=m.get("updatedAt") or ev.get("updatedAt"),
                raw_json=m,
            )
            # ADI gives structured type/outcome — pass them as canon overrides.
            if ctype:
                rec["canon_type_override"] = ctype
                if csel:
                    rec["canon_sel_override"] = csel
            out.append(rec)
    return out


def fetch_adi() -> list[dict]:
    """
    Discover World Cup match markets on ADI via /api/matches (fixture cards).
    Each match nests child events (1X2 / first-scorer / over-under) → markets.
    """
    params: dict = {}
    if ADI_TAG:
        params["tag"] = ADI_TAG
    payload = http_get_json(f"{ADI_API_URL}/api/matches", params=params or None)
    matches = payload.get("matches") if isinstance(payload, dict) else payload
    matches = matches or []
    log.info("adi: %d matches returned", len(matches))

    records: list[dict] = []
    seen: set[str] = set()
    kept_matches = 0
    for match in matches:
        if not isinstance(match, dict):
            continue
        if not _adi_match_is_world_cup(match):
            continue
        kept_matches += 1
        for rec in _adi_normalize_match(match):
            if rec["platform_market_id"] in seen:
                continue
            seen.add(rec["platform_market_id"])
            records.append(rec)
    log.info("adi: kept %d World Cup matches → %d markets", kept_matches, len(records))
    return records


# ============================================================================
# === SOURCE: LIMITLESS ======================================================
# ============================================================================

def _lmts_start_iso(m: dict) -> Optional[str]:
    ets = m.get("expirationTimestamp")
    if ets:
        try:
            return datetime.fromtimestamp(int(ets) / 1000, tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OSError):
            return None
    return m.get("expirationDate") or None


def _lmts_extract_markets(payload: object) -> list[dict]:
    """Search/browse responses may be a list or wrap items under a key."""
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for k in ("data", "markets", "results", "items"):
            v = payload.get(k)
            if isinstance(v, list):
                return [x for x in v if isinstance(x, dict)]
    return []


def _lmts_iter_active(category_id: Optional[str]) -> list[dict]:
    """
    Страничный обход /markets/active (опц. /markets/active/{categoryId}).
    Возвращает сырые элементы (одиночные маркеты И группы с детьми).
    """
    base = f"{LMTS_API_URL}/markets/active"
    if category_id:
        base = f"{base}/{category_id}"
    out: list[dict] = []
    page = 1
    while page <= LMTS_MAX_PAGES:
        try:
            payload = http_get_json(
                base, params={"page": page, "limit": 100, "sortBy": "newest"})
        except Exception as e:  # noqa: BLE001 — одна страница падает — не валим источник
            log.warning("limitless active p%d failed: %s", page, e)
            break
        items = _lmts_extract_markets(payload)
        if not items:
            break
        out.extend(items)
        total = payload.get("totalMarketsCount") if isinstance(payload, dict) else None
        if len(items) < 100 or (total is not None and len(out) >= total):
            break
        page += 1
    return out


def _lmts_record(m: dict, parent: Optional[dict] = None) -> Optional[dict]:
    slug = m.get("slug") or (str(m.get("id")) if m.get("id") else None)
    if not slug:
        return None

    title = m.get("title") or ""
    match_title = (parent or {}).get("title") or title

    team_a, team_b = parse_lmts_teams(match_title)

    expired = bool(m.get("expired"))
    cond = m.get("conditionId") or (parent or {}).get("conditionId")

    rec = make_record(
        platform=PLATFORM_LMTS,
        platform_market_id=slug,
        platform_event_id=(str(cond) if cond else None),
        competition_slug=(parent or {}).get("slug"),
        event_name=match_title,
        match_name=match_title,
        team_a=team_a,
        team_b=team_b,
        market_title=title or match_title,
        market_type=m.get("marketType"),
        market_url=f"https://limitless.exchange/markets/{slug}",
        event_start_time=_lmts_start_iso(m),
        market_status=m.get("status") or ("expired" if expired else "active"),
        is_active=not expired,
        volume_total=to_float(m.get("volumeFormatted") or m.get("volume")),
        volume_24h=None,
        volume_currency="USDC",
        raw_json=m,
    )

    # У Limitless parent = матч, child = исход: Uruguay / Draw / Cape Verde
    if parent:
        child_title = (title or "").strip()

        if _title_is_team_or_draw(child_title, team_a, team_b):
            rec["canon_type_override"] = "match_winner"
            rec["canon_sel_override"] = canon_selection(
                "match_winner", child_title, team_a, team_b
            )

    return rec

def fetch_limitless() -> list[dict]:
    """
    Дискавери WC-маркетов на Limitless.

    Основной путь — обход /markets/active (опц. по категории WC через
    LMTS_CATEGORY_ID): детерминированно, ловит и group-матчи, и одиночные.
    Для групп эмитим родителя (несёт «A vs B» + агрегатный объём — это и есть
    публичный URL матча) и детей с проброшенным title родителя, чтобы тиминг
    и кросс-платформенная группировка работали. Семантический /markets/search
    оставлен fallback'ом — добирает то, чего нет в выбранной категории.

    Заметка про объём: родитель-группа («Czech Republic vs South Africa») по
    canon_market_type уходит в "other" → без group_key → в build_groups НЕ
    агрегируется (остаётся просто строкой матча с нужным URL). Дети дают
    match_winner+selection и группируются с другими площадками. Двойного счёта
    объёма нет.
    """
    records: list[dict] = []
    seen: set[str] = set()

    def _consider(rec: Optional[dict], src: dict, parent: Optional[dict]) -> None:
        if rec is None or rec["platform_market_id"] in seen:
            return
        title = src.get("title") or ""
        ptitle = (parent or {}).get("title") or ""
        host = parent or src
        cats = " ".join(str(c) for c in (host.get("categories") or []))
        tags = " ".join(str(t) for t in (host.get("tags") or []))
        ta, tb = parse_lmts_teams(ptitle or title)
        if not (text_is_world_cup(title, ptitle, cats, tags, rec["platform_market_id"])
                or (ta and tb)):
            return
        seen.add(rec["platform_market_id"])
        records.append(rec)

    def _ingest(items: Iterable[dict]) -> None:
        for it in items:
            if not isinstance(it, dict):
                continue
            nested = it.get("markets")
            if isinstance(nested, list) and nested:
                _consider(_lmts_record(it), it, None)            # сам матч/группа
                for child in nested:
                    if isinstance(child, dict):
                        _consider(_lmts_record(child, parent=it), child, it)
            else:
                _consider(_lmts_record(it), it, None)

    # 1) Активные маркеты (+ дети групп) — основной, детерминированный путь.
    _ingest(_lmts_iter_active(LMTS_CATEGORY_ID or None))

    # 2) Fallback: семантический поиск — добирает то, что мимо категории.
    for query in LMTS_SEARCH_QUERIES:
        page = 1
        while page <= LMTS_MAX_PAGES:
            params = {"query": query, "limit": 50, "page": page,
                      "similarityThreshold": LMTS_SIMILARITY}
            try:
                payload = http_get_json(f"{LMTS_API_URL}/markets/search", params=params)
            except Exception as e:  # noqa: BLE001 — одна выдача падает — продолжаем
                log.warning("limitless search %r p%d failed: %s", query, page, e)
                break
            ms = _lmts_extract_markets(payload)
            if not ms:
                break
            _ingest(ms)
            if len(ms) < 50:
                break
            page += 1

    log.info("limitless: %d World Cup markets (active+search)", len(records))
    return records


# ============================================================================
# === MATCHING (cross-platform grouping) =====================================
# ============================================================================
#
# We group the SAME logical bet across platforms using a deterministic key
# built from canonical fields — no embeddings needed for World Cup, because
# teams are a small closed vocabulary and the bet structure is regular.
#
#   group_key = hash( sorted(teamA, teamB) | market_type | selection )
#
# Date is intentionally NOT part of the key: each platform's end/close date
# means a different thing (resolution vs kickoff vs deadline), so they don't
# align. A team pair is essentially unique within a World Cup.

# Canonical team codes. Extend freely — keys are matched case-insensitively
# after stripping to letters. Unknown teams fall back to a slug of their name,
# so two platforms using the same spelling still group together.
TEAM_ALIASES: dict[str, str] = {
    "argentina": "ARG", "arg": "ARG",
    "brazil": "BRA", "brasil": "BRA", "bra": "BRA",
    "france": "FRA", "fra": "FRA",
    "england": "ENG", "eng": "ENG",
    "spain": "ESP", "espana": "ESP", "esp": "ESP",
    "germany": "GER", "deutschland": "GER", "ger": "GER",
    "portugal": "POR", "por": "POR",
    "netherlands": "NED", "holland": "NED", "ned": "NED",
    "belgium": "BEL", "bel": "BEL",
    "italy": "ITA", "italia": "ITA", "ita": "ITA",
    "croatia": "CRO", "cro": "CRO",
    "uruguay": "URU", "uru": "URU",
    "usa": "USA", "unitedstates": "USA", "us": "USA",
    "mexico": "MEX", "mex": "MEX",
    "canada": "CAN", "can": "CAN",
    "morocco": "MAR", "mar": "MAR",
    "japan": "JPN", "jpn": "JPN",
    "southkorea": "KOR", "korea": "KOR", "korearepublic": "KOR", "kor": "KOR",
    "colombia": "COL", "col": "COL",
    "senegal": "SEN", "sen": "SEN",
    "switzerland": "SUI", "sui": "SUI",
    "denmark": "DEN", "den": "DEN",
    "ecuador": "ECU", "ecu": "ECU",
    "australia": "AUS", "aus": "AUS",
    "poland": "POL", "pol": "POL",
    "serbia": "SRB", "srb": "SRB",
    "ghana": "GHA", "gha": "GHA",
    "nigeria": "NGA", "nga": "NGA",
    "cameroon": "CMR", "cmr": "CMR",
    "saudiarabia": "KSA", "ksa": "KSA",
    "iran": "IRN", "irn": "IRN",
    "wales": "WAL", "wal": "WAL",
    "qatar": "QAT", "qat": "QAT",
    "tunisia": "TUN", "tun": "TUN",
    "costarica": "CRC", "crc": "CRC",
    "austria": "AUT", "aut": "AUT",
    "turkey": "TUR", "turkiye": "TUR", "tur": "TUR",
    "norway": "NOR", "nor": "NOR",
    "egypt": "EGY", "egy": "EGY",
    "ivorycoast": "CIV", "cotedivoire": "CIV", "civ": "CIV",
    "algeria": "ALG", "alg": "ALG",
    "paraguay": "PAR", "par": "PAR",
    "chile": "CHI", "chi": "CHI",
    "peru": "PER", "per": "PER",
    "scotland": "SCO", "sco": "SCO",
    "ukraine": "UKR", "ukr": "UKR",
    "panama": "PAN", "pan": "PAN",
    "jamaica": "JAM", "jam": "JAM",
    "newzealand": "NZL", "nzl": "NZL",
    "southafrica": "RSA", "rsa": "RSA",
    "haiti": "HAI", "hai": "HAI",
    "uzbekistan": "UZB", "uzb": "UZB",
    "jordan": "JOR", "jor": "JOR",
    "capeverde": "CPV", "caboverde": "CPV", "cpv": "CPV",
    "curacao": "CUW", "cuw": "CUW",
    "honduras": "HON", "hon": "HON",
    "elsalvador": "SLV", "slv": "SLV",
    "guatemala": "GUA", "gua": "GUA",
    "venezuela": "VEN", "ven": "VEN",
    "bolivia": "BOL", "bol": "BOL",
    "iraq": "IRQ", "irq": "IRQ",
    "unitedarabemirates": "UAE", "uae": "UAE",
    "drcongo": "COD", "congodr": "COD", "cod": "COD",
    "mali": "MLI", "mli": "MLI",
    "burkinafaso": "BFA", "bfa": "BFA",
    "northmacedonia": "MKD", "mkd": "MKD",
    "slovenia": "SVN", "svn": "SVN",
    "slovakia": "SVK", "svk": "SVK",
    "czechia": "CZE", "czechrepublic": "CZE", "cze": "CZE",
    "hungary": "HUN", "hun": "HUN",
    "romania": "ROU", "rou": "ROU",
    "greece": "GRE", "gre": "GRE",
    "sweden": "SWE", "swe": "SWE",
    "finland": "FIN", "fin": "FIN",
    "ireland": "IRL", "republicofireland": "IRL", "irl": "IRL",
    "northernireland": "NIR", "nir": "NIR",
    "albania": "ALB", "alb": "ALB",
    "georgia": "GEO", "geo": "GEO",
    "russia": "RUS", "rus": "RUS",
    "china": "CHN", "chn": "CHN",
    "indonesia": "IDN", "idn": "IDN",
    "thailand": "THA", "tha": "THA",
    "vietnam": "VIE", "vie": "VIE",
    "india": "IND", "ind": "IND",
    "kuwait": "KUW", "kuw": "KUW",
    "bahrain": "BHR", "bhr": "BHR",
    "oman": "OMA", "oma": "OMA",
    "palestine": "PLE", "ple": "PLE",
    "syria": "SYR", "syr": "SYR",
    "lebanon": "LBN", "lbn": "LBN",
    "kenya": "KEN", "ken": "KEN",
    "zambia": "ZAM", "zam": "ZAM",
    "angola": "ANG", "ang": "ANG",
    "gabon": "GAB", "gab": "GAB",
    "guinea": "GUI", "gui": "GUI",
    "benin": "BEN", "ben": "BEN",
    "togo": "TOG", "tog": "TOG",
    "ugandacranes": "UGA", "uganda": "UGA", "uga": "UGA",
    "tanzania": "TAN", "tan": "TAN",
    "mozambique": "MOZ", "moz": "MOZ",
    "madagascar": "MAD", "mad": "MAD",
    "namibia": "NAM", "nam": "NAM",
    "mauritania": "MTN", "mtn": "MTN",
    "trinidadandtobago": "TRI", "tri": "TRI",
    "suriname": "SUR", "sur": "SUR",
    "nicaragua": "NCA", "nca": "NCA",
    "capeverdeislands": "CPV",
}


def canon_team(name: Optional[str]) -> Optional[str]:
    """Map a team name/code to a canonical code; slug fallback for unknowns."""
    if not name:
        return None
    raw = name.strip()
    letters = re.sub(r"[^a-z]", "", raw.lower())
    if not letters:
        return None
    if letters in TEAM_ALIASES:
        return TEAM_ALIASES[letters]
    # Looks like an already-canonical 3-letter code.
    if re.fullmatch(r"[A-Za-z]{3}", raw):
        return raw.upper()
    return letters[:16]


def known_team(name: Optional[str]) -> Optional[str]:
    """
    Strict team resolver for KEYING: returns a code only if `name` is a known
    World Cup nation (in TEAM_ALIASES) or a clean 3-letter code. Unknowns return
    None — unlike canon_team's slug fallback. This stops Limitless/Polymarket
    prop titles (e.g. "Ronaldo to record 5 duels won vs Congo DR") from being
    split into pseudo-teams and grouped: the player-phrase side resolves to None,
    so no match_key/group_key forms and the prop is correctly left ungrouped.
    """
    if not name:
        return None
    raw = name.strip()
    letters = re.sub(r"[^a-z]", "", raw.lower())
    if not letters:
        return None
    if letters in TEAM_ALIASES:
        return TEAM_ALIASES[letters]
    if re.fullmatch(r"[A-Za-z]{3}", raw):
        return raw.upper()
    return None


_STAT_RE = re.compile(
    r"\b(shots?|assists?|saves?|tackles?|passes?|cards?|fouls?|offsides?|"
    r"clearances?|interceptions?|touches?|crosses?)\b", re.IGNORECASE
)


def _title_is_team_or_draw(t: str, team_a: Optional[str], team_b: Optional[str]) -> bool:
    """True if the (short) title is essentially just a team name or draw/tie."""
    bare = re.sub(r"[^a-z]", "", t)
    if bare in ("draw", "tie"):
        return True
    ct = known_team(t)  # strict: only a real team name counts, not a slug
    for name in (team_a, team_b):
        c = known_team(name)
        if c and ct and ct == c:
            return True
    return False


def canon_market_type(
    title: Optional[str], team_a: Optional[str] = None, team_b: Optional[str] = None,
) -> str:
    t = (title or "").lower()
    # Player props first (named player + a stat), so they don't masquerade as
    # match markets just because the event has two teams.
    if _STAT_RE.search(t):
        return "player_prop"
    if ("scor" in t or "goal" in t) and ("first" in t or "anytime" in t or "last" in t):
        return "scorer"
    if "correct score" in t or re.search(r"(?<!\d)\d{1,2}\s*[-:]\s*\d{1,2}(?!\d)", t):
        return "correct_score"
    if "both teams" in t or "btts" in t:
        return "btts"
    if "total goals" in t or "o/u" in t or re.search(r"\b(over|under)\b", t):
        return "total_goals"
    # Tournament-level outright (win the World Cup / group / golden boot).
    if re.search(r"\b(world cup|the group|the tournament|the title|outright|"
                 r"top scorer|golden boot)\b", t) and \
       re.search(r"\b(win|winner|qualify|advance|reach|lift|finish)\b", t):
        return "outright"
    if any(w in t for w in ("qualify", "advance", "to reach", "progress", "knockout")):
        return "to_qualify"
    # Match winner: explicit wording OR the title is just a team / draw.
    if re.search(r"\b(win|wins|winner|beat|beats|moneyline|1x2|match result|"
                 r"double chance|to lift)\b", t) \
       or _title_is_team_or_draw(t, team_a, team_b):
        return "match_winner"
    return "other"


def _first_team_code(
    title: Optional[str], team_a: Optional[str], team_b: Optional[str],
) -> Optional[str]:
    """Canon code of whichever team is mentioned earliest in the title."""
    t = (title or "").lower()
    pos: dict[str, int] = {}
    for name in (team_a, team_b):
        code = canon_team(name)
        if not name or not code:
            continue
        idxs = []
        if name.lower() in t:
            idxs.append(t.find(name.lower()))
        m = re.search(rf"\b{re.escape(code.lower())}\b", t)
        if m:
            idxs.append(m.start())
        if idxs:
            pos[code] = min(idxs)
    if not pos:
        return None
    return min(pos, key=pos.get)


def canon_selection(
    mtype: str, title: Optional[str], team_a: Optional[str], team_b: Optional[str],
) -> str:
    t = (title or "").lower()
    if mtype == "correct_score":
        m = re.search(r"(\d{1,2})\s*[-:]\s*(\d{1,2})", t)
        if not m:
            return "cs"
        x, y = m.group(1), m.group(2)
        ca, cb = canon_team(team_a), canon_team(team_b)
        teams = sorted(v for v in (ca, cb) if v)
        # The score X-Y belongs to (first-mentioned team, second). Re-orient so
        # it always reads sorted[0]-sorted[1], making home/away phrasing match.
        first = _first_team_code(title, team_a, team_b) or ca
        if len(teams) == 2 and first == teams[1]:
            x, y = y, x
        return f"{x}-{y}"
    if mtype == "total_goals":
        m = re.search(r"\b(over|under)\b\D*(\d+(?:\.\d+)?)", t)
        return f"{m.group(1)}_{m.group(2)}" if m else "ou"
    if mtype == "btts":
        return "no" if re.search(r"\b(no|ng)\b", t) else "yes"
    if mtype == "match_winner":
        if re.search(r"\b(draw|tie)\b", t):
            return "draw"
        ct = canon_team(title)
        ca, cb = canon_team(team_a), canon_team(team_b)
        if ct and ct == ca:
            return ca
        if ct and ct == cb:
            return cb
        first = _first_team_code(title, team_a, team_b)
        return first or "winner"
    return "na"


# Only these bet types are aligned across platforms; others stay ungrouped.
GROUPABLE_TYPES = {"match_winner", "correct_score", "total_goals", "btts"}


def compute_keys(rec: dict) -> tuple[Optional[str], Optional[str], str, str]:
    """Return (match_key, group_key, market_type_canon, selection)."""
    # Strict resolver: both sides must be real, known WC teams to form any key.
    # This is what keeps prop titles (one side is a player phrase → None) out of
    # the groups entirely.
    ca, cb = known_team(rec.get("team_a")), known_team(rec.get("team_b"))
    teams = sorted(x for x in (ca, cb) if x)
    # Adapters with structured metadata (e.g. ADI) can override canon directly.
    mtype = rec.get("canon_type_override") or canon_market_type(
        rec.get("market_title"), rec.get("team_a"), rec.get("team_b"))
    sel = rec.get("canon_sel_override") or canon_selection(
        mtype, rec.get("market_title"), rec.get("team_a"), rec.get("team_b"))
    match_key = group_key = None
    if len(teams) == 2:
        match_key = hashlib.sha1("|".join(teams).encode()).hexdigest()[:16]
        # Group only alignable bet types, and only with a concrete selection.
        if mtype in GROUPABLE_TYPES and sel not in ("winner", "na", "cs", "ou"):
            group_key = hashlib.sha1(
                "|".join(teams + [mtype, sel]).encode()
            ).hexdigest()[:16]
    return match_key, group_key, mtype, sel


def enrich_records(records: list[dict]) -> None:
    """Attach match_key / group_key / market_type_canon / selection in place."""
    for r in records:
        mk, gk, mtype, sel = compute_keys(r)
        ca, cb = known_team(r.get("team_a")), known_team(r.get("team_b"))
        teams = sorted(x for x in (ca, cb) if x)
        r["match_key"] = mk
        r["group_key"] = gk
        r["market_type_canon"] = mtype
        r["selection"] = sel
        r["team_a_canon"] = teams[0] if len(teams) == 2 else (ca or None)
        r["team_b_canon"] = teams[1] if len(teams) == 2 else (cb or None)


_USD_LIKE = {"USD", "USDC"}


def build_groups(records: list[dict]) -> list[dict]:
    """
    Aggregate active, group-keyed records into cross-platform groups.
    Volumes are kept per-platform (different currencies); a USD-comparable
    sum is computed over USD/USDC members only.
    """
    buckets: dict[str, dict] = {}
    for r in records:
        gk = r.get("group_key")
        if not gk or not r.get("is_active"):
            continue
        g = buckets.get(gk)
        if g is None:
            g = buckets[gk] = {
                "group_key": gk,
                "match_key": r.get("match_key"),
                "team_a": r.get("team_a_canon"),
                "team_b": r.get("team_b_canon"),
                "market_type": r.get("market_type_canon"),
                "selection": r.get("selection"),
                "sample_title": r.get("market_title"),
                "_platforms": set(),
                "_vol": defaultdict(lambda: {"total": 0.0, "h24": 0.0,
                                             "currency": None}),
                "_urls": {},
                "usd_volume_total": 0.0,
                "usd_volume_24h": 0.0,
            }
        plat = r["platform"]
        g["_platforms"].add(plat)
        v = g["_vol"][plat]
        v["currency"] = r.get("volume_currency")
        if r.get("volume_total") is not None:
            v["total"] += float(r["volume_total"])
        if r.get("volume_24h") is not None:
            v["h24"] += float(r["volume_24h"])
        if plat not in g["_urls"] and r.get("market_url"):
            g["_urls"][plat] = r["market_url"]
        if (r.get("volume_currency") in _USD_LIKE):
            g["usd_volume_total"] += float(r.get("volume_total") or 0)
            g["usd_volume_24h"] += float(r.get("volume_24h") or 0)

    out: list[dict] = []
    for g in buckets.values():
        platforms = sorted(g.pop("_platforms"))
        volumes = {p: {"total": round(v["total"], 4),
                       "h24": round(v["h24"], 4),
                       "currency": v["currency"]}
                   for p, v in g.pop("_vol").items()}
        urls = g.pop("_urls")
        out.append({
            "group_key": g["group_key"],
            "match_key": g["match_key"],
            "team_a": g["team_a"],
            "team_b": g["team_b"],
            "market_type": g["market_type"],
            "selection": g["selection"],
            "sample_title": g["sample_title"],
            "platforms": platforms,
            "platform_count": len(platforms),
            "volumes": volumes,
            "urls": urls,
            "usd_volume_total": round(g["usd_volume_total"], 4),
            "usd_volume_24h": round(g["usd_volume_24h"], 4),
            "is_active": True,
        })
    return out


# ============================================================================
# === STORAGE: SUPABASE ======================================================
# ============================================================================

def _supabase():
    from supabase import create_client
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return client.schema(SUPABASE_SCHEMA) if SUPABASE_SCHEMA != "public" else client


def _chunks(seq: list, n: int) -> Iterable[list]:
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


_MARKET_COLUMNS = (
    "platform", "platform_market_id", "platform_event_id", "competition_slug",
    "competition_name", "event_name", "match_name", "team_a", "team_b",
    "market_title", "market_type", "market_type_canon", "selection",
    "match_key", "group_key", "market_url", "event_start_time", "market_status",
    "is_active", "volume_total", "volume_24h", "volume_currency",
    "raw_updated_at", "raw_json", "last_seen_at", "updated_at",
)


def _read_platform_state(sb, platform: str) -> dict[str, dict]:
    """
    Read ALL current rows for a platform (paginated — Supabase caps a single
    response at ~1000 rows, and Polymarket alone has >11k). Returns
    {platform_market_id: {volume_total, volume_24h, market_status, is_active}}.
    Used both to detect volume changes and to find disappeared markets.
    """
    out: dict[str, dict] = {}
    start, PAGE = 0, 1000
    while True:
        res = (sb.table("markets_normalized")
                 .select("platform_market_id,volume_total,volume_24h,"
                         "market_status,is_active")
                 .eq("platform", platform)
                 .range(start, start + PAGE - 1)
                 .execute())
        rows = res.data or []
        for r in rows:
            out[r["platform_market_id"]] = r
        if len(rows) < PAGE:
            break
        start += PAGE
    return out


def _vol_eq(a: Any, b: Any) -> bool:
    fa, fb = to_float(a), to_float(b)
    if fa is None or fb is None:
        return fa is fb
    return abs(fa - fb) < 1e-9


def _is_changed(rec: dict, prev: Optional[dict]) -> bool:
    """True if this record is new or its volume/status/active flag moved."""
    if prev is None:
        return True
    return not (
        _vol_eq(rec.get("volume_total"), prev.get("volume_total"))
        and _vol_eq(rec.get("volume_24h"), prev.get("volume_24h"))
        and (rec.get("market_status") or "") == (prev.get("market_status") or "")
        and bool(rec.get("is_active")) == bool(prev.get("is_active"))
    )


def upsert_markets(sb, records: list[dict]) -> int:
    """Upsert current-state rows on (platform, platform_market_id)."""
    if not records:
        return 0
    now = NOW()
    rows = []
    for r in records:
        row = {k: r.get(k) for k in _MARKET_COLUMNS}
        row["last_seen_at"] = now
        row["updated_at"] = now
        # first_seen_at intentionally omitted → DB default on insert, kept on update.
        rows.append(row)
    written = 0
    for chunk in _chunks(rows, 500):
        sb.table("markets_normalized").upsert(
            chunk, on_conflict="platform,platform_market_id"
        ).execute()
        written += len(chunk)
    return written


def insert_snapshots(sb, records: list[dict]) -> int:
    """Insert one snapshot row per supplied market (caller filters to changed)."""
    if not records:
        return 0
    now = NOW()
    rows = [{
        "platform": r["platform"],
        "platform_market_id": r["platform_market_id"],
        "snapshot_at": now,
        "market_status": r["market_status"],
        "is_active": r["is_active"],
        "volume_total": r["volume_total"],
        "volume_24h": r["volume_24h"],
        "raw_json": r["raw_json"],
    } for r in records]
    written = 0
    for chunk in _chunks(rows, 500):
        sb.table("market_snapshots").insert(chunk).execute()
        written += len(chunk)
    return written


def mark_missing_inactive(sb, platform: str, seen_ids: set[str],
                          prev_state: dict[str, dict]) -> int:
    """
    Mark previously-active markets of this platform that were NOT seen this run
    as inactive (never delete). Uses the already-read state (fully paginated),
    so it stays correct even with >1000 active rows.
    """
    active_ids = {pid for pid, r in prev_state.items() if r.get("is_active")}
    to_close = list(active_ids - seen_ids)
    if not to_close:
        return 0
    now = NOW()
    for chunk in _chunks(to_close, 100):
        (sb.table("markets_normalized")
            .update({"is_active": False, "market_status": "inactive",
                     "updated_at": now})
            .eq("platform", platform)
            .in_("platform_market_id", chunk)
            .execute())
    return len(to_close)


def upsert_groups(sb, groups: list[dict]) -> int:
    """Upsert cross-platform groups on group_key, then mark missing inactive."""
    now = NOW()
    seen = {g["group_key"] for g in groups}
    rows = [{**g, "updated_at": now} for g in groups]
    for chunk in _chunks(rows, 500):
        sb.table("market_groups").upsert(chunk, on_conflict="group_key").execute()

    # Mark groups that have no active members this run as inactive.
    res = (sb.table("market_groups")
             .select("group_key")
             .eq("is_active", True)
             .execute())
    db_keys = {row["group_key"] for row in (res.data or [])}
    to_close = list(db_keys - seen)
    for chunk in _chunks(to_close, 100):
        (sb.table("market_groups")
            .update({"is_active": False, "platform_count": 0, "updated_at": now})
            .in_("group_key", chunk)
            .execute())
    return len(rows)


# ============================================================================
# === STORAGE: GOOGLE SHEETS (optional mirror) ===============================
# ============================================================================

def mirror_to_sheets(records: list[dict]) -> None:
    """Optional debug mirror. Best-effort: never breaks the run."""
    if not (GOOGLE_SHEETS_ENABLED and GOOGLE_SHEET_ID
            and GOOGLE_SERVICE_ACCOUNT_JSON_BASE64):
        return
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        info = json.loads(base64.b64decode(GOOGLE_SERVICE_ACCOUNT_JSON_BASE64))
        creds = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(GOOGLE_SHEET_ID)
        ws = sh.sheet1
        header = ["platform", "match_name", "market_title", "team_a", "team_b",
                  "market_status", "is_active", "volume_total", "volume_24h",
                  "volume_currency", "event_start_time", "market_url"]
        rows = [header] + [[
            r["platform"], r.get("match_name"), r.get("market_title"),
            r.get("team_a"), r.get("team_b"), r.get("market_status"),
            r.get("is_active"), r.get("volume_total"), r.get("volume_24h"),
            r.get("volume_currency"), r.get("event_start_time"), r.get("market_url"),
        ] for r in records]
        ws.clear()
        ws.update(rows, value_input_option="RAW")
        log.info("sheets: mirrored %d rows", len(records))
    except Exception as e:  # noqa: BLE001 — debug mirror must never break the run
        log.warning("sheets mirror failed (ignored): %s", e)


# ============================================================================
# === MAIN ===================================================================
# ============================================================================

SOURCES = [
    (PLATFORM_POLY, ENABLE_POLYMARKET, fetch_polymarket),
    (PLATFORM_KALSHI, ENABLE_KALSHI, fetch_kalshi),
    (PLATFORM_ADI, ENABLE_ADI, fetch_adi),
    (PLATFORM_LMTS, ENABLE_LIMITLESS, fetch_limitless),
]


def main() -> None:
    t0 = time.time()
    sb = _supabase()

    all_records: list[dict] = []
    seen_by_platform: dict[str, set[str]] = {}
    summary: list[str] = []

    for platform, enabled, fetch_fn in SOURCES:
        if not enabled:
            log.info("%s: disabled", platform)
            continue
        try:
            recs = fetch_fn()
        except Exception as e:  # noqa: BLE001 — isolate per-source failures
            log.exception("%s: fetch failed (continuing)", platform)
            summary.append(f"{platform}: FETCH FAILED ({e})")
            continue

        seen_ids = {r["platform_market_id"] for r in recs}
        seen_by_platform[platform] = seen_ids
        all_records.extend(recs)
        summary.append(f"{platform}: normalized {len(recs)}")

    # Cross-platform enrichment: canonical keys for grouping.
    enrich_records(all_records)

    # Read current DB state once per platform (paginated), then write only what
    # actually changed. Keeps current-state + group volumes fresh while the
    # append-only snapshots table stays tiny.
    prev_state: dict[str, dict] = {}
    for platform in seen_by_platform:
        try:
            prev_state[platform] = _read_platform_state(sb, platform)
        except Exception:
            log.exception("%s: state read failed", platform)
            prev_state[platform] = {}

    changed = [
        r for r in all_records
        if _is_changed(r, prev_state.get(r["platform"], {}).get(r["platform_market_id"]))
    ]

    # Persist only changed current-state rows.
    upserted = snap = 0
    try:
        upserted = upsert_markets(sb, changed)
    except Exception:
        log.exception("storage write failed")
        raise

    # Snapshot only changed markets, and (by default) only group members —
    # the only rows a current-volumes dashboard could chart over time.
    if ENABLE_SNAPSHOTS:
        snap_src = [r for r in changed
                    if (not SNAPSHOT_GROUPS_ONLY) or r.get("group_key")]
        try:
            snap = insert_snapshots(sb, snap_src)
        except Exception:
            log.exception("snapshot write failed")

    # Mark missing-as-inactive only for platforms we actually fetched OK,
    # reusing the state we already read.
    closed_total = 0
    for platform in seen_by_platform:
        try:
            closed = mark_missing_inactive(
                sb, platform, seen_by_platform[platform], prev_state.get(platform, {}))
            closed_total += closed
            summary.append(f"{platform}: closed {closed}")
        except Exception:
            log.exception("%s: mark-inactive failed", platform)

    # Build + persist cross-platform groups (same bet across platforms).
    groups = build_groups(all_records)
    multi = sum(1 for g in groups if g["platform_count"] >= 2)
    try:
        upsert_groups(sb, groups)
        summary.append(f"groups: {len(groups)} total, {multi} on 2+ platforms")
    except Exception:
        log.exception("group write failed")

    # Optional human-review mirror.
    mirror_to_sheets(all_records)

    dt = time.time() - t0
    log.info("=== RUN SUMMARY (%.1fs) ===", dt)
    for line in summary:
        log.info("  %s", line)
    log.info("  changed/upserted=%d snapshots=%d closed=%d fetched=%d",
             upserted, snap, closed_total, len(all_records))


if __name__ == "__main__":
    main()
