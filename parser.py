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


# --- Supabase ---
SUPABASE_URL = _env("SUPABASE_URL", required=True)
SUPABASE_KEY = _env("SUPABASE_KEY", required=True)  # use service_role key (write)
SUPABASE_SCHEMA = _env("SUPABASE_SCHEMA", "public")

# --- Source toggles ---
ENABLE_POLYMARKET = _env_bool("ENABLE_POLYMARKET", True)
ENABLE_KALSHI = _env_bool("ENABLE_KALSHI", True)
ENABLE_ADI = _env_bool("ENABLE_ADI", True)
ENABLE_LIMITLESS = _env_bool("ENABLE_LIMITLESS", True)

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
# Polymarket: comma-separated tag slugs to query directly, e.g. "world-cup".
POLY_TAG_SLUGS = [s.strip() for s in _env("POLY_TAG_SLUGS").split(",") if s.strip()]
POLY_MAX_PAGES = _env_int("POLY_MAX_PAGES", 50)  # keyword-scan page cap
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
# Limitless: automationType filter (manual | lumy | sports); empty scans all.
LMTS_AUTOMATION_TYPE = _env("LMTS_AUTOMATION_TYPE")
LMTS_MAX_PAGES = _env_int("LMTS_MAX_PAGES", 50)

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


def parse_teams(title: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Best-effort extraction of (team_a, team_b) from a match-style title."""
    if not title:
        return None, None
    # Cut off anything after a colon/parenthesis (often "... : To win").
    core = re.split(r"[:(]", title, maxsplit=1)[0].strip()
    parts = _VS_SPLIT.split(core)
    if len(parts) == 2:
        a, b = parts[0].strip(" -"), parts[1].strip(" -")
        if a and b and len(a) < 60 and len(b) < 60:
            return a, b
    return None, None


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
            market_type="match_winner" if (m_team_a or team_a) else "binary",
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


def fetch_polymarket() -> list[dict]:
    """
    Discover World Cup events on Gamma and normalize their markets.

    Two discovery modes:
      1. If POLY_TAG_SLUGS set → query /events?tag_slug=<slug> (precise).
      2. Else → page /events?closed=false and keep events whose title/slug
         matches WORLD_CUP_KEYWORDS (page cap = POLY_MAX_PAGES).
    """
    events: list[dict] = []

    def _coerce_events(payload: Any) -> list[dict]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            return payload.get("data") or payload.get("events") or []
        return []

    if POLY_TAG_SLUGS:
        for slug in POLY_TAG_SLUGS:
            offset = 0
            while True:
                payload = http_get_json(
                    f"{GAMMA_API_URL}/events",
                    params={"closed": "false", "tag_slug": slug,
                            "limit": 100, "offset": offset},
                )
                page = _coerce_events(payload)
                events.extend(page)
                if len(page) < 100:
                    break
                offset += 100
        log.info("polymarket: %d events via tag slugs %s", len(events), POLY_TAG_SLUGS)
    else:
        offset = 0
        for _ in range(POLY_MAX_PAGES):
            payload = http_get_json(
                f"{GAMMA_API_URL}/events",
                params={"closed": "false", "limit": 100, "offset": offset,
                        "order": "startDate", "ascending": "false"},
            )
            page = _coerce_events(payload)
            if not page:
                break
            for ev in page:
                if text_is_world_cup(ev.get("title"), ev.get("slug"),
                                     ev.get("description")):
                    events.append(ev)
            if len(page) < 100:
                break
            offset += 100
        log.info("polymarket: %d World Cup events found by keyword scan", len(events))
        # TODO: keyword scan can miss events outside the most-recent window.
        # For production precision, set POLY_TAG_SLUGS to the World Cup tag slug
        # (discover it via GET /tags or the event page URL on polymarket.com).

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

def _kalshi_market_url(event_ticker: Optional[str]) -> Optional[str]:
    # Best-effort; Kalshi canonical URLs are series/event based.
    return f"https://kalshi.com/markets/{event_ticker}" if event_ticker else None


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
            market_url=_kalshi_market_url(event_ticker),
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

def _adi_market_url(market_slug: Optional[str], event_slug: Optional[str]) -> Optional[str]:
    slug = market_slug or event_slug
    return f"https://app.adipredictstreet.com/markets/{slug}" if slug else None


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
            out.append(make_record(
                platform=PLATFORM_ADI,
                platform_market_id=mid,
                platform_event_id=ev_id,
                competition_slug=match_slug,
                competition_name=comp_name,
                event_name=ev_title,
                match_name=match_title,
                team_a=ev_team_a,
                team_b=ev_team_b,
                market_title=m.get("title") or m.get("question") or ev_title,
                market_type=m.get("type") or m.get("marketType"),
                market_url=_adi_market_url(m.get("slug"), ev_slug),
                event_start_time=ev_start,
                market_status=status,
                is_active=(status or "").upper() in ("OPEN", "PRE_MARKET", "PAUSED"),
                # Volume is reported at event level (USDC); attach to each market.
                volume_total=to_float(m.get("totalVolume") or ev_vol_total),
                volume_24h=to_float(m.get("volume24h") or ev_vol_24h),
                volume_currency="USDC",
                raw_updated_at=m.get("updatedAt") or ev.get("updatedAt"),
                raw_json=m,
            ))
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

def _lmts_flatten(items: list) -> list[dict]:
    """A group entry nests its child markets under `markets: [...]`; flatten."""
    flat: list[dict] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        nested = it.get("markets")
        if isinstance(nested, list) and nested:
            for child in nested:
                if isinstance(child, dict):
                    flat.append(child)
        else:
            flat.append(it)
    return flat


def _lmts_start_iso(m: dict) -> Optional[str]:
    ets = m.get("expirationTimestamp")
    if ets:
        try:
            return datetime.fromtimestamp(int(ets) / 1000, tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OSError):
            return None
    return m.get("expirationDate") or None


def fetch_limitless() -> list[dict]:
    """
    Discover World Cup markets on Limitless via /markets/active (paginated).
    This endpoint returns full market objects with title + volume in USDC
    (`volumeFormatted`, whole-USDC string) + openInterest + liquidity.

    Notes:
      - There's no 24h-volume field here, so volume_24h stays null; volume_total
        is the all-time USDC volume (`volumeFormatted`).
      - Set LMTS_AUTOMATION_TYPE=sports to fetch only auto-generated sports
        markets (cheaper). Default scans all active markets and keyword-filters.
    """
    items: list[dict] = []
    page = 1
    while page <= LMTS_MAX_PAGES:
        params: dict[str, object] = {"page": page, "limit": 100, "sortBy": "newest"}
        if LMTS_AUTOMATION_TYPE:
            params["automationType"] = LMTS_AUTOMATION_TYPE
        payload = http_get_json(f"{LMTS_API_URL}/markets/active", params=params)
        data = payload.get("data") if isinstance(payload, dict) else None
        if not data:
            break
        items.extend(data)
        total = int(payload.get("totalMarketsCount") or 0)
        if len(data) < 100 or page * 100 >= total:
            break
        page += 1

    records: list[dict] = []
    seen: set[str] = set()
    for m in _lmts_flatten(items):
        slug = m.get("slug") or (str(m.get("id")) if m.get("id") else None)
        if not slug or slug in seen:
            continue
        title = m.get("title") or ""
        cats = " ".join(str(c) for c in (m.get("categories") or []))
        tags = " ".join(str(t) for t in (m.get("tags") or []))
        if not text_is_world_cup(title, m.get("description"), cats, tags, slug):
            continue
        seen.add(slug)
        team_a, team_b = parse_teams(title)
        expired = bool(m.get("expired"))
        records.append(make_record(
            platform=PLATFORM_LMTS,
            platform_market_id=slug,
            platform_event_id=(str(m["conditionId"]) if m.get("conditionId") else None),
            event_name=title,
            match_name=title,
            team_a=team_a,
            team_b=team_b,
            market_title=title,
            market_type=m.get("marketType"),
            market_url=f"https://limitless.exchange/markets/{slug}",
            event_start_time=_lmts_start_iso(m),
            market_status=m.get("status") or ("expired" if expired else "active"),
            is_active=not expired,
            volume_total=to_float(m.get("volumeFormatted")),  # whole USDC
            volume_24h=None,  # not exposed by this endpoint
            volume_currency="USDC",
            raw_updated_at=None,
            raw_json=m,
        ))
    log.info("limitless: %d World Cup markets (USDC volume) from %d active",
             len(records), len(items))
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


def canon_market_type(title: Optional[str], src_type: Optional[str]) -> str:
    t = f"{src_type or ''} {title or ''}".lower()
    if "correct score" in t or re.search(r"\b\d{1,2}\s*[-:]\s*\d{1,2}\b", t):
        return "correct_score"
    if "both teams" in t or "btts" in t:
        return "btts"
    if "total goals" in t or "o/u" in t or re.search(r"\b(over|under)\b", t):
        return "total_goals"
    if ("scor" in t or "goal" in t) and ("first" in t or "anytime" in t or "last" in t):
        return "first_scorer"
    if any(w in t for w in ("qualify", "advance", "to reach", "progress", "knockout")):
        return "to_qualify"
    if any(w in t for w in ("to win", "winner", "1x2", "moneyline",
                            "match result", "draw", " beat ", " win ", "to lift")):
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
        if "draw" in t or " tie" in t:
            return "draw"
        for name in (team_a, team_b):
            code = canon_team(name)
            if name and code and (name.lower() in t or code.lower() in t):
                return code
        return "winner"
    return "na"


def compute_keys(rec: dict) -> tuple[Optional[str], Optional[str], str, str]:
    """Return (match_key, group_key, market_type_canon, selection)."""
    ca, cb = canon_team(rec.get("team_a")), canon_team(rec.get("team_b"))
    teams = sorted(x for x in (ca, cb) if x)
    mtype = canon_market_type(rec.get("market_title"), rec.get("market_type"))
    sel = canon_selection(mtype, rec.get("market_title"),
                          rec.get("team_a"), rec.get("team_b"))
    if len(teams) == 2:
        match_key = hashlib.sha1("|".join(teams).encode()).hexdigest()[:16]
        group_key = hashlib.sha1(
            "|".join(teams + [mtype, sel]).encode()
        ).hexdigest()[:16]
    else:
        match_key = group_key = None  # not enough structure to group reliably
    return match_key, group_key, mtype, sel


def enrich_records(records: list[dict]) -> None:
    """Attach match_key / group_key / market_type_canon / selection in place."""
    for r in records:
        mk, gk, mtype, sel = compute_keys(r)
        ca, cb = canon_team(r.get("team_a")), canon_team(r.get("team_b"))
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
    for chunk in _chunks(rows, 200):
        sb.table("markets_normalized").upsert(
            chunk, on_conflict="platform,platform_market_id"
        ).execute()
        written += len(chunk)
    return written


def insert_snapshots(sb, records: list[dict]) -> int:
    """Insert one snapshot row per observed market."""
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
    for chunk in _chunks(rows, 200):
        sb.table("market_snapshots").insert(chunk).execute()
        written += len(chunk)
    return written


def mark_missing_inactive(sb, platform: str, seen_ids: set[str]) -> int:
    """
    Mark previously-active markets of this platform that were NOT seen this run
    as inactive (never delete). Diff is computed in Python to avoid huge NOT IN.
    """
    res = (sb.table("markets_normalized")
             .select("platform_market_id")
             .eq("platform", platform)
             .eq("is_active", True)
             .execute())
    db_ids = {row["platform_market_id"] for row in (res.data or [])}
    to_close = list(db_ids - seen_ids)
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
    for chunk in _chunks(rows, 200):
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

    # Persist current state + snapshots.
    upserted = snap = 0
    try:
        upserted = upsert_markets(sb, all_records)
        snap = insert_snapshots(sb, all_records)
    except Exception:
        log.exception("storage write failed")
        raise

    # Mark missing-as-inactive only for platforms we actually fetched OK.
    closed_total = 0
    for platform in seen_by_platform:
        try:
            closed = mark_missing_inactive(sb, platform, seen_by_platform[platform])
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
    log.info("  upserted=%d snapshots=%d closed=%d total_records=%d",
             upserted, snap, closed_total, len(all_records))


if __name__ == "__main__":
    main()
