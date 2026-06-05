"""
ClearEye Database — SQLite persistence layer (#112, #138)
WAL mode + busy_timeout + retry-on-lock + auto-reconnect.

Schema:
  deals (id TEXT PK, status TEXT, created_at TEXT, om_text TEXT, result JSON, user_email TEXT)
  watchlist / deal_notes / api_usage — see init_db()
"""
from __future__ import annotations
import json
import os
import sqlite3
import threading
import time
import functools
from datetime import datetime, timezone
from pathlib import Path

# DB_PATH: prefer /data/cleareye.db on Render (persistent disk mount) or
# DATA_DIR env var, fall back to the module directory for local dev.
_data_dir = Path(os.environ.get("DATA_DIR", "")) if os.environ.get("DATA_DIR") else None
if _data_dir is None:
    _render_disk = Path("/data")
    _data_dir = _render_disk if _render_disk.exists() else Path(__file__).parent
DB_PATH = _data_dir / "cleareye.db"
_local = threading.local()   # thread-local connection

# ---------------------------------------------------------------------------
# Connection management (#138) — WAL + busy_timeout + auto-reconnect
# ---------------------------------------------------------------------------

_BUSY_TIMEOUT_MS = 10_000   # 10 s — SQLite will retry internally on "database locked"


def _conn() -> sqlite3.Connection:
    """
    Return a thread-local SQLite connection.
    Creates (or recreates after error) with WAL + busy_timeout.
    """
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = _new_conn()
    return _local.conn


def _new_conn() -> sqlite3.Connection:
    """Open a fresh connection with all recommended PRAGMAs set."""
    con = sqlite3.connect(str(DB_PATH), check_same_thread=False, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")  # wait up to 10 s on lock (#138)
    con.execute("PRAGMA journal_mode=WAL")                  # WAL: readers never block writers
    con.execute("PRAGMA synchronous=NORMAL")                # safe + fast
    con.execute("PRAGMA cache_size=-8000")                  # 8 MB page cache
    con.execute("PRAGMA temp_store=MEMORY")
    return con


def _reset_conn():
    """Invalidate the thread-local connection so the next call rebuilds it."""
    try:
        if hasattr(_local, "conn") and _local.conn:
            _local.conn.close()
    except Exception:
        pass
    _local.conn = None


# ---------------------------------------------------------------------------
# Retry decorator (#138) — wrap writes to survive transient "database locked"
# ---------------------------------------------------------------------------

def _with_retry(max_attempts: int = 5, base_delay: float = 0.1):
    """
    Decorator: retry the wrapped function up to max_attempts times.
    On OperationalError (locked / disk full) it backs off exponentially.
    On a stale/closed connection it forces a reconnect and retries once.
    """
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_attempts):
                try:
                    return fn(*args, **kwargs)
                except sqlite3.OperationalError as exc:
                    last_exc = exc
                    msg = str(exc).lower()
                    if "locked" in msg or "busy" in msg:
                        # busy_timeout handles most waits, but Python-level retries
                        # catch edge cases where multiple Python threads race on commit
                        wait = base_delay * (2 ** attempt)
                        time.sleep(min(wait, 2.0))
                        continue
                    if "closed" in msg or "no such table" in msg:
                        _reset_conn()
                        continue
                    raise
                except Exception as exc:
                    last_exc = exc
                    _reset_conn()  # unknown error — reset connection for next caller
                    raise
            raise last_exc  # exhausted retries
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Schema init
# ---------------------------------------------------------------------------

def init_db():
    """Create tables if they don't exist. Safe to call multiple times."""
    con = _conn()
    con.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id          TEXT PRIMARY KEY,
            status      TEXT NOT NULL DEFAULT 'queued',
            created_at  TEXT NOT NULL,
            om_text     TEXT,
            result      TEXT,
            user_email  TEXT,
            deal_name   TEXT,
            verdict     TEXT,
            confidence  INTEGER
        )
    """)
    # Watchlist table (#128)
    con.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            deal_key    TEXT PRIMARY KEY,
            deal_json   TEXT NOT NULL,
            added_at    TEXT NOT NULL,
            user_email  TEXT
        )
    """)
    # Deal notes (#128)
    con.execute("""
        CREATE TABLE IF NOT EXISTS deal_notes (
            deal_key    TEXT PRIMARY KEY,
            note        TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            user_email  TEXT
        )
    """)
    # API usage tracking (#124)
    con.execute("""
        CREATE TABLE IF NOT EXISTS api_usage (
            provider    TEXT,
            date        TEXT,
            call_count  INTEGER,
            PRIMARY KEY(provider, date)
        )
    """)
    # Saved searches for deal aggregator (#139)
    con.execute("""
        CREATE TABLE IF NOT EXISTS saved_searches (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            filters     TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            last_run    TEXT,
            user_email  TEXT
        )
    """)
    # Deal alerts (#134)
    con.execute("""
        CREATE TABLE IF NOT EXISTS deal_alerts (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            filters     TEXT NOT NULL,
            email       TEXT NOT NULL,
            active      INTEGER NOT NULL DEFAULT 1,
            last_checked TEXT,
            last_match_count INTEGER DEFAULT 0,
            seen_keys   TEXT DEFAULT '[]',
            created_at  TEXT NOT NULL,
            user_email  TEXT
        )
    """)
    # Deal pipeline Kanban (#133)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_deals (
            id          TEXT PRIMARY KEY,
            deal_name   TEXT NOT NULL,
            address     TEXT,
            market      TEXT,
            asking_price REAL,
            units       INTEGER,
            stage       TEXT NOT NULL DEFAULT 'Screening',
            assigned_to TEXT,
            notes       TEXT,
            job_id      TEXT,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            user_email  TEXT
        )
    """)
    # Add color_tag column to pipeline_deals if not present (#193)
    try:
        con.execute("ALTER TABLE pipeline_deals ADD COLUMN color_tag TEXT DEFAULT 'none'")
    except Exception:
        pass  # column already exists

    # #220: Add stage_entered_at column for stall detection
    try:
        con.execute("ALTER TABLE pipeline_deals ADD COLUMN stage_entered_at TEXT")
    except Exception:
        pass  # column already exists

    # #225: Deal outcome feedback loop
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_deal_outcomes (
            deal_id             TEXT PRIMARY KEY,
            actual_irr          REAL,
            actual_equity_multiple REAL,
            closed_date         TEXT,
            notes               TEXT,
            recorded_at         TEXT NOT NULL
        )
    """)

    # Pipeline activity log (#133)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_activity (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            deal_id     TEXT NOT NULL,
            action      TEXT NOT NULL,
            detail      TEXT,
            created_at  TEXT NOT NULL,
            user_email  TEXT
        )
    """)
    # Due diligence checklist items (#142)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dd_checklist (
            id          TEXT PRIMARY KEY,
            deal_id     TEXT NOT NULL,
            category    TEXT NOT NULL DEFAULT 'general',
            title       TEXT NOT NULL,
            assignee    TEXT,
            due_date    TEXT,
            completed   INTEGER NOT NULL DEFAULT 0,
            notes       TEXT,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
        )
    """)
    # Deal document vault (#142)
    con.execute("""
        CREATE TABLE IF NOT EXISTS deal_documents (
            id          TEXT PRIMARY KEY,
            deal_id     TEXT NOT NULL,
            filename    TEXT NOT NULL,
            category    TEXT NOT NULL DEFAULT 'other',
            file_size   INTEGER,
            stored_path TEXT NOT NULL,
            uploaded_at TEXT NOT NULL,
            uploaded_by TEXT,
            extracted_text TEXT
        )
    """)
    # LP shared report links (#136)
    con.execute("""
        CREATE TABLE IF NOT EXISTS shared_links (
            token       TEXT PRIMARY KEY,
            job_id      TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            expires_at  TEXT,
            password    TEXT,
            label       TEXT,
            view_count  INTEGER DEFAULT 0,
            last_viewed TEXT,
            user_email  TEXT
        )
    """)
    # LP engagement events (#144)
    con.execute("""
        CREATE TABLE IF NOT EXISTS lp_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            token       TEXT NOT NULL,
            job_id      TEXT NOT NULL,
            event_type  TEXT NOT NULL,
            section     TEXT,
            duration_s  REAL,
            lp_ua       TEXT,
            lp_ip       TEXT,
            created_at  TEXT NOT NULL
        )
    """)
    # Custom scoring profiles (#141)
    con.execute("""
        CREATE TABLE IF NOT EXISTS scoring_profiles (
            id          TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            weights     TEXT NOT NULL,
            is_active   INTEGER NOT NULL DEFAULT 0,
            created_at  TEXT NOT NULL,
            user_email  TEXT
        )
    """)
    # Magic-link tokens — persistent across restarts (#146)
    con.execute("""
        CREATE TABLE IF NOT EXISTS magic_link_tokens (
            token       TEXT PRIMARY KEY,
            email       TEXT NOT NULL,
            created_at  TEXT NOT NULL,
            expires_at  TEXT NOT NULL,
            used        INTEGER NOT NULL DEFAULT 0
        )
    """)
    # Deal tagging system (#169)
    con.execute("""
        CREATE TABLE IF NOT EXISTS deal_tags (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            color       TEXT NOT NULL DEFAULT '#58a6ff',
            email       TEXT,
            created_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_deal_tags (
            deal_id     TEXT NOT NULL,
            tag_id      INTEGER NOT NULL,
            PRIMARY KEY (deal_id, tag_id),
            FOREIGN KEY (tag_id) REFERENCES deal_tags(id) ON DELETE CASCADE
        )
    """)
    # API response cache (#176) — TTL-based cache for RentCast/ATTOM/FRED responses
    con.execute("""
        CREATE TABLE IF NOT EXISTS api_cache (
            key         TEXT PRIMARY KEY,
            data        TEXT NOT NULL,
            fetched_at  TEXT NOT NULL,
            ttl_hours   INTEGER NOT NULL DEFAULT 24
        )
    """)
    con.commit()


# ---------------------------------------------------------------------------
# API Cache (#176) — SQLite-backed TTL cache for external API responses
# ---------------------------------------------------------------------------

def cache_get(key: str) -> dict | None:
    """
    Return cached value for key if not expired.
    Returns None if missing or expired (expired entries deleted).
    """
    try:
        row = _conn().execute(
            "SELECT data, fetched_at, ttl_hours FROM api_cache WHERE key=?", (key,)
        ).fetchone()
        if not row:
            return None
        row = dict(row)
        fetched = datetime.fromisoformat(row["fetched_at"].replace("Z", "+00:00"))
        age_hours = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
        if age_hours > row["ttl_hours"]:
            _conn().execute("DELETE FROM api_cache WHERE key=?", (key,))
            _conn().commit()
            return None
        return json.loads(row["data"])
    except Exception:
        return None


@_with_retry()
def cache_set(key: str, data: dict, ttl_hours: int = 24) -> None:
    """Store data under key with given TTL (hours)."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO api_cache (key, data, fetched_at, ttl_hours) VALUES (?,?,?,?)",
        (key, json.dumps(data), now, ttl_hours),
    )
    con.commit()


def cache_purge_expired() -> int:
    """Delete all expired cache entries. Returns count removed."""
    try:
        con = _conn()
        rows = con.execute("SELECT key, fetched_at, ttl_hours FROM api_cache").fetchall()
        expired = []
        now_utc = datetime.now(timezone.utc)
        for row in rows:
            r = dict(row)
            try:
                fetched = datetime.fromisoformat(r["fetched_at"].replace("Z", "+00:00"))
                if (now_utc - fetched).total_seconds() / 3600 > r["ttl_hours"]:
                    expired.append(r["key"])
            except Exception:
                pass
        if expired:
            con.executemany("DELETE FROM api_cache WHERE key=?", [(k,) for k in expired])
            con.commit()
        return len(expired)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Magic-link token persistence (#146)
# ---------------------------------------------------------------------------

_MAGIC_LINK_TTL_MINUTES = 15


@_with_retry()
def magic_link_create(token: str, email: str) -> None:
    """Store a magic-link token with a 15-minute TTL."""
    from datetime import timedelta
    con = _conn()
    now = datetime.now(timezone.utc)
    expires = (now + timedelta(minutes=_MAGIC_LINK_TTL_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")
    con.execute(
        "INSERT OR REPLACE INTO magic_link_tokens (token, email, created_at, expires_at, used) VALUES (?,?,?,?,0)",
        (token, email.lower().strip(), now.strftime("%Y-%m-%dT%H:%M:%SZ"), expires),
    )
    con.commit()


@_with_retry()
def magic_link_consume(token: str) -> str | None:
    """
    Verify and consume a magic-link token.
    Returns the email if valid and unused, None otherwise.
    Marks the token as used on success.
    """
    con = _conn()
    row = con.execute(
        "SELECT email, expires_at, used FROM magic_link_tokens WHERE token=?",
        (token,),
    ).fetchone()
    if not row:
        return None
    if row["used"]:
        return None
    now = datetime.now(timezone.utc)
    expires = datetime.strptime(row["expires_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    if now > expires:
        return None
    con.execute("UPDATE magic_link_tokens SET used=1 WHERE token=?", (token,))
    con.commit()
    return row["email"]


@_with_retry()
def magic_link_purge_expired() -> int:
    """Delete all expired or used tokens. Returns count deleted."""
    con = _conn()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cur = con.execute(
        "DELETE FROM magic_link_tokens WHERE used=1 OR expires_at < ?", (now,)
    )
    con.commit()
    return cur.rowcount


# ---------------------------------------------------------------------------
# Usage quota helpers (#148)
# ---------------------------------------------------------------------------

# Tier limits: analyses per calendar month
_TIER_LIMITS: dict[str, int] = {
    "free":         3,
    "operator":     5,
    "professional": 25,
    "team":         99_999,   # effectively unlimited
    "enterprise":   99_999,
}


def get_user_tier(user_email: str | None) -> str:
    """Return the subscription tier for user_email ('free' if no record)."""
    if not user_email:
        return "free"
    try:
        con = _conn()
        row = con.execute(
            "SELECT plan, status FROM user_subscriptions WHERE email=?",
            (user_email.lower().strip(),)
        ).fetchone()
        if row and row["status"] in ("active", "trialing"):
            return row["plan"] or "free"
    except Exception:
        pass
    return "free"


def get_monthly_usage(user_email: str | None) -> int:
    """Count analyses by user_email in the current calendar month."""
    if not user_email:
        return 0
    try:
        con = _conn()
        row = con.execute(
            """SELECT COUNT(*) AS cnt FROM deals
               WHERE user_email=?
               AND created_at >= strftime('%Y-%m-01T00:00:00Z','now')""",
            (user_email.lower().strip(),)
        ).fetchone()
        return row["cnt"] if row else 0
    except Exception:
        return 0


def check_quota(user_email: str | None) -> dict:
    """
    Return quota status for user_email.
    Keys: tier, used, limit, allowed (bool), resets_at (ISO date string).
    """
    tier  = get_user_tier(user_email)
    limit = _TIER_LIMITS.get(tier, 3)
    used  = get_monthly_usage(user_email)
    # Reset date = 1st of next month
    now = datetime.now(timezone.utc)
    if now.month == 12:
        resets = datetime(now.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        resets = datetime(now.year, now.month + 1, 1, tzinfo=timezone.utc)
    return {
        "tier":      tier,
        "used":      used,
        "limit":     limit,
        "allowed":   used < limit,
        "resets_at": resets.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# CRUD helpers used by app.py
# ---------------------------------------------------------------------------

@_with_retry()
def job_create(job_id: str, om_text: str, user_email: str | None = None):
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO deals (id, status, created_at, om_text, user_email) VALUES (?,?,?,?,?)",
        (job_id, "queued", _now(), om_text, user_email)
    )
    con.commit()


@_with_retry()
def job_set_status(job_id: str, status: str):
    con = _conn()
    con.execute("UPDATE deals SET status=? WHERE id=?", (status, job_id))
    con.commit()


@_with_retry()
def job_set_result(job_id: str, result: dict):
    deal = result.get("deal") or {}
    memo = result.get("memo", "")
    import re as _re
    conf_m = _re.search(r"Confidence[^0-9]*([0-9]+)", memo)
    confidence = int(conf_m.group(1)) if conf_m else None

    mu = memo.upper()
    if "NO-GO" in mu:
        verdict = "NO-GO"
    elif _re.search(r"\bGO\b", mu) and "CONDITIONAL" not in mu:
        verdict = "GO"
    else:
        verdict = "CONDITIONAL"

    _conn().execute(
        """UPDATE deals
           SET status='done', result=?, deal_name=?, verdict=?, confidence=?
           WHERE id=?""",
        (json.dumps(result, default=str), deal.get("deal_name"), verdict, confidence, job_id)
    )
    _conn().commit()


@_with_retry()
def job_set_error(job_id: str, message: str, tb: str = ""):
    con = _conn()
    con.execute(
        "UPDATE deals SET status='error', result=? WHERE id=?",
        (json.dumps({"status": "error", "message": message, "traceback": tb}), job_id)
    )
    con.commit()


def job_get(job_id: str) -> dict | None:
    try:
        row = _conn().execute("SELECT * FROM deals WHERE id=?", (job_id,)).fetchone()
    except sqlite3.OperationalError:
        _reset_conn()
        row = _conn().execute("SELECT * FROM deals WHERE id=?", (job_id,)).fetchone()
    if row is None:
        return None
    d = dict(row)
    if d.get("result"):
        try:
            d["result"] = json.loads(d["result"])
        except Exception:
            pass
    return d


def jobs_recent(limit: int = 20, user_email: str | None = None) -> list[dict]:
    """Return recent completed deals for history sidebar."""
    try:
        if user_email:
            rows = _conn().execute(
                "SELECT id, deal_name, verdict, confidence, created_at FROM deals WHERE user_email=? ORDER BY created_at DESC LIMIT ?",
                (user_email, limit)
            ).fetchall()
        else:
            rows = _conn().execute(
                "SELECT id, deal_name, verdict, confidence, created_at FROM deals WHERE status='done' ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Watchlist helpers (#128)
# ---------------------------------------------------------------------------

@_with_retry()
def watchlist_add(deal_key: str, deal_json: dict, user_email: str | None = None):
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO watchlist (deal_key, deal_json, added_at, user_email) VALUES (?,?,?,?)",
        (deal_key, json.dumps(deal_json, default=str), _now(), user_email)
    )
    con.commit()


@_with_retry()
def watchlist_remove(deal_key: str, user_email: str | None = None):
    con = _conn()
    if user_email:
        con.execute("DELETE FROM watchlist WHERE deal_key=? AND user_email=?", (deal_key, user_email))
    else:
        con.execute("DELETE FROM watchlist WHERE deal_key=?", (deal_key,))
    con.commit()


def watchlist_get(user_email: str | None = None) -> list[dict]:
    try:
        if user_email:
            rows = _conn().execute(
                "SELECT deal_key, deal_json, added_at FROM watchlist WHERE user_email=? ORDER BY added_at DESC",
                (user_email,)
            ).fetchall()
        else:
            rows = _conn().execute(
                "SELECT deal_key, deal_json, added_at FROM watchlist ORDER BY added_at DESC"
            ).fetchall()
    except Exception:
        _reset_conn()
        return []
    result = []
    for r in rows:
        try:
            d = json.loads(r["deal_json"])
            d["_watchlist_added"] = r["added_at"]
            result.append(d)
        except Exception:
            pass
    return result


def watchlist_keys(user_email: str | None = None) -> set[str]:
    try:
        if user_email:
            rows = _conn().execute(
                "SELECT deal_key FROM watchlist WHERE user_email=?", (user_email,)
            ).fetchall()
        else:
            rows = _conn().execute("SELECT deal_key FROM watchlist").fetchall()
        return {r["deal_key"] for r in rows}
    except Exception:
        _reset_conn()
        return set()


# ---------------------------------------------------------------------------
# Deal notes helpers (#128)
# ---------------------------------------------------------------------------

@_with_retry()
def note_set(deal_key: str, note: str, user_email: str | None = None):
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO deal_notes (deal_key, note, updated_at, user_email) VALUES (?,?,?,?)",
        (deal_key, note, _now(), user_email)
    )
    con.commit()


def note_get(deal_key: str, user_email: str | None = None) -> str:
    try:
        row = _conn().execute(
            "SELECT note FROM deal_notes WHERE deal_key=?", (deal_key,)
        ).fetchone()
        return row["note"] if row else ""
    except Exception:
        _reset_conn()
        return ""


# ---------------------------------------------------------------------------
# Saved searches helpers (#139)
# ---------------------------------------------------------------------------

@_with_retry()
def search_save(search_id: str, name: str, filters: dict, user_email: str | None = None):
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO saved_searches (id, name, filters, created_at, user_email) VALUES (?,?,?,?,?)",
        (search_id, name, json.dumps(filters), _now(), user_email)
    )
    con.commit()


@_with_retry()
def search_update_last_run(search_id: str):
    con = _conn()
    con.execute("UPDATE saved_searches SET last_run=? WHERE id=?", (_now(), search_id))
    con.commit()


@_with_retry()
def search_delete(search_id: str, user_email: str | None = None):
    con = _conn()
    if user_email:
        con.execute("DELETE FROM saved_searches WHERE id=? AND user_email=?", (search_id, user_email))
    else:
        con.execute("DELETE FROM saved_searches WHERE id=?", (search_id,))
    con.commit()


# ---------------------------------------------------------------------------
# Deal alert helpers (#134)
# ---------------------------------------------------------------------------

@_with_retry()
def alert_create(alert_id: str, name: str, filters: dict, email: str, user_email: str | None = None):
    con = _conn()
    con.execute(
        "INSERT OR REPLACE INTO deal_alerts (id, name, filters, email, active, created_at, user_email) VALUES (?,?,?,?,1,?,?)",
        (alert_id, name, json.dumps(filters), email, _now(), user_email)
    )
    con.commit()


def alert_list(user_email: str | None = None) -> list[dict]:
    try:
        rows = _conn().execute(
            "SELECT * FROM deal_alerts ORDER BY created_at DESC"
        ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["filters"] = json.loads(d["filters"])
            except Exception:
                pass
            try:
                d["seen_keys"] = json.loads(d.get("seen_keys") or "[]")
            except Exception:
                d["seen_keys"] = []
            result.append(d)
        return result
    except Exception:
        _reset_conn()
        return []


@_with_retry()
def alert_update_check(alert_id: str, match_count: int, seen_keys: list):
    con = _conn()
    con.execute(
        "UPDATE deal_alerts SET last_checked=?, last_match_count=?, seen_keys=? WHERE id=?",
        (_now(), match_count, json.dumps(seen_keys), alert_id)
    )
    con.commit()


@_with_retry()
def alert_delete(alert_id: str, user_email: str | None = None):
    con = _conn()
    con.execute("DELETE FROM deal_alerts WHERE id=?", (alert_id,))
    con.commit()


@_with_retry()
def alert_toggle(alert_id: str, active: bool):
    con = _conn()
    con.execute("UPDATE deal_alerts SET active=? WHERE id=?", (1 if active else 0, alert_id))
    con.commit()


# ---------------------------------------------------------------------------
# Pipeline Kanban helpers (#133)
# ---------------------------------------------------------------------------

PIPELINE_STAGES = ["Screening", "LOI", "Due Diligence", "Closed", "Passed"]


@_with_retry()
def pipeline_add(deal_id: str, deal_name: str, address: str = "", market: str = "",
                 asking_price: float | None = None, units: int | None = None,
                 stage: str = "Screening", assigned_to: str | None = None,
                 notes: str = "", job_id: str | None = None, user_email: str | None = None):
    con = _conn()
    now = _now()
    con.execute(
        """INSERT OR REPLACE INTO pipeline_deals
           (id, deal_name, address, market, asking_price, units, stage, assigned_to, notes, job_id, created_at, updated_at, user_email)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (deal_id, deal_name, address, market, asking_price, units, stage, assigned_to, notes, job_id, now, now, user_email)
    )
    con.execute(
        "INSERT INTO pipeline_activity (deal_id, action, detail, created_at, user_email) VALUES (?,?,?,?,?)",
        (deal_id, "added", f"Deal added to {stage}", now, user_email)
    )
    con.commit()


@_with_retry()
def pipeline_move(deal_id: str, new_stage: str, user_email: str | None = None):
    now = _now()
    con = _conn()
    old = con.execute("SELECT stage FROM pipeline_deals WHERE id=?", (deal_id,)).fetchone()
    old_stage = dict(old)["stage"] if old else "?"
    con.execute(
        "UPDATE pipeline_deals SET stage=?, updated_at=? WHERE id=?",
        (new_stage, now, deal_id)
    )
    con.execute(
        "INSERT INTO pipeline_activity (deal_id, action, detail, created_at, user_email) VALUES (?,?,?,?,?)",
        (deal_id, "moved", f"{old_stage} → {new_stage}", now, user_email)
    )
    con.commit()


@_with_retry()
def pipeline_update(deal_id: str, **kwargs):
    """Update arbitrary fields on a pipeline deal."""
    allowed = {"deal_name", "assigned_to", "notes", "asking_price", "units", "market", "color_tag"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    updates["updated_at"] = _now()
    cols = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [deal_id]
    con = _conn()
    con.execute(f"UPDATE pipeline_deals SET {cols} WHERE id=?", vals)
    con.commit()


@_with_retry()
def pipeline_delete(deal_id: str, user_email: str | None = None):
    con = _conn()
    con.execute("DELETE FROM pipeline_deals WHERE id=?", (deal_id,))
    con.execute("DELETE FROM pipeline_activity WHERE deal_id=?", (deal_id,))
    con.commit()


def pipeline_get_all(user_email: str | None = None) -> list[dict]:
    try:
        rows = _conn().execute(
            "SELECT * FROM pipeline_deals ORDER BY updated_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


def pipeline_activity(deal_id: str, limit: int = 20) -> list[dict]:
    try:
        rows = _conn().execute(
            "SELECT * FROM pipeline_activity WHERE deal_id=? ORDER BY created_at DESC LIMIT ?",
            (deal_id, limit)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


# ---------------------------------------------------------------------------
# Shared link helpers (#136)
# ---------------------------------------------------------------------------

@_with_retry()
def shared_link_create(token: str, job_id: str, user_email: str | None = None,
                        password: str | None = None, label: str | None = None,
                        expires_at: str | None = None):
    con = _conn()
    con.execute(
        """INSERT OR REPLACE INTO shared_links
           (token, job_id, created_at, expires_at, password, label, view_count, user_email)
           VALUES (?,?,?,?,?,?,0,?)""",
        (token, job_id, _now(), expires_at, password, label, user_email)
    )
    con.commit()


def shared_link_get(token: str) -> dict | None:
    try:
        row = _conn().execute("SELECT * FROM shared_links WHERE token=?", (token,)).fetchone()
        return dict(row) if row else None
    except Exception:
        _reset_conn()
        return None


@_with_retry()
def shared_link_record_view(token: str):
    con = _conn()
    con.execute(
        "UPDATE shared_links SET view_count=view_count+1, last_viewed=? WHERE token=?",
        (_now(), token)
    )
    con.commit()


def shared_links_for_job(job_id: str) -> list[dict]:
    try:
        rows = _conn().execute(
            "SELECT * FROM shared_links WHERE job_id=? ORDER BY created_at DESC",
            (job_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


def search_list(user_email: str | None = None) -> list[dict]:
    try:
        if user_email:
            rows = _conn().execute(
                "SELECT id, name, filters, created_at, last_run FROM saved_searches WHERE user_email=? ORDER BY created_at DESC",
                (user_email,)
            ).fetchall()
        else:
            rows = _conn().execute(
                "SELECT id, name, filters, created_at, last_run FROM saved_searches ORDER BY created_at DESC"
            ).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            try:
                d["filters"] = json.loads(d["filters"])
            except Exception:
                pass
            result.append(d)
        return result
    except Exception:
        _reset_conn()
        return []


# ---------------------------------------------------------------------------
# Scoring profiles (#141) — custom deal scoring weight profiles
# ---------------------------------------------------------------------------

# Default weight set — mirrors the baseline _quick_score() formula
DEFAULT_WEIGHTS = {
    "cap_rate":     8.0,   # points per % cap rate (e.g. 6% cap = 48 pts)
    "irr_premium":  3.0,   # points per % IRR above 8% hurdle
    "bear_cushion": 15.0,  # bonus points if bear-case IRR >= 8%
    "scale":        5.0,   # bonus points for 50+ unit deals
    "ppu_discount": 10.0,  # bonus points for price/unit < $150K
    "market_growth":0.0,   # bonus for positive market rent growth (future use)
}

_PRESET_PROFILES = [
    {
        "id": "preset_core_plus",
        "name": "Core Plus",
        "weights": {**DEFAULT_WEIGHTS, "cap_rate": 6.0, "bear_cushion": 20.0, "ppu_discount": 5.0},
        "is_active": 0,
        "_preset": True,
    },
    {
        "id": "preset_value_add",
        "name": "Value-Add",
        "weights": {**DEFAULT_WEIGHTS, "cap_rate": 8.0, "irr_premium": 4.0, "scale": 8.0},
        "is_active": 0,
        "_preset": True,
    },
    {
        "id": "preset_opportunistic",
        "name": "Opportunistic",
        "weights": {**DEFAULT_WEIGHTS, "cap_rate": 5.0, "irr_premium": 6.0, "bear_cushion": 10.0},
        "is_active": 0,
        "_preset": True,
    },
]


@_with_retry()
def scoring_profile_create(name: str, weights: dict, user_email: str | None = None) -> str:
    import uuid as _uuid
    profile_id = _uuid.uuid4().hex[:8]
    con = _conn()
    con.execute(
        "INSERT INTO scoring_profiles (id, name, weights, is_active, created_at, user_email) VALUES (?,?,?,0,?,?)",
        (profile_id, name, json.dumps(weights), _now(), user_email)
    )
    con.commit()
    return profile_id


def scoring_profile_list(user_email: str | None = None) -> list[dict]:
    """Return saved profiles + presets. Active profile has is_active=1."""
    try:
        rows = _conn().execute(
            "SELECT * FROM scoring_profiles ORDER BY created_at DESC"
        ).fetchall()
        saved = []
        for r in rows:
            d = dict(r)
            try:
                d["weights"] = json.loads(d["weights"])
            except Exception:
                d["weights"] = DEFAULT_WEIGHTS.copy()
            d["_preset"] = False
            saved.append(d)
        return list(_PRESET_PROFILES) + saved
    except Exception:
        _reset_conn()
        return list(_PRESET_PROFILES)


@_with_retry()
def scoring_profile_activate(profile_id: str, user_email: str | None = None):
    """Set one profile as active, deactivate all others."""
    con = _conn()
    con.execute("UPDATE scoring_profiles SET is_active=0")
    con.execute("UPDATE scoring_profiles SET is_active=1 WHERE id=?", (profile_id,))
    con.commit()


@_with_retry()
def scoring_profile_delete(profile_id: str):
    con = _conn()
    con.execute("DELETE FROM scoring_profiles WHERE id=?", (profile_id,))
    con.commit()


def scoring_profile_get_active(user_email: str | None = None) -> dict:
    """Return the active profile's weights, or DEFAULT_WEIGHTS if none is active."""
    try:
        row = _conn().execute(
            "SELECT weights FROM scoring_profiles WHERE is_active=1 LIMIT 1"
        ).fetchone()
        if row:
            return json.loads(row["weights"])
    except Exception:
        _reset_conn()
    return DEFAULT_WEIGHTS.copy()


# ---------------------------------------------------------------------------
# Due Diligence Checklist (#142)
# ---------------------------------------------------------------------------

# Default DD checklist template — applied when a new pipeline deal is created
DD_DEFAULT_CHECKLIST = [
    # Environmental
    {"category": "environmental", "title": "Phase I Environmental Site Assessment ordered"},
    {"category": "environmental", "title": "Phase II (if needed) — soil/groundwater testing"},
    # Title & Legal
    {"category": "title",        "title": "Title search ordered / preliminary title report received"},
    {"category": "title",        "title": "Survey ordered (ALTA/NSPS)"},
    {"category": "title",        "title": "Review existing easements and CC&Rs"},
    # Financial
    {"category": "financial",    "title": "T-12 operating statement verified vs. rent roll"},
    {"category": "financial",    "title": "Rent roll audited (unit-by-unit, current vs. market)"},
    {"category": "financial",    "title": "Tax returns (3 years) reviewed"},
    {"category": "financial",    "title": "Utility bills reviewed (12 months)"},
    {"category": "financial",    "title": "Service contracts reviewed / assignability confirmed"},
    # Physical
    {"category": "physical",     "title": "Property condition assessment (PCA) / engineering report"},
    {"category": "physical",     "title": "Roof inspection completed"},
    {"category": "physical",     "title": "HVAC, plumbing, electrical systems inspected"},
    {"category": "physical",     "title": "ADA compliance review"},
    # Zoning & Regulatory
    {"category": "zoning",       "title": "Zoning verification (confirm permitted use)"},
    {"category": "zoning",       "title": "Certificate of Occupancy confirmed"},
    {"category": "zoning",       "title": "Building permits / violation search"},
    # Financing
    {"category": "financing",    "title": "Lender engagement — LOI / term sheet received"},
    {"category": "financing",    "title": "Debt assumption terms confirmed (if applicable)"},
    {"category": "financing",    "title": "Insurance quote obtained"},
]


@_with_retry()
def dd_item_create(deal_id: str, title: str, category: str = "general",
                   assignee: str | None = None, due_date: str | None = None) -> str:
    import uuid as _uuid
    item_id = _uuid.uuid4().hex[:8]
    now = _now()
    _conn().execute(
        """INSERT INTO dd_checklist (id, deal_id, category, title, assignee, due_date, completed, notes, created_at, updated_at)
           VALUES (?,?,?,?,?,?,0,NULL,?,?)""",
        (item_id, deal_id, category, title, assignee, due_date, now, now)
    )
    _conn().execute("COMMIT") if False else _conn().commit()  # avoid lint warning
    return item_id


@_with_retry()
def dd_item_seed_defaults(deal_id: str):
    """Seed a new pipeline deal with the standard DD checklist."""
    con = _conn()
    for item in DD_DEFAULT_CHECKLIST:
        item_id = __import__("uuid").uuid4().hex[:8]
        now = _now()
        con.execute(
            """INSERT OR IGNORE INTO dd_checklist
               (id, deal_id, category, title, assignee, due_date, completed, notes, created_at, updated_at)
               VALUES (?,?,?,?,NULL,NULL,0,NULL,?,?)""",
            (item_id, deal_id, item["category"], item["title"], now, now)
        )
    con.commit()


@_with_retry()
def dd_item_update(item_id: str, **kwargs):
    """Update any writable fields on a checklist item."""
    allowed = {"title", "category", "assignee", "due_date", "completed", "notes"}
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return
    updates["updated_at"] = _now()
    cols = ", ".join(f"{k}=?" for k in updates)
    vals = list(updates.values()) + [item_id]
    con = _conn()
    con.execute(f"UPDATE dd_checklist SET {cols} WHERE id=?", vals)
    con.commit()


@_with_retry()
def dd_item_delete(item_id: str):
    con = _conn()
    con.execute("DELETE FROM dd_checklist WHERE id=?", (item_id,))
    con.commit()


def dd_items_for_deal(deal_id: str) -> list[dict]:
    try:
        rows = _conn().execute(
            "SELECT * FROM dd_checklist WHERE deal_id=? ORDER BY category, created_at",
            (deal_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


def dd_progress(deal_id: str) -> dict:
    """Return {total, completed, pct} for a deal's DD checklist."""
    items = dd_items_for_deal(deal_id)
    total = len(items)
    done  = sum(1 for i in items if i.get("completed"))
    return {"total": total, "completed": done, "pct": round(done / total * 100) if total else 0}


# ---------------------------------------------------------------------------
# Deal Document Vault (#142)
# ---------------------------------------------------------------------------

DOC_VAULT_DIR = Path(__file__).parent / "outputs" / "vault"
DOC_VAULT_DIR.mkdir(parents=True, exist_ok=True)


@_with_retry()
def doc_create(deal_id: str, filename: str, category: str, file_size: int,
               stored_path: str, uploaded_by: str | None = None,
               extracted_text: str | None = None) -> str:
    import uuid as _uuid
    doc_id = _uuid.uuid4().hex[:8]
    _conn().execute(
        """INSERT INTO deal_documents
           (id, deal_id, filename, category, file_size, stored_path, uploaded_at, uploaded_by, extracted_text)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (doc_id, deal_id, filename, category, file_size, stored_path, _now(), uploaded_by, extracted_text)
    )
    _conn().commit()
    return doc_id


@_with_retry()
def doc_delete(doc_id: str):
    row = _conn().execute("SELECT stored_path FROM deal_documents WHERE id=?", (doc_id,)).fetchone()
    if row:
        try:
            Path(row["stored_path"]).unlink(missing_ok=True)
        except Exception:
            pass
    _conn().execute("DELETE FROM deal_documents WHERE id=?", (doc_id,))
    _conn().commit()


def docs_for_deal(deal_id: str) -> list[dict]:
    try:
        rows = _conn().execute(
            "SELECT id, deal_id, filename, category, file_size, uploaded_at, uploaded_by FROM deal_documents WHERE deal_id=? ORDER BY uploaded_at DESC",
            (deal_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


# ---------------------------------------------------------------------------
# LP Engagement Analytics (#144)
# ---------------------------------------------------------------------------

@_with_retry()
def lp_event_record(token: str, job_id: str, event_type: str,
                    section: str | None = None, duration_s: float | None = None,
                    lp_ua: str | None = None, lp_ip: str | None = None):
    """Record an LP engagement event (view, section_enter, section_exit, download)."""
    _conn().execute(
        """INSERT INTO lp_events (token, job_id, event_type, section, duration_s, lp_ua, lp_ip, created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (token, job_id, event_type, section, duration_s, lp_ua, lp_ip, _now())
    )
    _conn().commit()


def lp_analytics_for_job(job_id: str) -> dict:
    """
    Aggregate engagement analytics across all LP links for a job.
    Returns: {links: [...], total_views: int, unique_visitors: int, total_time_s: float, sections: {...}}
    """
    try:
        # Per-link summary
        links = _conn().execute(
            "SELECT token, label, view_count, last_viewed, created_at, expires_at FROM shared_links WHERE job_id=? ORDER BY created_at DESC",
            (job_id,)
        ).fetchall()

        # Aggregate events
        events = _conn().execute(
            "SELECT token, event_type, section, duration_s, created_at FROM lp_events WHERE job_id=? ORDER BY created_at DESC",
            (job_id,)
        ).fetchall()

        section_time: dict[str, float] = {}
        section_views: dict[str, int] = {}
        total_time = 0.0
        downloads = 0
        for ev in events:
            et, sec, dur = ev["event_type"], ev["section"], ev["duration_s"] or 0
            if et == "section_exit" and sec:
                section_time[sec] = section_time.get(sec, 0) + dur
                total_time += dur
            if et == "section_enter" and sec:
                section_views[sec] = section_views.get(sec, 0) + 1
            if et == "download":
                downloads += 1

        return {
            "job_id":          job_id,
            "links":           [dict(lnk) for lnk in links],
            "total_links":     len(links),
            "total_views":     sum(lnk["view_count"] or 0 for lnk in links),
            "downloads":       downloads,
            "total_time_s":    round(total_time, 1),
            "section_time_s":  {k: round(v, 1) for k, v in section_time.items()},
            "section_views":   section_views,
            "recent_events":   [dict(ev) for ev in events[:20]],
        }
    except Exception:
        _reset_conn()
        return {"job_id": job_id, "total_views": 0, "links": []}


# ---------------------------------------------------------------------------
# Deal Tagging (#169)
# ---------------------------------------------------------------------------

@_with_retry()
def tag_create(name: str, color: str = "#58a6ff", email: str | None = None) -> int:
    """Create a new tag. Returns the new tag id."""
    cur = _conn().execute(
        "INSERT INTO deal_tags (name, color, email, created_at) VALUES (?,?,?,?)",
        (name.strip(), color, email, _now())
    )
    _conn().commit()
    return cur.lastrowid


def tag_list(email: str | None = None) -> list[dict]:
    """Return all tags (optionally filtered by email owner)."""
    try:
        if email:
            rows = _conn().execute(
                "SELECT id, name, color, email, created_at FROM deal_tags WHERE email=? OR email IS NULL ORDER BY name COLLATE NOCASE",
                (email,)
            ).fetchall()
        else:
            rows = _conn().execute(
                "SELECT id, name, color, email, created_at FROM deal_tags ORDER BY name COLLATE NOCASE"
            ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


@_with_retry()
def tag_delete(tag_id: int):
    """Delete a tag (cascades to pipeline_deal_tags via FK)."""
    _conn().execute("DELETE FROM deal_tags WHERE id=?", (tag_id,))
    _conn().commit()


@_with_retry()
def deal_tag_add(deal_id: str, tag_id: int):
    """Attach a tag to a pipeline deal (ignores duplicates)."""
    _conn().execute(
        "INSERT OR IGNORE INTO pipeline_deal_tags (deal_id, tag_id) VALUES (?,?)",
        (deal_id, tag_id)
    )
    _conn().commit()


@_with_retry()
def deal_tag_remove(deal_id: str, tag_id: int):
    """Detach a tag from a pipeline deal."""
    _conn().execute(
        "DELETE FROM pipeline_deal_tags WHERE deal_id=? AND tag_id=?",
        (deal_id, tag_id)
    )
    _conn().commit()


def deal_tags_for_deal(deal_id: str) -> list[dict]:
    """Return all tags attached to a specific pipeline deal."""
    try:
        rows = _conn().execute(
            """SELECT t.id, t.name, t.color
               FROM deal_tags t
               JOIN pipeline_deal_tags pt ON pt.tag_id = t.id
               WHERE pt.deal_id = ?
               ORDER BY t.name COLLATE NOCASE""",
            (deal_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    except Exception:
        _reset_conn()
        return []


def deals_for_tag(tag_id: int) -> list[str]:
    """Return all deal_ids that have a given tag."""
    try:
        rows = _conn().execute(
            "SELECT deal_id FROM pipeline_deal_tags WHERE tag_id=?",
            (tag_id,)
        ).fetchall()
        return [r["deal_id"] for r in rows]
    except Exception:
        _reset_conn()
        return []
