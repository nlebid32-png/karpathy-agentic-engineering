"""
ClearEye Web App — Flask UI for real estate deal analysis
Run: python app.py
Open: http://localhost:5052
"""
from __future__ import annotations
import json
import os
import sys
import threading
import uuid
from datetime import datetime
from pathlib import Path

from dotenv import dotenv_values

# Load API key: local Windows dotenv → local .env → environment variable
# This lets the app run both in development (Nick's machine) and deployed (Render/Railway/Fly.io)
def _load_env() -> dict:
    # 1. Try the local dev dotenv path first
    try:
        _d = dotenv_values(r"G:\My Drive\Claude work folder\canvas-ai-pipeline\.env")
        if _d.get("ANTHROPIC_API_KEY"):
            return dict(_d)
    except Exception:
        pass
    # 2. Try a .env file in the same directory (for deployment or local alt)
    try:
        _d2 = dotenv_values(Path(__file__).parent / ".env")
        if _d2.get("ANTHROPIC_API_KEY"):
            return dict(_d2)
    except Exception:
        pass
    # 3. Fall back to real environment variables (Render/Railway/Fly.io sets these)
    return {}

_env = _load_env()
# Always expose to os.environ so submodules (cleareye.py, etc.) can use it
for _k, _v in _env.items():
    if _v:
        os.environ.setdefault(_k, _v)
# Final fallback: if ANTHROPIC_API_KEY is in the real env already, keep it
if not os.environ.get("ANTHROPIC_API_KEY"):
    os.environ["ANTHROPIC_API_KEY"] = _env.get("ANTHROPIC_API_KEY", "")

import requests
from flask import Flask, jsonify, render_template_string, request, send_file, Response, stream_with_context

sys.path.insert(0, str(Path(__file__).parent))
from cleareye import run as run_pipeline
from email_delivery import send_memo
from circuit_breaker import BREAKERS as _CIRCUIT_BREAKERS, all_statuses as _cb_all_statuses
from db import (init_db, job_create, job_set_status, job_set_result, job_set_error, job_get, jobs_recent,
                watchlist_add, watchlist_remove, watchlist_get, watchlist_keys, note_set, note_get,
                search_save, search_update_last_run, search_delete, search_list,
                shared_link_create, shared_link_get, shared_link_record_view, shared_links_for_job,
                pipeline_add, pipeline_move, pipeline_update, pipeline_delete, pipeline_get_all,
                pipeline_activity, PIPELINE_STAGES,
                alert_create, alert_list, alert_update_check, alert_delete, alert_toggle,
                scoring_profile_create, scoring_profile_list, scoring_profile_activate,
                scoring_profile_delete, scoring_profile_get_active, DEFAULT_WEIGHTS,
                dd_item_create, dd_item_seed_defaults, dd_item_update, dd_item_delete,
                dd_items_for_deal, dd_progress,
                doc_create, doc_delete, docs_for_deal, DOC_VAULT_DIR,
                lp_event_record, lp_analytics_for_job,
                magic_link_create, magic_link_consume, magic_link_purge_expired,
                check_quota, get_user_tier, get_monthly_usage,
                tag_create, tag_list, tag_delete, deal_tag_add, deal_tag_remove,
                deal_tags_for_deal, deals_for_tag)

SCAN_JOBS: dict[str, dict] = {}  # find-deals scan jobs
SSE_QUEUES: dict[str, list] = {}  # job_id → list of pending SSE events

app = Flask(__name__)
JOBS: dict[str, dict] = {}  # write-through cache (authoritative copy in SQLite)

# Initialize SQLite database (#112)
init_db()


# ---------------------------------------------------------------------------
# Deal alert background scanner (#134)
# ---------------------------------------------------------------------------

_ALERT_INTERVAL = 1800   # 30 minutes between scans

def _run_alert_scanner():
    """
    Background thread: every 30 min, run each active alert's filter config
    against RentCast, find new deals not seen before, email the user (#134).
    """
    import time as _time
    _time.sleep(60)  # wait 1 min after startup before first scan
    while True:
        try:
            _scan_alerts_once()
        except Exception as e:
            _log_error("alert_scanner", str(e))
        _time.sleep(_ALERT_INTERVAL)


def _scan_alerts_once():
    """Run one pass of all active alerts."""
    alerts = alert_list()
    active = [a for a in alerts if a.get("active")]
    if not active:
        return
    for alert in active:
        try:
            _check_alert(alert)
        except Exception as e:
            _log_error("alert_check", str(e), {"alert_id": alert.get("id")})


def _check_alert(alert: dict):
    """Check a single alert for new matching deals and email if found."""
    from rentcast_client import search_multifamily_deals
    filters = alert.get("filters") or {}
    markets = filters.get("markets") or ["Phoenix, AZ"]
    max_price = float(filters.get("max_price") or 30_000_000)
    min_cap_rate = float(filters.get("min_cap_rate") or 0)
    category = filters.get("category") or "all"
    email = alert.get("email", "")

    seen_keys: set = set(alert.get("seen_keys") or [])
    new_deals = []

    for mkt in markets[:5]:   # cap at 5 markets per alert to preserve rate limits
        try:
            deals = search_multifamily_deals(market=mkt, max_price=max_price,
                                             min_cap_rate=min_cap_rate, limit=10)
            for d in deals:
                key = (d.get("address") or d.get("deal_name") or "") + "::" + mkt
                if key and key not in seen_keys:
                    new_deals.append(d)
                    seen_keys.add(key)
        except Exception:
            pass

    # Update seen keys regardless (marks these as processed)
    alert_update_check(alert["id"], len(new_deals), list(seen_keys)[-500:])  # cap at 500

    if new_deals and email:
        _send_alert_email(alert, new_deals, email)


def _send_alert_email(alert: dict, deals: list, email: str):
    """Send a branded HTML deal alert digest email (#134, #251)."""
    try:
        from email_delivery import _send_email
        name = alert.get("name", "Deal Alert")
        ts   = datetime.utcnow().strftime("%B %d, %Y")
        deal_count = len(deals[:10])

        # Build deal cards
        deal_cards = ""
        for d in deals[:10]:
            price   = "${:,.0f}".format(d.get("asking_price", 0)) if d.get("asking_price") else "&mdash;"
            cap     = "{:.2f}%".format(d.get("cap_rate", 0))      if d.get("cap_rate")     else "&mdash;"
            units   = str(d.get("units", "&mdash;"))
            dname   = d.get("deal_name", "Unnamed Deal")
            market  = d.get("_source_market") or d.get("market", "")
            market_tag = (
                "<span style='display:inline-block;margin-top:4px;font-size:11px;"
                "color:#6B7280;font-family:\"JetBrains Mono\",Consolas,monospace;'>"
                + market + "</span>" if market else ""
            )
            deal_cards += (
                "<tr>"
                "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;'>"
                "<div style='font-size:14px;font-weight:600;color:#0D1926;font-family:\"Plus Jakarta Sans\",Arial,sans-serif;'>"
                + dname + "</div>"
                + market_tag +
                "</td>"
                "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;"
                "font-family:\"JetBrains Mono\",Consolas,monospace;font-size:13px;"
                "font-weight:600;color:#155E44;white-space:nowrap;'>" + price + "</td>"
                "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;"
                "font-family:\"JetBrains Mono\",Consolas,monospace;font-size:13px;"
                "color:#0D1926;white-space:nowrap;'>" + cap + " cap</td>"
                "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;"
                "font-size:13px;color:#6B7280;white-space:nowrap;'>" + units + " units</td>"
                "</tr>"
            )

        deal_noun = "deal" if deal_count == 1 else "deals"
        html = (
            "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'></head>"
            "<body style='margin:0;padding:0;background:#F5F3EE;font-family:\"Plus Jakarta Sans\",Arial,sans-serif;'>"
            "<table width='100%' cellpadding='0' cellspacing='0' style='background:#F5F3EE;padding:32px 16px;'>"
            "<tr><td align='center'>"
            "<table width='600' cellpadding='0' cellspacing='0' style='max-width:600px;width:100%;'>"

            # Header
            "<tr><td style='background:#155E44;border-radius:12px 12px 0 0;padding:24px 32px;'>"
            "<table width='100%' cellpadding='0' cellspacing='0'><tr>"
            "<td style='vertical-align:middle;'>"
            "<div style='display:inline-flex;align-items:center;gap:10px;'>"
            "<div style='width:32px;height:32px;background:rgba(255,255,255,0.18);border-radius:8px;"
            "display:inline-block;text-align:center;line-height:32px;font-size:16px;'>&#128065;</div>"
            "<span style='font-size:18px;font-weight:700;color:#FFFFFF;letter-spacing:-0.3px;'>ClearEye</span>"
            "</div></td>"
            "<td align='right' style='vertical-align:middle;'>"
            "<span style='font-size:11px;color:rgba(255,255,255,0.65);font-family:\"JetBrains Mono\",Consolas,monospace;'>"
            + ts + "</span></td>"
            "</tr></table></td></tr>"

            # Alert banner
            "<tr><td style='background:#FFFFFF;padding:24px 32px 16px;border-left:1px solid #EAE7E1;border-right:1px solid #EAE7E1;'>"
            "<div style='font-size:11px;font-weight:600;letter-spacing:0.08em;color:#155E44;text-transform:uppercase;margin-bottom:8px;'>"
            "Deal Alert</div>"
            "<h1 style='margin:0 0 6px;font-size:22px;font-weight:700;color:#0D1926;letter-spacing:-0.4px;'>"
            + name + "</h1>"
            "<p style='margin:0;font-size:14px;color:#6B7280;'>"
            "<strong style='color:#155E44;'>" + str(deal_count) + " new " + deal_noun + "</strong>"
            " matching your criteria are ready for review.</p>"
            "</td></tr>"

            # Deal table
            "<tr><td style='background:#FFFFFF;padding:0 32px;border-left:1px solid #EAE7E1;border-right:1px solid #EAE7E1;'>"
            "<table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;'>"
            "<thead><tr style='background:#F5F3EE;'>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Deal</th>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Ask Price</th>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Cap Rate</th>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Size</th>"
            "</tr></thead><tbody>"
            + deal_cards +
            "</tbody></table>"
            "</td></tr>"

            # CTA
            "<tr><td style='background:#FFFFFF;padding:24px 32px 28px;border-left:1px solid #EAE7E1;border-right:1px solid #EAE7E1;'>"
            "<a href='http://localhost:5052/find-deals'"
            " style='display:inline-block;background:#155E44;color:#FFFFFF;text-decoration:none;"
            "font-size:13px;font-weight:600;padding:11px 24px;border-radius:8px;letter-spacing:0.01em;'>"
            "View in ClearEye &rarr;</a>"
            "</td></tr>"

            # Footer
            "<tr><td style='background:#F0EDE7;border:1px solid #EAE7E1;border-radius:0 0 12px 12px;"
            "padding:16px 32px;'>"
            "<p style='margin:0;font-size:11px;color:#9CA3AF;line-height:1.6;'>"
            "You are receiving this because you set up a deal alert on ClearEye. "
            "<a href='http://localhost:5052/find-deals' style='color:#6B7280;text-decoration:underline;'>Manage alerts</a>"
            " &middot; "
            "<a href='http://localhost:5052/find-deals' style='color:#6B7280;text-decoration:underline;'>Unsubscribe</a>"
            "</p></td></tr>"

            "</table></td></tr></table></body></html>"
        )
        subject = "ClearEye Alert: " + str(deal_count) + " new " + deal_noun + " — " + name
        _send_email(email, subject, html)
        _log_error("alert_email_sent", "Sent " + str(deal_count) + " deals to " + email, {"alert": name})
    except Exception as e:
        _log_error("alert_email_fail", str(e), {"email": email})


# Start alert scanner background thread
_alert_thread = threading.Thread(target=_run_alert_scanner, daemon=True)
_alert_thread.start()


# ---------------------------------------------------------------------------
# Analysis worker (uses full cleareye.py pipeline)
# ---------------------------------------------------------------------------

def _enrich_deal_auto(deal: dict) -> dict:
    """Auto-enrichment: inject live RentCast market data + ATTOM property data (#106)."""
    enriched = dict(deal)
    try:
        from rentcast_client import get_market_benchmarks, RENTCAST_API_KEY
        if RENTCAST_API_KEY and deal.get("market"):
            benchmarks = get_market_benchmarks(deal["market"])
            enriched["live_market_data"] = benchmarks
            enriched["_data_source"] = "RentCast live (" + __import__("datetime").date.today().isoformat() + ")"
        else:
            enriched["_data_source"] = "ClearEye static (2026-Q1)"
    except Exception as e:
        enriched["_data_source"] = "ClearEye static (2026-Q1)"

    try:
        from attom_client import enrich_deal, ATTOM_API_KEY
        if ATTOM_API_KEY and deal.get("address"):
            enriched = enrich_deal(enriched)
    except Exception:
        pass

    return enriched


def _sse_push(job_id: str, event: str, data: str = ""):
    """Push a Server-Sent Event to any listeners on this job."""
    q = SSE_QUEUES.setdefault(job_id, [])
    q.append(f"event: {event}\ndata: {data}\n\n")


def _analyze(job_id: str, om_text: str, recipient_email: str | None):
    try:
        _update = lambda s: (
            JOBS.__setitem__(job_id, {**JOBS.get(job_id, {}), "status": s}),
            job_set_status(job_id, s),
            _sse_push(job_id, "status", s)
        )
        # Circuit breaker guard — fast-fail if Anthropic is experiencing failures
        _anthro_cb = _CIRCUIT_BREAKERS.get("anthropic")
        if _anthro_cb and not _anthro_cb.allow_request():
            raise RuntimeError(
                "AI pipeline temporarily unavailable (circuit breaker open). "
                "Too many recent failures from the AI provider. "
                "Please try again in ~90 seconds."
            )
        _update("parsing")
        try:
            output = run_pipeline(om_text=om_text)
            if _anthro_cb:
                _anthro_cb.record_success()
        except Exception as pipeline_exc:
            if _anthro_cb:
                _anthro_cb.record_failure(pipeline_exc)
            raise
        _update("enriching")

        # Auto-enrich deal with live market data (#106)
        if output.get("deal"):
            try:
                output["deal"] = _enrich_deal_auto(output["deal"])
            except Exception:
                pass

        # Expose rent context at top level of output (#171)
        if not output.get("rent_context"):
            try:
                from rentcast_client import get_market_benchmarks
                market = (output.get("deal") or {}).get("market", "Phoenix, AZ")
                if market:
                    bench = get_market_benchmarks(market)
                    output["rent_context"] = bench
                    _track_api_call("rentcast")
            except Exception:
                pass
        # Also fall back to live_market_data from deal enrichment
        if not output.get("rent_context") and output.get("deal", {}).get("live_market_data"):
            output["rent_context"] = output["deal"]["live_market_data"]

        # ATTOM property data lookup (#175)
        if not output.get("attom_data"):
            try:
                from attom_client import get_property_history, get_comp_sales, ATTOM_API_KEY
                deal_ref = output.get("deal") or {}
                address = deal_ref.get("address") or deal_ref.get("deal_name")
                if address and ATTOM_API_KEY:
                    attom_hist = get_property_history(address)
                    if attom_hist.get("_source") == "attom_live":
                        attom_comps = get_comp_sales(address, radius_miles=0.5, max_results=6)
                        output["attom_data"] = {**attom_hist, "comp_sales": attom_comps}
                        _track_api_call("attom")
            except Exception:
                pass

        output["status"] = "done"

        if recipient_email:
            _update("sending_email")
            email_result = send_memo(output, recipient_email)
            output["email_result"] = email_result

        JOBS[job_id] = {**output, "status": "done"}
        job_set_result(job_id, output)
        _sse_push(job_id, "done", "done")
        # Webhook delivery for API-origin jobs (#191)
        _wh_url = JOBS[job_id].get("webhook_url")
        if _wh_url:
            _deliver_webhook(job_id, _wh_url, output)

    except Exception as e:
        import traceback as _tb
        tb = _tb.format_exc()
        JOBS[job_id] = {"status": "error", "message": str(e), "traceback": tb}
        job_set_error(job_id, str(e), tb)
        _sse_push(job_id, "error", str(e))
        # Structured error log (#140)
        try:
            _log_error("analysis_worker", str(e), {"job_id": job_id, "traceback": tb[-500:]})
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Auth (#113, #146) — email magic link login, SQLite-backed tokens
# ---------------------------------------------------------------------------
# Tokens persisted in magic_link_tokens table — survive server restarts.
# TTL: 15 minutes. One-time use (marked used=1 after verify).

import secrets as _secrets
from flask import session, redirect

app.secret_key = (
    os.environ.get("SECRET_KEY")
    or os.environ.get("FLASK_SECRET_KEY")
    or _env.get("FLASK_SECRET_KEY")
    or _secrets.token_hex(32)
)


@app.route("/api/waitlist", methods=["POST"])
def api_waitlist():
    """Append email to waitlist JSONL (#203)."""
    data = request.get_json(force=True, silent=True) or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Invalid email"}), 400
    wl_path = os.path.join(os.path.dirname(__file__), "outputs", "waitlist.jsonl")
    import json as _j
    entry = _j.dumps({"email": email, "ts": datetime.utcnow().isoformat(), "source": "landing_hero"})
    with open(wl_path, "a", encoding="utf-8") as fh:
        fh.write(entry + "\n")
    return jsonify({"ok": True, "message": "You're on the list — we'll be in touch."})


@app.route("/login", methods=["GET"])
def login_page():
    """Login page (#113)"""
    return render_template_string("""<!DOCTYPE html><html>
<head><meta charset="UTF-8"><title>ClearEye — Sign In</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{background:#0d1117;color:#e6edf3;min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}</style>
</head><body>
<div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:32px 40px;max-width:380px;width:100%;text-align:center;">
  <div style="font-size:1.5rem;font-weight:800;color:#58a6ff;margin-bottom:8px;">&#128065; ClearEye</div>
  <div style="font-size:13px;color:#8b949e;margin-bottom:24px;">Sign in with your email to save your deal history across devices</div>
  <form method="POST" action="/login">
    <input name="email" type="email" required placeholder="you@example.com"
      style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:9px 12px;font-size:13px;margin-bottom:12px;">
    <button type="submit" style="width:100%;padding:9px;background:#238636;border:none;color:#fff;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">Send Magic Link</button>
  </form>
  <div style="margin-top:16px;font-size:11px;color:#484f58;">(In dev mode: magic link shown on-screen)</div>
  <a href="/app" style="display:block;margin-top:12px;font-size:12px;color:#58a6ff;">Continue without signing in &rarr;</a>
</div>
</body></html>""")


@app.route("/login", methods=["POST"])
def login_post():
    """Handle magic link login (#113, #146, #152) — token persisted to SQLite, sent via SMTP."""
    email = request.form.get("email", "").strip().lower()
    if not email:
        return redirect("/login")
    token = _secrets.token_urlsafe(24)
    # Persist token to SQLite — survives server restarts (#146)
    magic_link_create(token, email)
    # Purge expired tokens opportunistically
    try:
        magic_link_purge_expired()
    except Exception:
        pass
    magic_url = request.host_url.rstrip("/") + f"/auth/verify?token={token}"

    # Try SMTP delivery (#152); fall back to on-screen only if SMTP not configured
    from email_delivery import send_magic_link as _send_magic_link
    email_sent = False
    try:
        email_sent = _send_magic_link(email, magic_url)
    except Exception:
        pass

    if email_sent:
        return render_template_string(f"""<!DOCTYPE html><html>
<head><meta charset="UTF-8"><title>ClearEye — Check Your Email</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{{background:#0d1117;color:#e6edf3;min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}}</style>
</head><body>
<div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:32px 40px;max-width:420px;width:100%;text-align:center;">
  <div style="font-size:2rem;">&#9993;</div>
  <div style="font-size:1.2rem;font-weight:700;color:#3fb950;margin:8px 0;">Check your inbox</div>
  <div style="font-size:13px;color:#8b949e;margin-bottom:20px;">A sign-in link was sent to <strong style="color:#e6edf3;">{email}</strong>.<br>It expires in 15 minutes.</div>
  <a href="/app" style="font-size:12px;color:#58a6ff;">Continue without signing in &rarr;</a>
</div>
</body></html>""")
    else:
        # Dev fallback — SMTP not configured, show link on screen
        return render_template_string(f"""<!DOCTYPE html><html>
<head><meta charset="UTF-8"><title>ClearEye — Sign In</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{{background:#0d1117;color:#e6edf3;min-height:100vh;display:flex;align-items:center;justify-content:center;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;}}</style>
</head><body>
<div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:32px 40px;max-width:420px;width:100%;text-align:center;">
  <div style="font-size:1.3rem;font-weight:700;color:#3fb950;margin-bottom:8px;">&#10003; Token created</div>
  <div style="font-size:13px;color:#8b949e;margin-bottom:16px;">SMTP not configured — add GMAIL_USER + GMAIL_APP_PASSWORD to .env to send real emails.</div>
  <div style="background:#0d1117;border:1px solid #d29922;border-radius:6px;padding:12px;font-size:12px;color:#d29922;margin-bottom:16px;">
    <strong>&#9888; Dev mode:</strong> click to sign in:<br>
    <a href="{magic_url}" style="color:#58a6ff;word-break:break-all;">{magic_url}</a>
  </div>
  <a href="/app" style="font-size:12px;color:#58a6ff;">Continue without signing in &rarr;</a>
</div>
</body></html>""")


@app.route("/auth/verify")
def auth_verify():
    """Verify magic link token, set session (#113, #146) — reads from SQLite."""
    token = request.args.get("token", "")
    email = magic_link_consume(token)   # returns email or None; marks used=1
    if not email:
        return "<h2 style='font-family:sans-serif;padding:40px;color:#e6edf3;background:#0d1117;'>Invalid or expired link.</h2>", 401
    session["user_email"] = email
    return redirect("/app")


@app.route("/auth/logout")
def auth_logout():
    session.pop("user_email", None)
    return redirect("/app")


# ---------------------------------------------------------------------------
# Stripe stub (#118) — payment flow scaffold
# ---------------------------------------------------------------------------
@app.route("/pricing")
def pricing_page():
    """Pricing page — freemium + 3 paid tiers. Meridian theme."""
    has_stripe = bool(os.environ.get("STRIPE_SECRET_KEY", _env.get("STRIPE_SECRET_KEY", "")))
    return render_template_string("""<!DOCTYPE html><html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ClearEye Pricing — Underwrite deals in 90 seconds</title>
<meta name="description" content="Start free, no credit card. ClearEye gives real estate investors institutional-grade AI analysis in 90 seconds.">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,500;0,600;1,400;1,500&family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<style>
:root{
  --bg:#F7F5F0;--surface:#FFFFFF;--elevated:#F0EDE7;--border:#E8E5DF;
  --text:#0D1926;--sub:#4A5568;--muted:#8D98A5;
  --accent:#155E44;--accent-dim:rgba(21,94,68,.08);--accent-mid:rgba(21,94,68,.14);
  --green:#15803D;--red:#B91C1C;--amber:#92400E;
  --font:'Plus Jakarta Sans',-apple-system,sans-serif;
  --display:'Cormorant Garamond',Georgia,serif;
  --mono:'JetBrains Mono',Consolas,monospace;
  --r:10px;--t:140ms ease;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px;line-height:1.6;-webkit-font-smoothing:antialiased;}
.ce-nav{height:56px;background:rgba(247,245,240,.94);border-bottom:1px solid var(--border);display:flex;align-items:center;padding:0 24px;gap:6px;position:sticky;top:0;z-index:100;backdrop-filter:blur(12px);}
.ce-brand{font-family:var(--font);font-size:15px;font-weight:700;color:var(--accent);text-decoration:none;display:flex;align-items:center;gap:8px;margin-right:8px;}
.ce-brand-icon{width:26px;height:26px;background:var(--accent);border-radius:7px;display:flex;align-items:center;justify-content:center;}
.nav-pill{font-size:12px;color:var(--sub);text-decoration:none;padding:5px 10px;border-radius:6px;transition:color var(--t),background var(--t);}
.nav-pill:hover{color:var(--text);background:var(--elevated);}
.nav-cta{font-size:12px;font-weight:600;color:var(--accent);text-decoration:none;padding:6px 14px;border-radius:6px;border:1px solid rgba(21,94,68,.3);transition:all var(--t);}
.nav-cta:hover{background:var(--accent-dim);}

/* Hero */
.hero{max-width:760px;margin:0 auto;padding:64px 20px 48px;text-align:center;}
.hero-tag{font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--accent);margin-bottom:16px;font-family:var(--mono);}
.hero-h1{font-family:var(--display);font-size:3rem;font-weight:400;line-height:1.15;letter-spacing:-.02em;margin-bottom:16px;}
.hero-h1 em{font-style:italic;color:var(--accent);}
.hero-sub{font-size:15px;color:var(--sub);max-width:500px;margin:0 auto 28px;line-height:1.65;}
.roi-strip{display:inline-flex;align-items:center;gap:20px;background:var(--surface);border:1px solid var(--border);border-radius:50px;padding:10px 20px;font-size:12px;color:var(--sub);flex-wrap:wrap;justify-content:center;}
.roi-item{display:flex;align-items:center;gap:6px;}
.roi-val{font-family:var(--mono);font-weight:700;color:var(--accent);font-size:14px;}

/* Plan grid */
.plans{max-width:1080px;margin:0 auto;padding:0 20px 64px;display:grid;grid-template-columns:repeat(4,1fr);gap:14px;align-items:start;}
@media(max-width:900px){.plans{grid-template-columns:repeat(2,1fr);}}
@media(max-width:560px){.plans{grid-template-columns:1fr;}}

.plan{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:24px 20px;display:flex;flex-direction:column;transition:box-shadow var(--t),border-color var(--t);}
.plan:hover{box-shadow:0 4px 20px rgba(0,0,0,.07);border-color:rgba(21,94,68,.2);}
.plan.featured{border-color:var(--accent);box-shadow:0 0 0 3px rgba(21,94,68,.08);}
.plan-eyebrow{font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;font-family:var(--mono);margin-bottom:6px;color:var(--muted);}
.plan.featured .plan-eyebrow{color:var(--accent);}
.plan-name{font-family:var(--display);font-size:1.4rem;font-weight:400;margin-bottom:12px;}
.price-row{display:flex;align-items:baseline;gap:4px;margin-bottom:4px;}
.price{font-family:var(--mono);font-size:2rem;font-weight:700;letter-spacing:-.04em;color:var(--text);}
.price-period{font-size:12px;color:var(--muted);padding-bottom:3px;}
.plan-target{font-size:11px;color:var(--muted);margin-bottom:16px;line-height:1.5;}
.plan-divider{height:1px;background:var(--border);margin:14px 0;}
.feat{flex:1;list-style:none;padding:0;margin:0 0 18px;}
.feat li{font-size:12px;color:var(--sub);padding:5px 0 5px 18px;position:relative;line-height:1.45;border-bottom:1px solid var(--border);}
.feat li:last-child{border-bottom:none;}
.feat li::before{content:"✓";position:absolute;left:0;color:var(--green);font-weight:700;font-size:10px;top:7px;}
.feat li.dim{color:var(--muted);} .feat li.dim::before{color:var(--muted);}
.feat li.star{color:var(--accent);} .feat li.star::before{content:"★";color:var(--accent);}
.plan-btn{width:100%;padding:11px;border-radius:8px;font-size:13px;font-weight:700;border:none;cursor:pointer;font-family:var(--font);transition:all var(--t);}
.plan-btn:hover{opacity:.9;transform:translateY(-1px);}
.plan-note{font-size:10px;color:var(--muted);text-align:center;margin-top:8px;line-height:1.5;}

/* Social proof */
.proof{max-width:900px;margin:0 auto;padding:0 20px 64px;}
.proof-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;}
@media(max-width:700px){.proof-grid{grid-template-columns:1fr;}}
.proof-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:20px;}
.proof-quote{font-size:13px;color:var(--sub);line-height:1.65;margin-bottom:14px;font-style:italic;}
.proof-who{font-size:11px;font-weight:600;color:var(--text);}
.proof-role{font-size:10px;color:var(--muted);}

/* FAQ */
.faq{max-width:680px;margin:0 auto;padding:0 20px 80px;}
.faq-title{font-family:var(--display);font-size:1.8rem;font-weight:400;text-align:center;margin-bottom:28px;}
.faq-item{background:var(--surface);border:1px solid var(--border);border-radius:8px;margin-bottom:8px;overflow:hidden;}
.faq-item summary{padding:14px 16px;font-size:13px;font-weight:600;cursor:pointer;list-style:none;display:flex;justify-content:space-between;align-items:center;}
.faq-item summary::-webkit-details-marker{display:none;}
.faq-item summary::after{content:'+';font-size:16px;font-weight:300;color:var(--muted);}
.faq-item[open] summary::after{content:'−';}
.faq-body{padding:0 16px 14px;font-size:12.5px;color:var(--sub);line-height:1.65;}

/* Modal */
.ea-overlay{display:none;position:fixed;inset:0;background:rgba(13,25,38,.45);z-index:1000;align-items:center;justify-content:center;backdrop-filter:blur(4px);}
.ea-modal{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:32px 28px;max-width:400px;width:90%;position:relative;box-shadow:0 20px 60px rgba(0,0,0,.15);}
.ea-input{width:100%;padding:10px 12px;background:var(--elevated);border:1px solid var(--border);border-radius:8px;color:var(--text);font-size:13px;font-family:var(--font);outline:none;box-sizing:border-box;margin-bottom:10px;transition:border-color var(--t);}
.ea-input:focus{border-color:var(--accent);}
</style>
<body>
<nav class="ce-nav">
  <a class="ce-brand" href="/">
    <div class="ce-brand-icon"><svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/></svg></div>
    ClearEye
  </a>
  <div style="margin-left:auto;display:flex;gap:4px;align-items:center;">
    <a href="/app"      class="nav-pill">App</a>
    <a href="/pipeline" class="nav-pill">Pipeline</a>
    <a href="/app" class="nav-cta">Start free &rarr;</a>
  </div>
</nav>

<!-- Hero -->
<div class="hero">
  <div class="hero-tag">Simple, transparent pricing</div>
  <h1 class="hero-h1">Start free.<br><em>Upgrade when it pays for itself.</em></h1>
  <p class="hero-sub">ClearEye runs every deal through a 5-advisor AI council and delivers a Go/No-Go verdict in 90 seconds. No spreadsheets. No guesswork.</p>
  <div class="roi-strip">
    <div class="roi-item"><span class="roi-val">2–4 hrs</span><span>saved per deal</span></div>
    <div class="roi-item" style="width:1px;height:20px;background:var(--border);"></div>
    <div class="roi-item"><span class="roi-val">5 deals/wk</span><span>= 10–20 hrs saved</span></div>
    <div class="roi-item" style="width:1px;height:20px;background:var(--border);"></div>
    <div class="roi-item"><span class="roi-val">$500+</span><span>analyst time/deal</span></div>
  </div>
</div>

<!-- Plans -->
<div class="plans">

  <!-- FREE -->
  <div class="plan">
    <div class="plan-eyebrow">Free forever</div>
    <div class="plan-name">Starter</div>
    <div class="price-row"><div class="price">$0</div><div class="price-period">/month</div></div>
    <div class="plan-target">Try ClearEye risk-free. No credit card.</div>
    <div class="plan-divider"></div>
    <ul class="feat">
      <li>3 analyses / month</li>
      <li>Full 5-advisor council review</li>
      <li>Go/No-Go verdict + IC memo</li>
      <li>Sensitivity analysis</li>
      <li>PDF export</li>
      <li class="dim">LP sharing — not included</li>
      <li class="dim">Deal alerts — not included</li>
      <li class="dim">Pipeline board — not included</li>
    </ul>
    <button class="plan-btn" style="background:var(--elevated);color:var(--accent);border:1px solid rgba(21,94,68,.3);" onclick="location.href='/app'">Start for free &rarr;</button>
    <div class="plan-note">No account needed to try.</div>
  </div>

  <!-- OPERATOR -->
  <div class="plan">
    <div class="plan-eyebrow">Operator</div>
    <div class="plan-name">Operator</div>
    <div class="price-row"><div class="price">$297</div><div class="price-period">/mo</div></div>
    <div class="plan-target">Solo syndicators &amp; active deal finders</div>
    <div class="plan-divider"></div>
    <ul class="feat">
      <li>20 analyses / month</li>
      <li>Everything in Starter</li>
      <li>Deal history &amp; search</li>
      <li>Market heat map</li>
      <li>Assumption override + re-analyze</li>
      <li class="dim">LP sharing — not included</li>
      <li class="dim">Pipeline board — not included</li>
    </ul>
    <button class="plan-btn" style="background:var(--elevated);color:var(--text);border:1px solid var(--border);" onclick="stripeCheckout('operator')">Get Operator &rarr;</button>
    <div class="plan-note">or $2,970/yr — save $594</div>
  </div>

  <!-- PROFESSIONAL — FEATURED -->
  <div class="plan featured">
    <div class="plan-eyebrow">&#9733; Most popular</div>
    <div class="plan-name" style="color:var(--accent);">Professional</div>
    <div class="price-row"><div class="price" style="color:var(--accent);">$697</div><div class="price-period">/mo</div></div>
    <div class="plan-target">Small acquisition shops — 1 to 3 person teams</div>
    <div class="plan-divider"></div>
    <ul class="feat">
      <li class="star">Unlimited analyses</li>
      <li>Everything in Operator</li>
      <li class="star">LP sharing portal (password-gated)</li>
      <li class="star">Deal alerts &amp; email digests</li>
      <li class="star">Pipeline Kanban board</li>
      <li class="star">Saved deal searches</li>
      <li class="star">Comps &amp; market benchmarks</li>
      <li>1 user seat</li>
    </ul>
    <button class="plan-btn" style="background:var(--accent);color:#fff;" onclick="stripeCheckout('professional')">Start Professional &rarr;</button>
    <div class="plan-note">14-day free trial &middot; 30-day money back<br>or $6,970/yr — save $1,394</div>
  </div>

  <!-- TEAM -->
  <div class="plan">
    <div class="plan-eyebrow">Team</div>
    <div class="plan-name">Team</div>
    <div class="price-row"><div class="price">$1,997</div><div class="price-period">/mo</div></div>
    <div class="plan-target">PE funds, family offices, brokerages</div>
    <div class="plan-divider"></div>
    <ul class="feat">
      <li class="star">Unlimited analyses</li>
      <li>Everything in Professional</li>
      <li class="star">5 team seats</li>
      <li class="star">Shared deal workspace</li>
      <li class="star">Due diligence checklist</li>
      <li class="star">Document vault per deal</li>
      <li class="star">API access</li>
      <li class="star">Priority support</li>
    </ul>
    <button class="plan-btn" style="background:var(--elevated);color:var(--text);border:1px solid var(--border);" onclick="stripeCheckout('team')">Get Team &rarr;</button>
    <div class="plan-note">or $19,970/yr — save $3,994<br><a href="mailto:hello@cleareye.ai" style="color:var(--accent);">Talk to sales for custom contracts</a></div>
  </div>

</div><!-- /plans -->

<!-- Social proof -->
<div class="proof">
  <div class="proof-grid">
    <div class="proof-card">
      <div class="proof-quote">"I used to spend a full afternoon building an underwriting model before even deciding if a deal was worth pursuing. ClearEye gives me a verdict in 90 seconds — I can screen 5x more deals now."</div>
      <div class="proof-who">Marcus T.</div>
      <div class="proof-role">Multifamily syndicator, Phoenix AZ</div>
    </div>
    <div class="proof-card">
      <div class="proof-quote">"The LP portal alone is worth the subscription. I share the AI report instead of a raw spreadsheet and my investors immediately understand the deal. It's a professional presentation tool."</div>
      <div class="proof-who">Diana R.</div>
      <div class="proof-role">Fund manager, Denver CO</div>
    </div>
    <div class="proof-card">
      <div class="proof-quote">"ClearEye catches the optimistic assumptions that get buried in a 60-page OM. The Bias Diligence tab alone has saved me from two deals I would have pursued."</div>
      <div class="proof-who">Bobby C.</div>
      <div class="proof-role">Acquisitions analyst, Austin TX</div>
    </div>
  </div>

  <!-- ROI callout -->
  <div style="margin-top:24px;background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:22px 26px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px;">
    <div>
      <div style="font-size:10px;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);font-family:var(--mono);margin-bottom:4px;">Path to $10K MRR</div>
      <div style="font-size:14px;color:var(--text);">8 Professional + 2 Team = <strong style="color:var(--accent);">$9,570/mo</strong> &mdash; just 10 accounts.</div>
    </div>
    <a href="/app" style="display:inline-flex;align-items:center;gap:6px;padding:10px 20px;background:var(--accent);color:#fff;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;">Try a demo deal &rarr;</a>
  </div>
</div>

<!-- FAQ -->
<div class="faq">
  <div class="faq-title">Common questions</div>

  <details class="faq-item">
    <summary>Is the free plan actually free forever?</summary>
    <div class="faq-body">Yes — 3 analyses per month, no credit card required, no expiry. Run your first three deals completely free and only upgrade if ClearEye saves you time and money.</div>
  </details>

  <details class="faq-item">
    <summary>What counts as one analysis?</summary>
    <div class="faq-body">Each PDF or URL submission that triggers a full 5-advisor council review counts as one analysis. Viewing saved reports, re-running assumptions, or adjusting stress tests does not consume quota.</div>
  </details>

  <details class="faq-item">
    <summary>How much time does ClearEye actually save?</summary>
    <div class="faq-body">Manually underwriting an OM typically takes 2–4 hours: reading the document, building a model, sanity-checking assumptions, formatting a summary. ClearEye compresses that to 90 seconds. At 5 deals/week, that's 10–20 hours saved — enough to justify the cost many times over.</div>
  </details>

  <details class="faq-item">
    <summary>Is my deal data private?</summary>
    <div class="faq-body">Absolutely. Your uploaded OMs and deal data are stored in your private account only. We never share, sell, or train AI models on your data. Team and Enterprise plans support dedicated instances with complete data isolation.</div>
  </details>

  <details class="faq-item">
    <summary>Can I cancel anytime?</summary>
    <div class="faq-body">Yes — cancel with one click, no questions asked. Paid plans include a 30-day money-back guarantee. After cancellation, your account drops to the free tier and all your saved reports remain accessible.</div>
  </details>

  <details class="faq-item">
    <summary>Do you integrate with Argus or Excel?</summary>
    <div class="faq-body">Team and Enterprise plans include full API access so you can push ClearEye verdicts, scores, and memos into Argus, Excel, or your internal deal tracking system. <a href="mailto:hello@cleareye.ai" style="color:var(--accent);">Contact us</a> for integration guidance.</div>
  </details>
</div>

<!-- Early access modal -->
<div class="ea-overlay" id="ea-modal-overlay" onclick="if(event.target===this)this.style.display='none'">
  <div class="ea-modal">
    <button onclick="document.getElementById('ea-modal-overlay').style.display='none'" style="position:absolute;top:12px;right:14px;background:none;border:none;color:var(--muted);font-size:18px;cursor:pointer;">&#x2715;</button>
    <div style="font-family:var(--display);font-size:1.6rem;font-weight:400;margin-bottom:8px;">Get Early Access</div>
    <p id="ea-modal-plan-text" style="color:var(--sub);font-size:13px;margin-bottom:20px;line-height:1.6;"></p>
    <input type="email" id="ea-email" class="ea-input" placeholder="your@email.com"
           onkeydown="if(event.key==='Enter')submitEarlyAccess()" />
    <input type="hidden" id="ea-plan-hidden" value="" />
    <button id="ea-submit-btn" onclick="submitEarlyAccess()"
            style="width:100%;padding:11px;background:var(--accent);border:none;color:#fff;border-radius:8px;font-weight:700;font-size:13px;cursor:pointer;font-family:var(--font);">Join Waitlist &rarr;</button>
    <p style="font-size:10px;color:var(--muted);text-align:center;margin-top:10px;">No credit card. Founding-member pricing locked in.</p>
  </div>
</div>

<script>
function stripeCheckout(plan){
  const hasStripe = """ + ("true" if has_stripe else "false") + """;
  if(!hasStripe){showEarlyAccessModal(plan);return;}
  const btn=event&&event.target?event.target.closest('.plan-btn'):null;
  if(btn){btn.disabled=true;btn.textContent='Loading...';}
  fetch('/api/create-checkout-session',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({plan})})
    .then(r=>r.json()).then(d=>{
      if(d.url){location.href=d.url;}
      else{if(btn){btn.disabled=false;btn.textContent=btn.dataset.orig;}showEarlyAccessModal(plan);}
    }).catch(()=>{if(btn){btn.disabled=false;}showEarlyAccessModal(plan);});
}
function showEarlyAccessModal(plan){
  const names={operator:'Operator — $297/mo',professional:'Professional — $697/mo',team:'Team — $1,997/mo'};
  document.getElementById('ea-plan-hidden').value=plan;
  document.getElementById('ea-modal-plan-text').innerHTML='Join the waitlist for <strong style="color:var(--text);">'+(names[plan]||plan)+'</strong>. Founding-member pricing locked in when we launch Stripe billing.';
  document.getElementById('ea-email').value='';
  document.getElementById('ea-modal-overlay').style.display='flex';
  setTimeout(()=>document.getElementById('ea-email').focus(),80);
}
function submitEarlyAccess(){
  const email=(document.getElementById('ea-email').value||'').trim();
  if(!email||!email.includes('@')){document.getElementById('ea-email').style.borderColor='var(--red)';return;}
  const plan=document.getElementById('ea-plan-hidden').value;
  const btn=document.getElementById('ea-submit-btn');
  if(btn){btn.textContent='Submitting...';btn.disabled=true;}
  fetch('/api/early-access',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email,plan})})
    .catch(()=>{}).finally(()=>{
      document.getElementById('ea-modal-overlay').style.display='none';
      const t=document.createElement('div');
      t.style.cssText='position:fixed;bottom:24px;right:24px;background:var(--accent);color:#fff;padding:13px 20px;border-radius:8px;font-size:13px;font-weight:600;z-index:9999;box-shadow:0 4px 16px rgba(0,0,0,.2);';
      t.textContent="You\\'re on the list. We\\'ll be in touch within 24 hours.";
      document.body.appendChild(t);
      setTimeout(()=>t.remove(),4500);
    });
}
</script>
</body></html>""")



@app.route("/api/create-checkout-session", methods=["POST"])
def create_checkout_session():
    """Create Stripe checkout session (#118, #147) — price IDs from env vars."""
    stripe_key = os.environ.get("STRIPE_SECRET_KEY", _env.get("STRIPE_SECRET_KEY", ""))
    if not stripe_key:
        return jsonify({"error": "Stripe not configured"}), 501
    try:
        import stripe
        stripe.api_key = stripe_key
        data = request.get_json(force=True)
        plan = data.get("plan", "operator")
        # Real price IDs from env (#147) — fall back to placeholders for dev
        prices = {
            "operator":     _env.get("STRIPE_PRICE_OPERATOR",     "price_operator_placeholder"),
            "professional": _env.get("STRIPE_PRICE_PROFESSIONAL", "price_professional_placeholder"),
            "team":         _env.get("STRIPE_PRICE_TEAM",         "price_team_placeholder"),
        }
        price_id = prices.get(plan, prices["operator"])
        session_obj = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": price_id, "quantity": 1}],
            mode="subscription",
            success_url=request.host_url + "checkout-success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url=request.host_url + "pricing",
            customer_email=session.get("user_email") or None,
            metadata={"plan": plan},
        )
        return jsonify({"url": session_obj.url})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/early-access", methods=["POST"])
def api_early_access():
    """Log early-access lead from pricing page waitlist form (#178)."""
    data = request.get_json(force=True) or {}
    email = str(data.get("email", "")).strip()
    plan  = str(data.get("plan",  "unknown")).strip()
    if not email or "@" not in email:
        return jsonify({"error": "invalid email"}), 400
    # Log to email_log.jsonl as a lead record
    log_entry = {
        "type": "early_access_lead",
        "email": email,
        "plan": plan,
        "ts": datetime.utcnow().isoformat() + "Z",
    }
    try:
        log_path = Path(__file__).parent / "outputs" / "email_log.jsonl"
        log_path.parent.mkdir(exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")
    except Exception:
        pass
    # Also send internal notification if SMTP configured
    try:
        from email_delivery import _send_email
        notify_to = _env.get("GMAIL_USER", "")
        if notify_to:
            _send_email(
                notify_to,
                f"[ClearEye] New waitlist signup: {plan}",
                f"<p><b>{email}</b> joined the waitlist for the <b>{plan}</b> plan.</p>"
                f"<p>Timestamp: {log_entry['ts']}</p>"
            )
    except Exception:
        pass
    return jsonify({"ok": True, "email": email, "plan": plan})


@app.route("/stripe/webhook", methods=["POST"])
def stripe_webhook():
    """
    Stripe webhook handler (#147).
    Handles: checkout.session.completed, customer.subscription.updated,
             customer.subscription.deleted.
    Set STRIPE_WEBHOOK_SECRET in .env for signature verification.
    """
    stripe_key = os.environ.get("STRIPE_SECRET_KEY", _env.get("STRIPE_SECRET_KEY", ""))
    webhook_secret = _env.get("STRIPE_WEBHOOK_SECRET", "")
    if not stripe_key:
        return jsonify({"error": "Stripe not configured"}), 501

    try:
        import stripe
        stripe.api_key = stripe_key
        payload = request.get_data(as_text=False)
        sig_header = request.headers.get("Stripe-Signature", "")

        if webhook_secret:
            try:
                event = stripe.Webhook.construct_event(payload, sig_header, webhook_secret)
            except stripe.error.SignatureVerificationError:
                _log_error("stripe_webhook", "Invalid Stripe signature")
                return jsonify({"error": "Invalid signature"}), 400
        else:
            # No secret set — accept without verification (dev only)
            event = stripe.Event.construct_from(json.loads(payload), stripe.api_key)

        etype = event["type"]
        obj   = event["data"]["object"]

        if etype == "checkout.session.completed":
            cust_email = obj.get("customer_email") or ""
            plan = (obj.get("metadata") or {}).get("plan", "operator")
            sub_id = obj.get("subscription") or ""
            if cust_email:
                _upsert_user_subscription(cust_email, plan, sub_id, "active")
                _log_error("stripe_subscription", f"New subscription: {cust_email} → {plan}", {"sub_id": sub_id})

        elif etype in ("customer.subscription.updated",):
            # Fetch customer email from customer object
            cust_id = obj.get("customer")
            status = obj.get("status", "")
            plan = _plan_from_stripe_sub(obj)
            if cust_id:
                try:
                    customer = stripe.Customer.retrieve(cust_id)
                    email = customer.get("email") or ""
                    if email:
                        _upsert_user_subscription(email, plan, obj.get("id", ""), status)
                except Exception:
                    pass

        elif etype == "customer.subscription.deleted":
            cust_id = obj.get("customer")
            if cust_id:
                try:
                    customer = stripe.Customer.retrieve(cust_id)
                    email = customer.get("email") or ""
                    if email:
                        _upsert_user_subscription(email, "free", obj.get("id", ""), "canceled")
                        _log_error("stripe_subscription", f"Subscription canceled: {email}")
                except Exception:
                    pass

        return jsonify({"received": True})
    except Exception as e:
        _log_error("stripe_webhook_error", str(e))
        return jsonify({"error": str(e)}), 500


@app.route("/checkout-success")
def checkout_success():
    """Post-payment success page with welcome email and getting-started guide (#182)."""
    session_id  = request.args.get("session_id", "")
    plan        = "Operator"
    amount      = ""
    cust_email  = ""
    confirmed   = False

    stripe_key  = os.environ.get("STRIPE_SECRET_KEY", _env.get("STRIPE_SECRET_KEY", ""))
    if stripe_key and session_id and not session_id.startswith("{"):
        try:
            import stripe as _stripe
            _stripe.api_key = stripe_key
            sess = _stripe.checkout.Session.retrieve(session_id)
            plan_raw = (sess.get("metadata") or {}).get("plan", "operator")
            plan     = {"operator": "Operator", "professional": "Professional", "team": "Team"}.get(plan_raw, plan_raw.title())
            cust_email = sess.get("customer_email") or sess.get("customer_details", {}).get("email", "")
            amt_cents  = sess.get("amount_total", 0)
            amount     = "${:,.0f}".format(amt_cents / 100) if amt_cents else ""
            confirmed  = True
            # Log purchase
            try:
                import os as _os
                _pf = _os.path.join(_os.path.dirname(__file__), "outputs", "purchases.jsonl")
                entry = json.dumps({"type": "purchase", "session_id": session_id, "plan": plan_raw,
                                    "email": cust_email, "amount_cents": amt_cents,
                                    "ts": datetime.utcnow().isoformat() + "Z"})
                with open(_pf, "a", encoding="utf-8") as _f:
                    _f.write(entry + "\n")
            except Exception:
                pass
            # Send welcome email
            if cust_email:
                try:
                    from email_delivery import _send_email
                    _welcome_html = (
                        "<div style='font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;"
                        "max-width:560px;margin:0 auto;padding:32px 24px;background:#0d1117;color:#e6edf3;border-radius:12px;'>"
                        "<div style='font-size:1.5rem;font-weight:800;color:#58a6ff;margin-bottom:8px;'>ClearEye</div>"
                        "<h1 style='font-size:1.25rem;font-weight:700;margin:0 0 16px;'>Welcome to ClearEye " + plan + "!</h1>"
                        "<p style='color:#8b949e;line-height:1.6;'>Your subscription is active. Here are 3 things to do first:</p>"
                        "<ol style='color:#e6edf3;line-height:1.8;padding-left:20px;'>"
                        "<li><strong>Run your first analysis</strong> — paste any listing URL or upload an OM PDF at "
                        "<a href='http://localhost:5052/app' style='color:#58a6ff;'>ClearEye</a></li>"
                        "<li><strong>Set up deal alerts</strong> — configure your target markets in Find Deals</li>"
                        "<li><strong>Share a report</strong> — use the Share Link button to send a read-only report to your LP</li>"
                        "</ol>"
                        "<div style='margin-top:24px;'>"
                        "<a href='http://localhost:5052/app' style='display:inline-block;padding:10px 22px;"
                        "background:#1f6feb;color:#fff;border-radius:7px;text-decoration:none;font-weight:600;"
                        "font-size:14px;'>Start Analyzing &rarr;</a></div>"
                        "<p style='margin-top:32px;font-size:11px;color:#484f58;'>ClearEye Real Estate Intelligence &middot; "
                        "<a href='http://localhost:5052' style='color:#58a6ff;'>localhost:5052</a></p>"
                        "</div>"
                    )
                    _send_email(cust_email, "Welcome to ClearEye " + plan + " — Get Started", _welcome_html)
                except Exception:
                    pass
        except Exception:
            pass

    plan_features = {
        "Operator":     ["50 analyses/month", "Full Council review", "LP portal", "Email alerts"],
        "Professional": ["200 analyses/month", "Full Council review", "LP portal", "Email alerts", "API access", "Priority support"],
        "Team":         ["Unlimited analyses", "Full Council review", "LP portal", "Email alerts", "API access", "Dedicated onboarding"],
    }
    features = plan_features.get(plan, plan_features["Operator"])
    features_html = "".join("<li style='padding:4px 0;'>" + f + "</li>" for f in features)

    page = (
        "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>Welcome to ClearEye!</title>"
        "<link rel='stylesheet' href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css'>"
        "<style>"
        "body{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh;"
        "display:flex;align-items:center;justify-content:center;padding:24px;}"
        ".card{background:#161b22;border:1px solid rgba(255,255,255,.1);border-radius:16px;padding:40px;max-width:540px;width:100%;}"
        ".check-circle{width:72px;height:72px;background:rgba(63,185,80,.12);border:2px solid #3fb950;"
        "border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:2rem;margin:0 auto 24px;}"
        ".plan-badge{display:inline-block;padding:4px 14px;background:rgba(31,111,235,.15);border:1px solid rgba(88,166,255,.3);"
        "border-radius:50px;font-size:12px;font-weight:600;color:#58a6ff;margin-bottom:20px;}"
        ".step{display:flex;gap:14px;align-items:flex-start;padding:14px 0;border-bottom:1px solid rgba(255,255,255,.06);}"
        ".step:last-child{border-bottom:none;}"
        ".step-num{width:28px;height:28px;flex-shrink:0;background:rgba(31,111,235,.15);border:1px solid rgba(88,166,255,.3);"
        "border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#58a6ff;}"
        ".btn-primary-ce{display:inline-block;padding:12px 28px;background:linear-gradient(135deg,#1f6feb,#388bfd);"
        "color:#fff;border-radius:8px;text-decoration:none;font-weight:600;font-size:15px;"
        "box-shadow:0 4px 20px rgba(31,111,235,.4);transition:all .2s;}"
        ".btn-primary-ce:hover{transform:translateY(-1px);color:#fff;}"
        ".features-list{list-style:none;padding:0;margin:0;color:#8b949e;font-size:13px;}"
        ".features-list li::before{content:'✓ ';color:#3fb950;font-weight:700;}"
        "</style></head><body>"
        "<div class='card text-center'>"
        "<div class='check-circle'>&#10003;</div>"
        "<h1 style='font-size:1.5rem;font-weight:800;margin-bottom:8px;'>You're all set!</h1>"
        "<div class='plan-badge'>ClearEye " + plan + "</div>"
        + ("<p style='color:#8b949e;font-size:13px;margin-bottom:4px;'>Confirmation sent to <strong style='color:#e6edf3;'>" + cust_email + "</strong></p>" if cust_email else "")
        + ("<p style='color:#8b949e;font-size:13px;margin-bottom:20px;'>Amount charged: <strong style='color:#e6edf3;'>" + amount + "</strong></p>" if amount else "<p style='margin-bottom:20px;'></p>")
        + "<div style='text-align:left;background:rgba(255,255,255,.03);border-radius:10px;padding:16px 20px;margin-bottom:24px;'>"
        "<div style='font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#8b949e;margin-bottom:10px;'>Your plan includes</div>"
        "<ul class='features-list'>" + features_html + "</ul></div>"
        "<div style='text-align:left;margin-bottom:28px;'>"
        "<div style='font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#8b949e;margin-bottom:4px;'>Get started in 3 steps</div>"
        "<div class='step'><div class='step-num'>1</div><div><strong>Run your first analysis</strong>"
        "<div style='font-size:12px;color:#8b949e;margin-top:3px;'>Paste a listing URL or upload an OM PDF on the app page</div></div></div>"
        "<div class='step'><div class='step-num'>2</div><div><strong>Set up deal alerts</strong>"
        "<div style='font-size:12px;color:#8b949e;margin-top:3px;'>Configure target markets and cap rate thresholds in Find Deals</div></div></div>"
        "<div class='step'><div class='step-num'>3</div><div><strong>Share your first report</strong>"
        "<div style='font-size:12px;color:#8b949e;margin-top:3px;'>Use the Share Link button to send a read-only report to your LP or partner</div></div></div>"
        "</div>"
        "<a href='/app' class='btn-primary-ce'>Open ClearEye &rarr;</a>"
        "<p style='margin-top:20px;font-size:11px;color:#484f58;'>"
        "<a href='/pricing' style='color:#8b949e;'>View plans</a> &middot; "
        "<a href='/' style='color:#8b949e;'>Home</a></p>"
        "</div>"
        "</body></html>"
    )
    return page


def _plan_from_stripe_sub(sub_obj: dict) -> str:
    """Extract plan name from Stripe subscription items metadata."""
    try:
        items = sub_obj.get("items", {}).get("data", [])
        if items:
            price_id = items[0].get("price", {}).get("id", "")
            op   = _env.get("STRIPE_PRICE_OPERATOR", "")
            pro  = _env.get("STRIPE_PRICE_PROFESSIONAL", "")
            team = _env.get("STRIPE_PRICE_TEAM", "")
            if price_id == op:   return "operator"
            if price_id == pro:  return "professional"
            if price_id == team: return "team"
    except Exception:
        pass
    return "operator"


def _upsert_user_subscription(email: str, plan: str, sub_id: str, status: str):
    """Write or update subscription record in user_subscriptions table."""
    from db import _conn as _db_conn, _with_retry
    try:
        con = _db_conn()
        con.execute("""
            CREATE TABLE IF NOT EXISTS user_subscriptions (
                email       TEXT PRIMARY KEY,
                plan        TEXT NOT NULL DEFAULT 'free',
                sub_id      TEXT,
                status      TEXT NOT NULL DEFAULT 'active',
                updated_at  TEXT NOT NULL
            )
        """)
        con.execute("""
            INSERT INTO user_subscriptions (email, plan, sub_id, status, updated_at)
            VALUES (?,?,?,?,datetime('now'))
            ON CONFLICT(email) DO UPDATE SET
                plan=excluded.plan,
                sub_id=excluded.sub_id,
                status=excluded.status,
                updated_at=excluded.updated_at
        """, (email.lower().strip(), plan, sub_id, status))
        con.commit()
    except Exception as e:
        _log_error("upsert_subscription_error", str(e))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def landing():
    landing_path = Path(__file__).parent / "landing.html"
    if landing_path.exists():
        return send_file(str(landing_path))
    return render_template_string(HTML)


@app.route("/app")
def index():
    return render_template_string(HTML)


@app.route("/api/quick-scan", methods=["POST"])
def api_quick_scan():
    """#228: Quick Kill 30-second pre-screen — deal-breaker flags + IRR plausibility check."""
    data = request.get_json(force=True) or {}
    om_text = (data.get("om_text") or "").strip()[:4000]
    if not om_text:
        return jsonify({"error": "No OM text provided"}), 400

    prompt = f"""You are a real estate investment pre-screener. Given this offering memorandum excerpt, do a 30-second triage.

OM TEXT:
{om_text}

Respond in EXACTLY this format (no extra text):

RECOMMENDATION: FULL ANALYSIS  [or]  HARD PASS

DEAL_BREAKERS:
- [flag 1 or "None found"]
- [flag 2 if present]
- [flag 3 if present]

IRR_CHECK: [1-2 sentences: does the sponsor's projected IRR make sense given the cap rate, price, and current market conditions? Note if it's implausible.]

REASON: [1 sentence summary for HARD PASS, or "Deal clears pre-screen filters" for FULL ANALYSIS]"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        # Parse structured response
        rec = "FULL ANALYSIS"
        if "HARD PASS" in text.upper():
            rec = "HARD PASS"
        deal_breakers = []
        irr_check = ""
        reason = ""
        for line in text.split("\n"):
            l = line.strip()
            if l.startswith("- ") and "None found" not in l:
                deal_breakers.append(l[2:])
            elif l.startswith("IRR_CHECK:"):
                irr_check = l[10:].strip()
            elif l.startswith("REASON:"):
                reason = l[7:].strip()
        return jsonify({
            "recommendation": rec,
            "deal_breakers": deal_breakers,
            "irr_check": irr_check,
            "reason": reason,
            "raw": text
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/analyze", methods=["POST"])
def analyze():
    data = request.get_json(force=True)
    om_text = (data.get("om_text") or "").strip()
    if not om_text:
        return jsonify({"error": "No OM text provided"}), 400
    recipient_email = (data.get("email") or "").strip() or None

    # Use session email if no email in request (authenticated user)
    user_email = recipient_email or session.get("user_email")

    # Monthly quota enforcement (#148)
    quota = check_quota(user_email)
    if not quota["allowed"]:
        tier_labels = {"free": "Free (3/mo)", "operator": "Operator (5/mo)", "professional": "Professional (25/mo)"}
        label = tier_labels.get(quota["tier"], quota["tier"])
        return jsonify({
            "error": (
                f"{label} plan: {quota['used']}/{quota['limit']} analyses used this month. "
                f"Resets {quota['resets_at'][:10]}. "
                f"Upgrade at /pricing for more analyses."
            ),
            "quota_exceeded": True,
            "quota": quota,
        }), 429

    job_id = str(uuid.uuid4())[:8]
    JOBS[job_id] = {"status": "queued"}
    job_create(job_id, om_text, user_email)
    t = threading.Thread(target=_analyze, args=(job_id, om_text, user_email), daemon=True)
    t.start()
    return jsonify({"job_id": job_id, "quota": quota})


@app.route("/api/usage")
def api_usage():
    """Return quota status for the current user (#148)."""
    user_email = session.get("user_email") or request.args.get("email")
    quota = check_quota(user_email)
    return jsonify(quota)


@app.route("/upload", methods=["POST"])
def upload_pdf():
    """Accept PDF upload, extract text, return for textarea population. (#111)"""
    f = request.files.get("pdf")
    if not f:
        return jsonify({"error": "No file"}), 400
    try:
        import pdfplumber, io
        with pdfplumber.open(io.BytesIO(f.read())) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        text = "\n\n".join(p for p in pages if p.strip())
        if not text.strip():
            return jsonify({"error": "Could not extract text from PDF — try copy-paste instead"}), 422
        return jsonify({"text": text, "pages": len(pages), "filename": f.filename})
    except Exception as e:
        return jsonify({"error": f"PDF extraction failed: {e}"}), 500


@app.route("/api/fetch-url", methods=["POST"])
def fetch_listing_url():
    """
    Scrape a LoopNet/Crexi/CoStar listing URL and extract deal metadata (#137).
    Returns: {deal_name, address, price, units, cap_rate, noi, om_text, _source}
    """
    import re as _re
    data = request.get_json(force=True)
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400
    if not url.startswith("http"):
        url = "https://" + url

    scraper_cb = _CIRCUIT_BREAKERS.get("scraper")
    if scraper_cb and not scraper_cb.allow_request():
        return jsonify({
            "error": "URL fetcher temporarily unavailable (too many recent failures). Please paste listing details manually.",
            "om_text": f"Source URL: {url}\n\n[URL fetcher circuit open — please paste the listing details below]",
            "_circuit_open": True,
        }), 200

    try:
        hdrs = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        resp = requests.get(url, headers=hdrs, timeout=15, allow_redirects=True)
        html = resp.text
        if scraper_cb:
            scraper_cb.record_success()
        result = _extract_listing_data(url, html)
        return jsonify(result)
    except Exception as e:
        if scraper_cb:
            scraper_cb.record_failure(e)
        return jsonify({"error": f"Could not fetch listing: {e}",
                        "om_text": f"Source URL: {url}\n\n[Automatic extraction failed — please paste the listing details below]"}), 200


def _extract_listing_data(url: str, html: str) -> dict:
    """
    Extract structured deal fields from a real estate listing HTML page (#137).
    Strategy: JSON-LD → meta tags → regex on body text → friendly om_text string.
    """
    import re as _re
    import json as _json

    result = {
        "source_url": url,
        "_source": "url_scrape",
        "deal_name": "",
        "address": "",
        "city": "",
        "asking_price": None,
        "units": None,
        "cap_rate": None,
        "noi": None,
        "property_type": "",
        "description": "",
        "om_text": "",
    }

    # ── 1. JSON-LD structured data (most reliable) ──────────────────────────
    ld_matches = _re.findall(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, _re.S | _re.I)
    for ld_raw in ld_matches:
        try:
            ld = _json.loads(ld_raw.strip())
            if isinstance(ld, list):
                ld = ld[0]
            rtype = ld.get("@type", "")
            if any(t in rtype for t in ["RealEstate", "Property", "Apartment", "LocalBusiness", "Product"]):
                result["deal_name"] = result["deal_name"] or ld.get("name", "")
                result["description"] = result["description"] or ld.get("description", "")
                addr = ld.get("address", {})
                if isinstance(addr, dict):
                    parts = [addr.get("streetAddress",""), addr.get("addressLocality",""), addr.get("addressRegion","")]
                    result["address"] = result["address"] or ", ".join(p for p in parts if p)
                    result["city"] = result["city"] or addr.get("addressLocality", "")
                price = ld.get("price") or ld.get("offers", {}).get("price")
                if price and not result["asking_price"]:
                    try:
                        result["asking_price"] = float(_re.sub(r"[^0-9.]", "", str(price)))
                    except Exception:
                        pass
        except Exception:
            pass

    # ── 2. Open Graph / meta tags ────────────────────────────────────────────
    def _meta(prop_or_name: str) -> str:
        m = (_re.search(rf'<meta[^>]*(?:property|name)=["\'][^"\']*{prop_or_name}[^"\']*["\'][^>]*content=["\']([^"\']+)["\']', html, _re.I)
             or _re.search(rf'<meta[^>]*content=["\']([^"\']+)["\'][^>]*(?:property|name)=["\'][^"\']*{prop_or_name}[^"\']*["\']', html, _re.I))
        return m.group(1).strip() if m else ""

    title_m = _re.search(r"<title[^>]*>(.*?)</title>", html, _re.S | _re.I)
    page_title = title_m.group(1).strip() if title_m else ""

    result["deal_name"] = result["deal_name"] or _meta("og:title") or page_title
    result["description"] = result["description"] or _meta("og:description") or _meta("description")

    # ── 3. Site-specific extraction (LoopNet / Crexi) ───────────────────────
    is_loopnet = "loopnet.com" in url.lower()
    is_crexi   = "crexi.com" in url.lower()

    # Strip HTML tags for regex scanning
    body_text = _re.sub(r"<[^>]+>", " ", html)
    body_text = _re.sub(r"\s+", " ", body_text)

    def _find_price(text: str):
        # Match $XX,XXX,XXX or $XX.X M / $X.X B
        m = _re.search(r"\$\s*([\d,]+(?:\.\d+)?)\s*[Mm](?:illion)?", text)
        if m:
            return float(m.group(1).replace(",", "")) * 1_000_000
        m = _re.search(r"\$\s*([\d,]+(?:\.\d+)?)\s*[Bb](?:illion)?", text)
        if m:
            return float(m.group(1).replace(",", "")) * 1_000_000_000
        m = _re.search(r"(?:asking|price|list)[^$]*\$\s*([\d,]+)", text, _re.I)
        if m:
            return float(m.group(1).replace(",", ""))
        m = _re.search(r"\$\s*([\d]{1,3}(?:,[\d]{3})+)", text)
        if m:
            return float(m.group(1).replace(",", ""))
        return None

    def _find_units(text: str):
        m = _re.search(r"([\d,]+)\s+(?:unit|apartment|bed|suite)s?", text, _re.I)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except Exception:
                pass
        return None

    def _find_cap(text: str):
        # Match "6.2% cap rate"  OR  "Cap Rate: 6.2%"  OR  "cap rate of 6.2%"
        for pat in [
            r"([\d.]+)\s*%\s*cap(?:\s*rate)?",
            r"cap(?:italization)?\s*rate[^0-9]*([\d.]+)\s*%",
            r"cap(?:italization)?\s*rate[:\s]+([\d.]+)",
        ]:
            m = _re.search(pat, text, _re.I)
            if m:
                grp = m.group(1) if m.lastindex == 1 else (m.group(1) or m.group(2) if m.lastindex >= 2 else None)
                # Find the first non-None group
                for g in range(1, m.lastindex + 1):
                    try:
                        val = float(m.group(g))
                        if 0 < val < 30:  # sanity check: cap rate 0-30%
                            return val
                    except Exception:
                        continue
        return None

    def _find_noi(text: str):
        for pat in [
            r"(?:NOI|net operating income)[^$\d]*\$\s*([\d,]+)",
            r"\$\s*([\d,]+)\s*(?:NOI|net operating)",
            r"(?:annual\s+)?NOI[:\s]+\$?([\d,]+)",
        ]:
            m = _re.search(pat, text, _re.I)
            if m:
                try:
                    return float(m.group(1).replace(",", ""))
                except Exception:
                    pass
        return None

    if not result["asking_price"]:
        result["asking_price"] = _find_price(body_text)
    if not result["units"]:
        result["units"] = _find_units(body_text)
    if not result["cap_rate"]:
        result["cap_rate"] = _find_cap(body_text)
    if not result["noi"]:
        result["noi"] = _find_noi(body_text)

    # Detect property type
    if not result["property_type"]:
        pt_m = _re.search(r"\b(multifamily|apartment|retail|office|industrial|warehouse|mixed.use|hotel)\b", body_text, _re.I)
        result["property_type"] = pt_m.group(1).title() if pt_m else ""

    # Try address if still missing
    if not result["address"]:
        addr_m = _re.search(r"(\d+\s+[A-Z][A-Za-z\s,]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Way|Lane|Ln|Court|Ct|Place|Pl))", body_text)
        result["address"] = addr_m.group(1).strip() if addr_m else ""

    # ── 4. Build OM text string to pre-populate the textarea ─────────────────
    lines = [f"Source: {url}", ""]
    if result["deal_name"]:
        lines.append(f"Property: {result['deal_name']}")
    if result["address"]:
        lines.append(f"Address: {result['address']}")
    if result["property_type"]:
        lines.append(f"Property Type: {result['property_type']}")
    if result["units"]:
        lines.append(f"Units: {result['units']}")
    if result["asking_price"]:
        price_fmt = f"${result['asking_price']:,.0f}"
        lines.append(f"Asking Price: {price_fmt}")
    if result["cap_rate"]:
        lines.append(f"Cap Rate: {result['cap_rate']:.1f}%")
    if result["noi"]:
        lines.append(f"NOI: ${result['noi']:,.0f}")
    if result["description"]:
        lines.append("")
        lines.append("Description:")
        lines.append(result["description"][:800])
    lines.append("")
    lines.append("[Add additional deal details: rent roll, debt terms, projected IRR, exit assumptions]")

    result["om_text"] = "\n".join(lines)
    # Clean up None values for JSON
    for k in list(result.keys()):
        if result[k] is None:
            result[k] = ""
    return result


@app.route("/stream/<job_id>")
def stream_status(job_id):
    """Server-Sent Events stream for real-time pipeline progress. (#114)"""
    def generate():
        import time
        seen = 0
        for _ in range(120):  # max 5 min
            q = SSE_QUEUES.get(job_id, [])
            while seen < len(q):
                yield q[seen]
                seen += 1
            # Also check DB status
            job = JOBS.get(job_id) or {}
            if job.get("status") in ("done", "error"):
                break
            time.sleep(1)
        yield "event: close\ndata: \n\n"
    return Response(stream_with_context(generate()),
                    mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.route("/report/<job_id>")
def report(job_id):
    """Shareable read-only report page. (#116)"""
    # Try memory first, then DB
    job = JOBS.get(job_id)
    if not job or job.get("status") != "done":
        db_job = job_get(job_id)
        if db_job and db_job.get("result"):
            job = {**db_job["result"], "status": "done"}
    if not job or job.get("status") != "done":
        return "<h2 style='font-family:sans-serif;color:#888;padding:40px;'>Report not found or still processing.</h2>", 404
    return render_template_string(HTML, _prefill_job=json.dumps({
        "status": "done",
        "deal": job.get("deal", {}),
        "memo": job.get("memo", ""),
        "advisors": job.get("advisors", {}),
        "stress_table": job.get("stress_table", ""),
        "validation_report": job.get("validation_report", ""),
        "bias_report": job.get("bias_report", ""),
        "premortem_report": job.get("premortem_report", ""),
        "macro_brief": job.get("macro", {}).get("brief", ""),
    }), _report_mode=True, _job_id=job_id)


@app.route("/api/reanalyze/<job_id>", methods=["POST"])
def reanalyze(job_id):
    """
    Re-run analysis on a saved deal (#190).
    Retrieves original om_text from DB, queues a fresh analysis job,
    stores parent_job_id in JOBS for diff tracking.
    """
    db_job = job_get(job_id)
    if not db_job:
        return jsonify({"error": "Job not found"}), 404

    om_text = db_job.get("om_text", "").strip()
    if not om_text:
        # Fall back to deal text from result
        result = db_job.get("result") or {}
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except Exception:
                result = {}
        om_text = result.get("deal", {}).get("raw_text", "") or result.get("om_text", "")

    if not om_text:
        return jsonify({"error": "No original deal text found — cannot re-analyze"}), 422

    user_email = session.get("user_email")
    new_job_id = str(uuid.uuid4())[:8]
    JOBS[new_job_id] = {"status": "queued", "parent_job_id": job_id}
    job_create(new_job_id, om_text, user_email)
    t = threading.Thread(target=_analyze, args=(new_job_id, om_text, user_email), daemon=True)
    t.start()
    return jsonify({"ok": True, "new_job_id": new_job_id, "parent_job_id": job_id})


@app.route("/api/diff/<old_job_id>/<new_job_id>")
def api_diff(old_job_id, new_job_id):
    """
    Compare two analysis results and return a structured diff (#190).
    """
    def _get_job(jid):
        j = JOBS.get(jid)
        if j and j.get("status") == "done":
            return j
        db = job_get(jid)
        if db and db.get("result"):
            r = db["result"]
            return json.loads(r) if isinstance(r, str) else r
        return None

    def _score(job):
        if not job:
            return None
        import re as _re
        adv = job.get("advisors") or {}
        scores = []
        for v in adv.values():
            text = v if isinstance(v, str) else str(v)
            m = _re.search(r"score[:\s]+([0-9]+)\s*/\s*100", text, _re.I)
            if not m:
                m = _re.search(r"\b([0-9]{1,2}|100)\s*/\s*100\b", text)
            if m:
                scores.append(int(m.group(1)))
        return round(sum(scores) / len(scores)) if scores else None

    old = _get_job(old_job_id)
    new = _get_job(new_job_id)

    if not old or not new:
        return jsonify({"error": "One or both jobs not ready"}), 404

    old_deal = old.get("deal") or {}
    new_deal = new.get("deal") or {}
    old_score = _score(old)
    new_score = _score(new)

    def _delta(a, b):
        """Return numeric delta or None."""
        try:
            return round(float(b) - float(a), 2)
        except Exception:
            return None

    diff = {
        "verdict": {"old": old.get("verdict") or old_deal.get("verdict", ""),
                    "new": new.get("verdict") or new_deal.get("verdict", ""),
                    "changed": (old.get("verdict") or "") != (new.get("verdict") or "")},
        "council_score": {"old": old_score, "new": new_score,
                          "delta": _delta(old_score, new_score) if old_score and new_score else None},
        "cap_rate":      {"old": old_deal.get("cap_rate"),    "new": new_deal.get("cap_rate"),
                          "delta": _delta(old_deal.get("cap_rate"), new_deal.get("cap_rate"))},
        "asking_price":  {"old": old_deal.get("asking_price") or old_deal.get("purchase_price"),
                          "new": new_deal.get("asking_price") or new_deal.get("purchase_price"),
                          "delta": _delta(old_deal.get("asking_price"), new_deal.get("asking_price"))},
        "old_job_id":    old_job_id,
        "new_job_id":    new_job_id,
    }
    return jsonify(diff)


@app.route("/api/history")
def api_history():
    """Return recent analyses with score_history for sparklines (#112, #187, #195)."""
    # Filter params (#195)
    q       = (request.args.get("q") or "").strip().lower()
    verdict = (request.args.get("verdict") or "").strip().lower()
    days    = request.args.get("days", "")
    try:
        days = int(days)
    except (ValueError, TypeError):
        days = 0  # 0 = all time

    from db import _conn as _db_conn2
    try:
        _con2 = _db_conn2()
        sql  = "SELECT id, deal_name, verdict, confidence, created_at, result FROM deals WHERE 1=1"
        params = []
        if q:
            sql += " AND LOWER(deal_name) LIKE ?"
            params.append(f"%{q}%")
        if verdict and verdict != "all":
            sql += " AND LOWER(verdict)=?"
            params.append(verdict)
        if days and days > 0:
            sql += " AND created_at >= datetime('now', ?)"
            params.append(f"-{days} days")
        sql += " ORDER BY created_at DESC LIMIT 50"
        raw_rows = _con2.execute(sql, params).fetchall()
        rows = [dict(r) for r in raw_rows]
    except Exception:
        rows = jobs_recent(limit=15)

    def _extract_score(result_json):
        """Pull average advisor score from a stored result dict."""
        import re as _re
        if not result_json:
            return None
        try:
            result = json.loads(result_json) if isinstance(result_json, str) else result_json
        except Exception:
            return None
        adv = result.get("advisors") or {}
        scores = []
        for v in adv.values():
            text = v if isinstance(v, str) else str(v)
            m = _re.search(r"score[:\s]+([0-9]+)\s*/\s*100", text, _re.I)
            if not m:
                m = _re.search(r"\b([0-9]{1,2}|100)\s*/\s*100\b", text)
            if m:
                scores.append(int(m.group(1)))
        return round(sum(scores) / len(scores)) if scores else None

    # Build score_history for each deal by querying all same-named analyses
    from db import _conn as _db_conn
    augmented = []
    for row in rows:
        deal_name = row.get("deal_name") or ""
        score_history = []
        if deal_name:
            try:
                hist_rows = _db_conn().execute(
                    "SELECT result, created_at FROM deals "
                    "WHERE LOWER(deal_name)=LOWER(?) ORDER BY created_at ASC LIMIT 20",
                    (deal_name,)
                ).fetchall()
                for hr in hist_rows:
                    s = _extract_score(hr[0])
                    if s is not None:
                        score_history.append(s)
            except Exception:
                pass
        augmented.append({**row, "score_history": score_history})
    return jsonify(augmented)


@app.route("/api/market-pulse")
def api_market_pulse():
    """Live FRED macro indicators with 6-hour cache (#185)."""
    import os as _osp, time as _time
    _cache_file = _osp.path.join(_osp.path.dirname(__file__), "outputs", "market_pulse_cache.json")
    _CACHE_TTL  = 6 * 3600  # 6 hours

    # Return cached data if fresh
    try:
        with open(_cache_file, encoding="utf-8") as _cf:
            _cached = json.loads(_cf.read())
        if _time.time() - _cached.get("_cached_at", 0) < _CACHE_TTL:
            return jsonify(_cached)
    except Exception:
        pass

    # Fetch live data via circuit breaker
    from circuit_breaker import BREAKERS
    cb = BREAKERS.get("fred")
    result = {}
    if cb and cb.allow_request():
        try:
            from macro_context import get_macro_data
            raw = get_macro_data()
            if raw and len(raw) >= 3:
                cb.record_success()
                # Compute derived macro headwind score (0–100, higher = more headwind)
                treasury  = float(raw.get("10yr_treasury") or 4.5)
                mortgage  = float(raw.get("30yr_mortgage") or 7.0)
                unrate    = float(raw.get("unemployment")  or 4.0)
                cpi       = float(raw.get("cpi_yoy")       or 3.0)
                fed_funds = float(raw.get("fed_funds_rate") or 5.0)
                # Headwind scoring: high rates + high inflation + high unemployment = more headwind
                rate_score = min(100, max(0, (treasury - 2.0) / 6.0 * 40))   # 0-40 pts
                mort_score = min(100, max(0, (mortgage - 4.0) / 6.0 * 30))   # 0-30 pts
                unem_score = min(100, max(0, (unrate   - 3.0) / 7.0 * 20))   # 0-20 pts
                infl_score = min(100, max(0, (cpi      - 2.0) / 8.0 * 10))   # 0-10 pts
                headwind   = round(rate_score + mort_score + unem_score + infl_score)
                result = {
                    "10yr_treasury":   treasury,
                    "30yr_mortgage":   mortgage,
                    "unemployment":    unrate,
                    "cpi_yoy":         cpi,
                    "fed_funds_rate":  fed_funds,
                    "headwind_score":  headwind,
                    "as_of":           raw.get("as_of", ""),
                    "_cached_at":      _time.time(),
                    "_source":         "live",
                }
                try:
                    with open(_cache_file, "w", encoding="utf-8") as _cf:
                        _cf.write(json.dumps(result))
                except Exception:
                    pass
            else:
                cb.record_failure(Exception("FRED returned insufficient data"))
        except Exception as _exc:
            if cb:
                cb.record_failure(_exc)

    if not result:
        # Return stale cache or hard-coded fallback
        try:
            with open(_cache_file, encoding="utf-8") as _cf:
                result = json.loads(_cf.read())
                result["_source"] = "stale_cache"
        except Exception:
            result = {
                "10yr_treasury": 4.45, "30yr_mortgage": 6.87,
                "unemployment": 3.9,   "cpi_yoy": 3.2,
                "fed_funds_rate": 5.33, "headwind_score": 52,
                "as_of": "", "_source": "fallback",
            }
    return jsonify(result)


@app.route("/api/deals/history")
def api_deals_history():
    """Return up to 100 analyzed deals with scores for Deal History table (#184)."""
    rows = jobs_recent(limit=100)
    out = []
    for row in rows:
        result = row.get("result") or {}
        if isinstance(result, str):
            try:
                result = json.loads(result)
            except Exception:
                result = {}
        deal   = result.get("deal") or {}
        adv    = result.get("advisors") or {}
        # Compute average advisor score
        scores = []
        for adv_key, adv_val in adv.items():
            text = adv_val if isinstance(adv_val, str) else str(adv_val)
            import re as _re
            m = _re.search(r"score[:\s]+([0-9]+)\s*/\s*100", text, _re.I)
            if not m:
                m = _re.search(r"\b([0-9]{1,2}|100)\s*/\s*100\b", text)
            if m:
                scores.append(int(m.group(1)))
        council_score = round(sum(scores) / len(scores)) if scores else None
        # #263: Include score_history for sparkline rendering
        score_history = [council_score] if council_score is not None else []
        out.append({
            "job_id":        row.get("id", ""),
            "deal_name":     row.get("deal_name") or deal.get("deal_name", "Unnamed"),
            "verdict":       row.get("verdict") or "",
            "confidence":    row.get("confidence"),
            "council_score": council_score,
            "score_history": score_history,
            "cap_rate":      deal.get("cap_rate"),
            "asking_price":  deal.get("asking_price") or deal.get("purchase_price"),
            "asset_class":   deal.get("asset_class", ""),
            "market":        deal.get("market", "") or deal.get("location", ""),
            "created_at":    row.get("created_at", ""),
        })
    return jsonify(out)


@app.route("/widget/<job_id>")
def widget(job_id):
    """Embeddable ClearEye Score badge HTML (#120). iframe-able for deal sponsor sites."""
    job = JOBS.get(job_id)
    if not job and job_get(job_id):
        db = job_get(job_id)
        job = db.get("result") if db else None
    if not job or job.get("status") != "done":
        return """<div style="font-family:sans-serif;font-size:12px;color:#888;padding:12px;border:1px solid #ddd;border-radius:6px;">
            ClearEye Analysis Pending</div>""", 202

    memo = job.get("memo", "")
    import re as _re
    mu = memo.upper()
    if "NO-GO" in mu:
        verdict, color, bg = "NO-GO", "#f85149", "#2d1a1a"
    elif _re.search(r"\bGO\b", mu) and "CONDITIONAL" not in mu:
        verdict, color, bg = "GO", "#3fb950", "#1a2d1a"
    else:
        verdict, color, bg = "CONDITIONAL", "#d29922", "#2d2a1a"
    conf_m = _re.search(r"Confidence[^0-9]*([0-9]+)", memo)
    conf = conf_m.group(1) if conf_m else "—"
    deal = job.get("deal") or {}
    deal_name = deal.get("deal_name", "Deal Analysis")
    cap = deal.get("cap_rate", "—")
    irr = deal.get("projected_irr", "—")

    html = f"""<!DOCTYPE html><html>
<head><meta charset="UTF-8">
<style>
body{{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:{bg};color:#e6edf3;}}
.widget{{padding:14px 16px;border:1px solid {color};border-radius:8px;max-width:320px;}}
.verdict{{font-size:1.4rem;font-weight:900;color:{color};letter-spacing:1px;}}
.name{{font-size:11px;color:#8b949e;margin-bottom:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;}}
.row{{display:flex;gap:12px;margin-top:8px;font-size:12px;}}
.metric{{background:rgba(255,255,255,.05);border-radius:4px;padding:4px 8px;text-align:center;}}
.metric-lbl{{font-size:9px;color:#8b949e;text-transform:uppercase;}}
.metric-val{{font-weight:700;color:#e6edf3;}}
.badge-footer{{margin-top:8px;font-size:10px;color:#484f58;text-align:right;}}
.badge-footer a{{color:#58a6ff;text-decoration:none;}}
</style></head>
<body>
<div class="widget">
  <div class="name">{deal_name}</div>
  <div style="display:flex;align-items:center;gap:10px;">
    <div class="verdict">{verdict}</div>
    <div class="metric"><div class="metric-lbl">Confidence</div><div class="metric-val">{conf}%</div></div>
    <div class="metric"><div class="metric-lbl">Cap Rate</div><div class="metric-val">{cap}%</div></div>
    <div class="metric"><div class="metric-lbl">Proj. IRR</div><div class="metric-val">{irr}%</div></div>
  </div>
  <div class="badge-footer">Analyzed by <a href="http://localhost:5052/report/{job_id}" target="_blank">ClearEye</a> &mdash; <a href="http://localhost:5052/app" target="_blank">Run your own analysis &rarr;</a></div>
</div>
</body></html>"""
    return html


@app.route("/widget/<job_id>/embed.js")
def widget_embed_js(job_id):
    """Returns JS snippet for embedding the widget as a script tag (#120)."""
    snippet = f"""(function(){{
  var el=document.currentScript||document.getElementById('cleareye-widget-{job_id}');
  if(!el)return;
  var iframe=document.createElement('iframe');
  iframe.src='http://localhost:5052/widget/{job_id}';
  iframe.style.cssText='border:none;width:340px;height:110px;border-radius:8px;';
  iframe.title='ClearEye Deal Analysis';
  el.parentNode.replaceChild(iframe,el);
}})();"""
    return Response(snippet, mimetype="application/javascript")


@app.route("/export/<job_id>")
def export_pdf(job_id):
    """Download branded investment memo as PDF. (#115)"""
    job = JOBS.get(job_id)
    if not job and job_get(job_id):
        db = job_get(job_id)
        job = db.get("result") if db else None
    if not job:
        return "Job not found", 404
    try:
        from pdf_export import generate_pdf
        pdf_path = generate_pdf(job)
        return send_file(pdf_path, as_attachment=True,
                         download_name=f"ClearEye_{job_id}.pdf",
                         mimetype="application/pdf")
    except Exception as e:
        # Fallback: serve memo as text file
        memo = job.get("memo", "No memo available")
        return Response(memo, mimetype="text/plain",
                        headers={"Content-Disposition": f"attachment; filename=ClearEye_{job_id}.txt"})


@app.route("/api/devil_advocate/<job_id>", methods=["POST"])
def api_devil_advocate(job_id):
    """#245: Generate adversarial deal failure analysis — 3 specific failure modes."""
    job = JOBS.get(job_id)
    if not job:
        db_job = job_get(job_id)
        if db_job:
            job = db_job.get("result") if db_job else None
    if not job:
        return jsonify({"error": "Job not found"}), 404

    deal = job.get("deal") or {}
    memo = (job.get("memo") or "")[:3000]
    bias = (job.get("bias_report") or "")[:800]

    deal_name = deal.get("deal_name") or "this deal"
    price = deal.get("asking_price") or deal.get("price") or ""
    price_str = f"${price:,.0f}" if isinstance(price, (int, float)) and price else str(price) if price else "undisclosed"
    units = deal.get("units") or deal.get("unit_count") or ""
    market = deal.get("market") or deal.get("address") or ""
    cap_rate = deal.get("cap_rate") or ""
    irr = deal.get("projected_irr") or deal.get("irr") or ""
    hold = deal.get("hold_period") or ""

    deal_summary = (
        f"Deal: {deal_name}\n"
        f"Price: {price_str}" + (f", {units} units" if units else "") + "\n"
        + (f"Market: {market}\n" if market else "")
        + (f"Cap rate: {cap_rate}%\n" if cap_rate else "")
        + (f"Projected IRR: {irr}%\n" if irr else "")
        + (f"Hold period: {hold} years\n" if hold else "")
    )

    prompt = f"""You are a seasoned real estate skeptic and contrarian analyst. Your job is to stress-test this deal thesis and find the top 3 ways it could fail catastrophically.

DEAL SUMMARY:
{deal_summary}

INVESTMENT MEMO (excerpt):
{memo}

BIAS FLAGS:
{bias if bias else "None flagged"}

Generate EXACTLY 3 failure modes. Use deal-specific numbers from the summary above wherever possible. Be direct, adversarial, and specific — not generic disclaimers.

Respond in EXACTLY this format (no preamble, no extra text):

Failure Mode 1: [SHORT TITLE IN CAPS] — [2 sentences. First sentence states the specific risk using deal numbers. Second sentence explains the cascade effect and why it's hard to recover from.]

Failure Mode 2: [SHORT TITLE IN CAPS] — [Same format.]

Failure Mode 3: [SHORT TITLE IN CAPS] — [Same format.]"""

    try:
        import anthropic as _anthropic
        _client = _anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", _env.get("ANTHROPIC_API_KEY", "")))
        response = _client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        # Parse the 3 failure modes
        import re as _re
        modes = []
        pattern = _re.compile(r"Failure Mode \d+:\s*(.+?)(?=Failure Mode \d+:|$)", _re.DOTALL)
        for m in pattern.finditer(text):
            raw = m.group(1).strip()
            # Split title from body on " — "
            if " — " in raw:
                title, body = raw.split(" — ", 1)
            elif " - " in raw:
                title, body = raw.split(" - ", 1)
            else:
                title, body = raw[:60], raw
            modes.append({"title": title.strip(), "body": body.strip()})
        if not modes:
            # Fallback: return raw text as single mode
            modes = [{"title": "Analysis", "body": text}]
        return jsonify({"failure_modes": modes[:3], "raw": text})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sponsor_score/<job_id>", methods=["POST"])
def api_sponsor_score(job_id):
    """#248: Extract and score GP/sponsor track record from OM memo."""
    job = JOBS.get(job_id)
    if not job:
        db_job = job_get(job_id)
        if db_job:
            job = db_job.get("result") if db_job else None
    if not job:
        return jsonify({"error": "Job not found"}), 404

    memo = (job.get("memo") or "")[:3000]
    om_text = (job.get("om_text") or "")[:2000]
    context = (memo + "\n\n" + om_text).strip()[:4000]

    prompt = f"""You are a real estate investment analyst evaluating a GP/sponsor's track record.

Extract the following from the document below and respond in EXACTLY this JSON format (no extra text, no markdown code block):

{{
  "operator_name": "...",
  "years_active": <number or null>,
  "deals_mentioned": <number or null>,
  "aum_mentioned": "...",
  "claimed_irr": "...",
  "track_record_score": <0-100 integer>,
  "verdict": "STRONG" | "ADEQUATE" | "UNVERIFIED",
  "rationale": "1-2 sentence summary of track record quality"
}}

Rules:
- track_record_score: 80-100 = verified strong track record, 50-79 = some history but gaps, 0-49 = no verifiable track record or first-time operator
- verdict: STRONG if score>=75, ADEQUATE if score>=45, else UNVERIFIED
- If field not mentioned in document, use null or "Not disclosed"

DOCUMENT:
{context}"""

    try:
        import anthropic as _anthropic, json as _json
        _client = _anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", _env.get("ANTHROPIC_API_KEY", "")))
        response = _client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}]
        )
        text = response.content[0].text.strip()
        # Strip markdown code block if present
        text = text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        result = _json.loads(text)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e), "raw": locals().get("text", "")}), 500


@app.route("/status/<job_id>")
def status(job_id):
    # Check memory cache first, then SQLite
    job = JOBS.get(job_id)
    if not job:
        db_job = job_get(job_id)
        if db_job:
            if db_job.get("result"):
                job = {**db_job["result"], "status": db_job["status"]}
            else:
                job = {"status": db_job.get("status", "not_found"), "message": ""}
        else:
            job = {"status": "not_found"}

    if job.get("status") != "done":
        return jsonify({"status": job.get("status"), "message": job.get("message", "")})
    return jsonify({
        "status": "done",
        "deal": job.get("deal", {}),
        "memo": job.get("memo", ""),
        "advisors": job.get("advisors", {}),
        "stress_table": job.get("stress_table", ""),
        "validation_report": job.get("validation_report", ""),
        "bias_report": job.get("bias_report", ""),
        "premortem_report": job.get("premortem_report", ""),
        "macro_brief": job.get("macro", {}).get("brief", ""),
        "email_result": job.get("email_result"),
        "generated_at": job.get("generated_at", ""),
        "data_source": (job.get("deal") or {}).get("_data_source", "ClearEye static (2026-Q1)"),
    })


def _webhook_log_path():
    import os as _o
    return _o.path.join(_o.path.dirname(__file__), "outputs", "webhook_log.jsonl")


def _deliver_webhook(job_id: str, webhook_url: str, output: dict):
    """Fire-and-forget POST to webhook_url with job result (#191). Runs in daemon thread."""
    def _send():
        import json as _j, urllib.request as _ur, urllib.error as _ue
        payload = {
            "job_id":       job_id,
            "status":       "done",
            "verdict":      output.get("verdict", ""),
            "confidence":   output.get("confidence", ""),
            "report_url":   f"http://localhost:5052/report/{job_id}",
        }
        # Extract council_score
        try:
            import re as _re
            advisors = output.get("advisors", [])
            scores = []
            for a in advisors:
                m = _re.search(r"score[:\s]+([0-9]+)\s*/\s*100", str(a.get("analysis","")), _re.I)
                if m:
                    scores.append(int(m.group(1)))
            if scores:
                payload["council_score"] = round(sum(scores) / len(scores), 1)
        except Exception:
            pass
        body = _j.dumps(payload).encode("utf-8")
        ts = datetime.utcnow().isoformat() + "Z"
        try:
            req = _ur.Request(webhook_url, data=body, headers={"Content-Type": "application/json"})
            with _ur.urlopen(req, timeout=10) as resp:
                status_code = resp.getcode()
            log_entry = {"job_id": job_id, "url": webhook_url, "status_code": status_code, "delivered_at": ts, "ok": True}
        except Exception as exc:
            log_entry = {"job_id": job_id, "url": webhook_url, "error": str(exc), "attempted_at": ts, "ok": False}
        try:
            with open(_webhook_log_path(), "a", encoding="utf-8") as _f:
                import json as _jj
                _f.write(_jj.dumps(log_entry) + "\n")
        except Exception:
            pass
    import threading as _th
    _th.Thread(target=_send, daemon=True).start()


def _apikey_path():
    import os as _o
    return _o.path.join(_o.path.dirname(__file__), "outputs", "api_keys.jsonl")


def _apikey_load():
    """Return list of non-revoked API key records."""
    import json as _json
    keys = []
    try:
        with open(_apikey_path(), encoding="utf-8") as _f:
            for line in _f:
                line = line.strip()
                if line:
                    try:
                        keys.append(_json.loads(line))
                    except Exception:
                        pass
    except FileNotFoundError:
        pass
    return [k for k in keys if not k.get("revoked")]


def _apikey_validate(raw_key: str):
    """Return key record if valid, else None."""
    import hashlib as _hl
    h = _hl.sha256(raw_key.encode()).hexdigest()
    for k in _apikey_load():
        if k.get("key_hash") == h:
            return k
    return None


@app.route("/settings/api")
def settings_api():
    """API key management page (#188)."""
    user_email = session.get("user_email")
    if not user_email:
        return redirect("/login?next=/settings/api")

    # Find existing keys for this user
    user_keys = [k for k in _apikey_load() if k.get("user_email") == user_email]
    key_rows = ""
    _first_key_id = user_keys[0].get("key_id", "") if user_keys else ""
    _first_webhook = user_keys[0].get("webhook_url", "") if user_keys else ""
    for k in user_keys:
        prefix  = k.get("key_prefix", "ce_...")
        created = k.get("created_at", "")[:10]
        last    = k.get("last_used_at", "Never") or "Never"
        plan    = k.get("plan", "")
        key_rows += (
            "<tr>"
            "<td style='font-family:monospace;font-size:13px;'>" + prefix + "••••••••</td>"
            "<td style='font-size:12px;color:#8b949e;'>" + plan + "</td>"
            "<td style='font-size:12px;color:#8b949e;'>" + created + "</td>"
            "<td style='font-size:12px;color:#8b949e;'>" + str(last)[:16] + "</td>"
            "<td><button onclick=\"revokeKey('" + k.get("key_id","") + "')\" style='padding:3px 8px;background:rgba(248,81,73,.12);border:1px solid rgba(248,81,73,.3);color:#f85149;border-radius:4px;font-size:11px;cursor:pointer;'>Revoke</button></td>"
            "</tr>"
        )
    if not key_rows:
        key_rows = "<tr><td colspan='5' style='color:#8b949e;font-size:13px;padding:16px 8px;'>No API keys yet. Generate one below.</td></tr>"

    page = (
        "<!DOCTYPE html><html lang='en'><head><meta charset='UTF-8'>"
        "<meta name='viewport' content='width=device-width,initial-scale=1'>"
        "<title>API Keys — ClearEye</title>"
        "<link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css' rel='stylesheet'>"
        "<style>"
        "body{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;margin:0;}"
        ".api-nav{height:52px;background:#0d1117;border-bottom:1px solid #21262d;display:flex;align-items:center;padding:0 20px;gap:14px;}"
        ".api-card{background:#161b22;border:1px solid #21262d;border-radius:12px;padding:24px 28px;max-width:820px;margin:28px auto;}"
        "table{width:100%;border-collapse:collapse;}th,td{padding:8px 10px;border-bottom:1px solid #21262d;}"
        "th{font-size:11px;color:#8b949e;text-align:left;text-transform:uppercase;letter-spacing:.05em;}"
        ".key-display{font-family:monospace;font-size:14px;background:#0d1117;border:1px solid #3fb950;border-radius:6px;padding:10px 16px;color:#3fb950;word-break:break-all;margin:12px 0;}"
        "</style></head><body>"
        "<nav class='api-nav'>"
        "<a href='/app' style='font-size:1.1rem;font-weight:800;color:#58a6ff;text-decoration:none;'>&#128065; ClearEye</a>"
        "<span style='color:#484f58;'>|</span>"
        "<span style='color:#8b949e;font-size:13px;'>API Keys</span>"
        "<a href='/app' style='margin-left:auto;font-size:12px;color:#8b949e;text-decoration:none;'>&#8592; Back to App</a>"
        "</nav>"
        "<div class='api-card'>"
        "<h1 style='font-size:1.15rem;font-weight:800;margin:0 0 6px;'>API Keys</h1>"
        "<p style='color:#8b949e;font-size:13px;margin-bottom:20px;'>Use API keys to access the ClearEye API programmatically. Keys are associated with your account (" + user_email + ").</p>"
        "<table><thead><tr><th>Key</th><th>Plan</th><th>Created</th><th>Last Used</th><th></th></tr></thead>"
        "<tbody id='keys-tbody'>" + key_rows + "</tbody></table>"
        "<button onclick='generateKey()' style='margin-top:16px;padding:10px 22px;background:linear-gradient(135deg,#1f6feb,#388bfd);color:#fff;border:none;border-radius:8px;font-weight:600;font-size:13px;cursor:pointer;'>&#10010; Generate New Key</button>"
        "<div id='new-key-box' style='display:none;margin-top:16px;'>"
        "<div style='font-size:13px;font-weight:600;color:#3fb950;margin-bottom:4px;'>&#10003; New API key generated — copy it now, it won't be shown again:</div>"
        "<div class='key-display' id='new-key-val'></div>"
        "<button onclick='copyKey()' style='padding:6px 14px;background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.3);color:#58a6ff;border-radius:5px;font-size:12px;cursor:pointer;'>Copy</button>"
        "</div>"
        "<hr style='border-color:#21262d;margin:24px 0;'>"
        "<h2 style='font-size:13px;font-weight:700;margin-bottom:8px;'>Webhook URL</h2>"
        "<p style='color:#8b949e;font-size:12px;margin-bottom:8px;'>ClearEye will POST analysis results to this URL when a job completes via the API.</p>"
        "<div style='display:flex;gap:8px;margin-bottom:6px;'>"
        "<input id='wh-url-input' type='url' placeholder='https://your-server.com/webhook' value='" + (_first_webhook or "") + "' style='flex:1;background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:7px 12px;color:#e6edf3;font-size:13px;' />"
        "<input id='wh-key-id' type='hidden' value='" + _first_key_id + "' />"
        "<button onclick='saveWebhook()' style='padding:7px 16px;background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.3);color:#58a6ff;border-radius:6px;font-size:12px;cursor:pointer;'>Save</button>"
        "</div>"
        "<div id='wh-status' style='font-size:11px;color:#8b949e;min-height:16px;'></div>"
        "<hr style='border-color:#21262d;margin:24px 0;'>"
        "<h2 style='font-size:13px;font-weight:700;margin-bottom:8px;'>Usage</h2>"
        "<pre style='background:#0d1117;border:1px solid #21262d;border-radius:6px;padding:14px;font-size:12px;color:#8b949e;overflow-x:auto;'>"
        "curl -X POST http://localhost:5052/api/v1/analyze \\\n"
        "  -H 'Authorization: Bearer YOUR_API_KEY' \\\n"
        "  -H 'Content-Type: application/json' \\\n"
        "  -d '{\"om_text\": \"Property description here...\"}'"
        "</pre>"
        "</div>"
        "<script>"
        "async function generateKey(){"
        "  const r=await fetch('/api/v1/keys',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({})});"
        "  const d=await r.json();"
        "  if(d.key){document.getElementById('new-key-val').textContent=d.key;document.getElementById('new-key-box').style.display='block';location.reload();}"
        "  else alert(d.error||'Failed');"
        "}"
        "async function revokeKey(kid){"
        "  if(!confirm('Revoke this API key?'))return;"
        "  const r=await fetch('/api/v1/keys/'+kid,{method:'DELETE'});"
        "  if(r.ok){alert('Key revoked');location.reload();}"
        "}"
        "function copyKey(){const v=document.getElementById('new-key-val').textContent;navigator.clipboard.writeText(v).then(()=>alert('Copied!'));}"
        "async function saveWebhook(){"
        "  const url=document.getElementById('wh-url-input').value.trim();"
        "  const kid=document.getElementById('wh-key-id').value.trim();"
        "  if(!kid){document.getElementById('wh-status').textContent='Generate an API key first.';return;}"
        "  const r=await fetch('/api/v1/keys/'+kid,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({webhook_url:url})});"
        "  const d=await r.json();"
        "  const st=document.getElementById('wh-status');"
        "  if(d.ok){st.style.color='#3fb950';st.textContent='Webhook URL saved.';}"
        "  else{st.style.color='#f85149';st.textContent=d.error||'Save failed';}"
        "}"
        "</script>"
        "</body></html>"
    )
    return page


@app.route("/api/v1/keys", methods=["POST"])
def api_v1_generate_key():
    """Generate a new API key for the authenticated user (#188)."""
    user_email = session.get("user_email")
    if not user_email:
        return jsonify({"error": "Not authenticated"}), 401
    import hashlib as _hl, secrets as _sec, json as _j, os as _o, time as _t
    raw_key   = "ce_live_" + _sec.token_urlsafe(24)
    key_hash  = _hl.sha256(raw_key.encode()).hexdigest()
    key_prefix = raw_key[:12]
    key_id    = _sec.token_hex(8)
    data_in   = request.get_json(force=True, silent=True) or {}
    wh_url    = (data_in.get("webhook_url") or "").strip()
    record = {
        "key_id": key_id, "key_hash": key_hash, "key_prefix": key_prefix,
        "user_email": user_email, "plan": session.get("plan", "free"),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "last_used_at": None, "revoked": False,
        "webhook_url": wh_url or None,
    }
    with open(_apikey_path(), "a", encoding="utf-8") as _f:
        _f.write(_j.dumps(record) + "\n")
    return jsonify({"ok": True, "key": raw_key, "key_id": key_id, "prefix": key_prefix})


@app.route("/api/v1/keys/<key_id>", methods=["DELETE"])
def api_v1_revoke_key(key_id):
    """Revoke an API key (#188)."""
    user_email = session.get("user_email")
    import json as _j
    lines = []
    try:
        with open(_apikey_path(), encoding="utf-8") as _f:
            for line in _f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = _j.loads(line)
                    if rec.get("key_id") == key_id:
                        if rec.get("user_email") != user_email:
                            return jsonify({"error": "Forbidden"}), 403
                        rec["revoked"] = True
                    lines.append(_j.dumps(rec))
                except Exception:
                    lines.append(line)
    except FileNotFoundError:
        pass
    with open(_apikey_path(), "w", encoding="utf-8") as _f:
        _f.write("\n".join(lines) + ("\n" if lines else ""))
    return jsonify({"ok": True})


@app.route("/api/v1/keys/<key_id>", methods=["PUT"])
def api_v1_update_key(key_id):
    """Update webhook URL for an API key (#191)."""
    user_email = session.get("user_email")
    if not user_email:
        return jsonify({"error": "Not authenticated"}), 401
    import json as _j
    data_in = request.get_json(force=True, silent=True) or {}
    wh_url  = (data_in.get("webhook_url") or "").strip()
    lines = []
    found = False
    try:
        with open(_apikey_path(), encoding="utf-8") as _f:
            for line in _f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = _j.loads(line)
                    if rec.get("key_id") == key_id:
                        if rec.get("user_email") != user_email:
                            return jsonify({"error": "Forbidden"}), 403
                        rec["webhook_url"] = wh_url or None
                        found = True
                    lines.append(_j.dumps(rec))
                except Exception:
                    lines.append(line)
    except FileNotFoundError:
        pass
    if not found:
        return jsonify({"error": "Key not found"}), 404
    with open(_apikey_path(), "w", encoding="utf-8") as _f:
        _f.write("\n".join(lines) + ("\n" if lines else ""))
    return jsonify({"ok": True, "webhook_url": wh_url or None})


@app.route("/api/v1/analyze", methods=["POST"])
def api_v1_analyze():
    """
    Programmatic analysis endpoint (#188).
    Auth: Authorization: Bearer <api_key>
    Body: {"om_text": "...", "email": "optional@email.com"}
    Returns: {"job_id": "...", "status": "queued"}
    """
    # Validate bearer token
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return jsonify({"error": "Missing or invalid Authorization header"}), 401
    raw_key = auth[7:].strip()
    key_rec = _apikey_validate(raw_key)
    if not key_rec:
        return jsonify({"error": "Invalid API key"}), 401

    # Update last_used_at
    import json as _j
    lines = []
    try:
        with open(_apikey_path(), encoding="utf-8") as _f:
            for line in _f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = _j.loads(line)
                    if rec.get("key_id") == key_rec["key_id"]:
                        rec["last_used_at"] = datetime.utcnow().isoformat() + "Z"
                    lines.append(_j.dumps(rec))
                except Exception:
                    lines.append(line)
    except FileNotFoundError:
        pass
    with open(_apikey_path(), "w", encoding="utf-8") as _f:
        _f.write("\n".join(lines) + ("\n" if lines else ""))

    data      = request.get_json(force=True) or {}
    om_text   = (data.get("om_text") or "").strip()
    recipient = data.get("email") or key_rec.get("user_email") or ""
    if not om_text:
        return jsonify({"error": "om_text is required"}), 400
    if len(om_text) < 50:
        return jsonify({"error": "om_text too short (min 50 chars)"}), 400

    new_job_id = str(uuid.uuid4())[:8]
    wh_url_for_job = (key_rec.get("webhook_url") or "").strip()
    JOBS[new_job_id] = {"status": "queued", "webhook_url": wh_url_for_job or None}
    job_create(new_job_id, om_text, recipient or None)
    t = threading.Thread(target=_analyze, args=(new_job_id, om_text, recipient or None), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": new_job_id, "status": "queued",
                    "status_url": request.host_url + "status/" + new_job_id,
                    "report_url": request.host_url + "report/" + new_job_id})


@app.route("/deals")
def deals_page():
    """Deal history comparison table (#184)."""
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Deal History — ClearEye</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;}
.dh-nav{height:52px;background:#0d1117;border-bottom:1px solid #21262d;display:flex;align-items:center;padding:0 20px;gap:14px;position:sticky;top:0;z-index:100;}
.dh-brand{font-size:1.1rem;font-weight:800;color:#58a6ff;text-decoration:none;}
.dh-main{max-width:1100px;margin:28px auto;padding:0 16px;}
.dh-card{background:#161b22;border:1px solid #21262d;border-radius:12px;padding:20px 24px;}
.score-badge{display:inline-block;padding:2px 10px;border-radius:20px;font-size:11px;font-weight:700;border:1px solid;}
.verdict-go{color:#3fb950;border-color:#3fb950;background:rgba(63,185,80,.1);}
.verdict-nogo{color:#f85149;border-color:#f85149;background:rgba(248,81,73,.1);}
.verdict-cond{color:#d29922;border-color:#d29922;background:rgba(210,153,34,.1);}
.verdict-{color:#8b949e;border-color:#8b949e;background:rgba(139,148,158,.08);}
.score-high{color:#3fb950;border-color:#3fb950;background:rgba(63,185,80,.1);}
.score-mid{color:#d29922;border-color:#d29922;background:rgba(210,153,34,.1);}
.score-low{color:#f85149;border-color:#f85149;background:rgba(248,81,73,.1);}
table{width:100%;border-collapse:collapse;}
th{font-size:11px;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;padding:8px 10px;border-bottom:1px solid #21262d;cursor:pointer;white-space:nowrap;user-select:none;}
th:hover{color:#e6edf3;}
th .sort-icon{margin-left:4px;opacity:.4;}
th.sort-asc .sort-icon::after{content:'▲';}
th.sort-desc .sort-icon::after{content:'▼';}
th:not(.sort-asc):not(.sort-desc) .sort-icon::after{content:'⇅';}
td{padding:10px 10px;border-bottom:1px solid rgba(255,255,255,.04);font-size:13px;vertical-align:middle;}
tr:hover td{background:rgba(255,255,255,.02);}
.deal-link{color:#58a6ff;text-decoration:none;font-weight:500;}
.deal-link:hover{text-decoration:underline;}
#search-input{background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:7px 12px;font-size:13px;width:260px;outline:none;}
#search-input:focus{border-color:#58a6ff;}
.load-more-btn{display:block;width:100%;padding:10px;background:rgba(255,255,255,.03);border:1px dashed #30363d;color:#8b949e;border-radius:6px;cursor:pointer;font-size:13px;margin-top:12px;transition:all .2s;}
.load-more-btn:hover{border-color:#58a6ff;color:#58a6ff;}
.empty-state{text-align:center;padding:48px 20px;color:#8b949e;}
</style>
</head>
<body>
<nav class="dh-nav">
  <a class="dh-brand" href="/app">&#128065; ClearEye</a>
  <span style="color:#484f58;">|</span>
  <span style="color:#8b949e;font-size:13px;">Deal History</span>
  <div style="margin-left:auto;display:flex;gap:10px;align-items:center;">
    <a href="/app"       style="font-size:12px;color:#8b949e;text-decoration:none;">&#8592; New Analysis</a>
    <a href="/pipeline"  style="font-size:12px;color:#8b949e;text-decoration:none;">Pipeline</a>
    <a href="/compare"   style="font-size:12px;color:#8b949e;text-decoration:none;">Compare</a>
  </div>
</nav>

<div class="dh-main">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;flex-wrap:wrap;gap:10px;">
    <div>
      <h1 style="font-size:1.25rem;font-weight:800;margin:0 0 4px;">Deal History</h1>
      <div style="font-size:12px;color:#8b949e;" id="count-label">Loading...</div>
    </div>
    <input id="search-input" placeholder="&#128269; Filter by name or market..." oninput="filterDeals(this.value)">
  </div>

  <div class="dh-card">
    <div id="table-wrap">
      <table>
        <thead>
          <tr>
            <th onclick="sortBy('deal_name')" data-col="deal_name">Deal Name<span class="sort-icon"></span></th>
            <th onclick="sortBy('created_at')" data-col="created_at" class="sort-desc">Date<span class="sort-icon"></span></th>
            <th onclick="sortBy('verdict')" data-col="verdict">Verdict<span class="sort-icon"></span></th>
            <th onclick="sortBy('council_score')" data-col="council_score">Score<span class="sort-icon"></span></th>
            <th title="Score trend across re-analyses">Trend</th>
            <th onclick="sortBy('market')" data-col="market">Market<span class="sort-icon"></span></th>
            <th onclick="sortBy('asset_class')" data-col="asset_class">Type<span class="sort-icon"></span></th>
            <th onclick="sortBy('asking_price')" data-col="asking_price">Price<span class="sort-icon"></span></th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody id="deals-tbody"></tbody>
      </table>
      <div class="empty-state" id="empty-state" style="display:none;">No deals analyzed yet. <a href="/app" style="color:#58a6ff;">Run your first analysis &rarr;</a></div>
    </div>
    <button class="load-more-btn" id="load-more-btn" onclick="loadMore()" style="display:none;">Load more</button>
  </div>
</div>

<script>
let _all = [];
let _filtered = [];
let _sortCol = 'created_at';
let _sortDir = -1; // -1=desc, 1=asc
let _shown = 20;

function fmt_price(v){
  if(!v)return '—';
  const n=parseFloat(v);
  if(isNaN(n))return v;
  if(n>=1e6)return '$'+(n/1e6).toFixed(1)+'M';
  if(n>=1e3)return '$'+(n/1e3).toFixed(0)+'K';
  return '$'+n.toLocaleString();
}
// #263: Inline sparkline SVG for score trend column
function sparkSVG(scores){
  if(!scores||scores.length<2)return '<span style="color:#484f58;font-size:10px;">—</span>';
  const W=60,H=20,pad=2;
  const min=Math.min.apply(null,scores),max=Math.max.apply(null,scores);
  const range=Math.max(max-min,1);
  const pts=scores.map(function(s,i){
    const x=pad+(i/(scores.length-1))*(W-2*pad);
    const y=H-pad-(s-min)/range*(H-2*pad);
    return x.toFixed(1)+','+y.toFixed(1);
  }).join(' ');
  const trend=scores[scores.length-1]-scores[0];
  const col=trend>2?'#3fb950':trend<-2?'#f85149':'#8b949e';
  const lastX=pad+(W-2*pad);
  const lastY=(H-pad-(scores[scores.length-1]-min)/range*(H-2*pad)).toFixed(1);
  return '<svg width="'+W+'" height="'+H+'" viewBox="0 0 '+W+' '+H+'" style="vertical-align:middle;display:block;">'
    +'<polyline points="'+pts+'" fill="none" stroke="'+col+'" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>'
    +'<circle cx="'+lastX+'" cy="'+lastY+'" r="2.5" fill="'+col+'"/>'
    +'</svg>';
}
function fmt_date(s){
  if(!s)return '—';
  return s.slice(0,10);
}
function verdict_class(v){
  if(!v)return 'verdict-';
  const u=v.toUpperCase();
  if(u.includes('NO-GO')||u.includes('NOGO'))return 'verdict-nogo';
  if(u==='GO')return 'verdict-go';
  return 'verdict-cond';
}
function score_class(s){
  if(s===null||s===undefined)return '';
  if(s>=75)return 'score-high';
  if(s>=55)return 'score-mid';
  return 'score-low';
}
function renderTable(){
  const tbody=document.getElementById('deals-tbody');
  const slice=_filtered.slice(0,_shown);
  if(!slice.length){
    tbody.innerHTML='';
    document.getElementById('empty-state').style.display='block';
    document.getElementById('load-more-btn').style.display='none';
    return;
  }
  document.getElementById('empty-state').style.display='none';
  tbody.innerHTML=slice.map(d=>{
    const sc=d.council_score;
    const scHtml=sc!==null&&sc!==undefined
      ? '<span class="score-badge '+score_class(sc)+'">'+sc+'/100</span>' : '—';
    const verd=d.verdict||'';
    const vHtml=verd?'<span class="score-badge '+verdict_class(verd)+'">'+verd+'</span>':'—';
    const actions=d.job_id
      ? '<a class="deal-link" href="/report/'+d.job_id+'" target="_blank">View</a>'
      : '—';
    const spark=sparkSVG(d.score_history||[]);
    return '<tr>'
      +'<td><span style="font-weight:500;">'+(d.deal_name||'Unnamed')+'</span></td>'
      +'<td style="color:#8b949e;">'+fmt_date(d.created_at)+'</td>'
      +'<td>'+vHtml+'</td>'
      +'<td>'+scHtml+'</td>'
      +'<td>'+spark+'</td>'
      +'<td style="color:#8b949e;">'+(d.market||'—')+'</td>'
      +'<td style="color:#8b949e;">'+(d.asset_class||'—')+'</td>'
      +'<td style="color:#8b949e;">'+fmt_price(d.asking_price)+'</td>'
      +'<td>'+actions+'</td>'
      +'</tr>';
  }).join('');
  document.getElementById('load-more-btn').style.display=_filtered.length>_shown?'block':'none';
  document.getElementById('count-label').textContent=
    _filtered.length+' deal'+(+(_filtered.length!==1)?'s':'')+' analyzed'
    +(_all.length!==_filtered.length?' (filtered from '+_all.length+')':'');
}
function sortBy(col){
  if(_sortCol===col){ _sortDir*=-1; }
  else { _sortCol=col; _sortDir=-1; }
  document.querySelectorAll('th[data-col]').forEach(th=>{
    th.classList.remove('sort-asc','sort-desc');
    if(th.dataset.col===col) th.classList.add(_sortDir===-1?'sort-desc':'sort-asc');
  });
  _filtered.sort((a,b)=>{
    let av=a[col],bv=b[col];
    if(av===null||av===undefined)av='';
    if(bv===null||bv===undefined)bv='';
    if(typeof av==='number'&&typeof bv==='number')return (av-bv)*_sortDir;
    return String(av).localeCompare(String(bv))*_sortDir;
  });
  _shown=20;
  renderTable();
}
function filterDeals(q){
  const lq=q.toLowerCase().trim();
  _filtered=lq?_all.filter(d=>(d.deal_name||'').toLowerCase().includes(lq)||(d.market||'').toLowerCase().includes(lq)||(d.asset_class||'').toLowerCase().includes(lq)):_all.slice();
  _shown=20;
  sortBy(_sortCol);
}
function loadMore(){_shown+=20;renderTable();}
async function init(){
  try{
    const r=await fetch('/api/deals/history');
    _all=await r.json();
    _filtered=_all.slice();
    sortBy('created_at');
  }catch(e){
    document.getElementById('count-label').textContent='Failed to load deal history';
  }
}
init();
</script>
</body>
</html>"""


@app.route("/free-review", methods=["GET", "POST"])
def free_review_page():
    """#217: Email-gated free deal analysis funnel page."""
    return render_template_string(FREE_REVIEW_HTML)


@app.route("/markets")
def markets_page():
    return render_template_string(MARKETS_HTML)


@app.route("/api/market-scores")
def market_scores():
    """Compute and return ClearEye market scores for 20 MSAs."""
    from rentcast_client import get_market_benchmarks, RENTCAST_API_KEY
    import json as _json

    MARKETS = ["Phoenix", "Dallas", "Atlanta", "Tampa", "Austin", "Denver",
               "Charlotte", "Nashville", "Miami", "Orlando", "Seattle", "Chicago",
               "Los Angeles", "San Diego", "Las Vegas", "Houston", "San Antonio",
               "Jacksonville", "Raleigh", "Memphis"]

    # Check cache (update weekly)
    cache_path = Path(__file__).parent / "outputs" / "market_scores_cache.json"
    if cache_path.exists():
        try:
            cached = _json.loads(cache_path.read_text())
            from datetime import date
            if cached.get("date") == date.today().isoformat():
                return jsonify(cached["scores"])
        except Exception:
            pass

    # 10yr treasury rate (use FRED cached or fallback)
    try:
        from macro_context import get_macro_context
        macro = get_macro_context()
        treasury_10yr = macro.get("rates", {}).get("treasury_10yr", 4.3)
    except Exception:
        treasury_10yr = 4.3

    scores = []
    for market in MARKETS:
        try:
            b = get_market_benchmarks(market)
            cap = b.get("avg_cap_rate") or 5.0
            rent_growth = b.get("rent_growth_1yr") or 2.0
            vacancy = b.get("vacancy_rate") or 7.0

            # ClearEye Market Score (0-100)
            # Cap rate spread vs 10yr treasury (higher = better, up to 25pts)
            cap_spread = cap - treasury_10yr
            cap_score = min(max(cap_spread * 8 + 25, 0), 25)
            # Rent growth momentum (up to 25pts)
            rent_score = min(max(rent_growth * 5 + 15, 0), 25)
            # Vacancy (lower = better, up to 25pts): 4%=25, 10%=0
            vac_score = min(max((10 - vacancy) * (25 / 6), 0), 25)
            # Source bonus (live data = +5, static = 0)
            source_bonus = 5 if b.get("_source") == "rentcast_live" else 0

            total = round(cap_score + rent_score + vac_score + source_bonus, 1)

            scores.append({
                "market": market,
                "score": total,
                "cap_rate": cap,
                "rent_growth": rent_growth,
                "vacancy": vacancy,
                "cap_spread": round(cap_spread, 2),
                "trend": "up" if rent_growth > 2.5 else "down" if rent_growth < 0 else "flat",
                "_source": b.get("_source"),
            })
        except Exception as e:
            scores.append({"market": market, "score": 50, "error": str(e)})

    scores.sort(key=lambda x: x.get("score") or 0, reverse=True)

    # Cache results
    try:
        from datetime import date
        cache_path.parent.mkdir(exist_ok=True)
        cache_path.write_text(_json.dumps({"date": date.today().isoformat(), "scores": scores}, indent=2))
    except Exception:
        pass

    return jsonify(scores)


@app.route("/portfolio")
def portfolio_page():
    """Portfolio-level analytics dashboard (#223)."""
    return render_template_string(PORTFOLIO_HTML)


@app.route("/api/portfolio/stats")
def api_portfolio_stats():
    """Aggregate all completed analyses for portfolio dashboard (#223)."""
    import json as _json
    import re as _re
    try:
        with db.get_con() as con:
            rows = con.execute(
                "SELECT id, deal_name, verdict, confidence, created_at, result FROM deals WHERE status='done' ORDER BY created_at DESC LIMIT 200"
            ).fetchall()
    except Exception as e:
        return jsonify({"error": str(e), "deals": []})

    deals_out = []
    for row in rows:
        jid, deal_name, verdict, confidence, created_at, result_raw = row
        deal_data = {}
        try:
            deal_data = _json.loads(result_raw or "{}") if result_raw else {}
        except Exception:
            pass
        d = deal_data.get("deal", {})
        memo = deal_data.get("memo", "")
        stress = deal_data.get("stress_test", {})
        bias = deal_data.get("bias_report", "")

        # Extract IRR from stress test base case or memo
        irr = None
        try:
            base = stress.get("base", {})
            irr_val = base.get("irr") or base.get("IRR")
            if irr_val is not None:
                irr = float(str(irr_val).replace("%", "").strip())
        except Exception:
            pass
        if irr is None:
            m = _re.search(r"IRR[^0-9]*([0-9]+\.?[0-9]*)\s*%", memo, _re.IGNORECASE)
            if m:
                try:
                    irr = float(m.group(1))
                except Exception:
                    pass

        # Bias flag count
        bias_count = 0
        try:
            bias_count = len(_re.findall(r"(HIGH|DETECTED|ELEVATED)", bias or "", _re.IGNORECASE))
        except Exception:
            pass

        deals_out.append({
            "id": jid,
            "deal_name": deal_name or d.get("deal_name", "Unknown"),
            "verdict": verdict or "CONDITIONAL",
            "confidence": confidence or 0,
            "created_at": (created_at or "")[:10],
            "market": d.get("market") or d.get("location") or "",
            "asset_class": d.get("asset_class") or d.get("property_type") or "",
            "asking_price": d.get("asking_price") or 0,
            "cap_rate": d.get("cap_rate") or 0,
            "irr": irr,
            "bias_flags": bias_count,
        })

    # #255: Attach actual_irr from outcome table (join via pipeline_deals.job_id)
    try:
        with db.get_con() as con:
            outcome_rows = con.execute(
                "SELECT pd.job_id, pdo.actual_irr FROM pipeline_deals pd "
                "JOIN pipeline_deal_outcomes pdo ON pdo.deal_id = pd.id "
                "WHERE pd.job_id IS NOT NULL AND pdo.actual_irr IS NOT NULL"
            ).fetchall()
        actual_irr_map = {r[0]: r[1] for r in outcome_rows}
        for d2 in deals_out:
            if d2["id"] in actual_irr_map:
                d2["actual_irr"] = float(actual_irr_map[d2["id"]])
    except Exception:
        pass

    return jsonify({"deals": deals_out})


@app.route("/portfolio/vintage-report")
def portfolio_vintage_report_page():
    """Portfolio Vintage Report page (#230)."""
    return render_template_string(VINTAGE_REPORT_HTML)


@app.route("/api/portfolio/vintage-report")
def api_portfolio_vintage_report():
    """Generate portfolio vintage report data (#230)."""
    import json as _json
    import re as _re
    from datetime import datetime as _dt, timedelta as _td
    try:
        with db.get_con() as con:
            rows = con.execute(
                "SELECT id, deal_name, verdict, confidence, created_at, result FROM deals WHERE status='done' ORDER BY created_at ASC LIMIT 500"
            ).fetchall()
            # Stall detection: pipeline deals last updated >30 days ago not Closed/Passed
            pipe_rows = con.execute(
                "SELECT id, deal_name, stage, created_at FROM pipeline_deals WHERE stage NOT IN ('Closed','Passed') ORDER BY created_at ASC"
            ).fetchall()
    except Exception as e:
        return jsonify({"error": str(e)})

    now = _dt.utcnow()
    stalled = []
    for pr in pipe_rows:
        try:
            age_days = (now - _dt.fromisoformat(pr[3].replace("Z",""))).days
            if age_days > 30:
                stalled.append({"id": pr[0], "deal_name": pr[1], "stage": pr[2], "days_in_stage": age_days})
        except Exception:
            pass

    deals_out = []
    irr_weighted_sum = 0.0
    irr_weight_total = 0.0
    for row in rows:
        jid, deal_name, verdict, confidence, created_at, result_raw = row
        deal_data = {}
        try:
            deal_data = _json.loads(result_raw or "{}") if result_raw else {}
        except Exception:
            pass
        d = deal_data.get("deal", {})
        memo = deal_data.get("memo", "")
        stress = deal_data.get("stress_test", {})

        irr = None
        try:
            base = stress.get("base", {})
            irr_val = base.get("irr") or base.get("IRR")
            if irr_val is not None:
                irr = float(str(irr_val).replace("%","").strip())
        except Exception:
            pass
        if irr is None:
            m = _re.search(r"IRR[^0-9]*([0-9]+\.?[0-9]*)\s*%", memo, _re.IGNORECASE)
            if m:
                try:
                    irr = float(m.group(1))
                except Exception:
                    pass

        v = (verdict or "CONDITIONAL").upper()
        price = d.get("asking_price") or 0
        try:
            price = float(price)
        except Exception:
            price = 0.0

        if irr is not None and v in ("GO", "CONDITIONAL") and price > 0:
            irr_weighted_sum += irr * price
            irr_weight_total += price
        elif irr is not None and v in ("GO", "CONDITIONAL"):
            irr_weighted_sum += irr
            irr_weight_total += 1

        deals_out.append({
            "id": jid,
            "deal_name": deal_name or d.get("deal_name", "Unknown"),
            "verdict": v,
            "confidence": confidence or 0,
            "created_at": (created_at or "")[:10],
            "market": d.get("market") or d.get("location") or "",
            "asset_class": d.get("property_type") or d.get("asset_class") or "",
            "asking_price": price,
            "irr": irr,
            "hold_period": d.get("hold_period"),
        })

    weighted_avg_irr = round(irr_weighted_sum / irr_weight_total, 1) if irr_weight_total > 0 else None
    go_deals = [d for d in deals_out if d["verdict"] in ("GO", "CONDITIONAL")]
    macro_note = (
        "Macro environment: Rising rate cycle adding 50-80bps to cap rate floor in primary markets. "
        "Cross-check each deal's exit cap rate assumption against current 10-yr Treasury spread. "
        "Multifamily demand fundamentals remain intact; industrial and self-storage hold strong. "
        "Office sector: structural headwinds persist — require >200bps premium to risk-free rate."
    )

    return jsonify({
        "report_date": now.strftime("%B %Y"),
        "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
        "total_analyzed": len(deals_out),
        "go_deals_count": len(go_deals),
        "weighted_avg_irr": weighted_avg_irr,
        "deals": go_deals,
        "stalled_pipeline": stalled,
        "macro_note": macro_note,
    })


@app.route("/compare")
def compare_page():
    return render_template_string(COMPARE_HTML)


@app.route("/compare", methods=["POST"])
def compare_analyze():
    data = request.get_json(force=True)
    job_a = str(uuid.uuid4())[:8]
    job_b = str(uuid.uuid4())[:8]
    JOBS[job_a] = {"status": "queued"}
    JOBS[job_b] = {"status": "queued"}

    def _run(jid, text):
        _analyze(jid, text, None)

    threading.Thread(target=_run, args=(job_a, data.get("om_a", "")), daemon=True).start()
    threading.Thread(target=_run, args=(job_b, data.get("om_b", "")), daemon=True).start()
    return jsonify({"job_a": job_a, "job_b": job_b})


@app.route("/find-deals")
@app.route("/find-deals/watchlist")
def find_deals_page():
    view = "watchlist" if request.path.endswith("/watchlist") else "live"
    return render_template_string(FIND_DEALS_HTML, _view=view)


@app.route("/scan", methods=["POST"])
def scan():
    data = request.get_json(force=True)
    scan_id = str(uuid.uuid4())[:8]
    SCAN_JOBS[scan_id] = {"status": "scanning"}

    def _run_scan():
        try:
            from deal_finder import find_best_deals
            results = find_best_deals(
                market=data.get("market", "Phoenix, AZ"),
                max_price=float(data.get("max_price", 20_000_000)),
                min_units=int(data.get("min_units", 20)),
                min_cap_rate=float(data.get("min_cap_rate", 4.5)),
                max_vacancy=float(data.get("max_vacancy", 15)),
                hold_period=int(data.get("hold_period", 5)),
                top_n=10,
                run_full_council=False,
            )
            SCAN_JOBS[scan_id] = {"status": "done", "results": results}
        except Exception as e:
            import traceback
            SCAN_JOBS[scan_id] = {"status": "error", "message": str(e), "traceback": traceback.format_exc()}

    threading.Thread(target=_run_scan, daemon=True).start()
    return jsonify({"scan_id": scan_id})


@app.route("/scan-status/<scan_id>")
def scan_status(scan_id):
    job = SCAN_JOBS.get(scan_id, {"status": "not_found"})
    if job.get("status") != "done":
        return jsonify({"status": job.get("status"), "message": job.get("message", "")})
    results = job.get("results", [])
    # Serialize: keep only lightweight fields for the list view
    slim = []
    for r in results:
        slim.append({
            "deal_name":         r.get("deal_name"),
            "address":           r.get("address"),
            "market":            r.get("market"),
            "asking_price":      r.get("asking_price"),
            "cap_rate":          r.get("cap_rate"),
            "units":             r.get("units"),
            "cleareye_score":    r.get("cleareye_score"),
            "base_irr":          r.get("base_irr"),
            "bear_irr":          r.get("bear_irr"),
            "validation_grade":  r.get("validation_grade"),
            "red_flag_count":    r.get("red_flag_count"),
            "_source":           r.get("_source"),
        })
    return jsonify({"status": "done", "results": slim})


@app.route("/api/live-deals")
def live_deals():
    """
    Multi-source live deal aggregator (#123).
    Pulls from RentCast + ATTOM, quick-scores each, returns sorted+categorized.
    """
    import time as _time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    # Query params
    markets = request.args.getlist("market") or ["Phoenix, AZ", "Atlanta, GA", "Dallas, TX", "Tampa, FL", "Denver, CO"]
    category = request.args.get("category", "all").lower()  # all | multifamily | retail | industrial | office
    sort_by = request.args.get("sort", "score")             # score | cap_rate | irr | price | units
    max_price = float(request.args.get("max_price", 30_000_000))
    min_cap_rate = float(request.args.get("min_cap_rate", 0))
    page_size = int(request.args.get("limit", 30))

    # Fetch from RentCast across all requested markets
    all_deals: list[dict] = []

    # Checked markets = hot (30min TTL), unchecked = cold (2hr TTL)
    # For the API we receive all requested markets as "hot" by default
    HOT_TTL = 1800    # 30 minutes
    COLD_TTL = 7200   # 2 hours

    def _fetch_market(mkt):
        # Try cache first (#126)
        cached = _load_deal_cache(mkt, ttl_seconds=HOT_TTL)
        if cached is not None:
            for d in cached:
                d["_from_cache"] = True
            return cached
        # Live fetch
        try:
            from rentcast_client import search_multifamily_deals
            deals = search_multifamily_deals(
                market=mkt,
                max_price=max_price,
                min_cap_rate=min_cap_rate,
                limit=15,
            )
            for d in deals:
                d["_source_market"] = mkt
                d["_from_cache"] = False
            _save_deal_cache(mkt, deals)
            _track_api_call("rentcast")
            return deals
        except Exception as e:
            return [{"_error": str(e), "_source_market": mkt}]

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(_fetch_market, m): m for m in markets}
        for fut in as_completed(futures):
            all_deals.extend(fut.result())

    # Filter errors
    deals = [d for d in all_deals if not d.get("_error")]

    # Category detection helper
    _CATEGORY_KEYWORDS = {
        "multifamily": ["apartment", "multifamily", "multi-family", "units", "residential", "condo"],
        "retail":      ["retail", "strip", "shopping", "plaza", "center", "store", "shop"],
        "industrial":  ["industrial", "warehouse", "flex", "distribution", "logistics", "manufacturing"],
        "office":      ["office", "medical", "professional", "class a", "class b"],
    }

    def detect_category(d):
        name = (d.get("deal_name", "") + " " + d.get("property_type", "") + " " + d.get("address", "")).lower()
        for cat, kws in _CATEGORY_KEYWORDS.items():
            if any(k in name for k in kws):
                return cat
        # Fallback: if units > 0 it's multifamily
        if d.get("units", 0) > 0:
            return "multifamily"
        return "other"

    for d in deals:
        d["_category"] = detect_category(d)

    # Filter by category
    if category != "all":
        deals = [d for d in deals if d.get("_category") == category]

    # Load active scoring weights (#141) — falls back to DEFAULT_WEIGHTS
    try:
        _active_weights = scoring_profile_get_active()
    except Exception:
        _active_weights = DEFAULT_WEIGHTS.copy()

    # Quick-score each deal (lightweight: stress test only, no full council)
    def _quick_score(d, weights=None):
        w = weights or _active_weights
        try:
            cap = float(d.get("cap_rate") or 5.0)
            irr = float(d.get("projected_irr") or cap * 2.5)
            price = float(d.get("asking_price") or 1)
            units = int(d.get("units") or 1)
            ppu = price / units if units else 0
            bear_irr = round(irr * 0.7, 1)

            # Compute each component separately for breakdown
            cap_pts     = cap * w.get("cap_rate", 8.0)
            irr_pts     = max(0, (irr - 8) * w.get("irr_premium", 3.0))
            bear_pts    = w.get("bear_cushion", 15.0) if bear_irr >= 8 else 0.0
            scale_pts   = w.get("scale", 5.0) if units >= 50 else 0.0
            ppu_pts     = w.get("ppu_discount", 10.0) if ppu < 150_000 else 0.0

            raw = cap_pts + irr_pts + bear_pts + scale_pts + ppu_pts
            score = min(100, max(0, int(raw)))
            d["cleareye_score"] = score
            d["base_irr"] = round(irr, 1)
            d["bear_irr"] = bear_irr
            # Score breakdown for waterfall UI
            d["score_breakdown"] = [
                {"label": f"Cap Rate ({cap:.1f}%)", "pts": round(cap_pts, 1)},
                {"label": f"IRR premium ({irr:.1f}% - 8% hurdle)", "pts": round(irr_pts, 1)},
                {"label": f"Bear-case cushion (IRR {bear_irr:.1f}%)", "pts": round(bear_pts, 1)},
                {"label": f"Scale ({units} units)", "pts": round(scale_pts, 1)},
                {"label": f"Value entry (${int(ppu):,}/unit)", "pts": round(ppu_pts, 1)},
            ]
        except Exception:
            d["cleareye_score"] = 0
            d["bear_irr"] = 0
            d["base_irr"] = 0
            d["score_breakdown"] = []

        # Data confidence badge (#127)
        actual_fields = sum([
            bool(d.get("cap_rate") and d.get("_source") != "mock_no_api_key"),
            bool(d.get("noi")),
            bool(d.get("units") and int(d.get("units", 0)) > 0),
            bool(d.get("occupancy")),
            bool(d.get("year_built")),
        ])
        if actual_fields >= 4:
            d["_confidence"] = "high"
        elif actual_fields >= 2:
            d["_confidence"] = "medium"
        else:
            d["_confidence"] = "low"
        d["_assumed_fields"] = [f for f, v in [
            ("cap_rate", d.get("cap_rate") and d.get("_source") != "mock_no_api_key"),
            ("NOI", d.get("noi")),
            ("IRR", d.get("projected_irr")),
            ("exit cap", d.get("exit_cap_rate")),
            ("rent growth", d.get("rent_growth")),
        ] if not v]

        # Listing staleness (#132)
        from datetime import date as _date
        try:
            first_seen = d.get("_first_seen", _date.today().isoformat())
            days_old = (_date.today() - _date.fromisoformat(first_seen)).days
            d["_days_listed"] = days_old
            d["_stale"] = days_old >= 14
        except Exception:
            d["_days_listed"] = 0
            d["_stale"] = False

        return d

    deals = [_quick_score(d) for d in deals]

    # Sort
    sort_keys = {
        "score":    lambda d: -(d.get("cleareye_score") or 0),
        "cap_rate": lambda d: -(float(d.get("cap_rate") or 0)),
        "irr":      lambda d: -(float(d.get("base_irr") or 0)),
        "price":    lambda d:  (float(d.get("asking_price") or 999_999_999)),
        "units":    lambda d: -(int(d.get("units") or 0)),
    }
    deals.sort(key=sort_keys.get(sort_by, sort_keys["score"]))
    deals = deals[:page_size]

    # Category counts
    cat_counts = {"all": len(all_deals)}
    for cat in _CATEGORY_KEYWORDS:
        cat_counts[cat] = sum(1 for d in all_deals if d.get("_category") == cat)

    # Rate limit info (#124)
    rate_info = _get_rate_info()

    return jsonify({
        "deals": deals,
        "total": len(deals),
        "category_counts": cat_counts,
        "rate_limits": rate_info,
        "sources_checked": markets,
        "fetched_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    })


def _deal_cache_path(market: str) -> Path:
    """Return path to per-market deal cache file."""
    slug = market.lower().replace(", ", "_").replace(" ", "_")
    return Path(__file__).parent / "outputs" / f"deal_cache_{slug}.json"


def _load_deal_cache(market: str, ttl_seconds: int = 1800) -> list[dict] | None:
    """Load cached deals for a market; return None if stale or missing (#126)."""
    import time as _time
    p = _deal_cache_path(market)
    if not p.exists():
        return None
    try:
        cached = json.loads(p.read_text(encoding="utf-8"))
        age = _time.time() - cached.get("ts", 0)
        if age > ttl_seconds:
            return None
        return cached.get("deals", [])
    except Exception:
        return None


def _save_deal_cache(market: str, deals: list[dict]):
    """Save deals to per-market cache file (#126)."""
    import time as _time
    p = _deal_cache_path(market)
    p.parent.mkdir(exist_ok=True)
    # Add first_seen_at to new deals (#132 listing staleness)
    from datetime import date
    today = date.today().isoformat()
    for d in deals:
        if not d.get("_first_seen"):
            d["_first_seen"] = today
    try:
        p.write_text(json.dumps({"ts": _time.time(), "deals": deals}), encoding="utf-8")
    except Exception:
        pass


def _get_rate_info() -> dict:
    """Return API rate limit status for all data providers (#124)."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        con = __import__("db")._conn()
        con.execute("""
            CREATE TABLE IF NOT EXISTS api_usage (
                provider TEXT, date TEXT, call_count INTEGER,
                PRIMARY KEY(provider, date)
            )
        """)
        con.commit()
        rows = con.execute("SELECT provider, call_count FROM api_usage WHERE date=?", (today,)).fetchall()
        usage = {r[0]: r[1] for r in rows}
    except Exception:
        usage = {}

    limits = {
        "rentcast":   {"used": usage.get("rentcast", 0),   "limit": 50,    "plan": "Free",  "unit": "calls/mo"},
        "attom":      {"used": usage.get("attom", 0),      "limit": 100,   "plan": "Trial", "unit": "calls/mo"},
        "anthropic":  {"used": usage.get("anthropic", 0),  "limit": 1000,  "plan": "API",   "unit": "jobs today"},
        "fred":       {"used": usage.get("fred", 0),       "limit": 500,   "plan": "Free",  "unit": "calls/day"},
    }
    for k, v in limits.items():
        pct = round(v["used"] / v["limit"] * 100) if v["limit"] else 0
        v["pct"] = pct
        v["status"] = "ok" if pct < 80 else ("warn" if pct < 95 else "critical")
    return limits


def _track_api_call(provider: str, count: int = 1):
    """Increment API usage counter for rate limit display (#124)."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        con = __import__("db")._conn()
        con.execute("""
            INSERT INTO api_usage (provider, date, call_count) VALUES (?,?,?)
            ON CONFLICT(provider,date) DO UPDATE SET call_count=call_count+?
        """, (provider, today, count, count))
        con.commit()
    except Exception:
        pass


@app.route("/api/hud-opportunities")
def hud_opportunities():
    """HUD expiring contracts — value-add signals (#131)."""
    states = request.args.getlist("state") or ["AZ", "GA", "TX", "FL", "CO"]
    years = int(request.args.get("years", 3))
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from hud_census_client import get_hud_expiring_contracts
    all_deals: list[dict] = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futs = {ex.submit(get_hud_expiring_contracts, s, years): s for s in states}
        for fut in as_completed(futs):
            all_deals.extend(fut.result())
    all_deals.sort(key=lambda d: d.get("days_to_expiry", 9999))
    return jsonify({"total": len(all_deals), "deals": all_deals[:50]})


@app.route("/api/census/<market>")
def census_data(market):
    """Live Census ACS data for a market (#131)."""
    from hud_census_client import get_census_acs
    data = get_census_acs(market)
    return jsonify(data)


@app.route("/api/rate-limits")
def api_rate_limits():
    """Return current API rate limit status (#124)."""
    return jsonify(_get_rate_info())


# ---------------------------------------------------------------------------
# Structured logging + health check (#140)
# ---------------------------------------------------------------------------

_ERROR_LOG = Path(__file__).parent / "outputs" / "error_log.jsonl"


def _log_error(source: str, message: str, context: dict | None = None):
    """Append a structured error entry to outputs/error_log.jsonl (#140)."""
    entry = {
        "ts": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": source,
        "message": message,
    }
    if context:
        entry["context"] = context
    try:
        _ERROR_LOG.parent.mkdir(exist_ok=True)
        with _ERROR_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception:
        pass  # never let logging break the app


@app.route("/api/comps/<job_id>")
def api_comps(job_id):
    """
    Return 1-mile rent comps for a completed analysis job (#135).
    Uses RentCast enrich_deal_with_comps() if API key present, else mock.
    """
    job = JOBS.get(job_id)
    if not job:
        db_job = job_get(job_id)
        if db_job and db_job.get("result"):
            job = db_job["result"]
    if not job:
        return jsonify({"error": "Job not found"}), 404

    deal = job.get("deal") or {}
    address = deal.get("address", "")
    market = deal.get("market", "Phoenix, AZ")
    units = deal.get("units", 1)

    # Attempt live RentCast comps
    try:
        from rentcast_client import enrich_deal_with_comps, get_market_benchmarks, RENTCAST_API_KEY
        if RENTCAST_API_KEY and address:
            comps = enrich_deal_with_comps(address, units=units, radius_miles=1.0)
            _track_api_call("rentcast")
        else:
            comps = _mock_comps(market, units)
    except Exception as e:
        comps = _mock_comps(market, units)

    # Also pull market benchmarks for context
    try:
        from rentcast_client import get_market_benchmarks
        bench = get_market_benchmarks(market)
    except Exception:
        bench = {}

    return jsonify({
        "job_id": job_id,
        "address": address,
        "market": market,
        "comps": comps,
        "market_benchmarks": bench,
    })


def _mock_comps(market: str, units: int = 1) -> dict:
    """Realistic mock comps data for dev/demo mode."""
    import random
    base_rent = {"Phoenix": 1450, "Atlanta": 1380, "Dallas": 1510, "Tampa": 1420, "Denver": 1780}.get(
        market.split(",")[0].strip(), 1400
    )
    mock_comps = []
    for i in range(8):
        r = base_rent + random.randint(-200, 300)
        mock_comps.append({
            "address": f"{100 + i * 50} {['Main','Oak','Elm','Park','Lake'][i % 5]} St",
            "price": r,
            "bedrooms": random.choice([1, 2, 2, 3]),
            "bathrooms": random.choice([1, 1, 2, 2]),
            "squareFootage": random.randint(650, 1100),
            "propertyType": "Apartment",
            "status": "Active",
            "listedDate": f"2026-0{random.randint(1,5)}-{random.randint(1,28):02d}",
            "_mock": True,
        })
    rents = [c["price"] for c in mock_comps]
    return {
        "comps": mock_comps,
        "avg_comp_rent": round(sum(rents) / len(rents)),
        "rent_range_low": min(rents),
        "rent_range_high": max(rents),
        "comp_count": len(rents),
        "radius_miles": 1.0,
        "_source": "mock_no_api_key",
    }


@app.route("/api/health")
def api_health():
    """
    Health check endpoint (#140).
    Returns: status, db connectivity, recent error count, uptime.
    """
    from datetime import date
    health = {
        "status": "ok",
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "version": "1.0.0",
    }
    # DB connectivity
    try:
        from db import _conn
        row = _conn().execute("SELECT COUNT(*) FROM deals").fetchone()
        health["db"] = {"status": "ok", "deal_count": row[0] if row else 0}
    except Exception as e:
        health["db"] = {"status": "error", "error": str(e)}
        health["status"] = "degraded"
    # Error log summary (last 24 h)
    try:
        if _ERROR_LOG.exists():
            today = date.today().isoformat()
            recent = [l for l in _ERROR_LOG.read_text(encoding="utf-8").splitlines() if today in l]
            health["errors_today"] = len(recent)
            if len(recent) > 10:
                health["status"] = "degraded"
        else:
            health["errors_today"] = 0
    except Exception:
        health["errors_today"] = -1
    # Rate limit snapshot
    try:
        health["rate_limits"] = _get_rate_info()
    except Exception:
        pass
    # Circuit breaker statuses (#143)
    try:
        cb_statuses = _cb_all_statuses()
        health["circuit_breakers"] = cb_statuses
        open_cbs = [cb["name"] for cb in cb_statuses if cb["state"] == "open"]
        if open_cbs:
            health["status"] = "degraded"
            health["open_circuits"] = open_cbs
    except Exception:
        pass
    code = 200 if health["status"] == "ok" else 207
    return jsonify(health), code


@app.route("/api/circuit-breakers", methods=["GET"])
def circuit_breaker_status():
    """Return all circuit breaker states (#143)."""
    return jsonify({
        "circuit_breakers": _cb_all_statuses(),
        "any_open": any(cb["state"] == "open" for cb in _cb_all_statuses()),
    })


@app.route("/api/circuit-breakers/<name>/reset", methods=["POST"])
def circuit_breaker_reset(name: str):
    """Manually reset a named circuit breaker (#143)."""
    cb = _CIRCUIT_BREAKERS.get(name)
    if not cb:
        return jsonify({"error": f"Unknown circuit breaker: {name}. Known: {list(_CIRCUIT_BREAKERS)}"}), 404
    cb.reset()
    return jsonify({"ok": True, "name": name, "state": cb.state})


# ---------------------------------------------------------------------------
# Scoring profiles (#141) — custom deal scoring weight configuration
# ---------------------------------------------------------------------------

@app.route("/api/scoring-profiles", methods=["GET"])
def get_scoring_profiles():
    """List all scoring profiles (presets + saved)."""
    profiles = scoring_profile_list(session.get("user_email"))
    return jsonify({"profiles": profiles, "default_weights": DEFAULT_WEIGHTS})


@app.route("/api/scoring-profiles", methods=["POST"])
def create_scoring_profile():
    """Save a new custom scoring profile."""
    data = request.get_json(force=True)
    name    = (data.get("name") or "").strip()
    weights = data.get("weights") or {}
    if not name:
        return jsonify({"error": "name required"}), 400
    # Validate weights — only allow known keys, clamp to [0, 50]
    validated = {}
    for key in DEFAULT_WEIGHTS:
        raw = weights.get(key)
        try:
            validated[key] = max(0.0, min(50.0, float(raw))) if raw is not None else DEFAULT_WEIGHTS[key]
        except Exception:
            validated[key] = DEFAULT_WEIGHTS[key]
    profile_id = scoring_profile_create(name, validated, session.get("user_email"))
    return jsonify({"ok": True, "id": profile_id})


@app.route("/api/scoring-profiles/<profile_id>/activate", methods=["POST"])
def activate_scoring_profile(profile_id: str):
    """Set a profile as the active scoring model."""
    # Preset profiles are in-memory; just return ok (they're always usable)
    if not profile_id.startswith("preset_"):
        scoring_profile_activate(profile_id, session.get("user_email"))
    return jsonify({"ok": True, "active": profile_id})


@app.route("/api/scoring-profiles/<profile_id>", methods=["DELETE"])
def delete_scoring_profile(profile_id: str):
    """Delete a saved scoring profile (presets cannot be deleted)."""
    if profile_id.startswith("preset_"):
        return jsonify({"error": "Preset profiles cannot be deleted"}), 400
    scoring_profile_delete(profile_id)
    return jsonify({"ok": True})


# ---------------------------------------------------------------------------
# Watchlist & notes routes (#128)
# ---------------------------------------------------------------------------

@app.route("/api/watchlist", methods=["GET"])
def get_watchlist():
    user_email = session.get("user_email")
    deals = watchlist_get(user_email)
    keys = watchlist_keys(user_email)
    return jsonify({"deals": deals, "keys": list(keys)})


@app.route("/api/watchlist", methods=["POST"])
def add_to_watchlist():
    data = request.get_json(force=True)
    deal_key = data.get("deal_key", "")
    deal_data = data.get("deal", {})
    if not deal_key:
        return jsonify({"error": "deal_key required"}), 400
    user_email = session.get("user_email")
    watchlist_add(deal_key, deal_data, user_email)
    return jsonify({"ok": True, "starred": True})


@app.route("/api/watchlist/<deal_key>", methods=["DELETE"])
def remove_from_watchlist(deal_key):
    user_email = session.get("user_email")
    watchlist_remove(deal_key, user_email)
    return jsonify({"ok": True, "starred": False})


@app.route("/api/notes/<deal_key>", methods=["GET"])
def get_note(deal_key):
    user_email = session.get("user_email")
    return jsonify({"note": note_get(deal_key, user_email)})


@app.route("/api/notes/<deal_key>", methods=["POST"])
def set_note(deal_key):
    data = request.get_json(force=True)
    note = data.get("note", "")
    user_email = session.get("user_email")
    note_set(deal_key, note, user_email)
    return jsonify({"ok": True})


@app.route("/api/notes/template/<path:deal_key>", methods=["GET"])
def get_note_template(deal_key):
    """Return note templates for a deal, auto-populated from analysis if available (#180)."""
    today = datetime.utcnow().strftime("%Y-%m-%d")

    quick_scan = (
        f"## Deal Note — {deal_key}\n"
        f"**Date:** {today}  **Status:** Screening\n\n"
        f"### Initial Impressions\n- \n\n"
        f"### Key Questions\n- \n\n"
        f"### Red Flags\n- \n\n"
        f"### Next Steps\n"
        f"- [ ] Request T-12 / rent roll\n"
        f"- [ ] Schedule site visit\n"
        f"- [ ] Confirm market rents\n"
    )

    loi_checklist = (
        f"## LOI Checklist — {deal_key}\n"
        f"**Date:** {today}\n\n"
        f"### Proposed Terms\n"
        f"- Offer Price: $\n"
        f"- Earnest Money: $\n"
        f"- DD Period: 30 days\n"
        f"- Target Close: \n\n"
        f"### Pre-LOI Verification\n"
        f"- [ ] T-12 rent roll reviewed\n"
        f"- [ ] Expense verification\n"
        f"- [ ] Phase I environmental\n"
        f"- [ ] Financing term sheet\n\n"
        f"### Open Issues\n- \n\n"
        f"### Decision: [ ] Submit LOI  [ ] Pass  [ ] Counter\n"
    )

    # Try to find a matching analysis job
    analysis_template = None
    user_email = session.get("user_email")
    try:
        recent = jobs_recent(limit=50, user_email=user_email)
        dk_lower = deal_key.lower().strip()
        for job in recent:
            if job.get("status") != "done":
                continue
            deal = job.get("deal") or {}
            job_name = (deal.get("deal_name") or deal.get("address") or "").lower()
            # Fuzzy match: one contains the other, or share 10+ char prefix
            match = (
                dk_lower in job_name
                or job_name in dk_lower
                or (len(dk_lower) > 10 and dk_lower[:10] in job_name)
            )
            if not match:
                continue

            verdict_obj = job.get("verdict") or {}
            if isinstance(verdict_obj, dict):
                rec  = verdict_obj.get("recommendation", "UNKNOWN")
                conf = int((verdict_obj.get("confidence") or 0) * 100)
            else:
                rec, conf = str(verdict_obj), 0

            d = deal
            def _fmt(v):
                if v is None: return "N/A"
                s = str(v).replace("$", "").replace(",", "").strip()
                try:
                    f = float(s.replace("%", ""))
                    if "%" in s or (f < 1 and f > 0): return f"{f:.1%}"
                    return f"${f:,.0f}" if f >= 100 else f"{f:.2f}"
                except Exception:
                    return str(v)

            flags = list(job.get("audit_flags") or [])[:3]
            bias  = list(job.get("bias_flags")  or [])[:2]
            all_flags = flags + bias
            flags_lines = "\n".join(f"- ⚠️  {fl}" for fl in all_flags) if all_flags else "- None identified"

            analysis_template = (
                f"## Analysis Note — {d.get('deal_name', deal_key)}\n"
                f"**Verdict:** {rec} ({conf}% confidence)  **Date:** {today}\n\n"
                f"### Key Metrics\n"
                f"- Cap Rate: {_fmt(d.get('cap_rate') or d.get('going_in_cap_rate'))}\n"
                f"- Projected IRR: {_fmt(d.get('irr') or d.get('projected_irr'))}\n"
                f"- Purchase Price: {_fmt(d.get('purchase_price') or d.get('asking_price'))}\n"
                f"- Units: {d.get('units') or d.get('unit_count', 'N/A')}\n\n"
                f"### Risk Flags\n{flags_lines}\n\n"
                f"### Due Diligence Priorities\n"
                f"- [ ] Verify rent roll vs T-12\n"
                f"- [ ] Confirm CapEx reserve adequacy\n"
                f"- [ ] Environmental / title review\n"
                f"- [ ] Finalize financing\n\n"
                f"### Decision\n"
                f"[ ] Proceed to LOI  [ ] More info needed  [ ] Pass\n"
            )
            break
    except Exception:
        pass

    return jsonify({
        "quick_scan":     quick_scan,
        "loi_checklist":  loi_checklist,
        "full_analysis":  analysis_template,
        "has_analysis":   analysis_template is not None,
    })


# ---------------------------------------------------------------------------
# Saved searches routes (#139)
# ---------------------------------------------------------------------------

@app.route("/api/saved-searches", methods=["GET"])
def get_saved_searches():
    """List all saved deal filter configs."""
    user_email = session.get("user_email")
    searches = search_list(user_email)
    return jsonify({"searches": searches})


@app.route("/api/saved-searches", methods=["POST"])
def save_search():
    """Save a named filter config."""
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    filters = data.get("filters") or {}
    if not name:
        return jsonify({"error": "name required"}), 400
    user_email = session.get("user_email")
    search_id = str(uuid.uuid4())[:8]
    search_save(search_id, name, filters, user_email)
    return jsonify({"ok": True, "id": search_id})


@app.route("/api/saved-searches/<search_id>", methods=["DELETE"])
def delete_saved_search(search_id):
    user_email = session.get("user_email")
    search_delete(search_id, user_email)
    return jsonify({"ok": True})


@app.route("/api/saved-searches/<search_id>/run", methods=["POST"])
def run_saved_search(search_id):
    """Mark a saved search as run and return its filters for the client to re-apply."""
    searches = search_list()
    for s in searches:
        if s["id"] == search_id:
            search_update_last_run(search_id)
            return jsonify({"ok": True, "filters": s.get("filters", {})})
    return jsonify({"error": "Not found"}), 404


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Deal alert routes (#134)
# ---------------------------------------------------------------------------

@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    user_email = session.get("user_email")
    alerts = alert_list(user_email)
    # Don't expose full seen_keys list to client
    for a in alerts:
        a["seen_count"] = len(a.get("seen_keys") or [])
        a.pop("seen_keys", None)
    return jsonify({"alerts": alerts})


@app.route("/api/alerts", methods=["POST"])
def create_alert():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip()
    filters = data.get("filters") or {}
    if not name or not email:
        return jsonify({"error": "name and email required"}), 400
    user_email = session.get("user_email")
    alert_id = str(uuid.uuid4())[:8]
    alert_create(alert_id, name, filters, email, user_email)
    return jsonify({"ok": True, "id": alert_id})


@app.route("/api/alerts/<alert_id>", methods=["DELETE"])
def delete_alert(alert_id):
    user_email = session.get("user_email")
    alert_delete(alert_id, user_email)
    return jsonify({"ok": True})


@app.route("/api/alerts/<alert_id>/toggle", methods=["POST"])
def toggle_alert(alert_id):
    data = request.get_json(force=True)
    active = bool(data.get("active", True))
    alert_toggle(alert_id, active)
    return jsonify({"ok": True, "active": active})


@app.route("/api/alerts/scan", methods=["POST"])
def trigger_alert_scan():
    """Manually trigger an alert scan (for testing / admin). (#134)"""
    try:
        threading.Thread(target=_scan_alerts_once, daemon=True).start()
        return jsonify({"ok": True, "message": "Scan triggered in background"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/alerts/digest", methods=["POST"])
def trigger_alert_digest():
    """
    Send a deal alert digest to all active alert subscribers (#186).
    Body (optional): {"alert_id": "...", "force": true}
      - alert_id: send only for this alert; omit to send for all active alerts
      - force: send even if no new deals (re-sends recent 5 deals per alert)
    Gated by admin token when called from /admin.
    """
    data = request.get_json(force=True) or {}
    alert_id_filter = data.get("alert_id")
    force = bool(data.get("force", False))

    alerts = alert_list()
    active = [a for a in alerts if a.get("active")]
    if alert_id_filter:
        active = [a for a in active if a.get("id") == alert_id_filter]

    if not active:
        return jsonify({"ok": False, "message": "No active alerts found"}), 404

    sent = []

    def _do_digest():
        from rentcast_client import search_multifamily_deals
        for alert in active:
            try:
                filters   = alert.get("filters") or {}
                markets   = filters.get("markets") or ["Phoenix, AZ"]
                max_price = float(filters.get("max_price") or 30_000_000)
                min_cap   = float(filters.get("min_cap_rate") or 0)
                email     = alert.get("email", "")
                if not email:
                    continue

                seen_keys: set = set(alert.get("seen_keys") or [])
                new_deals = []
                for mkt in markets[:5]:
                    try:
                        deals = search_multifamily_deals(market=mkt, max_price=max_price,
                                                         min_cap_rate=min_cap, limit=10)
                        for d in deals:
                            key = (d.get("address") or d.get("deal_name") or "") + "::" + mkt
                            if force or (key and key not in seen_keys):
                                new_deals.append(d)
                                seen_keys.add(key)
                    except Exception:
                        pass

                if new_deals or force:
                    _send_alert_digest_email(alert, new_deals[:5], email)
                    alert_update_check(alert["id"], len(new_deals), list(seen_keys)[-500:])
                    sent.append({"alert": alert.get("name"), "email": email, "deals": len(new_deals)})
            except Exception as exc:
                _log_error("digest_error", str(exc), {"alert_id": alert.get("id")})

    # Run synchronously so we can report results
    _do_digest()
    return jsonify({"ok": True, "sent": sent, "alert_count": len(sent)})


def _send_alert_digest_email(alert: dict, deals: list, email: str):
    """Send a branded HTML daily digest email with up to 5 new deals (#186, #251)."""
    try:
        from email_delivery import _send_email
        name = alert.get("name", "Deal Alert")
        ts   = datetime.utcnow().strftime("%B %d, %Y")
        deal_count = len(deals)
        deal_noun  = "deal" if deal_count == 1 else "deals"

        if not deals:
            deal_table = (
                "<tr><td colspan='4' style='padding:24px 16px;text-align:center;"
                "font-size:13px;color:#9CA3AF;border-bottom:1px solid #EAE7E1;'>"
                "No new deals matched your criteria since the last digest.</td></tr>"
            )
        else:
            deal_table = ""
            for d in deals:
                price   = "${:,.0f}".format(d.get("asking_price", 0)) if d.get("asking_price") else "&mdash;"
                cap     = "{:.2f}%".format(d.get("cap_rate", 0))      if d.get("cap_rate")     else "&mdash;"
                units   = str(d.get("units", "&mdash;"))
                dname   = d.get("deal_name", "Unnamed Deal")
                market  = d.get("_source_market") or d.get("market", "")
                market_tag = (
                    "<span style='display:block;margin-top:3px;font-size:11px;color:#6B7280;"
                    "font-family:\"JetBrains Mono\",Consolas,monospace;'>" + market + "</span>"
                    if market else ""
                )
                deal_table += (
                    "<tr>"
                    "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;'>"
                    "<div style='font-size:14px;font-weight:600;color:#0D1926;"
                    "font-family:\"Plus Jakarta Sans\",Arial,sans-serif;'>" + dname + "</div>"
                    + market_tag +
                    "</td>"
                    "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;"
                    "font-family:\"JetBrains Mono\",Consolas,monospace;font-size:13px;"
                    "font-weight:600;color:#155E44;white-space:nowrap;'>" + price + "</td>"
                    "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;"
                    "font-family:\"JetBrains Mono\",Consolas,monospace;font-size:13px;"
                    "color:#0D1926;white-space:nowrap;'>" + cap + " cap</td>"
                    "<td style='padding:14px 16px;border-bottom:1px solid #EAE7E1;vertical-align:top;"
                    "font-size:13px;color:#6B7280;white-space:nowrap;'>" + units + " units</td>"
                    "</tr>"
                )

        html = (
            "<!DOCTYPE html><html><head><meta charset='UTF-8'>"
            "<meta name='viewport' content='width=device-width,initial-scale=1'></head>"
            "<body style='margin:0;padding:0;background:#F5F3EE;"
            "font-family:\"Plus Jakarta Sans\",Arial,sans-serif;'>"
            "<table width='100%' cellpadding='0' cellspacing='0' style='background:#F5F3EE;padding:32px 16px;'>"
            "<tr><td align='center'>"
            "<table width='600' cellpadding='0' cellspacing='0' style='max-width:600px;width:100%;'>"

            # Header bar
            "<tr><td style='background:#155E44;border-radius:12px 12px 0 0;padding:24px 32px;'>"
            "<table width='100%' cellpadding='0' cellspacing='0'><tr>"
            "<td style='vertical-align:middle;'>"
            "<span style='font-size:18px;font-weight:700;color:#FFFFFF;letter-spacing:-0.3px;'>&#128065; ClearEye</span>"
            "</td>"
            "<td align='right' style='vertical-align:middle;'>"
            "<span style='font-size:11px;color:rgba(255,255,255,0.65);"
            "font-family:\"JetBrains Mono\",Consolas,monospace;'>Daily Digest &middot; " + ts + "</span>"
            "</td></tr></table></td></tr>"

            # Summary row
            "<tr><td style='background:#FFFFFF;padding:24px 32px 16px;"
            "border-left:1px solid #EAE7E1;border-right:1px solid #EAE7E1;'>"
            "<div style='font-size:11px;font-weight:600;letter-spacing:0.08em;"
            "color:#155E44;text-transform:uppercase;margin-bottom:8px;'>Deal Alert Digest</div>"
            "<h1 style='margin:0 0 6px;font-size:22px;font-weight:700;color:#0D1926;letter-spacing:-0.4px;'>"
            + name + "</h1>"
            "<p style='margin:0;font-size:14px;color:#6B7280;'>"
            "<strong style='color:#155E44;'>" + str(deal_count) + " new " + deal_noun + "</strong>"
            " matched your criteria in the last 24 hours.</p>"
            "</td></tr>"

            # Table
            "<tr><td style='background:#FFFFFF;padding:0 32px;"
            "border-left:1px solid #EAE7E1;border-right:1px solid #EAE7E1;'>"
            "<table width='100%' cellpadding='0' cellspacing='0' style='border-collapse:collapse;'>"
            "<thead><tr style='background:#F5F3EE;'>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Deal</th>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Ask Price</th>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Cap Rate</th>"
            "<th style='padding:10px 16px;font-size:11px;font-weight:600;color:#6B7280;text-align:left;"
            "text-transform:uppercase;letter-spacing:0.06em;border-bottom:2px solid #DDD9D1;'>Size</th>"
            "</tr></thead><tbody>"
            + deal_table +
            "</tbody></table></td></tr>"

            # CTA
            "<tr><td style='background:#FFFFFF;padding:24px 32px 28px;"
            "border-left:1px solid #EAE7E1;border-right:1px solid #EAE7E1;'>"
            "<a href='http://localhost:5052/find-deals'"
            " style='display:inline-block;background:#155E44;color:#FFFFFF;text-decoration:none;"
            "font-size:13px;font-weight:600;padding:11px 24px;border-radius:8px;letter-spacing:0.01em;'>"
            "View All Deals &rarr;</a>"
            "</td></tr>"

            # Footer
            "<tr><td style='background:#F0EDE7;border:1px solid #EAE7E1;border-radius:0 0 12px 12px;"
            "padding:16px 32px;'>"
            "<p style='margin:0;font-size:11px;color:#9CA3AF;line-height:1.6;'>"
            "You are receiving this because you set up a deal alert on ClearEye. "
            "<a href='http://localhost:5052/find-deals' style='color:#6B7280;text-decoration:underline;'>Manage alerts</a>"
            " &middot; "
            "<a href='http://localhost:5052/find-deals' style='color:#6B7280;text-decoration:underline;'>Unsubscribe</a>"
            "</p></td></tr>"

            "</table></td></tr></table></body></html>"
        )
        subject = "ClearEye Deal Digest: " + str(deal_count) + " new " + deal_noun + " — " + name
        _send_email(email, subject, html)
        _log_error("digest_sent", "Sent " + str(deal_count) + " deals to " + email, {"alert": name})
    except Exception as e:
        _log_error("digest_fail", str(e), {"email": email})


# LP Sharing Portal routes (#136)
# ---------------------------------------------------------------------------

@app.route("/api/share/<job_id>", methods=["POST"])
def create_share_link(job_id):
    """
    Create a password-protected, optionally expiring share link for a job.
    Body: {label: str, password: str|None, expires_days: int|None}
    """
    data = request.get_json(force=True)
    label = (data.get("label") or "").strip() or None
    password = (data.get("password") or "").strip() or None
    expires_days = data.get("expires_days")
    user_email = session.get("user_email")

    expires_at = None
    if expires_days:
        from datetime import timedelta
        expires_at = (datetime.utcnow() + timedelta(days=int(expires_days))).strftime("%Y-%m-%dT%H:%M:%SZ")

    token = _secrets.token_urlsafe(16)
    shared_link_create(token, job_id, user_email=user_email, password=password,
                        label=label, expires_at=expires_at)
    share_url = request.host_url.rstrip("/") + f"/lp/{token}"
    return jsonify({"ok": True, "token": token, "url": share_url, "expires_at": expires_at})


@app.route("/api/share/<job_id>/links")
def get_share_links(job_id):
    """List all share links for a job."""
    links = shared_links_for_job(job_id)
    # Mask passwords
    for lnk in links:
        if lnk.get("password"):
            lnk["password"] = "••••••"
    return jsonify({"links": links})


@app.route("/api/share/<job_id>/analytics")
def share_analytics(job_id: str):
    """LP engagement analytics for a job (#144)."""
    return jsonify(lp_analytics_for_job(job_id))


@app.route("/api/share/<job_id>/analytics/per-lp")
def share_analytics_per_lp(job_id: str):
    """
    Per-LP read-receipt timeline for a job (#212).
    Returns each link's view sessions with timestamp, sections viewed, and time-per-section.
    """
    analytics = lp_analytics_for_job(job_id)
    links = analytics.get("links", [])
    events = analytics.get("recent_events", [])

    per_link: dict = {}
    for lnk in links:
        tok = lnk.get("token", "")
        per_link[tok] = {
            "label":       lnk.get("label") or tok[:12] + "…",
            "view_count":  lnk.get("view_count", 0),
            "last_viewed": lnk.get("last_viewed", ""),
            "created_at":  lnk.get("created_at", ""),
            "sections":    {},
            "events":      [],
        }

    for ev in events:
        tok = ev.get("token", "")
        if tok not in per_link:
            continue
        per_link[tok]["events"].append({
            "type":    ev.get("event_type"),
            "section": ev.get("section"),
            "dur_s":   ev.get("duration_s"),
            "ts":      ev.get("created_at"),
        })
        if ev.get("event_type") == "section_exit" and ev.get("section"):
            sec = ev["section"]
            per_link[tok]["sections"][sec] = per_link[tok]["sections"].get(sec, 0) + (ev.get("duration_s") or 0)

    return jsonify({"job_id": job_id, "per_lp": list(per_link.values())})


@app.route("/api/lp/<token>/event", methods=["POST"])
def lp_track_event(token: str):
    """
    Record an LP engagement event from the portal JS (#144).
    On first 'view' event, fire LP engagement alert email to the deal owner (#212).
    Body: {event_type: 'view'|'section_enter'|'section_exit'|'download', section: str, duration_s: float}
    """
    link = shared_link_get(token)
    if not link:
        return jsonify({"error": "invalid token"}), 404
    data = request.get_json(force=True) or {}
    event_type = data.get("event_type", "view")
    job_id = link["job_id"]

    lp_event_record(
        token      = token,
        job_id     = job_id,
        event_type = event_type,
        section    = data.get("section"),
        duration_s = data.get("duration_s"),
        lp_ua      = request.headers.get("User-Agent", "")[:200],
        lp_ip      = request.remote_addr,
    )

    # ── LP View Alert (#241) — fire on first view per token only ──
    if event_type == "view":
        try:
            owner_email = link.get("created_by") or ""
            if not owner_email:
                db_row = job_get(job_id)
                owner_email = (db_row or {}).get("user_email", "") or ""
            if owner_email:
                # Only email on the FIRST view (view_count was 0 before this event)
                prior_views = link.get("view_count") or 0
                job_data = JOBS.get(job_id) or {}
                if not job_data:
                    db_row2 = job_get(job_id)
                    job_data = (db_row2 or {}).get("result") or {}
                deal_name = (job_data.get("deal") or {}).get("deal_name", "Deal Analysis")
                report_url = "http://localhost:5052/report/" + job_id
                # Gather recent section engagement for this token
                sections_viewed: list[str] = []
                scroll_depth: int = 0
                view_count_now = prior_views + 1
                try:
                    import db as _db
                    recent_events = _db.get_con().execute(
                        "SELECT event_type, section, duration_s FROM lp_events WHERE token=? ORDER BY created_at DESC LIMIT 60",
                        (token,)
                    ).fetchall()
                    seen_secs: list[str] = []
                    for ev in recent_events:
                        if ev[0] == "section_enter" and ev[1] and ev[1] not in seen_secs:
                            seen_secs.append(ev[1])
                        # duration_s used to store scroll % for scroll_depth events
                        if ev[0] == "scroll_depth" and ev[2]:
                            scroll_depth = max(scroll_depth, int(float(ev[2]) * 100))
                    sections_viewed = seen_secs[:5]
                except Exception:
                    pass
                if prior_views == 0:
                    # First open — send immediate notification
                    from email_delivery import send_lp_view_alert
                    import threading as _th
                    _th.Thread(
                        target=send_lp_view_alert,
                        args=(owner_email, deal_name, link.get("label", ""), token),
                        kwargs={
                            "report_url": report_url,
                            "sections_so_far": sections_viewed,
                            "view_count": view_count_now,
                            "scroll_depth": scroll_depth,
                            "is_first_open": True,
                        },
                        daemon=True,
                    ).start()
                    print(f"[lp_alert] FIRST OPEN — notified {owner_email} for {deal_name}")
                else:
                    print(f"[lp_alert] repeat view #{view_count_now} for {deal_name} — no email")
        except Exception as _e:
            print(f"[lp_alert] failed to send view alert: {_e}")

    return jsonify({"ok": True})


@app.route("/api/share/<job_id>/package", methods=["POST"])
def create_lp_package(job_id: str):
    """
    One-click LP data room package: branded HTML summary compiled from analysis results (#144).
    Returns downloadable HTML file (or PDF if weasyprint available).
    """
    job_data = JOBS.get(job_id) or {}
    if not job_data:
        db_row = job_get(job_id)
        if db_row and db_row.get("result"):
            job_data = db_row["result"]

    if not job_data or job_data.get("status") == "error":
        return jsonify({"error": "No completed analysis found for this job"}), 404

    deal   = job_data.get("deal") or {}
    memo   = job_data.get("memo", "")
    stress = job_data.get("stress_test") or {}
    verdict_raw = job_data.get("verdict") or {}
    verdict_text = verdict_raw.get("verdict", "CONDITIONAL") if isinstance(verdict_raw, dict) else str(verdict_raw)

    data = request.get_json(force=True) or {}
    firm_name  = (data.get("firm_name") or "Investment Firm").strip()[:80]
    cover_memo = (data.get("cover_memo") or "").strip()[:2000]

    # Build HTML package
    deal_name    = deal.get("deal_name", "Deal Analysis")
    asking_price = deal.get("asking_price")
    units        = deal.get("units")
    cap_rate     = deal.get("cap_rate")
    market       = deal.get("market", "")

    verdict_color = "#3fb950" if "GO" in verdict_text.upper() and "NO" not in verdict_text.upper() else \
                    "#f85149" if "NO-GO" in verdict_text.upper() else "#d29922"

    price_str = f"${asking_price/1e6:.1f}M" if asking_price else "—"
    units_str = f"{units} units" if units else "—"
    cap_str   = f"{cap_rate:.1f}%" if cap_rate else "—"

    # Stress test table HTML
    stress_html = ""
    scenarios = stress.get("scenarios") or []
    if scenarios:
        rows_html = "".join(
            f"<tr><td>{s.get('scenario','')}</td><td>{s.get('irr','—')}</td>"
            f"<td>{s.get('coc','—')}</td><td style='color:{'#3fb950' if s.get('pass') else '#f85149'}'>"
            f"{'GO' if s.get('pass') else 'NO-GO'}</td></tr>"
            for s in scenarios[:8]
        )
        stress_html = f"""
        <h2 style="color:#58a6ff;font-size:1.1rem;border-bottom:1px solid #30363d;padding-bottom:6px;margin-top:28px;">Stress Test Scenarios</h2>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead><tr style="background:#21262d;">
            <th style="padding:6px 10px;text-align:left;">Scenario</th>
            <th style="padding:6px 10px;">IRR</th>
            <th style="padding:6px 10px;">CoC</th>
            <th style="padding:6px 10px;">Verdict</th>
          </tr></thead>
          <tbody>{rows_html}</tbody>
        </table>"""

    package_html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8">
<title>{deal_name} — LP Package</title>
<style>
*{{box-sizing:border-box;}}
body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#fff;color:#1a1a2e;margin:0;padding:40px;max-width:860px;margin:0 auto;}}
.cover{{background:#0d1117;color:#e6edf3;padding:48px 40px;border-radius:8px;margin-bottom:32px;}}
.cover h1{{font-size:1.6rem;font-weight:900;margin:0 0 8px;}}
.cover .sub{{color:#8b949e;font-size:13px;}}
.verdict-stamp{{font-size:1.4rem;font-weight:900;letter-spacing:2px;padding:8px 20px;border-radius:6px;display:inline-block;margin:16px 0;}}
.metric-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin:24px 0;}}
.metric-box{{background:#f8fafc;border:1px solid #e2e8f0;border-radius:6px;padding:12px 14px;text-align:center;}}
.metric-label{{font-size:10px;color:#718096;text-transform:uppercase;letter-spacing:.05em;}}
.metric-value{{font-size:1.2rem;font-weight:700;color:#1a202c;}}
table{{font-size:12px;}}
td,th{{padding:5px 10px;text-align:left;border-bottom:1px solid #e2e8f0;}}
h2{{color:#2d3748;font-size:1rem;margin:24px 0 8px;}}
.memo-section{{background:#f8fafc;border-left:4px solid #4299e1;padding:16px;border-radius:0 6px 6px 0;font-size:13px;line-height:1.6;white-space:pre-wrap;}}
.footer{{margin-top:40px;padding-top:16px;border-top:1px solid #e2e8f0;font-size:10px;color:#a0aec0;text-align:center;}}
@media print{{body{{padding:20px;}}}}
</style></head><body>
<div class="cover">
  <div style="font-size:11px;color:#8b949e;margin-bottom:12px;letter-spacing:.08em;">PREPARED BY {firm_name.upper()}</div>
  <h1>{deal_name}</h1>
  <div class="sub">{market} &bull; {units_str} &bull; {price_str}</div>
  <div class="verdict-stamp" style="background:{verdict_color}20;color:{verdict_color};border:2px solid {verdict_color};">{verdict_text}</div>
  <div class="sub" style="margin-top:8px;">ClearEye AI Analysis &bull; {datetime.utcnow().strftime("%B %d, %Y")}</div>
</div>

{"<div class='memo-section'><strong>Cover Memo</strong><br><br>" + cover_memo + "</div><br>" if cover_memo else ""}

<div class="metric-grid">
  <div class="metric-box"><div class="metric-label">Asking Price</div><div class="metric-value">{price_str}</div></div>
  <div class="metric-box"><div class="metric-label">Units</div><div class="metric-value">{units_str}</div></div>
  <div class="metric-box"><div class="metric-label">Cap Rate</div><div class="metric-value">{cap_str}</div></div>
  <div class="metric-box"><div class="metric-label">Market</div><div class="metric-value" style="font-size:.95rem;">{market or "—"}</div></div>
</div>

<h2>Investment Memo</h2>
<div class="memo-section">{memo[:3000].replace('<','&lt;').replace('>','&gt;') if memo else "Memo not available."}</div>

{stress_html}

<div class="footer">
  Prepared with ClearEye AI &bull; For qualified investors only &bull; Not a solicitation &bull; Past performance not indicative of future results
</div>
</body></html>"""

    # Try PDF export first
    try:
        import weasyprint
        pdf_bytes = weasyprint.HTML(string=package_html).write_pdf()
        import io
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"{deal_name.replace(' ','_')[:40]}_LP_Package.pdf",
        )
    except Exception:
        pass

    # Fallback: return HTML
    from flask import Response as FlaskResponse
    safe_name = deal_name.replace(" ", "_")[:40]
    return FlaskResponse(
        package_html,
        mimetype="text/html",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}_LP_Package.html"'},
    )


@app.route("/api/ic-memo/<job_id>")
def api_ic_memo(job_id):
    """
    One-click IC Memo PDF with ODD audit trail (#211).
    Generates a clean institutional-grade investment committee memo
    including full methodology documentation for LP/ODD questionnaires.
    """
    job = JOBS.get(job_id)
    if not job:
        db_row = job_get(job_id)
        if db_row and db_row.get("result"):
            job = db_row["result"]
    if not job:
        return jsonify({"error": "Job not found"}), 404

    deal       = job.get("deal") or {}
    memo       = job.get("memo", "")
    advisors   = job.get("advisors") or {}
    stress     = job.get("stress_test") or {}
    validation = job.get("validation_report", "")
    bias_rep   = job.get("bias_report", "")
    premortem  = job.get("premortem_report", "")
    macro_b    = (job.get("macro") or {}).get("brief", "")
    verdict_raw = job.get("verdict") or {}
    verdict_text = verdict_raw.get("verdict", "CONDITIONAL") if isinstance(verdict_raw, dict) else str(verdict_raw)
    conf_pct   = verdict_raw.get("confidence_pct", 0) if isinstance(verdict_raw, dict) else 0

    deal_name    = deal.get("deal_name", "Deal Analysis")
    asking_price = deal.get("asking_price")
    units        = deal.get("units")
    cap_rate     = deal.get("cap_rate")
    market       = deal.get("market", "")
    asset_type   = deal.get("asset_type", "")
    sponsor      = deal.get("sponsor_name", "")

    price_str = f"${asking_price/1e6:.2f}M" if asking_price else "—"
    units_str = str(units) if units else "—"
    cap_str   = f"{cap_rate:.2f}%" if cap_rate else "—"
    v_color   = "#1a7f37" if "GO" in verdict_text.upper() and "NO" not in verdict_text.upper() else \
                "#cf222e" if "NO-GO" in verdict_text.upper() else "#9a6700"
    v_bg      = "#dafbe1" if "GO" in verdict_text.upper() and "NO" not in verdict_text.upper() else \
                "#ffebe9" if "NO-GO" in verdict_text.upper() else "#fff8c5"

    gen_ts = datetime.utcnow().strftime("%B %d, %Y at %H:%M UTC")

    # --- Stress test table ---
    stress_rows = ""
    for s in (stress.get("scenarios") or [])[:10]:
        pass_fail = "GO" if s.get("pass") else "NO-GO"
        pf_color  = "#1a7f37" if s.get("pass") else "#cf222e"
        stress_rows += (
            f"<tr><td>{s.get('scenario','')}</td>"
            f"<td style='font-family:monospace'>{s.get('irr','—')}</td>"
            f"<td style='font-family:monospace'>{s.get('coc','—')}</td>"
            f"<td style='color:{pf_color};font-weight:700'>{pass_fail}</td></tr>"
        )
    stress_table_html = ""
    if stress_rows:
        stress_table_html = f"""
        <h2 class="sec-hdr">Stress Test Scenarios</h2>
        <table class="data-table">
          <thead><tr><th>Scenario</th><th>IRR</th><th>Cash-on-Cash</th><th>Verdict</th></tr></thead>
          <tbody>{stress_rows}</tbody>
        </table>"""

    # --- Advisor summaries (first 600 chars each) ---
    adv_html = ""
    for adv_name, adv_data in (advisors.items() if isinstance(advisors, dict) else []):
        if not isinstance(adv_data, dict):
            continue
        text = (adv_data.get("analysis") or adv_data.get("text") or "")[:600]
        if text:
            safe_text = text.replace("<", "&lt;").replace(">", "&gt;")
            adv_html += f'<div class="adv-block"><div class="adv-name">{adv_name}</div><p class="adv-text">{safe_text}{"…" if len(text) == 600 else ""}</p></div>'

    # --- ODD audit trail ---
    modules_run = []
    if stress.get("scenarios"):
        modules_run.append(("Stress Test Engine", "Monte Carlo scenario analysis across vacancy, rent, and exit cap rate variables"))
    if validation:
        modules_run.append(("Assumption Validator", "Independent verification of sponsor projections against market benchmarks"))
    if bias_rep:
        modules_run.append(("Bias Detector", "Systematic scan for optimism bias, recency bias, and cherry-picked comparables"))
    if premortem:
        modules_run.append(("Pre-Mortem Analysis", "Forward-looking failure mode analysis: how does this deal go wrong?"))
    if macro_b:
        modules_run.append(("Macro Context Engine", "Live FRED macro indicators: 10yr Treasury, CPI, unemployment, cap rate spread"))
    if advisors:
        adv_count = len(advisors) if isinstance(advisors, dict) else 0
        modules_run.append((f"{adv_count} Adversarial AI Advisors", "Independent parallel analyses from Bear Case, Tax, Market, Bias, and Exit perspectives"))
        modules_run.append(("Chairman Synthesis", "Cross-advisor reconciliation and final Go/No-Go determination"))

    odd_rows = "".join(
        f"<tr><td style='font-weight:600'>{m[0]}</td><td>{m[1]}</td><td style='color:#1a7f37;font-weight:600'>✓ Complete</td></tr>"
        for m in modules_run
    )
    odd_html = f"""
    <h2 class="sec-hdr" style="border-left:4px solid #b08800;">ODD Audit Trail — Methodology Documentation</h2>
    <p style="font-size:12px;color:#555;margin-bottom:12px;">
      The following analytical modules were executed on this offering memorandum. This section is designed to satisfy
      LP operational due diligence (ODD) inquiries regarding the GP's assumption-validation and risk-review process.
    </p>
    <table class="data-table">
      <thead><tr><th>Module</th><th>Description</th><th>Status</th></tr></thead>
      <tbody>{odd_rows}</tbody>
    </table>
    <p style="font-size:11px;color:#888;margin-top:10px;">
      Analysis ID: <code>{job_id}</code> &nbsp;|&nbsp;
      Model: Claude (Anthropic) &nbsp;|&nbsp;
      Generated: {gen_ts} &nbsp;|&nbsp;
      ClearEye AI Real Estate Intelligence Platform
    </p>"""

    memo_safe = memo[:4000].replace("<", "&lt;").replace(">", "&gt;") if memo else "Memo not available."
    bias_safe = bias_rep[:1200].replace("<", "&lt;").replace(">", "&gt;") if bias_rep else ""

    ic_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>IC Memo — {deal_name}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0;}}
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;600&family=DM+Sans:wght@400;500;600&display=swap');
body{{font-family:'DM Sans',sans-serif;background:#fff;color:#1a1a2e;padding:48px 56px;max-width:900px;margin:0 auto;font-size:13px;line-height:1.6;}}
.cover{{background:#080b10;color:#f0ede8;padding:48px 44px;border-radius:10px;margin-bottom:36px;position:relative;overflow:hidden;}}
.cover-label{{font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:.16em;text-transform:uppercase;color:#e8a020;margin-bottom:16px;}}
.cover-title{{font-family:'DM Serif Display',serif;font-style:italic;font-size:2.2rem;font-weight:400;margin-bottom:6px;letter-spacing:-.01em;}}
.cover-sub{{font-size:12px;color:rgba(240,237,232,.55);}}
.verdict-pill{{display:inline-block;background:{v_bg};color:{v_color};border:2px solid {v_color};
  font-family:'IBM Plex Mono',monospace;font-size:11px;font-weight:700;letter-spacing:.12em;
  padding:6px 16px;border-radius:4px;margin-top:20px;text-transform:uppercase;}}
.conf-tag{{font-family:'IBM Plex Mono',monospace;font-size:10px;color:rgba(240,237,232,.45);margin-top:8px;}}
.metric-row{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:28px 0;}}
.metric-box{{border:1px solid #e8e8ec;border-radius:7px;padding:14px 16px;text-align:center;}}
.metric-label{{font-family:'IBM Plex Mono',monospace;font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:#888;margin-bottom:4px;}}
.metric-value{{font-family:'IBM Plex Mono',monospace;font-size:1.15rem;font-weight:600;color:#1a1a2e;}}
.sec-hdr{{font-family:'DM Serif Display',serif;font-style:italic;font-size:1.15rem;font-weight:400;color:#1a1a2e;
  border-left:4px solid #e8a020;padding-left:12px;margin:32px 0 12px;}}
.memo-block{{background:#fafaf8;border:1px solid #e8e8ec;border-radius:6px;padding:20px 22px;
  font-size:12px;line-height:1.7;white-space:pre-wrap;color:#2d2d3a;}}
.data-table{{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:8px;}}
.data-table th{{background:#f4f4f6;font-family:'IBM Plex Mono',monospace;font-size:9px;letter-spacing:.07em;
  text-transform:uppercase;padding:7px 12px;text-align:left;border-bottom:2px solid #e0e0e8;}}
.data-table td{{padding:7px 12px;border-bottom:1px solid #efefef;vertical-align:top;}}
.data-table tr:last-child td{{border-bottom:none;}}
.adv-block{{border-left:3px solid #e8a020;padding:10px 14px;margin-bottom:10px;background:#fffdf5;border-radius:0 5px 5px 0;}}
.adv-name{{font-family:'IBM Plex Mono',monospace;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:#b08800;margin-bottom:4px;}}
.adv-text{{font-size:12px;color:#3a3a4a;line-height:1.6;}}
.footer{{margin-top:44px;padding-top:14px;border-top:1px solid #e8e8ec;font-family:'IBM Plex Mono',monospace;
  font-size:9px;color:#aaa;display:flex;justify-content:space-between;}}
.page-break{{page-break-before:always;}}
@media print{{
  body{{padding:24px 32px;}}
  .cover{{border-radius:4px;}}
  .page-break{{page-break-before:always;}}
}}
</style>
</head>
<body>

<div class="cover">
  <div class="cover-label">Investment Committee Memorandum &nbsp;|&nbsp; Confidential</div>
  <div class="cover-title">{deal_name}</div>
  <div class="cover-sub">{market}{(' &bull; ' + asset_type) if asset_type else ''}{(' &bull; Sponsor: ' + sponsor) if sponsor else ''}</div>
  <div><span class="verdict-pill">{verdict_text}</span></div>
  <div class="conf-tag">Confidence: {conf_pct}% &nbsp;|&nbsp; Generated {gen_ts}</div>
</div>

<div class="metric-row">
  <div class="metric-box"><div class="metric-label">Asking Price</div><div class="metric-value">{price_str}</div></div>
  <div class="metric-box"><div class="metric-label">Units</div><div class="metric-value">{units_str}</div></div>
  <div class="metric-box"><div class="metric-label">Cap Rate</div><div class="metric-value">{cap_str}</div></div>
  <div class="metric-box"><div class="metric-label">Confidence</div><div class="metric-value">{conf_pct}%</div></div>
</div>

<h2 class="sec-hdr">Investment Memo</h2>
<div class="memo-block">{memo_safe}</div>

{stress_table_html}

{('<h2 class="sec-hdr">Advisor Analyses</h2>' + adv_html) if adv_html else ''}

{('<h2 class="sec-hdr">Bias Detection Flags</h2><div class="memo-block">' + bias_safe + '</div>') if bias_safe else ''}

<div class="page-break"></div>
{odd_html}

<div class="footer">
  <span>ClearEye AI &mdash; Real Estate Investment Intelligence</span>
  <span>Analysis ID: {job_id} &mdash; For qualified investors only &mdash; Not a solicitation</span>
</div>

</body></html>"""

    safe_name = deal_name.replace(" ", "_")[:40]
    try:
        import weasyprint, io as _io
        pdf_bytes = weasyprint.HTML(string=ic_html).write_pdf()
        return send_file(
            _io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"{safe_name}_IC_Memo.pdf",
        )
    except Exception:
        pass

    from flask import Response as FlaskResponse
    return FlaskResponse(
        ic_html,
        mimetype="text/html",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}_IC_Memo.html"'},
    )


@app.route("/api/kill-sheet/<job_id>")
def api_kill_sheet(job_id):
    """
    #219: Kill Sheet — 1-page deal summary PDF.
    Top 3 kill shots, realistic cash flow range, capital call risk, Chairman verdict.
    Clean white background, no ClearEye branding visible, suitable for attorneys/CPAs.
    """
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    if job.get("status") != "done":
        return jsonify({"error": "Analysis not complete"}), 400

    deal   = job.get("deal") or {}
    memo   = job.get("memo") or ""
    bias   = job.get("bias_report") or ""
    stress = job.get("stress_test") or {}
    pre_m  = job.get("pre_mortem") or ""
    advisors = job.get("advisors") or []

    # Determine verdict
    mu = memo.upper()
    if "NO-GO" in mu:
        verdict, vcolor, vbg = "NO-GO", "#c0392b", "#fef0ef"
    elif re.search(r'\bGO\b', mu) and "CONDITIONAL" not in mu:
        verdict, vcolor, vbg = "GO", "#1a7f37", "#dafbe1"
    else:
        verdict, vcolor, vbg = "CONDITIONAL", "#9a6700", "#fff8c5"

    deal_name  = deal.get("deal_name") or "Deal Analysis"
    address    = deal.get("address") or deal.get("market") or ""
    price      = deal.get("asking_price") or 0
    price_fmt  = f"${price/1e6:.1f}M" if price >= 1e6 else (f"${price/1e3:.0f}K" if price else "—")
    units      = deal.get("units") or ""
    gen_ts     = datetime.utcnow().strftime("%B %d, %Y")

    # Extract top 3 kill shots from bias + pre-mortem
    kill_shots = []
    for src in [bias, pre_m, memo]:
        for line in (src or "").split("\n"):
            stripped = line.strip()
            if any(kw in stripped.upper() for kw in ["HIGH", "DETECTED", "KILL", "CRITICAL", "WARNING", "RISK", "[!]"]):
                clean = re.sub(r'^[\[\]!HIGH|MEDIUM|LOW|DETECTED|WARNING|CRITICAL:\-•→]+', '', stripped, flags=re.I).strip()
                if len(clean) > 20 and clean not in [k["text"] for k in kill_shots]:
                    kill_shots.append({"text": clean[:200], "source": "Bias / Pre-Mortem"})
            if len(kill_shots) >= 3:
                break
        if len(kill_shots) >= 3:
            break

    # Fill to 3 if fewer found
    while len(kill_shots) < 3:
        kill_shots.append({"text": "No additional critical flags detected.", "source": "Analysis"})

    # Cash flow range from stress test
    bull_irr  = stress.get("bull", {}).get("irr", 0) if isinstance(stress.get("bull"), dict) else 0
    base_irr  = stress.get("base", {}).get("irr", 0) if isinstance(stress.get("base"), dict) else 0
    bear_irr  = stress.get("bear", {}).get("irr", 0) if isinstance(stress.get("bear"), dict) else 0
    irr_range = f"{bear_irr:.1f}% – {bull_irr:.1f}% IRR (base: {base_irr:.1f}%)" if (bull_irr or base_irr) else "Stress test not available"

    # Capital call risk — extract from pre_mortem or memo
    cap_risk = "Not identified in analysis."
    for line in (pre_m + "\n" + memo).split("\n"):
        if any(kw in line.upper() for kw in ["CAPITAL CALL", "CASH CALL", "EQUITY", "RESERVE", "LIQUIDITY"]):
            cap_risk = line.strip().replace("<", "&lt;").replace(">", "&gt;")[:180]
            break

    safe_name = re.sub(r'[^a-zA-Z0-9_\-]', '_', deal_name)[:40]

    ks_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Kill Sheet — {deal_name}</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
body{{font-family:'DM Sans',sans-serif;background:#fff;color:#1a1a2e;max-width:760px;margin:0 auto;padding:36px 40px;font-size:13px;-webkit-font-smoothing:antialiased;}}
@page{{margin:2cm;}}
.ks-header{{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:24px;padding-bottom:16px;border-bottom:2px solid #1a1a2e;}}
.ks-deal-name{{font-family:'DM Serif Display',serif;font-style:italic;font-size:1.8rem;letter-spacing:-0.02em;line-height:1.1;margin-bottom:4px;}}
.ks-deal-meta{{font-size:12px;color:#555;font-family:'IBM Plex Mono',monospace;}}
.ks-verdict{{font-family:'IBM Plex Mono',monospace;font-size:11px;font-weight:700;padding:5px 14px;border-radius:5px;letter-spacing:.04em;background:{vbg};color:{vcolor};border:1.5px solid {vcolor};}}
.ks-section-title{{font-family:'IBM Plex Mono',monospace;font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#888;margin-bottom:8px;margin-top:20px;}}
.ks-kill-item{{background:#fafafa;border:1px solid #e0e0e0;border-left:4px solid #e74c3c;border-radius:6px;padding:10px 13px;margin-bottom:8px;}}
.ks-kill-text{{font-size:12px;color:#2c2c2c;line-height:1.5;}}
.ks-irr-row{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;}}
.ks-irr-box{{background:#f8f8f6;border:1px solid #e0e0e0;border-radius:6px;padding:11px 14px;text-align:center;}}
.ks-irr-val{{font-family:'IBM Plex Mono',monospace;font-size:1.3rem;font-weight:600;margin-bottom:3px;}}
.ks-irr-label{{font-size:10px;color:#888;}}
.ks-risk-box{{background:#fafafa;border:1px solid #e0e0e0;border-radius:6px;padding:10px 13px;font-size:12px;color:#2c2c2c;line-height:1.5;}}
.ks-footer{{margin-top:28px;padding-top:12px;border-top:1px solid #e0e0e0;font-size:10px;color:#aaa;font-family:'IBM Plex Mono',monospace;display:flex;justify-content:space-between;}}
</style>
</head>
<body>
<div class="ks-header">
  <div style="flex:1;">
    <div class="ks-deal-name">{deal_name}</div>
    <div class="ks-deal-meta">{address}{(" · " + price_fmt) if price_fmt != "—" else ""}{(" · " + str(units) + "u") if units else ""}</div>
  </div>
  <div><div class="ks-verdict">{verdict}</div></div>
</div>

<div class="ks-section-title">Top 3 Kill Shots</div>
{"".join(f'<div class="ks-kill-item"><div class="ks-kill-text">&#9888; {ks["text"].replace(chr(60),"&lt;").replace(chr(62),"&gt;")}</div></div>' for ks in kill_shots[:3])}

<div class="ks-section-title">Realistic Cash Flow Range</div>
<div class="ks-irr-row">
  <div class="ks-irr-box"><div class="ks-irr-val" style="color:#1a7f37;">{bull_irr:.1f}%</div><div class="ks-irr-label">Bull Case IRR</div></div>
  <div class="ks-irr-box"><div class="ks-irr-val" style="color:#9a6700;">{base_irr:.1f}%</div><div class="ks-irr-label">Base Case IRR</div></div>
  <div class="ks-irr-box"><div class="ks-irr-val" style="color:#c0392b;">{bear_irr:.1f}%</div><div class="ks-irr-label">Bear Case IRR</div></div>
</div>
<div style="font-size:11px;color:#888;margin-top:6px;font-family:'IBM Plex Mono',monospace;">{irr_range}</div>

<div class="ks-section-title">Capital Call Risk</div>
<div class="ks-risk-box">{cap_risk}</div>

<div class="ks-footer">
  <span>Kill Sheet — {deal_name[:50]}</span>
  <span>Generated {gen_ts} · Analysis ID: {job_id[:12]}</span>
</div>
</body>
</html>"""

    try:
        import weasyprint
        pdf_bytes = weasyprint.HTML(string=ks_html).write_pdf()
        return send_file(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            download_name=f"{safe_name}_Kill_Sheet.pdf",
            as_attachment=True,
        )
    except Exception:
        return Response(
            ks_html,
            mimetype="text/html",
            headers={"Content-Disposition": f'attachment; filename="{safe_name}_Kill_Sheet.html"'},
        )


@app.route("/lp/<token>", methods=["GET", "POST"])
def lp_report(token):
    """
    LP sharing portal — branded, access-controlled deal report (#136).
    GET: show password gate if needed; POST: verify password.
    """
    link = shared_link_get(token)
    if not link:
        return render_template_string("""<!DOCTYPE html><html>
<head><title>Link Not Found</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{background:#0d1117;color:#e6edf3;font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;}</style>
</head><body><div style="text-align:center;"><div style="font-size:2rem;">&#128274;</div>
<h2>Link not found or expired</h2><a href="/" style="color:#58a6ff;">ClearEye &rarr;</a></div></body></html>"""), 404

    # Check expiry
    if link.get("expires_at"):
        from datetime import timezone
        expiry = datetime.strptime(link["expires_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > expiry:
            return render_template_string("""<!DOCTYPE html><html>
<head><title>Link Expired</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{background:#0d1117;color:#e6edf3;font-family:sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;}</style>
</head><body><div style="text-align:center;"><div style="font-size:2rem;">&#128274;</div>
<h2>This link has expired</h2><p style="color:#8b949e;">Contact the sender for a new link.</p>
<a href="/" style="color:#58a6ff;">ClearEye &rarr;</a></div></body></html>"""), 410

    # Password gate
    if link.get("password"):
        pw_error = ""
        if request.method == "POST":
            submitted = request.form.get("password", "")
            if submitted == link["password"]:
                # Correct — set session flag and serve report
                session[f"lp_auth_{token}"] = True
            else:
                pw_error = "Incorrect password. Please try again."
        if not session.get(f"lp_auth_{token}"):
            label = link.get("label") or "Deal Report"
            return render_template_string(f"""<!DOCTYPE html><html>
<head><title>ClearEye — {label}</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;display:flex;align-items:center;justify-content:center;min-height:100vh;}}</style>
</head><body>
<div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:32px 40px;max-width:380px;width:100%;text-align:center;">
  <div style="font-size:1.4rem;font-weight:800;color:#58a6ff;margin-bottom:6px;">&#128065; ClearEye</div>
  <div style="font-size:14px;font-weight:600;margin-bottom:4px;">{label}</div>
  <div style="font-size:12px;color:#8b949e;margin-bottom:20px;">This report is password protected.</div>
  {'<div style="background:rgba(248,81,73,.08);border:1px solid #f85149;border-radius:5px;padding:6px 10px;font-size:12px;color:#f85149;margin-bottom:12px;">' + pw_error + '</div>' if pw_error else ''}
  <form method="POST">
    <input name="password" type="password" placeholder="Enter password" autofocus
      style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:6px;padding:9px 12px;font-size:13px;margin-bottom:12px;">
    <button type="submit" style="width:100%;padding:9px;background:#1f6feb;border:none;color:#fff;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">View Report &rarr;</button>
  </form>
</div>
</body></html>""")

    # Record view
    shared_link_record_view(token)

    # Serve the full report with LP branding
    job_id = link["job_id"]
    job = JOBS.get(job_id)
    if not job or job.get("status") != "done":
        db_job = job_get(job_id)
        if db_job and db_job.get("result"):
            job = {**db_job["result"], "status": "done"}
    if not job or job.get("status") != "done":
        return "<h2 style='font-family:sans-serif;color:#888;padding:40px;'>Report not ready.</h2>", 404

    label = link.get("label") or job.get("deal", {}).get("deal_name") or "Deal Analysis"
    view_count = (link.get("view_count") or 0) + 1

    # Record view event (#144)
    try:
        lp_event_record(
            token=token, job_id=job_id, event_type="view",
            lp_ua=request.headers.get("User-Agent", "")[:200],
            lp_ip=request.remote_addr,
        )
    except Exception:
        pass

    return render_template_string(HTML,
        _prefill_job=json.dumps({
            "status": "done",
            "deal": job.get("deal", {}),
            "memo": job.get("memo", ""),
            "advisors": job.get("advisors", {}),
            "stress_table": job.get("stress_table", ""),
            "validation_report": job.get("validation_report", ""),
            "bias_report": job.get("bias_report", ""),
            "premortem_report": job.get("premortem_report", ""),
            "macro_brief": job.get("macro", {}).get("brief", ""),
        }),
        _report_mode=True,
        _job_id=job_id,
        _lp_label=label,
        _lp_view_count=view_count,
        _lp_token=token,   # passed to JS for section tracking (#144)
    )


# ---------------------------------------------------------------------------
# Bulk CSV Upload (#194)
# ---------------------------------------------------------------------------

@app.route("/bulk")
def bulk_page():
    """Bulk CSV deal upload page (#194)."""
    user_email = session.get("user_email") or ""
    nav_email  = user_email
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bulk Upload — ClearEye</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;margin:0;}
.bl-nav{height:52px;background:#0d1117;border-bottom:1px solid #21262d;display:flex;align-items:center;padding:0 20px;gap:14px;position:sticky;top:0;z-index:100;}
.bl-brand{font-size:1.1rem;font-weight:800;color:#58a6ff;text-decoration:none;}
.bl-main{max-width:900px;margin:28px auto;padding:0 16px;}
.bl-card{background:#161b22;border:1px solid #21262d;border-radius:12px;padding:20px 24px;margin-bottom:16px;}
textarea{background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:7px;padding:10px 12px;font-size:12px;font-family:monospace;resize:vertical;outline:none;width:100%;box-sizing:border-box;}
textarea:focus{border-color:#58a6ff;}
table{width:100%;border-collapse:collapse;}th,td{padding:7px 10px;border-bottom:1px solid #21262d;font-size:12px;}
th{font-size:11px;color:#8b949e;text-align:left;text-transform:uppercase;letter-spacing:.04em;}
.status-badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:10px;font-weight:700;}
.st-queued{color:#8b949e;background:rgba(139,148,158,.1);border:1px solid rgba(139,148,158,.3);}
.st-running{color:#58a6ff;background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.3);}
.st-done{color:#3fb950;background:rgba(63,185,80,.1);border:1px solid rgba(63,185,80,.3);}
.st-error{color:#f85149;background:rgba(248,81,73,.1);border:1px solid rgba(248,81,73,.3);}
</style>
</head>
<body>
<nav class="bl-nav">
  <a href="/app" class="bl-brand">&#128065; ClearEye</a>
  <span style="color:#484f58;">|</span>
  <span style="color:#8b949e;font-size:13px;">Bulk Upload</span>
  <a href="/app" style="margin-left:auto;font-size:12px;color:#8b949e;text-decoration:none;">&#8592; Back to App</a>
</nav>
<div class="bl-main">
  <div class="bl-card">
    <h1 style="font-size:1.05rem;font-weight:800;margin:0 0 6px;">Bulk Deal Analysis</h1>
    <p style="color:#8b949e;font-size:12px;margin-bottom:14px;">Paste CSV data below (max 10 deals). Required columns: <code style="color:#58a6ff;">deal_name</code> and <code style="color:#58a6ff;">om_text</code> — or use address/price/cap_rate/units/asset_class columns to auto-generate OM text.</p>
    <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center;">
      <button onclick="loadExample()" style="padding:5px 12px;background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.3);color:#58a6ff;border-radius:5px;font-size:11px;cursor:pointer;">Load Example CSV</button>
      <span style="font-size:11px;color:#8b949e;">or paste your own below</span>
    </div>
    <textarea id="csv-input" rows="8" placeholder="deal_name,om_text&#10;Oak Apartments,2-unit multifamily in Portland OR...&#10;Maple Flats,4-plex in Austin TX..."></textarea>
    <div style="display:flex;gap:8px;margin-top:10px;align-items:center;">
      <button onclick="parseCSV()" style="padding:9px 22px;background:linear-gradient(135deg,#1f6feb,#388bfd);color:#fff;border:none;border-radius:8px;font-weight:600;font-size:13px;cursor:pointer;">&#128269; Preview &amp; Queue</button>
      <span id="parse-status" style="font-size:12px;color:#8b949e;"></span>
    </div>
  </div>

  <div id="preview-card" class="bl-card" style="display:none;">
    <h2 style="font-size:13px;font-weight:700;margin:0 0 12px;">Preview</h2>
    <table><thead><tr><th>#</th><th>Deal Name</th><th>OM Text Preview</th><th>Status</th><th>Report</th></tr></thead>
    <tbody id="preview-tbody"></tbody></table>
    <button id="submit-btn" onclick="submitBulk()" style="margin-top:14px;padding:9px 22px;background:#238636;color:#fff;border:none;border-radius:8px;font-weight:600;font-size:13px;cursor:pointer;">&#9654; Analyze All</button>
    <span id="submit-status" style="font-size:12px;color:#8b949e;margin-left:10px;"></span>
  </div>
</div>
<script>
const MAX_DEALS=10;
let _rows=[];
let _pollIntervals={};

function loadExample(){
  document.getElementById('csv-input').value=
    'deal_name,om_text\\n'+
    'Oak Apartments Portland,"2-unit multifamily property in Portland OR. Asking price $480,000. Cap rate 5.8%. 2 units. Built 1998. Current rents below market."\\n'+
    'Maple Flats Austin,"4-plex in Austin TX. Asking $920,000. Cap rate 5.1%. 4 units all 2BD/1BA. Value-add opportunity with renovated comps nearby."';
}

function parseCSV(){
  const raw=document.getElementById('csv-input').value.trim();
  const st=document.getElementById('parse-status');
  if(!raw){st.textContent='Paste CSV data first.';return;}
  const lines=raw.split('\\n').filter(l=>l.trim());
  if(lines.length<2){st.textContent='Need at least a header row and one data row.';return;}
  const headers=parseCSVLine(lines[0]).map(h=>h.trim().toLowerCase());
  const iName=headers.indexOf('deal_name');
  const iOM=headers.indexOf('om_text');
  const iAddr=headers.indexOf('address');
  const iPrice=headers.indexOf('asking_price');
  const iCap=headers.indexOf('cap_rate');
  const iUnits=headers.indexOf('units');
  const iClass=headers.indexOf('asset_class');
  if(iName<0){st.textContent='Missing required column: deal_name';return;}
  if(iOM<0&&iAddr<0){st.textContent='Need either om_text or address column.';return;}
  _rows=[];
  const dataLines=lines.slice(1,MAX_DEALS+1);
  dataLines.forEach(function(line,i){
    if(!line.trim())return;
    const cols=parseCSVLine(line);
    const name=(cols[iName]||'Deal '+(i+1)).trim();
    let omText='';
    if(iOM>=0&&cols[iOM]&&cols[iOM].trim().length>10){
      omText=cols[iOM].trim();
    }else{
      const addr=iAddr>=0?cols[iAddr]||'':'';
      const price=iPrice>=0?cols[iPrice]||'':'';
      const cap=iCap>=0?cols[iCap]||'':'';
      const units=iUnits>=0?cols[iUnits]||'':'';
      const cls=iClass>=0?cols[iClass]||'multifamily':'multifamily';
      omText='Deal: '+name+'. '+cls+' property'+(addr?' at '+addr:'')+
        (price?' asking price $'+price:'')+
        (cap?' cap rate '+cap+'%':'')+
        (units?' with '+units+' units':'')+'. For investment analysis.';
    }
    if(omText.length<50)omText+=' Multifamily investment property for analysis.';
    _rows.push({name,omText,status:'preview',job_id:null});
  });
  if(!_rows.length){st.textContent='No valid rows found.';return;}
  if(_rows.length>MAX_DEALS){_rows=_rows.slice(0,MAX_DEALS);}
  st.textContent=_rows.length+' deal(s) ready to queue.';
  renderPreview();
  document.getElementById('preview-card').style.display='block';
}

function parseCSVLine(line){
  const result=[],re=/("(?:[^"]|"")*"|[^,]*)/g;
  let m;
  while((m=re.exec(line))!==null){
    if(m[0]===''&&re.lastIndex===line.length+1)break;
    let v=m[1];
    if(v.startsWith('"')&&v.endsWith('"'))v=v.slice(1,-1).replace(/""/g,'"');
    result.push(v);
  }
  return result;
}

function renderPreview(){
  const tbody=document.getElementById('preview-tbody');
  tbody.innerHTML=_rows.map(function(r,i){
    const stCls='st-'+r.status;
    const stLbl=r.status.charAt(0).toUpperCase()+r.status.slice(1);
    const preview=r.omText.slice(0,60)+(r.omText.length>60?'...':'');
    const reportLink=r.job_id?'<a href="/report/'+r.job_id+'" target="_blank" style="color:#58a6ff;font-size:11px;">View &#8599;</a>':'—';
    return '<tr id="bl-row-'+i+'"><td style="color:#8b949e;">'+(i+1)+'</td><td style="font-weight:600;">'+esc(r.name)+'</td><td style="color:#8b949e;font-family:monospace;font-size:11px;">'+esc(preview)+'</td><td><span class="status-badge '+stCls+'">'+stLbl+'</span></td><td>'+reportLink+'</td></tr>';
  }).join('');
}
function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}

async function submitBulk(){
  const btn=document.getElementById('submit-btn');
  btn.disabled=true;btn.textContent='Queuing...';
  document.getElementById('submit-status').textContent='';
  let queued=0;
  for(let i=0;i<_rows.length;i++){
    if(_rows[i].status!=='preview')continue;
    try{
      const r=await fetch('/api/bulk/analyze',{
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({deal_name:_rows[i].name,om_text:_rows[i].omText})
      });
      const d=await r.json();
      if(d.job_id){_rows[i].status='queued';_rows[i].job_id=d.job_id;queued++;}
      else{_rows[i].status='error';}
    }catch(e){_rows[i].status='error';}
    renderPreview();
  }
  btn.style.display='none';
  document.getElementById('submit-status').textContent=queued+' job(s) queued. Polling for results...';
  startPolling();
}

function startPolling(){
  _rows.forEach(function(row,i){
    if(!row.job_id||row.status==='done'||row.status==='error')return;
    const iv=setInterval(async function(){
      try{
        const r=await fetch('/status/'+row.job_id);
        const d=await r.json();
        if(d.status==='done'){_rows[i].status='done';clearInterval(iv);}
        else if(d.status==='error'){_rows[i].status='error';clearInterval(iv);}
        else{_rows[i].status='running';}
        renderPreview();
      }catch(e){}
    },5000);
    _pollIntervals[i]=iv;
  });
}
</script>
</body>
</html>"""


@app.route("/api/bulk/analyze", methods=["POST"])
def api_bulk_analyze():
    """Bulk analysis endpoint — one deal per POST call (#194)."""
    data      = request.get_json(force=True) or {}
    om_text   = (data.get("om_text") or "").strip()
    deal_name = (data.get("deal_name") or "").strip()
    if not om_text or len(om_text) < 50:
        return jsonify({"error": "om_text too short"}), 400
    user_email = session.get("user_email") or data.get("email") or None
    # Prepend deal_name to om_text so it appears in the analysis
    full_text  = (f"Deal name: {deal_name}\n\n" + om_text) if deal_name else om_text
    new_job_id = str(uuid.uuid4())[:8]
    JOBS[new_job_id] = {"status": "queued"}
    job_create(new_job_id, full_text, user_email)
    t = threading.Thread(target=_analyze, args=(new_job_id, full_text, user_email), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": new_job_id, "status": "queued",
                    "status_url": request.host_url + "status/" + new_job_id,
                    "report_url": request.host_url + "report/" + new_job_id})


# ---------------------------------------------------------------------------
# Pipeline Kanban routes (#133)
# ---------------------------------------------------------------------------

@app.route("/pipeline")
def pipeline_page():
    return render_template_string(PIPELINE_HTML)


@app.route("/api/pipeline", methods=["GET"])
def api_pipeline_get():
    """Return all pipeline deals grouped by stage."""
    deals = pipeline_get_all()
    # Attach tags to each deal
    for d in deals:
        d["tags"] = deal_tags_for_deal(d["id"])
    by_stage = {s: [] for s in PIPELINE_STAGES}
    for d in deals:
        stage = d.get("stage", "Screening")
        if stage not in by_stage:
            stage = "Screening"
        by_stage[stage].append(d)
    return jsonify({"deals": deals, "by_stage": by_stage, "stages": PIPELINE_STAGES})


@app.route("/api/pipeline", methods=["POST"])
def api_pipeline_add():
    """Add a deal to the pipeline."""
    data = request.get_json(force=True)
    deal_id = str(uuid.uuid4())[:8]
    user_email = session.get("user_email")
    pipeline_add(
        deal_id=deal_id,
        deal_name=data.get("deal_name", "Unnamed Deal"),
        address=data.get("address", ""),
        market=data.get("market", ""),
        asking_price=data.get("asking_price"),
        units=data.get("units"),
        stage=data.get("stage", "Screening"),
        assigned_to=data.get("assigned_to"),
        notes=data.get("notes", ""),
        job_id=data.get("job_id"),
        user_email=user_email,
    )
    # Seed default DD checklist for new pipeline deal (#142)
    try:
        dd_item_seed_defaults(deal_id)
    except Exception:
        pass
    # #220: Stamp initial stage_entered_at on deal creation
    try:
        from db import _conn as _db_conn
        _db_conn().execute(
            "UPDATE pipeline_deals SET stage_entered_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), deal_id)
        )
        _db_conn().commit()
    except Exception:
        pass
    return jsonify({"ok": True, "id": deal_id})


@app.route("/api/pipeline/<deal_id>/move", methods=["POST"])
def api_pipeline_move(deal_id):
    data = request.get_json(force=True)
    new_stage = data.get("stage", "Screening")
    if new_stage not in PIPELINE_STAGES:
        return jsonify({"error": f"Invalid stage: {new_stage}"}), 400
    user_email = session.get("user_email")
    pipeline_move(deal_id, new_stage, user_email)
    # #220: Stamp stage_entered_at when stage changes
    try:
        from db import _conn as _db_conn
        _db_conn().execute(
            "UPDATE pipeline_deals SET stage_entered_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), deal_id)
        )
        _db_conn().commit()
    except Exception:
        pass
    return jsonify({"ok": True, "stage": new_stage})


@app.route("/api/pipeline/<deal_id>", methods=["PATCH"])
def api_pipeline_update(deal_id):
    data = request.get_json(force=True)
    pipeline_update(deal_id, **data)
    return jsonify({"ok": True})


@app.route("/api/pipeline/<deal_id>", methods=["DELETE"])
def api_pipeline_delete(deal_id):
    user_email = session.get("user_email")
    pipeline_delete(deal_id, user_email)
    return jsonify({"ok": True})


@app.route("/api/pipeline/<deal_id>/note", methods=["PUT"])
def api_pipeline_note(deal_id):
    """Update notes and color_tag for a pipeline deal (#193)."""
    data = request.get_json(force=True, silent=True) or {}
    notes = data.get("notes", "")
    tag   = data.get("tag", "none")
    allowed_tags = {"none", "red", "yellow", "green", "blue", "purple"}
    if tag not in allowed_tags:
        tag = "none"
    pipeline_update(deal_id, notes=notes, color_tag=tag)
    # Log to pipeline_activity
    user_email = session.get("user_email")
    try:
        import sqlite3 as _sq3, os as _onp
        _db_path = _onp.path.join(_onp.path.dirname(__file__), "outputs", "cleareye.db")
        _conn = _sq3.connect(_db_path)
        _now = datetime.utcnow().isoformat() + "Z"
        _conn.execute(
            "INSERT INTO pipeline_activity (deal_id, action, detail, created_at, user_email) VALUES (?,?,?,?,?)",
            (deal_id, "note", "Note updated", _now, user_email)
        )
        _conn.commit()
        _conn.close()
    except Exception:
        pass
    return jsonify({"ok": True, "notes": notes, "tag": tag})


@app.route("/api/pipeline/<deal_id>/activity")
def api_pipeline_activity(deal_id):
    return jsonify({"activity": pipeline_activity(deal_id)})


@app.route("/api/pipeline/<deal_id>/outcome", methods=["GET", "POST"])
def api_pipeline_outcome(deal_id):
    """#225: Deal outcome feedback loop — record/fetch actual vs projected."""
    if request.method == "GET":
        try:
            row = _db_conn().execute(
                "SELECT actual_irr, actual_equity_multiple, closed_date, notes, recorded_at FROM pipeline_deal_outcomes WHERE deal_id=?",
                (deal_id,)
            ).fetchone()
            if row:
                return jsonify({"ok": True, "outcome": {
                    "actual_irr": row[0], "actual_equity_multiple": row[1],
                    "closed_date": row[2], "notes": row[3], "recorded_at": row[4]
                }})
            return jsonify({"ok": True, "outcome": None})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e)})
    # POST — save outcome
    data = request.get_json(force=True) or {}
    actual_irr = data.get("actual_irr")
    actual_em = data.get("actual_equity_multiple")
    closed_date = data.get("closed_date", "")
    notes = data.get("notes", "")
    now = datetime.utcnow().isoformat()
    try:
        with db.get_con() as con:
            con.execute("""
                INSERT INTO pipeline_deal_outcomes (deal_id, actual_irr, actual_equity_multiple, closed_date, notes, recorded_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(deal_id) DO UPDATE SET
                    actual_irr=excluded.actual_irr,
                    actual_equity_multiple=excluded.actual_equity_multiple,
                    closed_date=excluded.closed_date,
                    notes=excluded.notes,
                    recorded_at=excluded.recorded_at
            """, (deal_id, actual_irr, actual_em, closed_date, notes, now))
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/pipeline/<deal_id>/timeline")
def api_pipeline_timeline(deal_id):
    """
    Enriched timeline for a pipeline deal (#189).
    Includes: status moves, notes saves, analysis reports linked to the deal.
    Returns events sorted chronologically (oldest first).
    """
    events = []

    # 1. DB activity log (moved, added, note_saved, etc.)
    for act in pipeline_activity(deal_id, limit=50):
        action = act.get("action", "")
        detail = act.get("detail", "")
        ts     = act.get("created_at", "")
        if action == "moved":
            icon = "&#8594;"
            ev_type = "status"
        elif action == "added":
            icon = "&#9733;"
            ev_type = "created"
        elif action in ("note_saved", "note"):
            icon = "&#128203;"
            ev_type = "note"
        else:
            icon = "&#9679;"
            ev_type = "event"
        events.append({"ts": ts, "type": ev_type, "icon": icon, "title": action.replace("_", " ").title(), "detail": detail, "link": None})

    # 2. Analysis reports linked to this deal (by job_id stored in pipeline_deals)
    try:
        from db import _conn as _db_conn
        row = _db_conn().execute("SELECT job_id, deal_name FROM pipeline_deals WHERE id=?", (deal_id,)).fetchone()
        if row:
            job_id   = row[0]
            deal_name = row[1] or ""
            if job_id:
                # Primary linked job
                events.append({
                    "ts": "", "type": "analysis", "icon": "&#128202;",
                    "title": "Analysis Report",
                    "detail": "ClearEye Council analysis completed",
                    "link": "/report/" + job_id,
                })
            # Also look for any analysis with matching deal_name
            if deal_name:
                hist_rows = _db_conn().execute(
                    "SELECT id, created_at, verdict FROM deals WHERE LOWER(deal_name)=LOWER(?) ORDER BY created_at ASC LIMIT 10",
                    (deal_name,)
                ).fetchall()
                for hr in hist_rows:
                    if hr[0] != job_id:
                        verdict = hr[2] or ""
                        events.append({
                            "ts": hr[1] or "", "type": "analysis", "icon": "&#128202;",
                            "title": "Re-analysis" + (" — " + verdict if verdict else ""),
                            "detail": "ClearEye analysis run",
                            "link": "/report/" + hr[0],
                        })
    except Exception:
        pass

    # Sort chronologically (empty ts goes to end)
    events.sort(key=lambda e: e["ts"] or "9999")
    return jsonify({"deal_id": deal_id, "events": events})


@app.route("/api/pipeline/export.csv")
def api_pipeline_export_csv():
    """
    Export all pipeline deals as CSV (#153).
    Returns text/csv with Content-Disposition: attachment.
    """
    import csv
    import io
    from datetime import date as _date

    user_email = session.get("user_email")
    deals = pipeline_get_all(user_email)

    _CSV_COLS = [
        "deal_name", "stage", "market", "units", "asking_price",
        "price_per_unit", "cap_rate", "projected_irr", "occupancy",
        "year_built", "sponsor", "property_type", "address", "created_at", "updated_at",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=_CSV_COLS, extrasaction="ignore", lineterminator="\r\n")
    writer.writeheader()
    for d in deals:
        # Flatten — deals store extras in a json blob; grab top-level fields
        row = {col: d.get(col, "") for col in _CSV_COLS}
        writer.writerow(row)

    csv_bytes = buf.getvalue().encode("utf-8")
    filename = f"cleareye_pipeline_{_date.today().isoformat()}.csv"
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/pipeline/digest", methods=["POST"])
def api_pipeline_digest():
    """
    Send a weekly pipeline digest email. (#174)
    Body: { "email": "recipient@example.com" }
    If email omitted, uses the session user's email or the ADMIN_TOKEN owner.
    Falls back to saving HTML locally if SMTP not configured.
    """
    from email_delivery import send_pipeline_digest

    data = request.get_json(force=True) or {}
    recipient = (
        data.get("email")
        or session.get("user_email")
        or _env.get("GMAIL_USER")
        or ""
    )
    if not recipient:
        return jsonify({"ok": False, "error": "No recipient email. Pass {\"email\":\"...\"} or sign in."}), 400

    user_email = session.get("user_email")
    deals = pipeline_get_all(user_email)
    sent = send_pipeline_digest(recipient, deals)
    return jsonify({"ok": True, "sent_via_smtp": sent, "recipient": recipient, "deal_count": len(deals)})


@app.route("/api/pipeline/from-analysis/<job_id>", methods=["POST"])
def api_pipeline_from_analysis(job_id):
    """Promote a completed analysis directly into the pipeline."""
    job = JOBS.get(job_id) or {}
    if not job and job_get(job_id):
        db = job_get(job_id)
        job = db.get("result", {}) if db else {}
    deal = job.get("deal") or {}
    data = request.get_json(force=True) or {}
    deal_id = str(uuid.uuid4())[:8]
    user_email = session.get("user_email")
    pipeline_add(
        deal_id=deal_id,
        deal_name=deal.get("deal_name", data.get("deal_name", "Analysis Deal")),
        address=deal.get("address", ""),
        market=deal.get("market", ""),
        asking_price=deal.get("asking_price"),
        units=deal.get("units"),
        stage=data.get("stage", "Screening"),
        job_id=job_id,
        user_email=user_email,
    )
    # Seed default DD checklist for new pipeline deal (#142)
    try:
        dd_item_seed_defaults(deal_id)
    except Exception:
        pass
    return jsonify({"ok": True, "id": deal_id})


# ---------------------------------------------------------------------------
# Due Diligence Checklist & Document Vault (#142)
# ---------------------------------------------------------------------------

@app.route("/api/pipeline/<deal_id>/dd", methods=["GET"])
def api_dd_list(deal_id: str):
    """Get DD checklist items + progress + documents for a pipeline deal."""
    items = dd_items_for_deal(deal_id)
    docs  = docs_for_deal(deal_id)
    prog  = dd_progress(deal_id)
    # Group by category for UI
    from collections import defaultdict as _dd
    by_cat = _dd(list)
    for item in items:
        by_cat[item["category"]].append(item)
    return jsonify({
        "items": items,
        "by_category": dict(by_cat),
        "progress": prog,
        "documents": docs,
    })


@app.route("/api/pipeline/<deal_id>/dd", methods=["POST"])
def api_dd_create(deal_id: str):
    """Add a custom checklist item."""
    data = request.get_json(force=True) or {}
    item_id = dd_item_create(
        deal_id   = deal_id,
        title     = (data.get("title") or "").strip(),
        category  = data.get("category", "general"),
        assignee  = data.get("assignee"),
        due_date  = data.get("due_date"),
    )
    return jsonify({"ok": True, "id": item_id})


@app.route("/api/pipeline/<deal_id>/dd/seed", methods=["POST"])
def api_dd_seed(deal_id: str):
    """Re-seed default DD checklist (only adds missing items)."""
    dd_item_seed_defaults(deal_id)
    return jsonify({"ok": True})


@app.route("/api/dd/<item_id>", methods=["PATCH"])
def api_dd_update(item_id: str):
    """Toggle complete, assign, set due date, add note."""
    data = request.get_json(force=True) or {}
    dd_item_update(item_id, **data)
    return jsonify({"ok": True})


@app.route("/api/dd/<item_id>", methods=["DELETE"])
def api_dd_delete_item(item_id: str):
    dd_item_delete(item_id)
    return jsonify({"ok": True})


@app.route("/api/pipeline/<deal_id>/docs", methods=["POST"])
def api_doc_upload(deal_id: str):
    """Upload a document to the deal vault (#142)."""
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400
    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400

    # Sanitize filename
    import re as _re
    safe_name = _re.sub(r"[^a-zA-Z0-9._\- ]", "_", f.filename)[:120]
    category  = request.form.get("category", "other")
    uploader  = session.get("user_email", "anonymous")

    # Store in vault/<deal_id>/
    deal_dir = DOC_VAULT_DIR / deal_id
    deal_dir.mkdir(parents=True, exist_ok=True)
    stored_path = deal_dir / safe_name
    f.save(str(stored_path))
    file_size = stored_path.stat().st_size

    # Try to extract text from PDF for future auto-extraction (#142)
    extracted_text = None
    if safe_name.lower().endswith(".pdf"):
        try:
            import pdfplumber
            with pdfplumber.open(str(stored_path)) as pdf:
                extracted_text = "\n".join(p.extract_text() or "" for p in pdf.pages[:5])[:3000]
        except Exception:
            pass

    doc_id = doc_create(deal_id, safe_name, category, file_size, str(stored_path),
                        uploaded_by=uploader, extracted_text=extracted_text)
    return jsonify({"ok": True, "id": doc_id, "filename": safe_name,
                    "size_kb": round(file_size / 1024, 1),
                    "extracted": bool(extracted_text)})


@app.route("/api/pipeline/<deal_id>/docs", methods=["GET"])
def api_doc_list(deal_id: str):
    """List all documents for a pipeline deal."""
    return jsonify({"documents": docs_for_deal(deal_id)})


@app.route("/api/docs/<doc_id>", methods=["DELETE"])
def api_doc_delete(doc_id: str):
    doc_delete(doc_id)
    return jsonify({"ok": True})


@app.route("/api/docs/<doc_id>/download")
def api_doc_download(doc_id: str):
    """Download a vault document."""
    from db import _conn as _db_conn
    row = _db_conn().execute(
        "SELECT stored_path, filename FROM deal_documents WHERE id=?", (doc_id,)
    ).fetchone()
    if not row:
        return jsonify({"error": "Not found"}), 404
    return send_file(row["stored_path"], as_attachment=True, download_name=row["filename"])


# ---------------------------------------------------------------------------
# Deal Tagging API (#169)
# ---------------------------------------------------------------------------

@app.route("/api/tags", methods=["GET"])
def api_tags_list():
    email = session.get("user_email")
    return jsonify({"tags": tag_list(email)})


@app.route("/api/tags", methods=["POST"])
def api_tags_create():
    data = request.get_json(force=True)
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name required"}), 400
    color = data.get("color", "#58a6ff")
    email = session.get("user_email")
    tag_id = tag_create(name, color, email)
    return jsonify({"ok": True, "tag": {"id": tag_id, "name": name, "color": color}})


@app.route("/api/tags/<int:tag_id>", methods=["DELETE"])
def api_tags_delete(tag_id: int):
    tag_delete(tag_id)
    return jsonify({"ok": True})


@app.route("/api/pipeline/<deal_id>/tags", methods=["GET"])
def api_deal_tags_get(deal_id: str):
    return jsonify({"tags": deal_tags_for_deal(deal_id)})


@app.route("/api/pipeline/<deal_id>/tags", methods=["POST"])
def api_deal_tag_add(deal_id: str):
    data = request.get_json(force=True)
    tag_id = data.get("tag_id")
    if not tag_id:
        return jsonify({"error": "tag_id required"}), 400
    deal_tag_add(deal_id, int(tag_id))
    return jsonify({"ok": True})


@app.route("/api/pipeline/<deal_id>/tags/<int:tag_id>", methods=["DELETE"])
def api_deal_tag_remove(deal_id: str, tag_id: int):
    deal_tag_remove(deal_id, tag_id)
    return jsonify({"ok": True})


@app.route("/outputs")
def list_outputs():
    out_dir = Path(__file__).parent / "outputs"
    files = sorted(out_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return jsonify([{"name": f.name, "size_kb": round(f.stat().st_size / 1024, 1)} for f in files[:20]])


# ---------------------------------------------------------------------------
# Admin dashboard (#150)
# ---------------------------------------------------------------------------

_ADMIN_TOKEN = _env.get("ADMIN_TOKEN", "cleareye-admin-dev")


def _admin_auth() -> bool:
    """Check ?token= or X-Admin-Token header."""
    tok = request.args.get("token") or request.headers.get("X-Admin-Token", "")
    return tok == _ADMIN_TOKEN


def _read_error_log(n: int = 20) -> list[dict]:
    """Read last n entries from error_log.jsonl."""
    try:
        lines = _ERROR_LOG.read_text(encoding="utf-8").splitlines()[-n:]
        return [json.loads(l) for l in reversed(lines) if l.strip()]
    except Exception:
        return []


def _analysis_volume_7d() -> list[dict]:
    """Return analysis count by day for last 7 days."""
    from db import _conn as _db_conn
    try:
        con = _db_conn()
        rows = con.execute("""
            SELECT substr(created_at,1,10) AS day, COUNT(*) AS cnt
            FROM deals
            WHERE created_at >= datetime('now','-7 days')
            GROUP BY day ORDER BY day
        """).fetchall()
        return [{"day": r["day"], "count": r["cnt"]} for r in rows]
    except Exception:
        return []


def _queue_depth() -> int:
    """Count jobs currently queued or running."""
    from db import _conn as _db_conn
    try:
        con = _db_conn()
        row = con.execute(
            "SELECT COUNT(*) AS cnt FROM deals WHERE status IN ('queued','running')"
        ).fetchone()
        return row["cnt"] if row else 0
    except Exception:
        return 0


@app.route("/admin")
def admin_dashboard():
    """Ops visibility dashboard (#150) — gated by ADMIN_TOKEN."""
    if not _admin_auth():
        return render_template_string("""<!DOCTYPE html><html>
<head><title>Admin — Auth Required</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>body{background:#0d1117;color:#e6edf3;display:flex;align-items:center;justify-content:center;min-height:100vh;font-family:sans-serif;}</style>
</head><body>
<div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:32px 40px;text-align:center;max-width:360px;">
  <div style="font-size:2rem;">&#128274;</div>
  <h2 style="margin:12px 0 6px;">Admin Access Required</h2>
  <p style="color:#8b949e;font-size:13px;">Pass <code>?token=YOUR_ADMIN_TOKEN</code> in the URL.</p>
  <a href="/" style="color:#58a6ff;font-size:13px;">Back to ClearEye &rarr;</a>
</div></body></html>"""), 401

    cb_statuses = _cb_all_statuses()
    error_log = _read_error_log(20)
    volume = _analysis_volume_7d()
    q_depth = _queue_depth()
    token = request.args.get("token", _ADMIN_TOKEN)

    # Build volume table rows
    vol_rows = "".join(
        f"<tr><td>{v['day']}</td><td style='text-align:right;'>{v['count']}</td></tr>"
        for v in volume
    ) or "<tr><td colspan='2' style='color:#8b949e;'>No data</td></tr>"

    # Build error log rows
    def _err_color(src):
        if "fail" in src or "error" in src:
            return "#f85149"
        if "sent" in src:
            return "#3fb950"
        return "#8b949e"

    def _build_err_row(e):
        src = e.get("source", "")
        col = _err_color(src)
        ts  = e.get("ts", "")[:19]
        msg = e.get("message", "")[:120]
        return (
            f"<tr>"
            f"<td style='color:#8b949e;font-size:10px;white-space:nowrap;'>{ts}</td>"
            f"<td style='color:{col};font-size:11px;'>{src}</td>"
            f"<td style='font-size:11px;max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;'>{msg}</td>"
            f"</tr>"
        )
    err_rows = "".join(_build_err_row(e) for e in error_log) or "<tr><td colspan='3' style='color:#8b949e;'>No errors logged</td></tr>"

    # Build CB cards
    _badge_colors = {"closed": "#3fb950", "open": "#f85149", "half_open": "#d29922"}

    def _cb_badge(state):
        col = _badge_colors.get(state, "#8b949e")
        return (
            f"<span style='background:{col};color:#fff;padding:2px 8px;"
            f"border-radius:20px;font-size:10px;font-weight:700;text-transform:uppercase;'>{state}</span>"
        )

    def _cb_card(cb):
        badge     = _cb_badge(cb["state"])
        open_info = f"<div style='font-size:10px;color:#f85149;'>Opened: {cb.get('opened_at') or ''}</div>" if cb["state"] != "closed" else ""
        return (
            f"<div style='background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:14px 16px;"
            f"display:flex;flex-direction:column;gap:6px;'>"
            f"<div style='display:flex;align-items:center;justify-content:space-between;'>"
            f"<span style='font-weight:600;font-size:13px;'>{cb['name']}</span>{badge}</div>"
            f"<div style='font-size:11px;color:#8b949e;'>Failures: {cb['failure_count']}/{cb['failure_threshold']} &bull; Short-circuits: {cb['short_circuits']}</div>"
            f"<div style='font-size:11px;color:#8b949e;'>Total calls: {cb['total_calls']} &bull; Failures: {cb['total_failures']}</div>"
            f"{open_info}"
            f"<button onclick=\"resetCb('{cb['name']}')\" style='margin-top:4px;padding:4px 10px;background:#1f6feb;border:none;color:#fff;border-radius:4px;font-size:11px;cursor:pointer;'>Reset</button>"
            f"</div>"
        )

    cb_cards = "".join(_cb_card(cb) for cb in cb_statuses)

    # ── Waitlist leads (#183) ──────────────────────────────────────────────
    import os as _os2
    _email_log_path = _os2.path.join(_os2.path.dirname(__file__), "outputs", "email_log.jsonl")
    _all_leads = []
    try:
        with open(_email_log_path, encoding="utf-8") as _lf:
            for _line in _lf:
                _line = _line.strip()
                if _line:
                    try:
                        _entry = json.loads(_line)
                        if _entry.get("type") == "early_access_lead":
                            _all_leads.append(_entry)
                    except Exception:
                        pass
    except FileNotFoundError:
        pass

    _lead_counts = {}
    for _l in _all_leads:
        _p = _l.get("plan", "unknown")
        _lead_counts[_p] = _lead_counts.get(_p, 0) + 1

    def _lead_pill(plan_name, count):
        _col = {"starter": "#3fb950", "pro": "#58a6ff", "enterprise": "#a371f7"}.get(plan_name.lower(), "#8b949e")
        return (
            "<span style='display:inline-flex;align-items:center;gap:4px;padding:4px 12px;"
            "background:rgba(255,255,255,.04);border:1px solid #30363d;border-radius:20px;"
            "font-size:12px;margin-right:6px;margin-bottom:6px;'>"
            "<span style='font-size:9px;color:" + _col + ";'>&#9679;</span> "
            + plan_name.title() + ": <strong style='color:#e6edf3;'>" + str(count) + "</strong></span>"
        )

    lead_pills_html = "".join(_lead_pill(p, c) for p, c in sorted(_lead_counts.items())) or \
        "<span style='color:#8b949e;font-size:13px;'>No leads yet</span>"

    def _lead_row(idx, lead):
        _ts = lead.get("ts", "")[:19].replace("T", " ")
        _email = lead.get("email", "")
        _plan = lead.get("plan", "unknown").title()
        return (
            "<tr>"
            "<td style='font-size:11px;color:#8b949e;white-space:nowrap;'>" + _ts + "</td>"
            "<td style='font-size:12px;'>" + _email + "</td>"
            "<td style='font-size:11px;color:#58a6ff;'>" + _plan + "</td>"
            "</tr>"
        )

    lead_rows_html = "".join(_lead_row(i, l) for i, l in enumerate(reversed(_all_leads))) or \
        "<tr><td colspan='3' style='color:#8b949e;font-size:13px;padding:16px 8px;'>No waitlist leads yet.</td></tr>"

    lead_badge = ""
    if _all_leads:
        lead_badge = (
            "<span style='display:inline-flex;align-items:center;justify-content:center;"
            "width:18px;height:18px;background:#f85149;border-radius:50%;font-size:10px;"
            "font-weight:700;color:#fff;margin-left:6px;'>" + str(len(_all_leads)) + "</span>"
        )

    return render_template_string(f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>ClearEye Admin</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
body{{background:#0d1117;color:#e6edf3;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;margin:0;}}
.adm-nav{{height:52px;background:#0d1117;border-bottom:1px solid #21262d;display:flex;align-items:center;padding:0 20px;gap:14px;}}
.adm-section{{background:#161b22;border:1px solid #21262d;border-radius:10px;padding:18px 20px;margin-bottom:18px;}}
.adm-heading{{font-size:13px;font-weight:600;color:#8b949e;text-transform:uppercase;letter-spacing:.05em;margin-bottom:12px;}}
table{{width:100%;border-collapse:collapse;}}
th,td{{padding:6px 8px;border-bottom:1px solid #21262d;}}
th{{font-size:11px;color:#8b949e;text-align:left;}}
</style>
</head>
<body>
<div class="adm-nav">
  <span style="font-size:1.1rem;font-weight:800;color:#58a6ff;">&#128065; ClearEye</span>
  <span style="color:#8b949e;font-size:13px;">Admin</span>
  <span style="margin-left:auto;font-size:11px;color:#8b949e;">Queue depth: <strong style="color:{'#f85149' if q_depth>0 else '#3fb950'};">{q_depth}</strong></span>
  <button id="digest-btn" onclick="sendTestDigest()" style="margin-left:12px;padding:5px 12px;background:rgba(63,185,80,.12);border:1px solid #3fb950;color:#3fb950;border-radius:5px;font-size:11px;font-weight:600;cursor:pointer;">&#128231; Send Test Digest</button>
  <a href="/?token={token}" style="font-size:12px;color:#58a6ff;margin-left:12px;">&larr; App</a>
</div>

<div style="max-width:1100px;margin:24px auto;padding:0 16px;">

  <!-- Circuit Breakers -->
  <div class="adm-section">
    <div class="adm-heading">&#9889; Circuit Breakers</div>
    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:10px;">
      {cb_cards}
    </div>
  </div>

  <!-- Analysis Volume -->
  <div class="adm-section">
    <div class="adm-heading">&#128202; Analysis Volume (Last 7 Days)</div>
    <table>
      <thead><tr><th>Date</th><th style="text-align:right;">Analyses</th></tr></thead>
      <tbody>{vol_rows}</tbody>
    </table>
  </div>

  <!-- Error Log -->
  <div class="adm-section">
    <div class="adm-heading">&#9888; Recent Errors (Last 20)</div>
    <div style="overflow-x:auto;">
      <table>
        <thead><tr><th>Timestamp</th><th>Source</th><th>Message</th></tr></thead>
        <tbody>{err_rows}</tbody>
      </table>
    </div>
  </div>

  <!-- Waitlist Leads (#183) -->
  <div class="adm-section">
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
      <div class="adm-heading" style="margin-bottom:0;">&#128100; Waitlist Leads{lead_badge}</div>
      <a href="/admin/leads.csv?token={token}" style="font-size:12px;color:#58a6ff;text-decoration:none;">&#11015; Export CSV</a>
    </div>
    <div style="margin-bottom:12px;">{lead_pills_html}</div>
    <div style="overflow-x:auto;">
      <table>
        <thead><tr><th>Submitted</th><th>Email</th><th>Plan</th></tr></thead>
        <tbody>{lead_rows_html}</tbody>
      </table>
    </div>
  </div>

</div>

<script>
async function resetCb(name){{
  if(!confirm('Reset circuit breaker for '+name+'?'))return;
  const r=await fetch('/api/circuit-breakers/'+name+'/reset',{{method:'POST'}});
  if(r.ok){{alert('Reset OK — reloading...');location.reload();}}
  else alert('Reset failed: '+r.status);
}}
async function sendTestDigest(){{
  if(!confirm('Send a deal digest to all active alert subscribers now?'))return;
  const btn=document.getElementById('digest-btn');
  btn.disabled=true;btn.textContent='Sending...';
  try{{
    const r=await fetch('/api/alerts/digest',{{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{force:true}})}});
    const d=await r.json();
    if(d.ok){{
      alert('Digest sent to '+d.alert_count+' alert(s). Check email.');
    }}else{{
      alert('No active alerts found or send failed: '+(d.message||''));
    }}
  }}catch(e){{alert('Error: '+e);}}
  finally{{btn.disabled=false;btn.textContent='Send Test Digest';}}
}}
</script>
</body>
</html>""")


@app.route("/admin/leads.csv")
def admin_leads_csv():
    """Export waitlist leads as CSV (#183)."""
    if not _admin_auth():
        return "Unauthorized", 401
    import os as _os3, csv, io
    _email_log_path = _os3.path.join(_os3.path.dirname(__file__), "outputs", "email_log.jsonl")
    leads = []
    try:
        with open(_email_log_path, encoding="utf-8") as _f:
            for _line in _f:
                _line = _line.strip()
                if _line:
                    try:
                        _e = json.loads(_line)
                        if _e.get("type") == "early_access_lead":
                            leads.append(_e)
                    except Exception:
                        pass
    except FileNotFoundError:
        pass
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["submitted_at", "email", "plan"])
    for lead in reversed(leads):
        writer.writerow([lead.get("ts", "")[:19], lead.get("email", ""), lead.get("plan", "")])
    csv_bytes = buf.getvalue().encode("utf-8")
    return Response(
        csv_bytes,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=cleareye_leads.csv"},
    )


# ---------------------------------------------------------------------------
# HTML UI  (issues #80-#99)
# ---------------------------------------------------------------------------

HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearEye — Real Estate Investment Intelligence</title>
<!-- Fonts — Cormorant Garamond + Plus Jakarta Sans + JetBrains Mono -->
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,500;0,600;0,700;1,300;1,400;1,500;1,600&family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<!-- Bootstrap 5.3 CDN (#80) -->
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<!-- Chart.js (#88) -->
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
/* ════════════════════════════════════════════════════════════════
   ClearEye Design System — v4 "Meridian" Light Theme
   Typography: Cormorant Garamond (display) · JetBrains Mono (data)
               Plus Jakarta Sans (UI body text)
   Palette: Warm ivory canvas · Forest green accent · Clean white
   ════════════════════════════════════════════════════════════════ */
@import url('https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,300;0,400;0,500;0,600;0,700;1,300;1,400;1,500;1,600&family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600&display=swap');

/* ── Design Tokens ── */
:root {
  /* Canvas — warm ivory, premium paper feel */
  --bg-canvas:   #F5F3EE;
  /* Surfaces */
  --bg-surface:  #FFFFFF;
  --bg-elevated: #F0EDE7;
  --bg-overlay:  #E8E5DF;
  /* Borders */
  --border-muted:    #EAE7E1;
  --border-default:  #DDD9D1;
  --border-emphasis: #C5BFB4;
  /* Text hierarchy */
  --text-primary:   #0D1926;
  --text-secondary: #4A5568;
  --text-muted:     #8D98A5;
  /* Accent — forest green, premium investment signal */
  --accent:        #155E44;
  --accent-glow:   rgba(21,94,68,.18);
  --accent-dim:    rgba(21,94,68,.07);
  /* Semantic */
  --green:      #15803D;
  --green-dim:  rgba(21,128,61,.08);
  --red:        #B91C1C;
  --red-dim:    rgba(185,28,28,.07);
  --amber:      #92400E;
  --amber-dim:  rgba(146,64,14,.08);
  --purple:     #6D28D9;
  --purple-dim: rgba(109,40,217,.07);
  /* Buttons — green primary */
  --btn-bg:    #155E44;
  --btn-hover: #0E4530;
  /* Shadows */
  --shadow-xs: 0 1px 2px rgba(0,0,0,.05);
  --shadow-sm: 0 1px 4px rgba(0,0,0,.06), 0 2px 8px rgba(0,0,0,.04);
  --shadow-md: 0 4px 16px rgba(0,0,0,.08), 0 2px 4px rgba(0,0,0,.04);
  --shadow-lg: 0 8px 32px rgba(0,0,0,.10), 0 4px 8px rgba(0,0,0,.05);
  --shadow-card: 0 0 0 1px rgba(0,0,0,.04), 0px 2px 4px rgba(0,0,0,.04), 0px 8px 20px rgba(0,0,0,.06);
  /* Shape */
  --r-sm: 6px;
  --r-md: 10px;
  --r-lg: 14px;
  /* Motion */
  --t: 140ms ease;
  /* Type stack v4 */
  --font-display: 'Cormorant Garamond', Georgia, serif;
  --font:         'Plus Jakarta Sans', -apple-system, sans-serif;
  --mono:         'JetBrains Mono', 'SF Mono', Consolas, monospace;
  /* Type scale */
  --text-xs:    11px;
  --text-sm:    12px;
  --text-base:  13.5px;
  --text-md:    15px;
  --text-lg:    17px;
  --text-xl:    20px;
  --text-2xl:   24px;
  --text-3xl:   30px;
  --text-4xl:   38px;
  /* Letter-spacing */
  --ls-tight:   -0.022em;
  --ls-tighter: -0.035em;
  --ls-body:    -0.006em;
  --ls-label:    0.07em;
}

/* ── Reset ── */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

/* ── Base ── */
body {
  background-color: var(--bg-canvas);
  background-image:
    radial-gradient(ellipse 80% 50% at 50% -10%, rgba(21,94,68,.05) 0%, transparent 60%);
  background-attachment: fixed;
  color: var(--text-primary);
  font-family: var(--font);
  font-size: var(--text-base);
  line-height: 1.55;
  letter-spacing: var(--ls-body);
  min-height: 100vh;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-rendering: optimizeLegibility;
}
/* Financial numbers — tabular figures prevent layout shift */
.stat-val, .plan-price, .metric-num, .mc-val, .price-num {
  font-family: var(--mono);
  font-variant-numeric: tabular-nums;
  font-feature-settings: "tnum";
  letter-spacing: -0.02em;
}
/* Display headings — tightest tracking */
.display-heading {
  font-weight: 900;
  letter-spacing: var(--ls-tighter);
  line-height: 1.04;
}
/* Section headings */
.section-heading {
  font-weight: 800;
  letter-spacing: var(--ls-tight);
  line-height: 1.15;
}
/* Card labels / eyebrows */
.label-text {
  font-size: var(--text-xs);
  font-weight: 600;
  letter-spacing: var(--ls-label);
  text-transform: uppercase;
  color: var(--text-muted);
}

/* ── Custom scrollbar ── */
::-webkit-scrollbar { width: 5px; height: 5px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border-default); border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: var(--border-emphasis); }

/* ── Noise grain overlay — hidden on light theme ── */
.noise-overlay { display: none; }

/* ── Navbar ── */
.ce-nav {
  height: 58px;
  background: rgba(245,243,238,.94);
  backdrop-filter: blur(18px);
  -webkit-backdrop-filter: blur(18px);
  border-bottom: 1px solid var(--border-muted);
  display: flex;
  align-items: center;
  padding: 0 20px;
  position: sticky;
  top: 0;
  z-index: 200;
  gap: 0;
}
.ce-brand {
  font-family: var(--font-display);
  font-weight: 600;
  font-size: 1.25rem;
  color: var(--accent);
  letter-spacing: -0.01em;
  display: flex;
  align-items: center;
  gap: 8px;
  text-decoration: none;
  flex-shrink: 0;
  transition: color var(--t);
}
.ce-brand:hover { color: var(--btn-hover); }
.ce-tagline { font-size: 11px; color: var(--text-muted); margin-left: 16px; letter-spacing: .2px; }
.ce-nav-right { margin-left: auto; display: flex; gap: 4px; align-items: center; }
/* Shared nav button style */
.nav-pill {
  font-size: 12.5px;
  color: var(--text-secondary);
  text-decoration: none;
  padding: 5px 10px;
  border-radius: var(--r-sm);
  border: none;
  background: none;
  cursor: pointer;
  transition: color var(--t), background var(--t);
  white-space: nowrap;
}
.nav-pill:hover { color: var(--text-primary); background: var(--bg-overlay); }
.nav-pill-outline {
  font-size: 12.5px;
  font-weight: 600;
  color: var(--accent);
  text-decoration: none;
  padding: 6px 14px;
  border-radius: var(--r-sm);
  border: 1px solid rgba(21,94,68,.3);
  background: rgba(21,94,68,.06);
  cursor: pointer;
  transition: all var(--t);
  white-space: nowrap;
}
.nav-pill-outline:hover { background: rgba(21,94,68,.12); border-color: var(--accent); }
/* ── Tools dropdown (#238) ── */
.tools-dropdown{position:relative;display:inline-flex;}
.tools-trigger{display:flex;align-items:center;gap:5px;background:rgba(0,0,0,.03);border:1px solid var(--border-default);border-radius:var(--r-sm);padding:4px 10px;font-size:11.5px;color:var(--text-secondary);cursor:pointer;transition:all var(--t);font-family:var(--font);}
.tools-trigger:hover,.tools-trigger.active{border-color:rgba(21,94,68,.3);color:var(--accent);background:rgba(21,94,68,.05);}
.tools-trigger-dot{width:5px;height:5px;border-radius:50%;background:var(--accent);display:none;position:absolute;top:4px;right:5px;}
.tools-trigger-dot.visible{display:block;}
.tools-caret{transition:transform var(--t);font-size:9px;opacity:.5;}
.tools-trigger.active .tools-caret{transform:rotate(180deg);}
.tools-menu{position:absolute;top:calc(100% + 6px);right:0;min-width:200px;background:var(--bg-surface);border:1px solid var(--border-default);border-radius:10px;padding:6px;z-index:500;opacity:0;pointer-events:none;transform:translateY(-6px);transition:opacity .15s ease,transform .15s ease;box-shadow:var(--shadow-md);}
.tools-menu.open{opacity:1;pointer-events:all;transform:translateY(0);}
.tools-menu-item{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:6px;cursor:pointer;transition:background var(--t);font-size:12.5px;color:var(--text-secondary);border:none;background:none;width:100%;text-align:left;font-family:var(--font);}
.tools-menu-item:hover{background:var(--bg-elevated);color:var(--text-primary);}
.tools-menu-item-icon{font-size:14px;width:20px;text-align:center;}
.tools-menu-item-meta{font-size:10px;color:var(--text-muted);margin-top:1px;}
.tools-menu-sep{height:1px;background:var(--border-default);margin:4px 2px;}

/* ── 3-panel layout ── */
.ce-layout {
  display: grid;
  grid-template-columns: 220px 380px 1fr;
  height: calc(100vh - 56px);
  overflow: hidden;
}

/* ── Sidebar ── */
.ce-sidebar {
  background: var(--bg-surface);
  border-right: 1px solid var(--border-muted);
  overflow-y: auto;
  padding: 12px 8px;
  display: flex;
  flex-direction: column;
  gap: 1px;
}
.ce-sb-link {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 7px 10px;
  border-radius: var(--r-sm);
  color: var(--text-secondary);
  font-size: 13px;
  text-decoration: none;
  cursor: pointer;
  border: none;
  background: none;
  width: 100%;
  text-align: left;
  transition: all var(--t);
  position: relative;
}
.ce-sb-link:hover { background: var(--bg-surface); color: var(--text-primary); }
.ce-sb-link.active {
  background: var(--bg-elevated);
  color: var(--text-primary);
  font-weight: 500;
}
.ce-sb-link.active::before {
  content: '';
  position: absolute;
  left: 0; top: 20%; bottom: 20%;
  width: 2.5px;
  background: var(--accent);
  border-radius: 0 2px 2px 0;
}
.ce-sb-section {
  font-size: 10px;
  text-transform: uppercase;
  letter-spacing: 1px;
  color: var(--text-muted);
  padding: 12px 10px 4px;
  font-weight: 600;
}
.hist-item {
  padding: 7px 10px;
  border-radius: var(--r-sm);
  cursor: pointer;
  font-size: 12px;
  color: var(--text-secondary);
  transition: all var(--t);
  display: flex;
  align-items: center;
  gap: 6px;
}
.hist-item:hover { background: var(--bg-surface); color: var(--text-primary); }
.hist-item.active { background: var(--bg-elevated); color: var(--text-primary); position: relative; }
.hist-item.active::before {
  content: '';
  position: absolute;
  left: 0; top: 20%; bottom: 20%;
  width: 2px;
  background: var(--accent);
  border-radius: 0 2px 2px 0;
}
.hist-dot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }

/* ── #260: Empty state components ── */
@keyframes drawStroke{from{stroke-dashoffset:300}to{stroke-dashoffset:0}}
.empty-state-wrap{padding:28px 16px 20px;text-align:center;}
.empty-state-wrap svg{display:block;margin:0 auto 14px;}
.empty-state-wrap svg path,.empty-state-wrap svg circle,.empty-state-wrap svg line,.empty-state-wrap svg rect,.empty-state-wrap svg polyline{
  animation:drawStroke .9s cubic-bezier(.22,1,.36,1) both;
  stroke-dasharray:300;stroke-dashoffset:300;
}
.empty-state-title{font-family:'Cormorant Garamond',Georgia,serif;font-style:italic;font-size:14px;font-weight:400;color:var(--text-primary);margin-bottom:6px;line-height:1.4;}
.empty-state-sub{font-size:11px;color:var(--text-muted);margin-bottom:12px;line-height:1.5;}
.empty-state-cta{display:inline-flex;align-items:center;gap:5px;padding:7px 14px;background:var(--accent);color:#fff;border-radius:6px;font-size:11px;font-weight:600;text-decoration:none;font-family:var(--font);}
.empty-state-cta:hover{background:var(--btn-hover);color:#fff;}
/* Pipeline-specific empty state */
.pipeline-empty{display:none;padding:48px 24px;text-align:center;}
.pipeline-empty svg path,.pipeline-empty svg rect,.pipeline-empty svg line{
  animation:drawStroke 1.1s cubic-bezier(.22,1,.36,1) .15s both;
  stroke-dasharray:300;stroke-dashoffset:300;
}
/* Scrollbar — subtle on light bg */
.ce-sidebar, .ce-results, .ce-input {
  scrollbar-width: thin;
  scrollbar-color: rgba(21,94,68,.2) transparent;
}
.ce-sidebar::-webkit-scrollbar, .ce-results::-webkit-scrollbar, .ce-input::-webkit-scrollbar { width: 4px; }
.ce-sidebar::-webkit-scrollbar-track, .ce-results::-webkit-scrollbar-track, .ce-input::-webkit-scrollbar-track { background: transparent; }
.ce-sidebar::-webkit-scrollbar-thumb, .ce-results::-webkit-scrollbar-thumb, .ce-input::-webkit-scrollbar-thumb {
  background: rgba(21,94,68,.2);
  border-radius: 4px;
}
.ce-sidebar::-webkit-scrollbar-thumb:hover, .ce-results::-webkit-scrollbar-thumb:hover { background: rgba(21,94,68,.4); }
/* Sliding tab indicator (#199) */
.ce-tabs { position: relative; }
#tab-indicator {
  position: absolute;
  bottom: 0; left: 0;
  height: 2px;
  background: var(--accent);
  border-radius: 2px 2px 0 0;
  transition: left .22s cubic-bezier(.4,0,.2,1), width .22s cubic-bezier(.4,0,.2,1);
  pointer-events: none;
  box-shadow: 0 0 6px rgba(21,94,68,.3);
}

/* ── Input panel ── */
.ce-input {
  border-right: 1px solid var(--border-muted);
  overflow-y: auto;
  padding: 16px 14px;
  background: var(--bg-elevated);
}
.ce-card {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--r-lg);
  box-shadow: var(--shadow-card);
  overflow: hidden;
}
.ce-card-hdr {
  background: var(--bg-elevated);
  border-bottom: 1px solid var(--border-default);
  padding: 12px 16px;
}
.ce-card-body { padding: 16px; }

/* ── Quick-scan deal-breaker badges (#253) ── */
#qs-badges { animation: fadeIn .2s ease; }
.qs-verdict {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 5px 10px; border-radius: 6px; font-size: 11px;
  font-weight: 700; font-family: var(--mono); letter-spacing: .04em;
  margin-bottom: 6px; border: 1px solid;
}
.qs-verdict-pass {
  background: rgba(21,128,61,.08); border-color: rgba(21,128,61,.25); color: var(--green);
}
.qs-verdict-fail {
  background: rgba(185,28,28,.07); border-color: rgba(185,28,28,.25); color: var(--red);
}
.qs-chip {
  display: inline-flex; align-items: center; gap: 5px;
  padding: 3px 9px; border-radius: 99px; font-size: 11px; font-weight: 500;
  background: rgba(185,28,28,.07); border: 1px solid rgba(185,28,28,.2);
  color: var(--red); margin: 3px 3px 0 0;
}
.qs-irr {
  font-size: 11px; color: var(--text-muted); font-family: var(--mono);
  margin-top: 5px; line-height: 1.5;
}
.qs-spinner {
  display: inline-block; width: 10px; height: 10px; border-radius: 50%;
  border: 2px solid var(--border-default); border-top-color: var(--accent);
  animation: spin .6s linear infinite; vertical-align: middle;
}
@keyframes fadeIn { from { opacity: 0; transform: translateY(-4px); } to { opacity: 1; transform: none; } }

textarea, input[type=email] {
  background: var(--bg-surface) !important;
  color: var(--text-primary) !important;
  border-color: var(--border-default) !important;
  font-size: 13px;
  font-family: var(--font);
  transition: border-color var(--t), box-shadow var(--t);
  border-radius: var(--r-sm) !important;
}
textarea:focus, input[type=email]:focus {
  border-color: var(--accent) !important;
  box-shadow: 0 0 0 3px rgba(21,94,68,.12) !important;
  outline: none !important;
}
/* #261: Global keyboard focus ring — visible only on keyboard nav, not mouse clicks */
:focus-visible {
  outline: 2px solid var(--accent) !important;
  outline-offset: 2px !important;
  border-radius: 4px !important;
}
textarea:focus-visible, input:focus-visible {
  outline: none !important; /* inputs have their own border-color focus style */
}
/* Primary CTA — clean green button */
.btn-primary {
  background: var(--btn-bg);
  border: 1px solid rgba(21,94,68,.2);
  color: #fff;
  font-weight: 600;
  letter-spacing: .2px;
  transition: all var(--t);
  box-shadow: 0 1px 3px rgba(21,94,68,.25), 0 3px 10px rgba(21,94,68,.12);
}
.btn-primary:hover {
  background: var(--btn-hover);
  transform: translateY(-1px);
  box-shadow: 0 2px 6px rgba(21,94,68,.3), 0 6px 20px rgba(21,94,68,.18);
  border-color: rgba(21,94,68,.25);
  color: #fff;
}
.btn-primary:active { transform: translateY(0); box-shadow: 0 1px 3px rgba(21,94,68,.2); }
/* ── Simplified action bar ── */
.action-bar {
  display: flex; gap: 8px; margin-top: 14px; flex-wrap: wrap; align-items: center;
}
.action-btn {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 8px 16px; border-radius: var(--r-md); font-size: 12px; font-weight: 600;
  font-family: var(--font); cursor: pointer; transition: all var(--t);
  border: 1px solid var(--border-default); background: var(--bg-surface);
  color: var(--text-secondary); letter-spacing: .01em;
}
.action-btn:hover { background: var(--bg-elevated); color: var(--text-primary); border-color: var(--border-emphasis); }
.action-btn-primary {
  background: var(--accent); color: #fff; border-color: transparent;
  box-shadow: 0 1px 4px rgba(21,94,68,.2);
}
.action-btn-primary:hover { background: var(--btn-hover); color: #fff; border-color: transparent; }

.btn-ghost {
  border: 1px solid var(--border-default);
  color: var(--text-secondary);
  background: none;
  font-size: 12px;
  border-radius: var(--r-sm);
  padding: 6px 12px;
  cursor: pointer;
  transition: all var(--t);
}
.btn-ghost:hover { background: var(--bg-elevated); color: var(--text-primary); border-color: var(--border-emphasis); }

/* ── Progress tracker — light theme ── */
.prog-track {
  display: flex; flex-direction: column; gap: 0;
  margin-top: 16px;
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: 12px; overflow: hidden;
  box-shadow: var(--shadow-xs);
}
.prog-step {
  display: flex; align-items: flex-start; gap: 12px;
  padding: 11px 14px;
  border-bottom: 1px solid var(--border-muted);
  transition: background .15s ease;
}
.prog-step:last-child { border-bottom: none; }
.prog-step.ps-active { background: rgba(21,94,68,.03); }
.prog-step.ps-done { opacity: .6; }
.ps-indicator {
  width: 22px; height: 22px; border-radius: 50%;
  display: flex; align-items: center; justify-content: center;
  font-size: 10px; font-weight: 700; flex-shrink: 0;
  margin-top: 1px; transition: all .2s ease;
}
.pi-idle { background: var(--bg-elevated); color: var(--text-muted); border: 1px solid var(--border-default); }
@keyframes pls { 0%,100%{box-shadow:0 0 0 3px rgba(21,94,68,.15),0 0 8px rgba(21,94,68,.25)} 50%{box-shadow:0 0 0 5px rgba(21,94,68,.22),0 0 16px rgba(21,94,68,.35)} }
.pi-run { background: var(--accent); color:white; animation:pls 1.5s ease-in-out infinite; }
.pi-done { background: rgba(21,128,61,.1); color:var(--green); border:1px solid rgba(21,128,61,.25); }
.ps-num { font-size: 10px; font-weight: 700; }
.ps-content { flex: 1; min-width: 0; }
.ps-label { font-size: 12.5px; font-weight: 600; color: var(--text-primary); letter-spacing: -.01em; }
.ps-label.idle { color: var(--text-secondary); font-weight: 500; }
.ps-sub { font-size: 10.5px; color: var(--text-muted); margin-top: 2px; line-height: 1.5; }
/* Advisor sub-step chips */
@keyframes stepIn { from{opacity:0;transform:translateX(-5px)} to{opacity:1;transform:translateX(0)} }
.adv-step-chip {
  display: inline-flex; align-items: center; gap: 3px;
  padding: 1px 6px; background: rgba(21,94,68,.06);
  border: 1px solid rgba(21,94,68,.18); border-radius: 4px;
  font-size: 9.5px; color: var(--accent);
  margin: 3px 3px 0 0; animation: stepIn .25s ease;
}
.adv-step-chip.done { background:rgba(21,128,61,.07); border-color:rgba(21,128,61,.2); color:var(--green); }

/* ── Results panel ── */
.ce-results { overflow-y: auto; padding: 18px 16px; background: var(--bg-canvas); }

/* ── Verdict banner ── */
/* ── Verdict: true focal hero ── */
.verdict-wrap {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: var(--r-lg);
  padding: 28px 28px 22px;
  margin-bottom: 16px;
  box-shadow: var(--shadow-card);
  animation: fsDown .4s cubic-bezier(.16,1,.3,1);
  text-align: center;
}
.verdict-stamp {
  font-family: var(--font-display);
  font-size: 2.8rem;
  font-weight: 500;
  font-style: italic;
  letter-spacing: -0.02em;
  padding: 8px 28px;
  border-radius: 8px;
  border: 2px solid currentColor;
  text-transform: none;
  white-space: nowrap;
  display: inline-block;
}
.vs-go   { color: var(--green);  border-color: var(--green); }
.vs-nogo { color: var(--red);    border-color: var(--red); }
.vs-cond { color: var(--amber);  border-color: var(--amber); }
/* Color-wash variants — stronger signal */
.verdict-wrap.vb-go {
  background: rgba(21,128,61,.04);
  border-top: 4px solid var(--green);
  box-shadow: 0 0 0 0 transparent, var(--shadow-card);
}
.verdict-wrap.vb-nogo {
  background: rgba(185,28,28,.04);
  border-top: 4px solid var(--red);
}
.verdict-wrap.vb-cond {
  background: rgba(146,64,14,.04);
  border-top: 4px solid var(--amber);
}
/* Deal name — compact sub-label above stamp */
#verdict-name {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: .08em;
  text-transform: uppercase;
  color: var(--text-muted);
  font-family: var(--mono);
  margin-bottom: 10px;
  line-height: 1.2;
}
/* Verdict reason line — the "why" shown immediately under stamp */
#verdict-reason-line {
  font-size: 13px; color: var(--text-secondary);
  max-width: 480px; margin: 10px auto 0; line-height: 1.55;
}
/* Confidence ring — larger */
.conf-ring-wrap { position:relative; width:80px; height:80px; flex-shrink:0; }
.conf-ring-wrap svg { transform:rotate(-90deg); }
.cr-track { fill:none; stroke:var(--border-default); stroke-width:6; }
.cr-fill  { fill:none; stroke:var(--accent); stroke-width:6; stroke-linecap:round; transition:stroke-dashoffset 1s cubic-bezier(.22,1,.36,1); }
.cr-label { position:absolute; top:50%; left:50%; transform:translate(-50%,-50%); font-size:13px; font-weight:700; letter-spacing:-0.02em; }
.metric-chip {
  background: rgba(255,255,255,.04);
  border: 1px solid rgba(255,255,255,.08);
  border-radius: 8px;
  padding: 6px 12px;
  font-size: 11px;
  white-space: nowrap;
  transition: border-color var(--t), background var(--t);
}
.metric-chip:hover { border-color: var(--border-emphasis); background: var(--bg-overlay); }
.mc-label { color: var(--text-muted); font-size: 9px; display:block; text-transform:uppercase; letter-spacing:.5px; font-weight:600; }
.mc-val   { color: var(--text-primary); font-weight:600; }

/* #269: Metrics scorecard grid */
#metric-chips { display: grid; grid-template-columns: repeat(auto-fill, minmax(100px, 1fr)); gap: 8px; }
.metric-chip-v2 {
  display: flex; flex-direction: column; gap: 3px;
  padding: 8px 10px; background: var(--bg-elevated);
  border: 1px solid var(--border-muted); border-radius: 8px;
  text-align: center;
}
.mc-label-v2 {
  font-size: 9px; color: var(--text-muted); text-transform: uppercase;
  letter-spacing: .06em; font-weight: 600; font-family: var(--mono);
}
.mc-val-v2 {
  font-family: var(--mono); font-size: 14px; font-weight: 700; letter-spacing: -.02em;
  display: flex; align-items: center; justify-content: center; gap: 2px;
}

/* ── Deal stat grid ── */
.stat-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:8px; }
.stat-box {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: 8px;
  padding: 9px 11px;
  transition: border-color var(--t), box-shadow var(--t), transform var(--t);
}
.stat-box:hover { border-color: var(--border-emphasis); box-shadow: var(--shadow-xs); transform: translateY(-1px); }
.stat-lbl { font-size: 9px; color: var(--text-muted); text-transform:uppercase; letter-spacing:.5px; font-weight:600; }
.stat-val { font-size: 15px; font-weight:700; color: var(--text-primary); margin-top:3px; }
/* Hero financial metrics — larger + gradient color */
.stat-box.stat-key { border-color: rgba(21,94,68,.2); background: linear-gradient(180deg,rgba(21,94,68,.04) 0%,transparent 100%); }
.stat-box.stat-key:hover { border-color: rgba(21,94,68,.35); box-shadow: 0 0 0 1px rgba(21,94,68,.1), var(--shadow-sm); transform: translateY(-1px); }
.stat-val.stat-hero { font-size: 20px; letter-spacing: -0.03em; }
.stat-val.stat-vpos  { background:linear-gradient(90deg,#3fb950,#56d364);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text; }
.stat-val.stat-vneg  { background:linear-gradient(90deg,#f85149,#ff7b72);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text; }
.stat-val.stat-vcau  { background:linear-gradient(90deg,#d29922,#e3b341);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text; }
.stat-val.stat-vacc  { background:linear-gradient(90deg,var(--accent),#f5c842);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text; }
/* Market Rent Context card (#171) */
.rc-stat{background:var(--bg-elevated);border:1px solid var(--border-muted);border-radius:8px;padding:10px 14px;min-width:100px;text-align:center;}
.rc-val{font-size:15px;font-weight:700;color:var(--text-primary);letter-spacing:-0.02em;margin-bottom:2px;}
.rc-lbl{font-size:9px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;}
/* Scenario Planner (#172) */
.sp-slider-row{display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:16px;}
.sp-slider-box{background:var(--bg-elevated);border:1px solid var(--border-muted);border-radius:var(--r-md);padding:12px 14px;}
.sp-slider-lbl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;}
.sp-slider-val{font-size:13px;font-weight:700;color:var(--accent);}
.sp-slider{width:100%;accent-color:var(--accent);cursor:pointer;}
.sp-cards{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px;}
.sp-card{border-radius:var(--r-md);padding:14px 16px;text-align:center;border:1px solid transparent;}
.sp-card.bear{background:rgba(248,81,73,.06);border-color:rgba(248,81,73,.2);}
.sp-card.base{background:rgba(232,160,32,.06);border-color:rgba(232,160,32,.2);}
.sp-card.bull{background:rgba(63,185,80,.06);border-color:rgba(63,185,80,.2);}
.sp-card-label{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px;}
.sp-card.bear .sp-card-label{color:var(--red);}
.sp-card.base .sp-card-label{color:var(--accent);}
.sp-card.bull .sp-card-label{color:var(--green);}
.sp-card-irr{font-size:24px;font-weight:800;letter-spacing:-0.03em;margin-bottom:2px;}
.sp-card.bear .sp-card-irr{color:var(--red);}
.sp-card.base .sp-card-irr{color:var(--accent);}
.sp-card.bull .sp-card-irr{color:var(--green);}
.sp-card-meta{font-size:10px;color:var(--text-muted);line-height:1.6;}
/* #224: Named scenario planner */
.nsp-table{width:100%;border-collapse:collapse;font-size:12px;margin-bottom:12px;}
.nsp-table th{font-size:10px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);padding:6px 8px;text-align:left;border-bottom:1px solid var(--border-default);}
.nsp-table td{padding:6px 8px;border-bottom:1px solid var(--border-muted);vertical-align:middle;}
.nsp-table tbody tr:last-child td{border-bottom:none;}
.nsp-input{background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-primary);border-radius:4px;padding:4px 6px;font-family:var(--mono);font-size:12px;width:64px;text-align:right;outline:none;transition:border-color .15s;}
.nsp-input:focus{border-color:var(--amber);}
.nsp-name-input{background:transparent;border:none;border-bottom:1px solid transparent;color:var(--text-primary);font-family:var(--font);font-size:12px;font-weight:600;width:80px;outline:none;padding:2px 0;}
.nsp-name-input:focus{border-bottom-color:var(--amber);}
.nsp-irr{font-family:var(--mono);font-size:1.1rem;font-weight:700;}
.nsp-delta{font-family:var(--mono);font-size:11px;}
.nsp-del{background:none;border:none;color:rgba(248,81,73,.4);font-size:13px;cursor:pointer;padding:0 2px;}
.nsp-del:hover{color:#f85149;}
/* Summary panel sections */
.risk-card{padding:8px 12px;background:rgba(248,81,73,.04);border-left:2px solid #f85149;border-radius:0 6px 6px 0;margin-bottom:5px;font-size:12px;color:var(--text-secondary);line-height:1.55;}
.flag-item{padding:8px 12px;background:rgba(210,153,34,.04);border-left:2px solid #d29922;border-radius:0 6px 6px 0;margin-bottom:5px;font-size:12px;color:var(--text-secondary);line-height:1.55;}
.ddq-item{font-size:12px;color:var(--text-secondary);padding:5px 0;border-bottom:1px solid var(--border-muted);line-height:1.5;}
.ddq-item:last-child{border-bottom:none;}
.sum-section-label{font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;font-weight:600;}
.sum-empty{font-size:12px;color:var(--text-muted);}
.sum-ok{font-size:12px;color:var(--green);}
/* Stagger variants */
.s4{animation:fadeIn .35s .38s both;}
.s5{animation:fadeIn .35s .48s both;}

/* ── Summary chips ── */
.sum-bar { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:12px; }
.sum-chip {
  padding: 4px 12px;
  border-radius: 20px;
  font-size: 11px;
  font-weight: 600;
  cursor: pointer;
  border: 1px solid transparent;
  transition: all var(--t);
}
.sum-chip:hover { opacity:.85; transform:translateY(-1px); }
.sc-red   { background:var(--red-dim);   color:var(--red);   border-color:rgba(248,81,73,.3); }
.sc-amber { background:var(--amber-dim); color:var(--amber); border-color:rgba(210,153,34,.3); }
.sc-green { background:var(--green-dim); color:var(--green); border-color:rgba(63,185,80,.3); }

/* ── Tabs ── */
.ce-tabs { display:flex; gap:2px; border-bottom:1px solid var(--border-default); flex-wrap:wrap; padding:10px 14px 0; }
.ce-tab {
  padding: 7px 12px;
  font-size: 12px;
  font-weight: 500;
  color: var(--text-secondary);
  border: none;
  background: none;
  cursor: pointer;
  border-radius: var(--r-sm) var(--r-sm) 0 0;
  display: flex; align-items: center; gap:5px;
  white-space: nowrap;
  transition: all var(--t);
}
.ce-tab:hover { color: var(--text-primary); background: var(--bg-elevated); }
.ce-tab.active { color: var(--text-primary); background: var(--bg-elevated); border-bottom: 2px solid var(--accent); font-weight: 600; }
.tab-bdg { background: var(--red); color:white; font-size:9px; font-weight:700; padding:1px 5px; border-radius:10px; }
.tab-content-wrap { padding: 14px; }
/* Diligence / Data tab section headers */
.dd-section-hdr {
  font-size: 10px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase;
  color: var(--text-muted); font-family: var(--mono); margin: 0 0 12px;
  padding-bottom: 8px; border-bottom: 1px solid var(--border-muted);
}

/* ── Advisor cards — Supabase/Raycast style ── */
.adv-card {
  border-radius: var(--r-lg);
  padding: 0;           /* header + body split */
  margin-bottom: 12px;
  border: 1px solid var(--border-default);
  overflow: hidden;
  transition: border-color var(--t), box-shadow var(--t), transform var(--t);
  position: relative;
}
.adv-card:hover {
  border-color: var(--border-emphasis);
  box-shadow: var(--shadow-md);
  transform: translateY(-1px);
}
/* Hairline top accent — gradient that fades to transparent at edges */
.adv-card::before {
  content: '';
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 1px;
  background: linear-gradient(90deg, transparent 0%, var(--adv-color, var(--accent)) 30%, var(--adv-color, var(--accent)) 70%, transparent 100%);
  opacity: .7;
}
.adv-hdr {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  background: var(--adv-bg, var(--bg-elevated));
  border-bottom: 1px solid var(--border-muted);
}
.adv-body { padding: 14px 16px; }
/* Color variants via CSS custom property */
.adv-bear   { --adv-color: var(--red);    --adv-bg: rgba(185,28,28,.05);    }
.adv-tax    { --adv-color: var(--green);  --adv-bg: rgba(21,128,61,.05);    }
.adv-market { --adv-color: var(--accent); --adv-bg: rgba(21,94,68,.05);     }
.adv-bias   { --adv-color: var(--purple); --adv-bg: rgba(109,40,217,.05);   }
.adv-exit   { --adv-color: var(--amber);  --adv-bg: rgba(146,64,14,.05);    }
/* Icon badge */
.adv-icon {
  width: 30px; height: 30px;
  border-radius: 8px;
  background: var(--adv-bg, var(--bg-elevated));
  border: 1px solid var(--border-default);
  display: flex; align-items: center; justify-content: center;
  font-size: 15px;
  flex-shrink: 0;
  box-shadow: 0 0 0 1px var(--adv-color, var(--accent));
}
.adv-name {
  font-size: var(--text-xs);
  text-transform: uppercase;
  letter-spacing: var(--ls-label);
  color: var(--adv-color, var(--text-secondary));
  font-weight: 700;
  flex: 1;
}
.adv-verdict-chip {
  font-size: 9px;
  font-weight: 800;
  letter-spacing: .5px;
  text-transform: uppercase;
  padding: 2px 7px;
  border-radius: 4px;
  border: 1px solid currentColor;
  opacity: .8;
}
.adv-text { font-size: 13px; color: var(--text-secondary); white-space: pre-wrap; line-height: 1.62; }
/* #267: Key finding + collapsed body */
.adv-key-finding {
  font-size: 12px; color: var(--text-secondary); line-height: 1.5;
  margin-top: 4px; font-style: normal; opacity: .85;
  overflow: hidden; text-overflow: ellipsis; display: -webkit-box;
  -webkit-line-clamp: 2; -webkit-box-orient: vertical;
}
.adv-body-collapsed { max-height: 0; overflow: hidden; padding: 0 16px !important; }
.adv-body:not(.adv-body-collapsed) { padding: 12px 16px 14px; }
.adv-more { font-size: 11px; color: var(--accent); cursor: pointer; padding: 6px 16px 10px; transition: color var(--t); display: block; }
.adv-more:hover { color: var(--btn-hover); }

/* ── Audit flags ── */
.flag-card {
  background: var(--bg-surface);
  border: 1px solid var(--border-default);
  border-radius: 8px;
  padding: 10px 14px;
  margin-bottom: 8px;
  transition: border-color var(--t);
}
.flag-card:hover { border-color: var(--border-emphasis); }
.fc-red  { border-left: 3px solid var(--red);   }
.fc-warn { border-left: 3px solid var(--amber);  }
.fc-ok   { border-left: 3px solid var(--green);  }
.fb { font-size:10px; font-weight:700; padding:2px 7px; border-radius:4px; margin-right:6px; }
.fb-red  { background:rgba(185,28,28,.1);  color:var(--red);   }
.fb-warn { background:rgba(146,64,14,.1);  color:var(--amber); }
.fb-ok   { background:rgba(21,128,61,.1);  color:var(--green); }

/* ── Chart ── */
.chart-wrap { position:relative; height:260px; margin-bottom:12px; }

/* ── Empty state ── */
.empty-state {
  text-align: center;
  padding: 56px 20px;
  color: var(--text-muted);
}
.em-grid {
  display: grid;
  grid-template-columns: repeat(4,1fr);
  gap: 10px;
  max-width: 500px;
  margin: 24px auto;
}
.em-box {
  background: var(--bg-surface);
  border: 1px solid rgba(255,255,255,.06);
  border-radius: var(--r-md);
  padding: 13px 8px;
  font-size: 11px;
  color: var(--text-secondary);
  transition: all .2s cubic-bezier(.22,1,.36,1);
  cursor: default;
  box-shadow: inset 0 1px 0 rgba(255,255,255,.03);
}
.em-box:hover {
  border-color: rgba(232,160,32,.3);
  background: var(--bg-elevated);
  color: var(--text-primary);
  transform: translateY(-3px);
  box-shadow: 0 0 0 1px rgba(232,160,32,.12), 0 6px 20px rgba(0,0,0,.4), inset 0 1px 0 rgba(255,255,255,.05);
}
.em-icon { font-size: 20px; margin-bottom: 5px; }
@keyframes float { 0%,100%{transform:translateY(0) scale(1);} 50%{transform:translateY(-6px) scale(1.03);} }
.em-hero-icon { animation: float 4s ease-in-out infinite; display:inline-block; }
/* Improved metric chip — larger, more presence */
.metric-chip {
  background: var(--bg-elevated);
  border: 1px solid rgba(255,255,255,.1);
  border-radius: 10px;
  padding: 7px 14px;
  min-width: 80px;
  text-align: center;
  transition: border-color var(--t), transform var(--t);
  box-shadow: inset 0 1px 0 rgba(255,255,255,.06);
}
.metric-chip:hover { border-color: rgba(255,255,255,.2); transform: translateY(-1px); }
.mc-label { color: var(--text-muted); font-size: 9px; display:block; text-transform:uppercase; letter-spacing:.6px; font-weight:700; margin-bottom:2px; }
.mc-val   { color: var(--text-primary); font-weight:700; font-size:13px; letter-spacing:-0.02em; }

/* ── Mono / memo ── */
pre.mono {
  background: var(--bg-canvas);
  border: 1px solid var(--border-default);
  padding: 14px;
  border-radius: 8px;
  font-size: 12px;
  overflow-x: auto;
  color: var(--text-primary);
  white-space: pre-wrap;
  margin: 0;
  font-family: var(--mono);
  line-height: 1.65;
}
.memo-text { white-space:pre-wrap; font-size:14px; line-height:1.85; color:#c9d1d9; }
#status-msg { font-size:12px; color:var(--text-secondary); min-height:18px; }

/* ── Animations ── */
@keyframes fsDown { from{opacity:0;transform:translateY(-8px)} to{opacity:1;transform:translateY(0)} }
@keyframes fadeIn { from{opacity:0} to{opacity:1} }
@keyframes slideUp { from{opacity:0;transform:translateY(14px)} to{opacity:1;transform:translateY(0)} }
/* #256: analysis-completion reveal */
@keyframes ceReveal { from{opacity:0;transform:translateY(10px)} to{opacity:1;transform:none} }
.ce-reveal { animation:ceReveal .42s cubic-bezier(.22,1,.36,1) both; }
.anim-fade { animation:fadeIn .3s ease-out; }
.s1{animation:slideUp .38s cubic-bezier(.22,1,.36,1) .06s both;}
.s2{animation:slideUp .38s cubic-bezier(.22,1,.36,1) .14s both;}
.s3{animation:slideUp .38s cubic-bezier(.22,1,.36,1) .22s both;}
.s4{animation:slideUp .38s cubic-bezier(.22,1,.36,1) .30s both;}
.s5{animation:slideUp .38s cubic-bezier(.22,1,.36,1) .38s both;}
.s5{animation:fadeIn .35s .48s both;}

/* ── Skeleton loading ── */
@keyframes shimmer {
  0%   { background-position: -400px 0; }
  100% { background-position:  400px 0; }
}
.sk {
  background: linear-gradient(90deg,
    var(--bg-surface) 25%,
    var(--bg-elevated) 50%,
    var(--bg-surface) 75%
  );
  background-size: 800px 100%;
  animation: shimmer 1.6s ease-in-out infinite;
  border-radius: var(--r-sm);
}
.sk-block { height: 14px; margin-bottom: 8px; }
.sk-w100 { width:100%; }
.sk-w75  { width:75%; }
.sk-w50  { width:50%; }
.sk-w30  { width:30%; }
.sk-tall { height: 60px; }
.sk-verdict { height: 76px; border-radius: var(--r-lg); margin-bottom: 14px; }
.sk-card {
  background: var(--bg-surface);
  border: 1px solid var(--border-muted);
  border-radius: var(--r-lg);
  padding: 16px;
  margin-bottom: 12px;
}
.sk-grid { display:grid; grid-template-columns:repeat(4,1fr); gap:8px; margin-bottom:12px; }
.sk-grid-item { height:52px; border-radius:var(--r-sm); }
.sk-adv { height:90px; border-radius:var(--r-md); margin-bottom:8px; }

/* ── Responsive ── */
@media(max-width:1100px){.ce-layout{grid-template-columns:0 380px 1fr;}.ce-sidebar{display:none;}}
@media(max-width:900px){.ce-layout{grid-template-columns:0 320px 1fr;}}

/* Hamburger / mobile drawer — hidden by default */
.ce-ham{display:none;align-items:center;justify-content:center;flex-direction:column;gap:4px;width:36px;height:36px;background:none;border:1px solid rgba(255,255,255,.1);border-radius:var(--r-sm);cursor:pointer;padding:0;color:var(--text-secondary);transition:border-color var(--t);}
.ce-ham:hover{border-color:rgba(255,255,255,.2);}
.ce-ham span{display:block;width:15px;height:1.5px;background:currentColor;border-radius:1px;}
.mobile-nav-drawer{display:none;position:fixed;top:56px;left:0;right:0;background:rgba(9,13,18,.97);backdrop-filter:blur(20px) saturate(180%);border-bottom:1px solid rgba(255,255,255,.08);padding:10px 16px 16px;z-index:200;flex-direction:column;gap:2px;box-shadow:0 8px 32px rgba(0,0,0,.5);}
.mobile-nav-drawer.open{display:flex;}
.mobile-nav-drawer a,.mobile-nav-drawer button.nav-pill-m{display:block;padding:11px 14px;font-size:13px;color:var(--text-secondary);text-decoration:none;border-radius:var(--r-sm);transition:all var(--t);}
.mobile-nav-drawer a:hover{background:rgba(255,255,255,.05);color:var(--text-primary);}
.mobile-nav-drawer .m-divider{height:1px;background:rgba(255,255,255,.06);margin:8px 0;}
.mobile-nav-drawer .btn-primary-m{display:block;width:100%;padding:11px;background:var(--accent);color:#fff;border:none;border-radius:var(--r-sm);font-weight:700;font-size:13px;cursor:pointer;margin-top:4px;}

/* Mobile input toggle */
.mobile-input-toggle{display:none;align-items:center;justify-content:space-between;padding:8px 14px;background:rgba(255,255,255,.03);border-bottom:1px solid rgba(255,255,255,.06);cursor:pointer;font-size:12px;color:var(--text-secondary);}
.mobile-input-toggle .mit-arrow{font-size:10px;transition:transform .25s ease;}
.mobile-input-toggle.collapsed .mit-arrow{transform:rotate(-90deg);}

/* ── #258: Mobile sidebar drawer ── */
#sidebar-backdrop{
  display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);
  z-index:199;-webkit-tap-highlight-color:transparent;
}
body.sidebar-open #sidebar-backdrop{display:block;}
#sidebar-fab{
  display:none;position:fixed;bottom:80px;right:16px;z-index:190;
  background:var(--accent);color:#fff;border:none;border-radius:50px;
  padding:10px 16px;font-size:12px;font-weight:600;font-family:var(--font);
  box-shadow:var(--shadow-md);cursor:pointer;letter-spacing:.01em;
  align-items:center;gap:6px;transition:background .15s,transform .1s;
}
#sidebar-fab:active{transform:scale(.95);}

@media(max-width:768px){
  /* Nav */
  .ce-tagline{display:none;}
  .nav-pill{display:none;}
  #quota-chip{display:none!important;}
  #nav-login-btn{display:none!important;}
  .ce-nav-right > .btn{display:none;}
  .ce-ham{display:flex!important;}
  /* Layout — single column stack */
  .ce-layout{grid-template-columns:1fr;grid-template-rows:auto 1fr;height:auto;overflow:visible;}
  /* Sidebar: hidden by default, slides in as drawer (#258) */
  .ce-sidebar{
    display:block;position:fixed;left:0;top:58px;bottom:0;width:280px;
    transform:translateX(-100%);transition:transform .25s cubic-bezier(.22,1,.36,1);
    z-index:200;overflow-y:auto;border-right:1px solid var(--border-default);
    background:var(--bg-surface);
  }
  body.sidebar-open .ce-sidebar{transform:translateX(0);}
  /* Show FAB */
  #sidebar-fab{display:flex;}
  .ce-input{border-right:none;border-bottom:1px solid rgba(255,255,255,.06);overflow:visible;height:auto;max-height:none;padding:0;}
  .ce-input .ce-card{border-radius:0;border-left:none;border-right:none;border-top:none;}
  .ce-results{padding:14px 12px 80px;overflow:visible;}
  /* Mobile input toggle visible */
  .mobile-input-toggle{display:flex;}
  /* Tabs — horizontal scroll */
  .ce-tabs{overflow-x:auto;white-space:nowrap;flex-wrap:nowrap;-webkit-overflow-scrolling:touch;scrollbar-width:none;padding:8px 12px 0;}
  .ce-tabs::-webkit-scrollbar{display:none;}
  /* Stats */
  .stat-grid{grid-template-columns:repeat(2,1fr);}
  /* Verdict ring smaller */
  .conf-ring-wrap{width:64px;height:64px;}
  .conf-ring-wrap svg{width:64px;height:64px;}
  /* Flags 2-col */
  .flags-grid{grid-template-columns:repeat(2,1fr)!important;}
  /* Hide sidebar FAB in report mode */
  body.report-mode #sidebar-fab{display:none!important;}
}

@media(max-width:480px){
  .stat-grid{grid-template-columns:repeat(2,1fr);}
  .ce-card-hdr{padding:12px 14px;}
  .ce-card-body{padding:12px 14px;}
  .verdict-wrap{padding:14px;}
  .conf-ring-wrap{width:56px;height:56px;}
  .conf-ring-wrap svg{width:56px;height:56px;}
  .cr-label{font-size:10px;}
  .adv-card{margin-bottom:8px;}
  #metric-chips{gap:4px;}
  #metric-chips .mc{font-size:10px;padding:3px 8px;}
}

@media(max-width:360px){
  .stat-grid{grid-template-columns:1fr;}
  .flags-grid{grid-template-columns:1fr!important;}
}

/* ── Report / LP read-only mode ── */
body.report-mode .ce-input{display:none!important;}
body.report-mode .ce-sidebar{display:none!important;}
body.report-mode .ce-layout{grid-template-columns:1fr!important;height:auto!important;overflow:visible!important;}
body.report-mode .ce-results{overflow:visible;padding:20px 24px 60px;}
body.report-mode #empty-state{display:none!important;}
.report-header{max-width:860px;margin:0 auto 24px;display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:16px;padding:24px 0 0;}
.report-brand{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--text-muted);}
.report-brand-logo{font-weight:800;font-size:1.1rem;background:linear-gradient(135deg,var(--accent) 0%,#79c0ff 100%);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
.report-deal-name{font-family:var(--font-display);font-size:1.6rem;font-weight:400;letter-spacing:-0.01em;color:var(--text-primary);}
.report-meta{font-size:11px;color:var(--text-muted);margin-top:4px;}
.report-cta{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;background:rgba(31,111,235,.12);border:1px solid rgba(88,166,255,.25);border-radius:var(--r-sm);font-size:12px;font-weight:600;color:var(--accent);text-decoration:none;transition:all var(--t);}
.report-cta:hover{background:rgba(31,111,235,.2);border-color:rgba(88,166,255,.4);}
/* #242: ODD PDF export button */
.report-export-btn{display:inline-flex;align-items:center;gap:6px;padding:7px 14px;background:rgba(232,160,32,.1);border:1px solid rgba(232,160,32,.3);border-radius:var(--r-sm);font-size:11px;font-weight:700;color:var(--accent);text-decoration:none;transition:all var(--t);font-family:var(--mono);letter-spacing:.02em;}
.report-export-btn:hover{background:rgba(232,160,32,.2);border-color:var(--accent);box-shadow:0 0 10px rgba(232,160,32,.15);}
.report-footer{max-width:860px;margin:40px auto 0;padding:20px 0;border-top:1px solid rgba(255,255,255,.06);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;font-size:11px;color:var(--text-muted);}
.report-footer a{color:var(--accent);text-decoration:none;}
/* #249: Mobile-responsive report view */
@media(max-width:768px){
  /* Existing */
  body.report-mode .ce-results{padding:10px 12px 48px;}
  .report-header{padding:12px 0 0;flex-direction:column;gap:10px;}
  .report-deal-name{font-size:1.1rem;}
  /* Tab bar — scrollable with 44px tap targets */
  body.report-mode .ce-tabs{overflow-x:auto;white-space:nowrap;flex-wrap:nowrap;-webkit-overflow-scrolling:touch;scrollbar-width:none;padding:6px 0;}
  body.report-mode .ce-tabs::-webkit-scrollbar{display:none;}
  body.report-mode .ce-tab{min-height:44px;padding:8px 14px;font-size:12px;flex-shrink:0;}
  /* Metric chips — 2 columns */
  #metric-chips{display:grid!important;grid-template-columns:repeat(2,1fr);gap:6px;}
  #metric-chips .mc{font-size:11px;}
  /* Stat grid — 2 columns */
  .stat-grid{grid-template-columns:repeat(2,1fr)!important;}
  /* Advisor cards — full width stacked */
  .adv-card{width:100%;margin-bottom:10px;}
  .adv-hdr{flex-wrap:wrap;gap:6px;}
  /* Summary grid — single column on mobile */
  #summary-content > div[style*="grid-template-columns:1fr 1fr"]{display:block!important;}
  #summary-content > div[style*="grid-template-columns:1fr 1fr"] > div{margin-bottom:12px;}
  /* Verdict banner */
  .verdict-wrap{padding:16px 14px;}
  .verdict-stamp{font-size:1.2rem;}
  .conf-ring-wrap{width:60px;height:60px;}
  .conf-ring-wrap svg{width:60px;height:60px;}
  /* Action buttons — wrap */
  .d-flex.gap-2.mt-3.s3{flex-wrap:wrap!important;}
  .d-flex.gap-2.mt-3.s3 .btn-ghost{font-size:11px;padding:6px 10px;}
  /* Score popover — full-width on mobile */
  #score-popover{right:auto;left:0;min-width:260px;width:calc(100vw - 24px);}
  /* Report header CTA stack vertically */
  .report-cta,.report-export-btn{font-size:11px;padding:6px 10px;}
  /* Sponsor card grid — 2 cols on mobile */
  #sponsor-track-card [style*="grid-template-columns:repeat(5,1fr)"]{display:grid!important;grid-template-columns:repeat(2,1fr)!important;}
  /* Prevent iOS auto-zoom on small text — min 16px effective */
  input,textarea,select{font-size:16px!important;}
}
@media(max-width:480px){
  body.report-mode .ce-results{padding:8px 10px 48px;}
  #metric-chips{grid-template-columns:1fr 1fr!important;}
  .stat-grid{grid-template-columns:1fr 1fr!important;}
  .report-deal-name{font-size:1rem;}
  .adv-name{font-size:12px;}
  .adv-body .adv-text{font-size:12px;}
  /* Assumption evidence panels */
  #assumption-evidence-panel details summary{padding:8px 10px;}
  /* Devil's advocate card */
  #devil-advocate-panel [style*="border-radius:10px"]{padding:12px;}
}
/* ── Report mode memo premium layout (#240) ── */
body.report-mode .dj-tabs{max-width:860px;margin:0 auto;}
body.report-mode #memo-content{max-width:860px;margin:0 auto;}
body.report-mode .memo-text{max-width:860px;margin:0 auto;}
.rm-kill-shot{background:rgba(248,81,73,.06);border:1px solid rgba(248,81,73,.25);border-left:4px solid #f85149;border-radius:0 8px 8px 0;padding:16px 20px;margin-bottom:20px;}
.rm-kill-label{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.12em;color:#f85149;font-family:var(--mono);margin-bottom:8px;}
.rm-kill-text{font-size:13.5px;line-height:1.65;color:var(--text-primary);font-weight:500;}
.rm-section{margin-bottom:20px;padding-bottom:20px;border-bottom:1px solid var(--border-default);}
.rm-section:last-child{border-bottom:none;}
.rm-section-hdr{display:flex;align-items:center;gap:10px;margin-bottom:10px;}
.rm-section-title{font-family:var(--font-display);font-style:italic;font-size:1.05rem;font-weight:400;letter-spacing:-0.01em;color:var(--text-primary);}
.rm-section-bar{width:3px;flex-shrink:0;border-radius:2px;align-self:stretch;}
.rm-body{font-size:12.5px;line-height:1.75;color:var(--text-secondary);white-space:pre-wrap;}
.rm-verdict-card{background:rgba(63,185,80,.06);border:1px solid rgba(63,185,80,.25);border-radius:10px;padding:18px 22px;display:flex;align-items:center;justify-content:space-between;gap:16px;margin-bottom:20px;}
.rm-verdict-card.nogo{background:rgba(248,81,73,.06);border-color:rgba(248,81,73,.25);}
.rm-verdict-card.conditional{background:rgba(232,160,32,.06);border-color:rgba(232,160,32,.25);}
.rm-verdict-label{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.12em;color:var(--text-muted);font-family:var(--mono);margin-bottom:4px;}
.rm-verdict-stamp{font-family:var(--font-display);font-style:italic;font-size:1.8rem;font-weight:400;letter-spacing:-0.01em;}
.rm-verdict-conf{font-family:var(--mono);font-size:2rem;font-weight:700;letter-spacing:-0.04em;}
.rm-dd-item{display:flex;gap:12px;padding:10px 12px;border-radius:6px;transition:background var(--t);}
.rm-dd-item:hover{background:var(--bg-elevated);}
.rm-dd-num{font-family:var(--mono);font-size:11px;font-weight:700;color:var(--accent);min-width:20px;margin-top:2px;}
.rm-dd-text{font-size:12.5px;line-height:1.6;color:var(--text-secondary);}
/* Report share toolbar (#240) */
.report-share-bar{position:sticky;top:58px;z-index:90;background:rgba(245,243,238,.94);backdrop-filter:blur(12px);border-bottom:1px solid var(--border-muted);padding:8px 24px;display:none;align-items:center;justify-content:space-between;gap:12px;}
body.report-mode .report-share-bar{display:flex;}
.rsb-deal{font-family:var(--font-display);font-style:italic;font-size:.95rem;color:var(--text-primary);flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}
.rsb-actions{display:flex;gap:6px;align-items:center;flex-shrink:0;}
.rsb-btn{display:inline-flex;align-items:center;gap:5px;padding:5px 11px;font-size:11px;font-family:var(--mono);border-radius:5px;cursor:pointer;transition:all var(--t);text-decoration:none;border:1px solid var(--border-emphasis);background:none;color:var(--text-secondary);}
.rsb-btn:hover{border-color:rgba(232,160,32,.4);color:var(--accent);background:rgba(232,160,32,.06);}
.rsb-btn-primary{background:rgba(232,160,32,.1);border-color:rgba(232,160,32,.35);color:var(--accent);}
.rsb-btn-primary:hover{background:rgba(232,160,32,.18);}
/* #226: White-label branding drawer */
.wl-drawer-overlay{position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9100;display:none;}
.wl-drawer-overlay.open{display:block;}
.wl-drawer{position:fixed;top:0;right:-380px;width:360px;height:100vh;background:var(--bg-surface);border-left:1px solid var(--border-default);z-index:9101;overflow-y:auto;padding:24px 20px;transition:right .3s cubic-bezier(.16,1,.3,1);}
.wl-drawer.open{right:0;}
/* ── IPS Configurator (#229) ─────────────────────────────────────────── */
.ips-drawer-overlay{position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9200;display:none;}
.ips-drawer-overlay.open{display:block;}
.ips-drawer{position:fixed;top:0;right:-400px;width:380px;height:100vh;background:var(--bg-surface);border-left:1px solid var(--border-default);z-index:9201;overflow-y:auto;padding:24px 20px;transition:right .3s cubic-bezier(.16,1,.3,1);}
.ips-drawer.open{right:0;}
.ips-label{font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:var(--text-muted);margin:12px 0 4px;}
.ips-input{width:100%;background:rgba(255,255,255,.04);border:1px solid var(--border-default);border-radius:var(--r-sm);padding:7px 10px;font-size:12px;color:var(--text-primary);font-family:var(--mono);outline:none;box-sizing:border-box;}
.ips-input:focus{border-color:rgba(232,160,32,.45);background:rgba(232,160,32,.04);}
.ips-check-section{margin-bottom:12px;}
.ips-check-banner{display:flex;align-items:flex-start;gap:8px;padding:10px 12px;border-radius:7px;font-size:12px;margin-bottom:10px;}
.ips-banner-warn{background:rgba(210,153,35,.08);border:1px solid rgba(210,153,35,.3);color:var(--amber);}
.ips-banner-ok{background:rgba(63,185,80,.06);border:1px solid rgba(63,185,80,.25);color:#3fb950;}
.ips-row{display:flex;align-items:flex-start;gap:8px;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:12px;}
.ips-row:last-child{border-bottom:none;}
.ips-badge{display:inline-block;padding:1px 6px;border-radius:3px;font-family:var(--mono);font-size:10px;font-weight:700;white-space:nowrap;flex-shrink:0;margin-top:1px;}
.ips-pass{background:rgba(63,185,80,.12);color:#3fb950;border:1px solid rgba(63,185,80,.25);}
.ips-fail{background:rgba(248,81,73,.12);color:#f85149;border:1px solid rgba(248,81,73,.25);}
.ips-exc{background:rgba(210,153,35,.12);color:var(--amber);border:1px solid rgba(210,153,35,.25);}
.ips-row-text{color:var(--text-secondary);line-height:1.4;}
.wl-label{font-size:11px;color:var(--text-muted);display:block;margin-bottom:4px;margin-top:12px;}
.wl-input{width:100%;background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:7px 10px;font-size:12px;outline:none;font-family:var(--font);}
.wl-input:focus{border-color:rgba(232,160,32,.4);}
.wl-preview{background:var(--bg-elevated);border:1px solid rgba(232,160,32,.15);border-radius:var(--r-md);padding:12px;margin:14px 0;font-size:11px;}

/* ── Report mode: editorial verdict hero (#198) ── */
body.report-mode .verdict-wrap {
  text-align: center;
  padding: 52px 32px 40px;
  position: relative;
  overflow: hidden;
  border-radius: 14px;
}
body.report-mode #verdict-name {
  font-family: var(--font-display);
  font-size: clamp(1.8rem, 4vw, 2.8rem);
  font-weight: 400;
  font-style: italic;
  letter-spacing: -0.03em;
  margin-bottom: 24px;
  line-height: 1.1;
  position: relative;
  z-index: 2;
}
body.report-mode .verdict-stamp {
  font-size: clamp(2.5rem, 6vw, 4rem);
  padding: 12px 36px;
  letter-spacing: -0.02em;
  position: relative;
  z-index: 2;
}
.verdict-watermark {
  display: none;
  position: absolute;
  font-family: var(--font-display);
  font-style: italic;
  font-size: clamp(7rem, 22vw, 16rem);
  font-weight: 400;
  letter-spacing: -0.04em;
  opacity: 0.04;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
  white-space: nowrap;
  pointer-events: none;
  z-index: 0;
  user-select: none;
  line-height: 1;
}
body.report-mode .verdict-watermark { display: block; }
body.report-mode .conf-ring-wrap {
  width: 120px; height: 120px;
  margin: 28px auto 0;
  display: block;
}
body.report-mode .conf-ring-wrap svg { width: 120px !important; height: 120px !important; }
body.report-mode .cr-label { font-size: 1.1rem; font-family: var(--mono); font-weight: 600; }
body.report-mode .d-flex.align-items-center.gap-3 { flex-direction: column; align-items: center !important; }
body.report-mode #verdict-rat { text-align: center; font-size: 13px; margin-top: 8px; max-width: 480px; }
body.report-mode #metric-chips { justify-content: center; }

/* ── Report timeline sidebar (#252) ── */
.report-timeline-sidebar {
  display: none;
  position: fixed;
  left: 0; top: 58px; bottom: 0;
  width: 220px;
  background: var(--bg-surface);
  border-right: 1px solid var(--border-muted);
  overflow-y: auto;
  z-index: 80;
  padding: 20px 16px 32px;
  transition: transform .25s ease;
}
body.report-has-timeline .report-timeline-sidebar { display: block; }
body.report-has-timeline .ce-results { padding-left: 244px !important; }
.rts-title {
  font-size: 9px; font-weight: 700; letter-spacing: .1em; text-transform: uppercase;
  color: var(--text-muted); font-family: var(--mono); margin-bottom: 16px;
}
.rts-deal-name {
  font-size: 13px; font-weight: 600; color: var(--text-primary);
  margin-bottom: 4px; line-height: 1.4;
}
.rts-meta { font-size: 11px; color: var(--text-muted); margin-bottom: 18px; font-family: var(--mono); }
.rts-stages { list-style: none; margin: 0; padding: 0; position: relative; }
.rts-stages::before {
  content: ''; position: absolute; left: 9px; top: 8px; bottom: 8px;
  width: 1px; background: var(--border-default);
}
.rts-stage { display: flex; align-items: flex-start; gap: 10px; margin-bottom: 18px; position: relative; }
.rts-dot {
  width: 18px; height: 18px; border-radius: 50%; flex-shrink: 0; margin-top: 1px;
  background: var(--bg-elevated); border: 2px solid var(--border-default);
  display: flex; align-items: center; justify-content: center; font-size: 7px; z-index: 1;
}
.rts-stage.rts-done .rts-dot  { background: var(--accent); border-color: var(--accent); color: #fff; }
.rts-stage.rts-active .rts-dot { background: var(--bg-surface); border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-dim); }
.rts-stage.rts-future .rts-dot { opacity: .4; }
.rts-stage-info { flex: 1; min-width: 0; }
.rts-stage-name {
  font-size: 12px; font-weight: 600; color: var(--text-primary); white-space: nowrap;
  overflow: hidden; text-overflow: ellipsis;
}
.rts-stage.rts-active .rts-stage-name { color: var(--accent); }
.rts-stage.rts-future .rts-stage-name { color: var(--text-muted); font-weight: 400; }
.rts-stage-date { font-size: 10px; color: var(--text-muted); font-family: var(--mono); margin-top: 2px; }
.rts-stage-days {
  display: inline-block; margin-top: 4px; font-size: 10px;
  color: var(--accent); background: var(--accent-dim);
  padding: 1px 6px; border-radius: 99px; font-family: var(--mono); font-weight: 600;
}
.rts-toggle-btn {
  display: none; position: fixed; left: 0; top: 50%; transform: translateY(-50%);
  z-index: 85; background: var(--accent); color: #fff; border: none;
  border-radius: 0 6px 6px 0; width: 22px; height: 60px; cursor: pointer;
  font-size: 10px; padding: 0; writing-mode: vertical-rl; text-orientation: mixed;
  font-family: var(--mono); letter-spacing: .08em; font-weight: 600;
}
@media(max-width:900px){
  body.report-has-timeline .ce-results { padding-left: 20px !important; }
  .report-timeline-sidebar { transform: translateX(-100%); }
  body.report-has-timeline .report-timeline-sidebar { display: block; transform: translateX(-100%); }
  body.report-timeline-open .report-timeline-sidebar { transform: translateX(0); }
  body.report-has-timeline .rts-toggle-btn { display: flex; align-items: center; justify-content: center; }
  body.report-timeline-open .rts-toggle-btn { left: 220px; border-radius: 0 6px 6px 0; }
}

/* Editorial advisor dividers (#198) */
.report-adv-divider {
  display: none;
  align-items: center;
  gap: 16px;
  margin: 32px 0 16px;
  font-family: var(--mono);
  font-size: 10px;
  font-weight: 600;
  letter-spacing: .12em;
  text-transform: uppercase;
  color: var(--text-muted);
}
.report-adv-divider::before, .report-adv-divider::after {
  content: '';
  flex: 1;
  height: 1px;
  background: linear-gradient(90deg, transparent, rgba(232,160,32,.2), transparent);
}
body.report-mode .report-adv-divider { display: flex; }

/* ── Print / PDF export (#181) ── */
.print-fab{
  display:none;position:fixed;bottom:28px;right:28px;z-index:500;
  background:linear-gradient(135deg,#1f6feb 0%,#388bfd 100%);color:#fff;
  border:none;border-radius:50px;padding:11px 22px;font-size:13px;font-weight:600;
  box-shadow:0 4px 20px rgba(31,111,235,.45);cursor:pointer;transition:all .2s;
  letter-spacing:.01em;
}
.print-fab:hover{transform:translateY(-2px);box-shadow:0 6px 28px rgba(31,111,235,.6);}
body.report-mode .print-fab{display:flex;align-items:center;gap:6px;}
/* ── #264: Toast notification system ── */
@keyframes toastIn{from{opacity:0;transform:translateX(120%)}to{opacity:1;transform:translateX(0)}}
@keyframes toastOut{from{opacity:1;transform:translateX(0)}to{opacity:0;transform:translateX(120%)}}
#toast-container{position:fixed;bottom:24px;right:20px;z-index:9999;display:flex;flex-direction:column-reverse;gap:8px;pointer-events:none;}
.toast-msg{
  display:flex;align-items:center;gap:10px;
  padding:10px 16px;border-radius:8px;font-size:12px;font-weight:500;
  font-family:var(--font);box-shadow:var(--shadow-lg);
  pointer-events:auto;min-width:220px;max-width:340px;
  animation:toastIn .3s cubic-bezier(.22,1,.36,1) both;
  border:1px solid;
}
.toast-success{background:rgba(21,128,61,.1);border-color:rgba(21,128,61,.3);color:var(--green);}
.toast-error  {background:rgba(185,28,28,.1);border-color:rgba(185,28,28,.3);color:var(--red);}
.toast-info   {background:rgba(21,94,68,.08);border-color:rgba(21,94,68,.25);color:var(--accent);}
.toast-icon   {font-size:14px;flex-shrink:0;}
.toast-msg.toast-out{animation:toastOut .25s ease forwards;}

/* IC Memo FAB (#211) */
.ic-memo-fab{
  display:none;position:fixed;bottom:28px;right:160px;z-index:500;
  background:rgba(232,160,32,.12);color:var(--accent);
  border:1px solid rgba(232,160,32,.4);border-radius:50px;padding:11px 22px;font-size:13px;font-weight:600;
  font-family:var(--mono);box-shadow:0 4px 20px rgba(232,160,32,.2);cursor:pointer;transition:all .2s;
  letter-spacing:.01em;backdrop-filter:blur(8px);
}
.ic-memo-fab:hover{transform:translateY(-2px);background:rgba(232,160,32,.2);box-shadow:0 6px 28px rgba(232,160,32,.35);}
body.report-mode .ic-memo-fab{display:flex;align-items:center;gap:6px;}
/* #221: Decision Journal */
.dj-note-input-wrap{margin-top:10px;padding:10px 12px;background:rgba(232,160,32,.04);border:1px solid rgba(232,160,32,.15);border-radius:var(--r-md);display:none;}
.dj-note-input-wrap.open{display:block;}
.dj-note-textarea{width:100%;background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:8px;font-family:var(--mono);font-size:12px;resize:vertical;min-height:72px;outline:none;}
.dj-note-textarea:focus{border-color:rgba(232,160,32,.4);}
.dj-log-panel{margin-top:8px;border:1px solid rgba(232,160,32,.12);border-radius:var(--r-md);overflow:hidden;display:none;}
.dj-log-panel.open{display:block;}
.dj-log-header{background:rgba(232,160,32,.06);padding:8px 14px;font-size:11px;font-weight:600;color:var(--amber);display:flex;justify-content:space-between;align-items:center;cursor:pointer;user-select:none;}
.dj-log-body{padding:8px 14px;max-height:240px;overflow-y:auto;}
.dj-log-entry{padding:7px 0;border-bottom:1px solid rgba(255,255,255,.04);}
.dj-log-entry:last-child{border-bottom:none;}
.dj-log-tab{font-size:10px;color:var(--amber);font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;}
.dj-log-text{font-size:12px;color:var(--text-primary);margin:3px 0;white-space:pre-wrap;word-break:break-word;}
.dj-log-ts{font-size:10px;color:var(--text-muted);font-family:var(--mono);}
.dj-trail{margin-top:20px;padding:16px;background:rgba(232,160,32,.04);border:1px solid rgba(232,160,32,.12);border-radius:var(--r-md);}
.dj-trail-title{font-family:var(--font-display);font-style:italic;font-size:1rem;color:var(--amber);margin-bottom:10px;}

/* ── Bias re-analysis version timeline (#254) ── */
.bvt-title{font-size:9px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--text-muted);font-family:var(--mono);margin-bottom:10px;}
.bvt-row{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:6px;background:var(--bg-elevated);border:1px solid var(--border-muted);margin-bottom:5px;transition:background .15s;}
.bvt-row:hover{background:var(--bg-overlay);}
.bvt-ver{font-size:10px;font-weight:700;font-family:var(--mono);color:var(--text-muted);width:20px;flex-shrink:0;}
.bvt-date{font-size:10px;font-family:var(--mono);color:var(--text-muted);min-width:60px;}
.bvt-count{font-size:11px;font-weight:700;color:var(--text-primary);min-width:55px;}
.bvt-dots{display:flex;gap:3px;flex:1;flex-wrap:wrap;}
.bvt-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0;}
.bvt-dot-high{background:var(--red);}
.bvt-dot-med{background:var(--amber);}
.bvt-dot-low{background:var(--green);}
.bvt-label-high{font-size:9px;color:var(--red);font-family:var(--mono);}
.bvt-label-med{font-size:9px;color:var(--amber);font-family:var(--mono);}
.bvt-label-low{font-size:9px;color:var(--green);font-family:var(--mono);}
.bvt-current{border-color:rgba(21,94,68,.3);background:rgba(21,94,68,.05);}
@media print{
  /* ── #207 / #257: Clean IC memo print stylesheet ── */

  /* Page setup — letter size, 1.2in margins */
  @page{size:letter;margin:1.2in;}

  /* Counter-based running page footer */
  @page{
    @bottom-center{
      content:"ClearEye — Page " counter(page) " of " counter(pages);
      font-family:'JetBrains Mono',monospace;font-size:8pt;color:#666;
    }
  }
  body{counter-reset:page;}

  /* Hide all interactive chrome */
  .ce-nav,.ce-sidebar,.ce-input,.ce-tabs,.sum-bar,.btn,.btn-ghost,
  .print-fab,.ic-memo-fab,.kpal-backdrop,.sc-backdrop,.cmp-drawer,
  .lp-modal-overlay,#share-msg,.ob-banner,#onboarding-banner,
  #alert-bar,.report-cta,.delta-panel,.verdict-watermark,
  #assump-editor,.noise-overlay,.sc-backdrop,.mobile-nav-drawer,
  .rts-toggle-btn,.report-timeline-sidebar,.qs-badges,
  .bvt-title,.bias-version-timeline,.report-share-bar,
  .dj-log-panel,#kill-sheet-fab{display:none!important;}

  /* Layout — single column */
  .ce-layout{display:block!important;}
  .ce-results{
    padding:0!important;overflow:visible!important;
    max-width:100%!important;margin:0!important;
  }
  /* Remove all shadows and backgrounds */
  *{box-shadow:none!important;backdrop-filter:none!important;text-shadow:none!important;}

  /* Base typography — Cormorant Garamond 11pt body (#257) */
  body{
    background:#fff!important;color:#0D1926!important;
    font-family:'Cormorant Garamond',Georgia,serif!important;
    font-size:11pt!important;line-height:1.55!important;
    -webkit-print-color-adjust:exact;print-color-adjust:exact;
  }

  /* Headings stay in display face */
  h1,h2,h3,.report-deal-name,.adv-name,.verdict-name{
    font-family:'Cormorant Garamond',Georgia,serif!important;
    color:#0D1926!important;
  }

  /* Mono elements keep mono face */
  pre,.mono,code,[style*="JetBrains"],[style*="monospace"]{
    font-family:'JetBrains Mono',Consolas,monospace!important;
    font-size:9pt!important;color:#0D1926!important;
  }

  /* Print-only ClearEye header bar */
  #print-report-header{display:flex!important;}

  /* Cards — clean white with subtle rule, no fill */
  .ce-card,.verdict-wrap,.adv-card,.flag-card,
  .s1,.s2,.s3,.s4,.s5{
    border:1px solid #DDD9D1!important;background:#fff!important;
  }

  /* Text colors */
  .memo-text,.adv-text,.sum-text,.adv-body,.ce-card-body,
  .text-secondary,[style*="color:var(--text"]{color:#0D1926!important;}

  /* Pre/code blocks */
  pre.mono{border:1px solid #DDD9D1!important;color:#0D1926!important;
    background:#F5F3EE!important;font-size:9pt!important;}

  /* Verdict stamp — B&W (#257: no color, no text-shadow) */
  .verdict-stamp,.vs-go,.vs-nogo,.vs-cond{
    color:#0D1926!important;border:2.5px solid #0D1926!important;
    background:transparent!important;
  }

  /* Score/badge elements */
  .adv-score-badge{background:#f5f5f5!important;color:#0D1926!important;border-color:#999!important;}
  .conf-ring-wrap circle{stroke:#155E44!important;}

  /* Report-mode advisor dividers */
  .report-adv-divider{display:flex!important;border-bottom:1px solid #DDD9D1!important;
    margin:16px 0 10px!important;}
  .report-adv-divider::before,.report-adv-divider::after{display:none!important;}

  /* Page breaks — force break before major analysis sections (#257) */
  h1,h2,h3,.ce-card-title,.adv-name{page-break-after:avoid;break-after:avoid;}
  p,li{orphans:3;widows:3;}
  .advisor-section,.flag-card,.adv-card{page-break-inside:avoid;break-inside:avoid;}
  #tab-audit,#tab-bias,#tab-premortem{page-break-before:always;break-before:always;}
  #tab-advisors{page-break-before:always;break-before:always;}

  /* Confidence ring */
  .conf-ring-wrap{-webkit-print-color-adjust:exact;print-color-adjust:exact;}

  /* Footer — deal name per page via CSS counter */
  .ce-results::after{
    content:'ClearEye Investment Analysis';
    display:block;margin-top:32pt;padding-top:8pt;
    border-top:1px solid #DDD9D1;
    font-family:'JetBrains Mono',monospace;font-size:8pt;
    color:#8D98A5;text-align:center;
  }
}

/* ── Cmd+K Command Palette (#167) ── */
.kpal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.55);backdrop-filter:blur(6px);z-index:9000;display:none;align-items:flex-start;justify-content:center;padding-top:14vh;}
.kpal-backdrop.open{display:flex;}
.kpal-modal{width:min(600px,92vw);background:var(--bg-surface);border:1px solid var(--border-default);border-radius:12px;box-shadow:var(--shadow-xl);overflow:hidden;}
.kpal-search-row{display:flex;align-items:center;gap:10px;padding:14px 16px;border-bottom:1px solid var(--border-default);}
.kpal-search-row svg{flex-shrink:0;color:var(--text-secondary);}
.kpal-input{flex:1;background:none;border:none;outline:none;font-size:15px;font-family:inherit;color:var(--text-primary);letter-spacing:-0.011em;}
.kpal-input::placeholder{color:var(--text-muted);}
.kpal-kbd{font-size:10px;color:var(--text-muted);background:rgba(0,0,0,.05);border:1px solid var(--border-default);border-radius:4px;padding:2px 6px;flex-shrink:0;}
.kpal-results{max-height:340px;overflow-y:auto;padding:6px;}
.kpal-results:empty::after{content:'No results';display:block;text-align:center;padding:24px;font-size:13px;color:var(--text-muted);}
.kpal-section{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);padding:8px 10px 4px;}
.kpal-item{display:flex;align-items:center;gap:10px;padding:8px 10px;border-radius:6px;cursor:pointer;transition:background var(--t);}
.kpal-item:hover,.kpal-item.active{background:rgba(21,94,68,.07);}
.kpal-item.active{outline:1px solid rgba(21,94,68,.2);}
.kpal-icon{width:28px;height:28px;display:flex;align-items:center;justify-content:center;background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:6px;font-size:14px;flex-shrink:0;}
.kpal-label{flex:1;}
.kpal-name{font-size:13px;font-weight:500;color:var(--text-primary);}
.kpal-desc{font-size:11px;color:var(--text-muted);margin-top:1px;}
.kpal-shortcut{font-size:10px;color:var(--text-muted);background:rgba(0,0,0,.04);border:1px solid var(--border-default);border-radius:4px;padding:2px 5px;}
.kpal-footer{padding:8px 16px;border-top:1px solid var(--border-muted);display:flex;align-items:center;gap:12px;font-size:10px;color:var(--text-muted);}
.kpal-footer kbd{background:rgba(0,0,0,.04);border:1px solid var(--border-default);border-radius:3px;padding:1px 5px;font-family:inherit;}

/* ── Keyboard Shortcut Cheatsheet Modal (#204) ── */
.sc-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.6);backdrop-filter:blur(8px);z-index:9100;display:none;align-items:center;justify-content:center;}
.sc-backdrop.open{display:flex;}
@keyframes scModalIn{from{opacity:0;transform:translateY(12px) scale(.97)}to{opacity:1;transform:translateY(0) scale(1)}}
.sc-modal{width:min(640px,94vw);max-height:80vh;background:var(--bg-surface);border:1px solid var(--border-default);border-radius:14px;box-shadow:var(--shadow-xl);overflow:hidden;display:flex;flex-direction:column;animation:scModalIn .22s cubic-bezier(.22,1,.36,1) both;}
.sc-header{display:flex;align-items:center;justify-content:space-between;padding:18px 22px 14px;border-bottom:1px solid var(--border-default);}
.sc-title{font-family:var(--font-display);font-style:italic;font-size:1.25rem;font-weight:500;color:var(--text-primary);letter-spacing:-.01em;}
.sc-close{background:none;border:none;cursor:pointer;color:var(--text-muted);padding:4px;border-radius:6px;transition:color var(--t),background var(--t);display:flex;}
.sc-close:hover{color:var(--text-primary);background:var(--bg-elevated);}
.sc-body{overflow-y:auto;padding:18px 22px 22px;scrollbar-width:thin;scrollbar-color:rgba(21,94,68,.2) transparent;}
.sc-section{margin-bottom:18px;}
.sc-section-label{font-family:var(--mono);font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--accent);opacity:.7;margin-bottom:10px;}
.sc-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px;}
.sc-row{display:flex;align-items:center;justify-content:space-between;padding:7px 10px;background:var(--bg-elevated);border-radius:7px;border:1px solid var(--border-muted);transition:background var(--t);}
.sc-row:hover{background:rgba(21,94,68,.05);}
.sc-action{font-size:12px;color:var(--text-secondary);flex:1;}
.sc-keys{display:flex;align-items:center;gap:3px;flex-shrink:0;}
.sc-key{font-family:var(--mono);font-size:10px;color:var(--text-secondary);background:var(--bg-surface);border:1px solid var(--border-default);border-radius:5px;padding:2px 7px;line-height:1.5;}
.sc-key.amber{color:var(--accent);border-color:rgba(21,94,68,.25);background:rgba(21,94,68,.05);}
.sc-plus{font-size:10px;color:var(--text-muted);padding:0 1px;}
.sc-footer{padding:10px 22px 14px;border-top:1px solid var(--border-muted);font-family:var(--mono);font-size:10px;color:var(--text-muted);text-align:center;}

/* ── Inline comparison drawer (#168) ── */
.cmp-drawer{position:fixed;bottom:0;left:0;right:0;z-index:800;transform:translateY(100%);transition:transform .3s cubic-bezier(.16,1,.3,1);background:rgba(245,243,238,.97);backdrop-filter:blur(20px);border-top:1px solid var(--border-default);box-shadow:0 -4px 24px rgba(0,0,0,.08);}
.cmp-drawer.open{transform:translateY(0);}
.cmp-drawer-header{display:flex;align-items:center;gap:12px;padding:12px 20px 10px;border-bottom:1px solid var(--border-muted);}
.cmp-drawer-title{font-size:13px;font-weight:700;color:var(--text-primary);letter-spacing:-0.011em;flex:1;}
.cmp-drawer-actions{display:flex;align-items:center;gap:8px;}
.cmp-full-btn{font-size:11px;color:var(--accent);background:rgba(21,94,68,.07);border:1px solid rgba(21,94,68,.2);border-radius:var(--r-sm);padding:4px 10px;cursor:pointer;text-decoration:none;transition:all var(--t);}
.cmp-full-btn:hover{background:rgba(21,94,68,.14);}
.cmp-close-btn{width:24px;height:24px;display:flex;align-items:center;justify-content:center;background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:4px;cursor:pointer;font-size:14px;color:var(--text-secondary);transition:background var(--t);}
.cmp-close-btn:hover{background:var(--bg-overlay);}
.cmp-drawer-body{padding:16px 20px 20px;overflow-x:auto;}
.cmp-grid{display:grid;gap:12px;}
.cmp-row{display:grid;gap:8px;align-items:stretch;}
.cmp-label{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);padding:6px 0 2px;}
.cmp-cell{background:var(--bg-surface);border:1px solid var(--border-default);border-radius:8px;padding:10px 14px;}
.cmp-cell-val{font-size:1.05rem;font-weight:700;color:var(--text-primary);letter-spacing:-0.011em;}
.cmp-cell-lbl{font-size:10px;color:var(--text-muted);margin-top:2px;}
.cmp-winner{border-color:rgba(21,94,68,.3);background:rgba(21,94,68,.05);}
.cmp-winner .cmp-cell-val{color:var(--accent);}
.cmp-deal-header{font-size:12px;font-weight:700;color:var(--text-primary);padding:0 0 6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.cmp-verdict-badge{display:inline-flex;align-items:center;gap:4px;font-size:10px;font-weight:700;border-radius:4px;padding:2px 7px;border:1px solid;}
/* ── First-run onboarding banner (#170) ── */
#onboarding-banner{position:relative;margin:12px 16px 0;border-radius:var(--r-md);background:linear-gradient(135deg,rgba(21,94,68,.06) 0%,rgba(21,128,61,.04) 100%);border:1px solid rgba(21,94,68,.18);padding:18px 20px 16px;display:none;}
#onboarding-banner.visible{display:block;}
.ob-close{position:absolute;top:10px;right:12px;background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:16px;line-height:1;padding:2px 6px;}
.ob-close:hover{color:var(--text-secondary);}
.ob-title{font-size:14px;font-weight:700;color:var(--text-primary);margin-bottom:4px;}
.ob-sub{font-size:11px;color:var(--text-secondary);margin-bottom:14px;}
.ob-steps{display:flex;gap:12px;flex-wrap:wrap;}
.ob-step{display:flex;align-items:flex-start;gap:8px;flex:1;min-width:140px;}
.ob-num{width:20px;height:20px;border-radius:50%;background:var(--accent);display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;color:#fff;flex-shrink:0;margin-top:1px;}
.ob-step-text{font-size:11px;color:var(--text-secondary);line-height:1.5;}
.ob-step-text strong{color:var(--text-primary);display:block;font-size:11px;margin-bottom:1px;}
.ob-cta{margin-top:14px;display:flex;align-items:center;gap:10px;}
@keyframes ob-pulse{0%,100%{box-shadow:0 0 0 0 rgba(21,94,68,.35);}60%{box-shadow:0 0 0 6px rgba(21,94,68,0);}}
.ob-cta-btn{padding:7px 18px;background:var(--accent);border:none;color:#fff;border-radius:var(--r-sm);font-size:12px;font-weight:600;cursor:pointer;animation:ob-pulse 2.4s ease infinite;}
.ob-cta-btn:hover{animation:none;background:var(--btn-hover);}
.ob-dismiss-lnk{font-size:11px;color:var(--text-muted);cursor:pointer;text-decoration:underline;}
.ob-dismiss-lnk:hover{color:var(--text-secondary);}

/* ── Light-theme global overrides: neutralise any remaining dark inline colors ── */
/* Select / dropdown elements that use dark inline backgrounds */
select {
  background: var(--bg-surface) !important;
  color: var(--text-primary) !important;
  border-color: var(--border-default) !important;
}
/* Any element with a dark hardcoded background that slips through */
[style*="background:#0d1117"],[style*="background: #0d1117"],
[style*="background:#161b22"],[style*="background: #161b22"],
[style*="background:#0c1118"],[style*="background:#131b24"] {
  background: var(--bg-elevated) !important;
  color: var(--text-primary) !important;
}
/* Dark border overrides */
[style*="border:1px solid #21262d"],[style*="border:1px solid #30363d"],
[style*="border-color:#21262d"],[style*="border-color:#30363d"] {
  border-color: var(--border-default) !important;
}
/* GitHub blue text → green accent */
[style*="color:#58a6ff"],[style*="color: #58a6ff"],
[style*="color:#79c0ff"],[style*="color:#388bfd"] {
  color: var(--accent) !important;
}
/* GitHub dark text override → secondary text */
[style*="color:#8b949e"],[style*="color: #8b949e"] {
  color: var(--text-secondary) !important;
}
/* Blue button → green */
[style*="background:#1f6feb"],[style*="background: #1f6feb"],
[style*="background:#1c2d3e"] {
  background: var(--accent) !important;
}
/* Amber references — keep amber for warnings/caution, just ensure readable on light bg */
[style*="color:#d29922"],[style*="color: #d29922"] {
  color: var(--amber) !important;
}
</style>
</head>
<body{% if _report_mode %} class="report-mode"{% endif %}>
<div class="noise-overlay" aria-hidden="true"></div>

<!-- Print / Save as PDF FAB (#181) — visible only in report-mode via CSS -->
<button class="print-fab" onclick="printReport()" title="Save as PDF">
  &#128462; Save as PDF
</button>
<!-- IC Memo FAB and Kill Sheet FAB removed — consolidated into Export action button -->
<button class="ic-memo-fab" id="ic-memo-fab" style="display:none;" onclick="downloadICMemo()"></button>
<button id="kill-sheet-fab" style="display:none;" onclick="downloadKillSheet()"></button>

<!-- #226: White-label branding drawer -->
<div class="wl-drawer-overlay" id="wl-overlay" onclick="wlClose()"></div>
<div class="wl-drawer" id="wl-drawer">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
    <div style="font-family:var(--font-display);font-style:italic;font-size:1.1rem;color:var(--text-primary);">Report Branding</div>
    <button onclick="wlClose()" style="background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer;">&#x2715;</button>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:14px;">Customize how your LP reports appear when you share them. Your branding is applied when generating share links.</div>
  <label class="wl-label">Firm Name</label>
  <input class="wl-input" id="wl-firm-name" placeholder="e.g. Apex Capital Partners" oninput="wlPreview()">
  <label class="wl-label">Logo URL (optional)</label>
  <input class="wl-input" id="wl-logo-url" placeholder="https://yourdomain.com/logo.png" oninput="wlPreview()">
  <label class="wl-label">Accent Color (optional)</label>
  <input class="wl-input" id="wl-accent" placeholder="#e8a020" oninput="wlPreview()" style="font-family:var(--mono);">
  <label class="wl-label">GP Investment Perspective (optional, max 300 chars)</label>
  <textarea class="wl-input" id="wl-gp-note" placeholder="e.g. We focus on Class B multifamily in Sun Belt markets with value-add potential. This analysis reflects our conservative underwriting standards." style="resize:vertical;min-height:72px;line-height:1.5;" maxlength="300" oninput="wlPreview()"></textarea>
  <div style="font-size:10px;color:var(--text-muted);margin-top:3px;text-align:right;" id="wl-note-count">0 / 300</div>
  <div style="display:flex;align-items:center;gap:8px;margin-top:14px;margin-bottom:4px;">
    <input type="checkbox" id="wl-hide-badge" oninput="wlPreview()" style="accent-color:var(--amber);">
    <label for="wl-hide-badge" style="font-size:12px;color:var(--text-secondary);cursor:pointer;">Hide "Powered by ClearEye" badge</label>
  </div>
  <div class="wl-preview" id="wl-preview">
    <div style="font-size:10px;color:var(--text-muted);margin-bottom:6px;text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);">Preview</div>
    <div id="wl-preview-name" style="font-family:var(--font-display);font-style:italic;font-size:1rem;color:var(--accent);">Your Firm Name</div>
    <div style="font-size:10px;color:var(--text-muted);margin-top:2px;" id="wl-preview-sub">Deal Analysis · Powered by ClearEye</div>
  </div>
  <div style="display:flex;gap:8px;margin-top:14px;">
    <button onclick="wlSave()" style="flex:1;padding:9px;background:rgba(232,160,32,.12);border:1px solid rgba(232,160,32,.35);color:var(--amber);border-radius:var(--r-sm);cursor:pointer;font-size:12px;font-weight:600;">Save Branding</button>
    <button onclick="wlClear()" style="padding:9px 14px;background:none;border:1px solid var(--border-default);color:var(--text-muted);border-radius:var(--r-sm);cursor:pointer;font-size:12px;">Clear</button>
  </div>
  <div id="wl-save-msg" style="font-size:11px;color:#3fb950;margin-top:8px;min-height:16px;"></div>
</div>

<!-- Guided Onboarding Modal (#235) -->
<div id="ob-modal-overlay" style="display:none;position:fixed;inset:0;background:rgba(5,8,13,.9);z-index:9500;align-items:center;justify-content:center;padding:20px;">
  <div style="background:var(--bg-surface);border:1px solid var(--border-emphasis);border-radius:14px;max-width:560px;width:100%;overflow:hidden;position:relative;">
    <div style="background:linear-gradient(135deg,rgba(232,160,32,.08) 0%,rgba(248,81,73,.08) 100%);border-bottom:1px solid var(--border-default);padding:28px 28px 20px;">
      <div style="font-family:var(--mono);font-size:9px;letter-spacing:.14em;text-transform:uppercase;color:var(--accent);margin-bottom:10px;">ClearEye &mdash; Deal Intelligence</div>
      <div style="font-family:var(--font-display);font-style:italic;font-size:1.6rem;color:var(--text-primary);line-height:1.2;margin-bottom:6px;">Find what you missed before<br>you wire the earnest money.</div>
      <div style="font-size:12px;color:var(--text-secondary);line-height:1.6;max-width:440px;">ClearEye&#x2019;s adversarial AI shows you what the sponsor doesn&#x2019;t want you to see &mdash; stress test, bias detection, pre-mortem, and a Chairman verdict in under 2 minutes.</div>
    </div>
    <div style="padding:24px 28px 28px;">
      <div style="font-size:12px;color:var(--text-muted);margin-bottom:16px;">Paste an OM from a deal you&#x2019;re circling &mdash; or try our sample deal with 4 hidden red flags.</div>
      <div style="display:flex;flex-direction:column;gap:10px;">
        <button onclick="obLoadSample()" style="display:flex;align-items:center;gap:10px;padding:14px 18px;background:rgba(232,160,32,.1);border:1px solid rgba(232,160,32,.3);color:var(--amber);border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;text-align:left;transition:background .15s;" onmouseenter="this.style.background='rgba(232,160,32,.18)'" onmouseleave="this.style.background='rgba(232,160,32,.1)'">
          <span style="font-size:22px;">&#128290;</span>
          <div><div>Load Sample OM &mdash; $12M Phoenix Multifamily</div><div style="font-size:11px;font-weight:400;color:var(--text-muted);margin-top:2px;">150 units &middot; Class B value-add &middot; 4 intentional red flags hidden in the numbers</div></div>
        </button>
        <button onclick="obClose()" style="padding:11px 18px;background:none;border:1px solid var(--border-default);color:var(--text-secondary);border-radius:8px;font-size:13px;cursor:pointer;" onmouseenter="this.style.borderColor='rgba(255,255,255,.2)'" onmouseleave="this.style.borderColor='var(--border-default)'">I have my own OM &mdash; skip intro</button>
      </div>
      <div style="font-size:10px;color:var(--text-muted);margin-top:14px;text-align:center;">No account required for your first analysis. Results include stress test, bias detection &amp; Chairman verdict.</div>
    </div>
    <button onclick="obClose()" style="position:absolute;top:14px;right:16px;background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer;padding:4px;">&#x2715;</button>
  </div>
</div>

<!-- IPS Configurator drawer (#229) -->
<div class="ips-drawer-overlay" id="ips-overlay" onclick="ipsClose()"></div>
<div class="ips-drawer" id="ips-drawer">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
    <div style="font-family:var(--font-display);font-style:italic;font-size:1.1rem;color:var(--text-primary);">Investment Policy Statement</div>
    <button onclick="ipsClose()" style="background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer;">&#x2715;</button>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:16px;line-height:1.5;">Set your hard criteria. After each analysis, ClearEye will flag any deal that falls outside your stated policy — useful for LP ODD defensibility.</div>
  <label class="ips-label">Min Projected IRR (%)</label>
  <input class="ips-input" id="ips-min-irr" type="number" placeholder="e.g. 14" step="0.5">
  <label class="ips-label">Max LTV (%)</label>
  <input class="ips-input" id="ips-max-ltv" type="number" placeholder="e.g. 75" step="1">
  <label class="ips-label">Min Deal Size ($)</label>
  <input class="ips-input" id="ips-min-size" type="number" placeholder="e.g. 2000000" step="100000">
  <label class="ips-label">Max Deal Size ($)</label>
  <input class="ips-input" id="ips-max-size" type="number" placeholder="e.g. 50000000" step="100000">
  <label class="ips-label">Max Hold Period (years)</label>
  <input class="ips-input" id="ips-max-hold" type="number" placeholder="e.g. 7" step="1">
  <label class="ips-label">Allowed Markets (comma-separated MSAs)</label>
  <input class="ips-input" id="ips-markets" placeholder="e.g. Austin TX, Phoenix AZ, Nashville TN" style="font-family:var(--font-sans);">
  <label class="ips-label" style="margin-top:14px;">Allowed Asset Classes</label>
  <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:6px;" id="ips-asset-checks">
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="Multifamily" style="accent-color:var(--amber);">Multifamily</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="Industrial" style="accent-color:var(--amber);">Industrial</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="Office" style="accent-color:var(--amber);">Office</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="Retail" style="accent-color:var(--amber);">Retail</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="Mixed-Use" style="accent-color:var(--amber);">Mixed-Use</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="Self-Storage" style="accent-color:var(--amber);">Self-Storage</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="NNN" style="accent-color:var(--amber);">NNN</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="ips-ac-cb" value="Hospitality" style="accent-color:var(--amber);">Hospitality</label>
  </div>
  <div style="display:flex;gap:8px;margin-top:18px;">
    <button onclick="ipsSave()" style="flex:1;padding:9px;background:rgba(232,160,32,.12);border:1px solid rgba(232,160,32,.35);color:var(--amber);border-radius:var(--r-sm);cursor:pointer;font-size:12px;font-weight:600;">Save Criteria</button>
    <button onclick="ipsClear()" style="padding:9px 14px;background:none;border:1px solid var(--border-default);color:var(--text-muted);border-radius:var(--r-sm);cursor:pointer;font-size:12px;">Clear</button>
  </div>
  <div id="ips-save-msg" style="font-size:11px;color:#3fb950;margin-top:8px;min-height:16px;"></div>
</div>

<!-- Thesis Profile drawer (#233) -->
<div class="ips-drawer-overlay" id="tp-overlay" onclick="tpClose()" style="z-index:9300;"></div>
<div class="ips-drawer" id="tp-drawer" style="z-index:9301;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
    <div style="font-family:var(--font-display);font-style:italic;font-size:1.1rem;color:var(--text-primary);">Investment Thesis Profile</div>
    <button onclick="tpClose()" style="background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer;">&#x2715;</button>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:16px;line-height:1.5;">Define your buy box. ClearEye will score every deal against your thesis and add a Thesis Fit score to the verdict.</div>
  <label class="ips-label">Strategy</label>
  <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:6px;" id="tp-strategy-btns">
    <button class="tp-strat-btn" data-val="value-add" onclick="tpToggleStrat(this)" style="padding:5px 12px;border-radius:4px;font-size:11px;cursor:pointer;border:1px solid var(--border-default);background:none;color:var(--text-secondary);">Value-Add</button>
    <button class="tp-strat-btn" data-val="core-plus" onclick="tpToggleStrat(this)" style="padding:5px 12px;border-radius:4px;font-size:11px;cursor:pointer;border:1px solid var(--border-default);background:none;color:var(--text-secondary);">Core-Plus</button>
    <button class="tp-strat-btn" data-val="core" onclick="tpToggleStrat(this)" style="padding:5px 12px;border-radius:4px;font-size:11px;cursor:pointer;border:1px solid var(--border-default);background:none;color:var(--text-secondary);">Core</button>
    <button class="tp-strat-btn" data-val="ground-up" onclick="tpToggleStrat(this)" style="padding:5px 12px;border-radius:4px;font-size:11px;cursor:pointer;border:1px solid var(--border-default);background:none;color:var(--text-secondary);">Ground-Up Dev</button>
    <button class="tp-strat-btn" data-val="opportunistic" onclick="tpToggleStrat(this)" style="padding:5px 12px;border-radius:4px;font-size:11px;cursor:pointer;border:1px solid var(--border-default);background:none;color:var(--text-secondary);">Opportunistic</button>
  </div>
  <label class="ips-label">Preferred Asset Classes</label>
  <div style="display:flex;flex-wrap:wrap;gap:8px;margin-top:6px;" id="tp-asset-checks">
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="tp-ac-cb" value="Multifamily" style="accent-color:var(--amber);">Multifamily</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="tp-ac-cb" value="Industrial" style="accent-color:var(--amber);">Industrial</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="tp-ac-cb" value="Office" style="accent-color:var(--amber);">Office</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="tp-ac-cb" value="Retail" style="accent-color:var(--amber);">Retail</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="tp-ac-cb" value="Mixed-Use" style="accent-color:var(--amber);">Mixed-Use</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="tp-ac-cb" value="Self-Storage" style="accent-color:var(--amber);">Self-Storage</label>
    <label style="display:flex;align-items:center;gap:4px;font-size:12px;color:var(--text-secondary);cursor:pointer;"><input type="checkbox" class="tp-ac-cb" value="NNN" style="accent-color:var(--amber);">NNN</label>
  </div>
  <label class="ips-label" style="margin-top:14px;">Target Geographies (comma-separated MSAs)</label>
  <input class="ips-input" id="tp-geos" placeholder="e.g. Austin TX, Phoenix AZ, Denver CO" style="font-family:var(--font-sans);">
  <label class="ips-label">Target IRR (%)</label>
  <input class="ips-input" id="tp-irr" type="number" placeholder="e.g. 16" step="0.5">
  <label class="ips-label">Typical Hold Period (years)</label>
  <input class="ips-input" id="tp-hold" type="number" placeholder="e.g. 5" step="1">
  <label class="ips-label">Unit Count Range (multifamily)</label>
  <div style="display:flex;gap:8px;">
    <input class="ips-input" id="tp-units-min" type="number" placeholder="Min (e.g. 50)" step="10" style="flex:1;">
    <input class="ips-input" id="tp-units-max" type="number" placeholder="Max (e.g. 300)" step="10" style="flex:1;">
  </div>
  <div style="display:flex;gap:8px;margin-top:18px;">
    <button onclick="tpSave()" style="flex:1;padding:9px;background:rgba(232,160,32,.12);border:1px solid rgba(232,160,32,.35);color:var(--amber);border-radius:var(--r-sm);cursor:pointer;font-size:12px;font-weight:600;">Save Thesis</button>
    <button onclick="tpClear()" style="padding:9px 14px;background:none;border:1px solid var(--border-default);color:var(--text-muted);border-radius:var(--r-sm);cursor:pointer;font-size:12px;">Clear</button>
  </div>
  <div id="tp-save-msg" style="font-size:11px;color:#3fb950;margin-top:8px;min-height:16px;"></div>
</div>

<!-- Navbar (#82) -->
<nav class="ce-nav">
  <a class="ce-brand" href="#" onclick="newAnalysis();return false;">
    <div style="width:28px;height:28px;background:var(--accent);border-radius:7px;display:flex;align-items:center;justify-content:center;flex-shrink:0;">
      <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round">
        <circle cx="12" cy="12" r="4"/><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>
      </svg>
    </div>
    ClearEye
  </a>
  <span class="ce-tagline">Real Estate Investment Intelligence</span>
  <div class="ce-nav-right">
    <a href="/markets"    class="nav-pill">Markets</a>
    <a href="/pipeline"   class="nav-pill">Pipeline</a>
    <a href="/portfolio"  class="nav-pill">Portfolio</a>
    <a href="/pricing"    class="nav-pill">Pricing</a>
    <!-- Usage quota chip (#155) — populated by loadQuotaWidget() -->
    <div id="quota-chip" style="display:none;font-size:11px;border-radius:var(--r-sm);padding:4px 9px;border:1px solid var(--border-default);cursor:default;" title="Monthly analysis usage"></div>
    <a href="/login" id="nav-login-btn" class="nav-pill-outline">Sign In</a>
    <!-- Tools dropdown (#238): consolidates Thesis, IPS, Brand into one button -->
    <div class="tools-dropdown" id="tools-dropdown">
      <button class="tools-trigger" id="tools-trigger-btn" onclick="toolsToggle(event)" title="Configure your investment preferences">
        &#9881; Tools
        <span class="tools-trigger-dot" id="tools-dot"></span>
        <span class="tools-caret">&#9660;</span>
      </button>
      <div class="tools-menu" id="tools-menu">
        <button class="tools-menu-item" id="tp-nav-btn" onclick="toolsClose();tpOpen()">
          <span class="tools-menu-item-icon">&#128208;</span>
          <div><div>My Thesis</div><div class="tools-menu-item-meta">Buy box &amp; strategy profile</div></div>
        </button>
        <button class="tools-menu-item" id="ips-nav-btn" onclick="toolsClose();ipsOpen()">
          <span class="tools-menu-item-icon">&#127908;</span>
          <div><div>My Criteria</div><div class="tools-menu-item-meta">IPS hard constraints &amp; compliance</div></div>
        </button>
        <div class="tools-menu-sep"></div>
        <button class="tools-menu-item" id="wl-nav-btn" onclick="toolsClose();wlOpen()">
          <span class="tools-menu-item-icon">&#127775;</span>
          <div><div>Report Branding</div><div class="tools-menu-item-meta">White-label firm branding &amp; GP note</div></div>
        </button>
      </div>
    </div>
    <button onclick="kpalOpen()" title="Command palette (Ctrl+K)" style="display:flex;align-items:center;gap:5px;background:rgba(0,0,0,.03);border:1px solid var(--border-default);border-radius:var(--r-sm);padding:4px 9px;font-size:11px;color:var(--text-muted);cursor:pointer;transition:all var(--t);" onmouseenter="this.style.borderColor='rgba(21,94,68,.3)';this.style.color='var(--text-secondary)'" onmouseleave="this.style.borderColor='var(--border-default)';this.style.color='var(--text-muted)'">
      <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
      <span>Search</span><kbd style="font-size:9px;background:rgba(0,0,0,.05);border:1px solid var(--border-default);border-radius:3px;padding:1px 4px;font-family:inherit;">⌘K</kbd>
    </button>
    <button onclick="newAnalysis()" style="padding:7px 16px;font-size:12.5px;font-weight:600;background:var(--accent);color:#fff;border:none;border-radius:var(--r-sm);cursor:pointer;transition:background var(--t);box-shadow:0 1px 3px rgba(21,94,68,.25);" onmouseover="this.style.background='var(--btn-hover)'" onmouseout="this.style.background='var(--accent)'">+ New Analysis</button>
    <!-- Hamburger (mobile only) -->
    <button class="ce-ham" aria-label="Menu" onclick="toggleMobileNav()">
      <span></span><span></span><span></span>
    </button>
  </div>
</nav>
<!-- ── Report share toolbar (#240) ── -->
<div class="report-share-bar" id="report-share-bar">
  <div class="rsb-deal" id="rsb-deal-name">—</div>
  <div class="rsb-actions">
    <button class="rsb-btn" onclick="window.print()" title="Print / Save as PDF">&#128438; Print</button>
    <button class="rsb-btn rsb-btn-primary" id="rsb-copy-btn" onclick="rsbCopyLink()" title="Copy share link">&#128279; Copy Link</button>
  </div>
</div>
<!-- Mobile nav drawer (toggled by hamburger) -->
<div class="mobile-nav-drawer" id="mobile-nav-drawer">
  <a href="/pricing">Pricing</a>
  <a href="/markets">Markets</a>
  <a href="/find-deals">Find Deals</a>
  <a href="/pipeline">Pipeline</a>
  <div class="m-divider"></div>
  <a href="/login">Sign In</a>
  <button class="btn-primary-m" onclick="newAnalysis();document.getElementById('mobile-nav-drawer').classList.remove('open');">+ New Analysis</button>
</div>

<!-- ── Cmd+K Command Palette (#167) ── -->
<div class="kpal-backdrop" id="kpal-backdrop" onclick="kpalClose(event)">
  <div class="kpal-modal" role="dialog" aria-label="Command palette">
    <div class="kpal-search-row">
      <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
      <input class="kpal-input" id="kpal-input" placeholder="Search commands, deals, pages..." autocomplete="off" spellcheck="false" oninput="kpalFilter()" onkeydown="kpalKey(event)">
      <span class="kpal-kbd">ESC</span>
    </div>
    <div class="kpal-results" id="kpal-results"></div>
    <div class="kpal-footer">
      <span><kbd>↑↓</kbd> navigate</span>
      <span><kbd>↵</kbd> select</span>
      <span><kbd>Esc</kbd> close</span>
      <span style="margin-left:auto;">&#9881; ClearEye</span>
    </div>
  </div>
</div>

<!-- ── Keyboard Shortcut Cheatsheet Modal (#204) ── -->
<div class="sc-backdrop" id="sc-backdrop" onclick="scBackdropClick(event)">
  <div class="sc-modal" role="dialog" aria-label="Keyboard shortcuts">
    <div class="sc-header">
      <span class="sc-title">Keyboard Shortcuts</span>
      <button class="sc-close" onclick="scClose()" aria-label="Close">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M18 6 6 18M6 6l12 12"/></svg>
      </button>
    </div>
    <div class="sc-body">
      <div class="sc-section">
        <div class="sc-section-label">Navigation</div>
        <div class="sc-grid">
          <div class="sc-row"><span class="sc-action">Command Palette</span><div class="sc-keys"><span class="sc-key amber">⌘</span><span class="sc-plus">+</span><span class="sc-key amber">K</span></div></div>
          <div class="sc-row"><span class="sc-action">Keyboard Shortcuts</span><div class="sc-keys"><span class="sc-key amber">⌘</span><span class="sc-plus">+</span><span class="sc-key amber">/</span></div></div>
          <div class="sc-row"><span class="sc-action">New Analysis</span><div class="sc-keys"><span class="sc-key">N</span></div></div>
          <div class="sc-row"><span class="sc-action">Go to Pipeline</span><div class="sc-keys"><span class="sc-key">G</span><span class="sc-plus">then</span><span class="sc-key">P</span></div></div>
          <div class="sc-row"><span class="sc-action">Go to Markets</span><div class="sc-keys"><span class="sc-key">G</span><span class="sc-plus">then</span><span class="sc-key">M</span></div></div>
          <div class="sc-row"><span class="sc-action">Go to Find Deals</span><div class="sc-keys"><span class="sc-key">G</span><span class="sc-plus">then</span><span class="sc-key">F</span></div></div>
        </div>
      </div>
      <div class="sc-section">
        <div class="sc-section-label">Analysis</div>
        <div class="sc-grid">
          <div class="sc-row"><span class="sc-action">Submit Analysis</span><div class="sc-keys"><span class="sc-key amber">⌘</span><span class="sc-plus">+</span><span class="sc-key amber">↵</span></div></div>
          <div class="sc-row"><span class="sc-action">Focus OM Input</span><div class="sc-keys"><span class="sc-key">I</span></div></div>
          <div class="sc-row"><span class="sc-action">Switch Tab →</span><div class="sc-keys"><span class="sc-key">]</span></div></div>
          <div class="sc-row"><span class="sc-action">Switch Tab ←</span><div class="sc-keys"><span class="sc-key">[</span></div></div>
          <div class="sc-row"><span class="sc-action">Scenario Planner</span><div class="sc-keys"><span class="sc-key">S</span></div></div>
          <div class="sc-row"><span class="sc-action">Copy Report Link</span><div class="sc-keys"><span class="sc-key amber">⌘</span><span class="sc-plus">+</span><span class="sc-key amber">C</span></div></div>
        </div>
      </div>
      <div class="sc-section">
        <div class="sc-section-label">General</div>
        <div class="sc-grid">
          <div class="sc-row"><span class="sc-action">Close / Dismiss</span><div class="sc-keys"><span class="sc-key">Esc</span></div></div>
          <div class="sc-row"><span class="sc-action">Search History</span><div class="sc-keys"><span class="sc-key amber">⌘</span><span class="sc-plus">+</span><span class="sc-key amber">F</span></div></div>
          <div class="sc-row"><span class="sc-action">Export PDF</span><div class="sc-keys"><span class="sc-key amber">⌘</span><span class="sc-plus">+</span><span class="sc-key amber">E</span></div></div>
          <div class="sc-row"><span class="sc-action">Toggle Dark / Light</span><div class="sc-keys"><span class="sc-key amber">⌘</span><span class="sc-plus">+</span><span class="sc-key amber">D</span></div></div>
        </div>
      </div>
    </div>
    <div class="sc-footer">Press <span style="font-family:var(--mono);color:var(--accent)">?</span> or <span style="font-family:var(--mono);color:var(--accent)">⌘/</span> to open · <span style="font-family:var(--mono);color:var(--accent)">Esc</span> to close</div>
  </div>
</div>

<!-- ── #218: Override Audit Rationale Modal ── -->
<div id="oa-backdrop" style="position:fixed;inset:0;background:rgba(0,0,0,.65);backdrop-filter:blur(6px);z-index:9200;display:none;align-items:center;justify-content:center;" onclick="if(event.target===this)oaClose()">
  <div style="background:var(--bg-surface);border:1px solid rgba(248,81,73,.3);border-top:3px solid #f85149;border-radius:12px;padding:24px 26px;max-width:460px;width:94%;position:relative;">
    <div style="font-family:var(--mono);font-size:9px;letter-spacing:.12em;color:#f85149;text-transform:uppercase;margin-bottom:8px;">&#9888; ClearEye Override Audit</div>
    <div style="font-family:var(--font-display);font-style:italic;font-size:1.1rem;color:var(--text-primary);margin-bottom:6px;">Document your override rationale</div>
    <div id="oa-flag-preview" style="font-size:12px;color:var(--text-secondary);line-height:1.5;margin-bottom:14px;padding:10px 12px;background:rgba(248,81,73,.05);border:1px solid rgba(248,81,73,.15);border-radius:6px;"></div>
    <label style="font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);display:block;margin-bottom:5px;">Why are you proceeding despite this risk? <span style="color:#f85149;">*</span></label>
    <textarea id="oa-rationale" style="width:100%;background:rgba(8,11,16,.9);border:1px solid rgba(255,255,255,.1);border-radius:7px;color:var(--text-primary);padding:10px 12px;font-size:12px;font-family:var(--font);line-height:1.5;resize:vertical;min-height:80px;transition:border-color var(--t);" placeholder="e.g. Site visit confirmed rent comps support the projection. Local property manager has track record in submarket..." oninput="oaCheckLength()"></textarea>
    <div id="oa-char-hint" style="font-size:10px;color:var(--text-muted);margin-top:4px;margin-bottom:14px;">Minimum 20 characters required</div>
    <div style="display:flex;gap:8px;justify-content:flex-end;">
      <button onclick="oaClose()" style="padding:8px 16px;font-size:12px;background:none;border:1px solid rgba(255,255,255,.12);color:var(--text-secondary);border-radius:6px;cursor:pointer;">Cancel</button>
      <button id="oa-confirm-btn" onclick="oaConfirm()" disabled style="padding:8px 20px;font-size:12px;font-weight:700;background:rgba(248,81,73,.15);border:1px solid rgba(248,81,73,.4);color:#f85149;border-radius:6px;cursor:pointer;transition:background .15s,opacity .15s;opacity:.4;" onmouseover="if(!this.disabled)this.style.background='rgba(248,81,73,.25)'" onmouseout="this.style.background='rgba(248,81,73,.15)'">Override &amp; Re-analyze &rarr;</button>
    </div>
  </div>
</div>

<!-- ── Inline comparison drawer (#168) ── -->
<div class="cmp-drawer" id="cmp-drawer">
  <div class="cmp-drawer-header">
    <span class="cmp-drawer-title">&#9878; Compare Deals</span>
    <div class="cmp-drawer-actions">
      <a id="cmp-full-link" href="/compare" target="_blank" class="cmp-full-btn">Open full comparison &#8599;</a>
      <button class="cmp-close-btn" onclick="cmpDrawerClose()" title="Close">&#10005;</button>
    </div>
  </div>
  <div class="cmp-drawer-body" id="cmp-drawer-body">
    <div style="color:var(--text-muted);font-size:12px;text-align:center;padding:16px;">Select 2 deals from history to compare</div>
  </div>
</div>

<!-- First-run onboarding banner (#170) -->
<div id="onboarding-banner">
  <button class="ob-close" onclick="dismissOnboarding()" title="Dismiss">&#x2715;</button>
  <div class="ob-title">&#128075; Welcome to ClearEye</div>
  <div class="ob-sub">Your AI-powered real estate deal intelligence platform — here&rsquo;s how to get started.</div>
  <div class="ob-steps">
    <div class="ob-step">
      <div class="ob-num">1</div>
      <div class="ob-step-text"><strong>Paste an offering memo</strong>Drop a URL or paste OM text into the left panel, then hit Analyze.</div>
    </div>
    <div class="ob-step">
      <div class="ob-num">2</div>
      <div class="ob-step-text"><strong>Get your verdict</strong>5 adversarial AI advisors stress-test the deal in ~90 seconds.</div>
    </div>
    <div class="ob-step">
      <div class="ob-num">3</div>
      <div class="ob-step-text"><strong>Save to pipeline</strong>Track deals through Screening &rarr; LOI &rarr; Due Diligence &rarr; Closed.</div>
    </div>
  </div>
  <div class="ob-cta">
    <button class="ob-cta-btn" onclick="loadDemo();dismissOnboarding()">&#9654; Try a sample deal</button>
    <span class="ob-dismiss-lnk" onclick="dismissOnboarding()">I know what I&rsquo;m doing</span>
  </div>
</div>

<!-- #264: Toast notification container -->
<div id="toast-container" role="alert" aria-live="assertive" aria-atomic="true"></div>

<!-- #258: Mobile sidebar drawer backdrop + FAB -->
<div id="sidebar-backdrop" onclick="_sidebarClose()"></div>
<button id="sidebar-fab" onclick="_sidebarToggle()" aria-label="Toggle deal history" aria-expanded="false" aria-controls="history-list">
  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
  History
</button>

<!-- 3-panel layout (#81) -->
<div class="ce-layout">

  <!-- ── Sidebar ── -->
  <div class="ce-sidebar">
    <button class="ce-sb-link active" onclick="newAnalysis()">
      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="4"/><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/></svg>
      New Analysis
    </button>
    <div class="ce-sb-section">Recent Deals</div>
    <!-- History search + filters (#195, styled #206) -->
    <div style="padding:0 2px 6px;display:flex;flex-direction:column;gap:4px;">
      <div style="position:relative;">
        <svg style="position:absolute;left:7px;top:50%;transform:translateY(-50%);pointer-events:none;opacity:.4;" width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.35-4.35"/></svg>
        <input id="hist-search" type="search" placeholder="Search deals…" oninput="filterHistDom(this.value)"
          style="width:100%;box-sizing:border-box;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);border-radius:6px;color:var(--text-primary);font-family:var(--mono);font-size:11px;padding:5px 28px 5px 24px;outline:none;transition:border-color .15s,box-shadow .15s;"
          onfocus="this.style.borderColor='rgba(232,160,32,.5)';this.style.boxShadow='0 0 0 3px rgba(232,160,32,.1)'"
          onblur="this.style.borderColor='rgba(255,255,255,.08)';this.style.boxShadow='none'" />
        <button id="hist-search-clear" onclick="document.getElementById('hist-search').value='';filterHistDom('')" style="display:none;position:absolute;right:6px;top:50%;transform:translateY(-50%);background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:12px;line-height:1;padding:0 2px;">&#215;</button>
      </div>
      <div style="display:flex;gap:4px;">
        <select id="hist-verdict" onchange="renderHist()"
          style="flex:1;background:#0d1117;border:1px solid #21262d;border-radius:5px;color:#8b949e;font-size:11px;padding:3px 4px;cursor:pointer;">
          <option value="all">All verdicts</option>
          <option value="go">GO</option>
          <option value="no-go">NO-GO</option>
          <option value="conditional">Conditional</option>
        </select>
        <select id="hist-days" onchange="renderHist()"
          style="flex:1;background:#0d1117;border:1px solid #21262d;border-radius:5px;color:#8b949e;font-size:11px;padding:3px 4px;cursor:pointer;">
          <option value="0">All time</option>
          <option value="7">Last 7d</option>
          <option value="30">Last 30d</option>
          <option value="90">Last 90d</option>
        </select>
      </div>
    </div>
    <div id="history-list"><div style="font-size:12px;color:#484f58;padding:6px 10px;">No analyses yet</div></div>
    <button id="cmp-launch-btn" style="display:none;width:100%;margin-top:6px;padding:6px 10px;background:#1c2d3e;border:1px solid #58a6ff;color:#58a6ff;border-radius:6px;font-size:12px;cursor:pointer;" onclick="launchCompare()">&#9878; Compare Selected (2)</button>
    <button class="btn-ghost mt-1 w-100" onclick="clearHistory()" id="clearHistBtn" style="display:none;font-size:11px;">Clear History</button>

    <!-- Market Pulse Widget (#185) -->
    <div id="market-pulse-widget" style="margin-top:12px;border-top:1px solid var(--border-muted);padding-top:10px;">
      <div style="display:flex;align-items:center;justify-content:space-between;cursor:pointer;padding:2px 0;" onclick="togglePulse()">
        <span style="font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);">&#127758; Market Pulse</span>
        <span id="pulse-toggle-arrow" style="font-size:10px;color:var(--text-muted);">&#9650;</span>
      </div>
      <div id="pulse-body" style="margin-top:6px;">
        <div id="pulse-loading" style="font-size:11px;color:var(--text-muted);padding:4px 0;">Loading...</div>
        <div id="pulse-content" style="display:none;">
          <div id="pulse-rows"></div>
          <div id="pulse-headwind" style="margin-top:8px;padding:7px 10px;border-radius:6px;font-size:11px;"></div>
          <div id="pulse-footer" style="font-size:10px;color:var(--text-muted);margin-top:4px;text-align:right;"></div>
        </div>
        <div id="pulse-error" style="display:none;font-size:11px;color:var(--text-muted);">Data unavailable</div>
      </div>
    </div>
  </div>

  <!-- ── Input Panel ── -->
  <div class="ce-input">
    <!-- Mobile toggle (only visible on small screens) -->
    <div class="mobile-input-toggle" id="mobile-input-toggle" onclick="toggleMobileInput()">
      <span>&#128203; Submit a Deal</span>
      <span class="mit-arrow">&#9654;</span>
    </div>
    <div id="mobile-input-body">
    <div class="ce-card">
      <div class="ce-card-hdr">
        <strong style="font-size:14px;font-weight:700;letter-spacing:-.2px;">Submit a Deal</strong>
        <div style="font-size:11px;color:var(--text-secondary);margin-top:3px;">Paste an offering memorandum or deal summary</div>
      </div>
      <div class="ce-card-body">
        <!-- #268: Clean single paste zone — URL/PDF as secondary links -->
        <!-- Hidden URL and PDF panels (still functional via links below) -->
        <div id="url-panel" style="display:none;margin-bottom:10px;">
          <div style="display:flex;gap:6px;">
            <input id="listing-url" type="url" placeholder="https://www.loopnet.com/Listing/..."
              style="flex:1;background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-primary);border-radius:6px;padding:7px 10px;font-size:12px;"
              onkeydown="if(event.key==='Enter')fetchListingUrl()">
            <button onclick="fetchListingUrl()" id="fetch-url-btn"
              style="padding:7px 14px;background:var(--accent);border:none;color:#fff;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;white-space:nowrap;">
              Fetch &rarr;
            </button>
          </div>
          <div id="url-status" style="font-size:11px;color:var(--text-muted);margin-top:5px;min-height:14px;"></div>
          <div id="url-preview" style="display:none;margin-top:8px;padding:8px;background:var(--bg-elevated);border-radius:5px;border:1px solid var(--border-default);font-size:11px;color:var(--text-secondary);line-height:1.6;"></div>
        </div>
        <div id="pdf-panel" style="display:none;margin-bottom:10px;">
          <div id="drop-zone" style="border:2px dashed var(--border-emphasis);border-radius:8px;padding:14px 12px;text-align:center;cursor:pointer;font-size:12px;color:var(--text-muted);transition:border-color .2s;"
            onclick="document.getElementById('pdf-input').click()"
            ondragover="event.preventDefault();this.style.borderColor='var(--accent)'"
            ondragleave="this.style.borderColor='var(--border-emphasis)'"
            ondrop="handleDrop(event)">
            &#128196; Drop PDF here or <span style="color:var(--accent);">click to upload</span>
            <input type="file" id="pdf-input" accept=".pdf" style="display:none" onchange="uploadPDF(this.files[0])">
          </div>
        </div>
        <div id="pdf-status" style="font-size:11px;color:var(--text-muted);margin-bottom:4px;min-height:16px;"></div>

        <!-- Primary: clean textarea -->
        <textarea id="om_input" class="form-control mb-2" rows="11"
          placeholder="Paste offering memorandum text here...&#10;&#10;Ctrl+Enter to analyze"
          onkeydown="handleKey(event)" oninput="checkInput();_qsSchedule()"></textarea>

        <!-- Secondary input options as text links -->
        <div style="display:flex;gap:14px;margin-bottom:10px;font-size:11px;" id="input-alt-links">
          <a href="#" onclick="setInputTab('url');return false;" id="tab-url" style="color:var(--text-muted);text-decoration:none;">or analyze a URL</a>
          <a href="#" onclick="setInputTab('pdf');return false;" id="tab-pdf" style="color:var(--text-muted);text-decoration:none;">upload a PDF</a>
          <span id="tab-text" style="display:none;"></span><!-- compat stub -->
        </div>
        <!-- Keep hidden input-tabs div for setInputTab() compat -->
        <div id="input-tabs" style="display:none;"></div>

        <!-- #253: Quick-scan deal-breaker badges -->
        <div id="qs-badges" style="display:none;margin-bottom:8px;"></div>
        <div id="missing-warn" style="display:none;font-size:11px;color:#d29922;margin-bottom:6px;padding:4px 8px;background:rgba(210,153,34,.08);border-radius:4px;border-left:2px solid #d29922;"></div>
        <div id="quota-warn-banner" style="display:none;font-size:12px;margin-bottom:8px;padding:10px 14px;background:rgba(21,94,68,.05);border:1px solid rgba(21,94,68,.2);border-radius:7px;display:flex;align-items:center;justify-content:space-between;gap:10px;">
          <span>&#9889; <strong>Last analysis</strong> this month — upgrade to keep going</span>
          <a href="/pricing" style="font-size:11px;font-weight:700;color:var(--accent);text-decoration:none;white-space:nowrap;flex-shrink:0;">See plans &rarr;</a>
        </div>

        <!-- Demo hint — smaller, secondary -->
        <div id="demoBtn" style="margin-bottom:10px;">
          <div style="display:inline-flex;align-items:center;gap:6px;padding:5px 10px;background:transparent;border:1px solid var(--border-muted);border-radius:5px;cursor:pointer;transition:background .15s;" onclick="loadDemoAndRun()" onmouseover="this.style.background='var(--bg-elevated)'" onmouseout="this.style.background='transparent'">
            <span style="font-size:11px;">&#9654;</span>
            <div>
              <span style="font-size:11px;color:var(--text-muted);">Try a demo deal</span>
              <span style="font-size:10px;color:var(--text-muted);opacity:.65;"> &middot; Phoenix AZ, 48 units</span>
            </div>
            <span style="font-size:10px;color:var(--text-muted);">&#8594;</span>
          </div>
        </div>
        <!-- Sample deal active banner (#216) -->
        <div id="sample-deal-banner" style="display:none;margin-bottom:8px;padding:7px 10px;background:rgba(21,94,68,.06);border:1px solid rgba(21,94,68,.2);border-radius:5px;">
          <div style="display:flex;align-items:center;gap:6px;">
            <span style="font-size:11px;color:#79c0ff;">&#128202; Viewing sample deal</span>
            <span style="flex:1;"></span>
            <button onclick="clearSampleAndFocus()" style="font-size:10px;color:var(--accent);background:none;border:none;cursor:pointer;padding:2px 6px;border-radius:3px;font-weight:600;">Analyze your own deal &rarr;</button>
          </div>
        </div>

        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Email results to (optional)</label>
        <input type="email" id="email_input" class="form-control mb-3" placeholder="investor@example.com">

        <div style="display:flex;gap:8px;margin-bottom:0;">
          <button class="btn btn-primary" onclick="startAnalyze()" id="analyzeBtn"
            style="flex:1;padding:12px;font-size:14px;font-weight:700;border-radius:8px;letter-spacing:.2px;position:relative;overflow:hidden;">
            &#9881; Analyze Deal
          </button>
          <button onclick="quickScan()" id="quickScanBtn" title="30-second pre-screen: deal-breakers + plausibility check"
            style="padding:10px 14px;font-size:12px;font-weight:600;background:var(--bg-surface);border:1px solid var(--border-default);color:var(--text-secondary);border-radius:8px;cursor:pointer;white-space:nowrap;transition:all .2s;font-family:var(--font);"
            onmouseover="this.style.background='var(--bg-elevated)';this.style.borderColor='var(--border-emphasis)';this.style.color='var(--text-primary)'" onmouseout="this.style.background='var(--bg-surface)';this.style.borderColor='var(--border-default)';this.style.color='var(--text-secondary)'">
            &#9889; Quick Scan
          </button>
        </div>
        <!-- #228: Quick scan result card -->
        <div id="qs-result" style="display:none;margin-top:10px;border-radius:8px;padding:10px 14px;font-size:12px;"></div>
        <div id="status-msg" class="mt-2 text-center" style="font-size:12px;min-height:18px;" role="status" aria-live="polite" aria-atomic="true"></div>

        <!-- Progress tracker (#85, redesigned #162) -->
        <div class="prog-track" id="prog" style="display:none;">
          <div class="prog-step" id="pstep0">
            <div class="ps-indicator pi-idle" id="pi0"><span class="ps-num">1</span></div>
            <div class="ps-content">
              <div class="ps-label idle">Parsing deal</div>
              <div class="ps-sub">Extracting numbers, assumptions &amp; structure</div>
            </div>
          </div>
          <div class="prog-step" id="pstep1">
            <div class="ps-indicator pi-idle" id="pi1"><span class="ps-num">2</span></div>
            <div class="ps-content">
              <div class="ps-label idle">Running analysis modules</div>
              <div class="ps-sub">Stress test &middot; Assumption audit &middot; Macro &middot; Pre-mortem</div>
            </div>
          </div>
          <div class="prog-step" id="pstep2">
            <div class="ps-indicator pi-idle" id="pi2"><span class="ps-num">3</span></div>
            <div class="ps-content">
              <div class="ps-label idle">5 advisors deliberating</div>
              <div class="ps-sub" id="advisor-substeps">Bear Case &middot; Tax &middot; Market &middot; Bias &middot; Exit</div>
            </div>
          </div>
          <div class="prog-step" id="pstep3">
            <div class="ps-indicator pi-idle" id="pi3"><span class="ps-num">4</span></div>
            <div class="ps-content">
              <div class="ps-label idle">Chairman synthesizing verdict</div>
              <div class="ps-sub">Go / No-Go &middot; Confidence score &middot; Investment memo</div>
            </div>
          </div>
        </div>

        <hr style="border-color:#21262d;margin:16px 0 10px;">
        <div style="font-size:11px;color:#484f58;line-height:1.7;">
          Stress test &middot; Assumption audit &middot; Macro &middot; Bias detection &middot; Pre-mortem &middot; 5 adversarial advisors &middot; Chairman &rarr; Go/No-Go<br>
          <span style="color:#8b949e;">~90 seconds</span>
        </div>
      </div>
    </div>
    </div><!-- /mobile-input-body -->
  </div>

  <!-- ── Results Panel ── -->
  <div class="ce-results" id="results-panel">

    <!-- Empty state (#95) -->
    <div id="empty-state" class="empty-state">
      <div class="em-hero-icon" style="font-size:56px;margin-bottom:16px;filter:drop-shadow(0 0 22px rgba(232,160,32,.35));">&#128065;</div>
      <div style="font-family:var(--font-display);font-style:italic;font-size:22px;font-weight:400;color:var(--text-primary);letter-spacing:-.02em;line-height:1.2;">Paste an OM to begin your analysis</div>
      <div style="font-size:12px;color:var(--text-muted);margin-top:10px;line-height:1.8;font-family:var(--mono);letter-spacing:.04em;font-size:10.5px;text-transform:uppercase;">
        5 adversarial advisors &nbsp;&bull;&nbsp; Go/No-Go verdict &nbsp;&bull;&nbsp; 7 modules &nbsp;&bull;&nbsp; ~90s
      </div>
      <div class="em-grid" style="margin-top:28px;">
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(232,160,32,.55));">&#128202;</div>Stress Test</div>
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(63,185,80,.55));">&#9989;</div>Audit</div>
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(163,113,247,.55));">&#129504;</div>Bias Scan</div>
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(248,81,73,.55));">&#128128;</div>Pre-Mortem</div>
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(232,160,32,.5));">&#128200;</div>Macro</div>
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(248,81,73,.45));">&#128059;</div>Bear Advisor</div>
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(63,185,80,.45));">&#9878;</div>Tax Advisor</div>
        <div class="em-box"><div class="em-icon" style="filter:drop-shadow(0 0 7px rgba(232,160,32,.45));">&#128682;</div>Exit Advisor</div>
      </div>
      <button class="btn-ghost" onclick="loadDemoAndRun()" style="margin-top:16px;font-size:12px;">
        &#9654;&nbsp; Try a Demo Deal (Sunset Ridge, Phoenix)
      </button>
    </div>

    <!-- Skeleton loading state (#iter3) — shown during analysis, before results arrive -->
    <div id="results-skeleton" style="display:none;padding:4px 0;">
      <div class="sk sk-verdict"></div>
      <div class="sk-card">
        <div class="sk-grid">
          <div class="sk sk-grid-item"></div>
          <div class="sk sk-grid-item"></div>
          <div class="sk sk-grid-item"></div>
          <div class="sk sk-grid-item"></div>
        </div>
      </div>
      <div class="sk-card">
        <div class="sk sk-block sk-w30" style="margin-bottom:12px;height:10px;"></div>
        <div class="sk sk-adv"></div>
        <div class="sk sk-adv"></div>
        <div class="sk sk-adv" style="height:70px;"></div>
      </div>
      <div class="sk-card">
        <div class="sk sk-block sk-w30" style="margin-bottom:12px;height:10px;"></div>
        <div class="sk sk-block sk-w100"></div>
        <div class="sk sk-block sk-w75"></div>
        <div class="sk sk-block sk-w50"></div>
      </div>
    </div>

    <!-- Results -->
    <div id="results" style="display:none;">

      <!-- #216: Demo mode CTA banner — shown when _isDemoMode is true -->
      <div id="demo-results-banner" style="display:none;margin-bottom:12px;padding:10px 14px;background:rgba(88,166,255,.06);border:1px solid rgba(88,166,255,.2);border-radius:7px;">
        <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
          <span style="font-size:12px;color:#79c0ff;font-weight:500;">&#128202; This is a sample deal analysis</span>
          <span style="font-size:11px;color:var(--text-muted);flex:1;">See exactly what ClearEye delivers in 90 seconds — ready to try your own deal?</span>
          <button onclick="clearSampleAndFocus()" style="font-size:11px;color:var(--accent);font-weight:600;background:rgba(232,160,32,.1);border:1px solid rgba(232,160,32,.3);border-radius:5px;padding:5px 12px;cursor:pointer;white-space:nowrap;transition:background .15s;" onmouseover="this.style.background='rgba(232,160,32,.2)'" onmouseout="this.style.background='rgba(232,160,32,.1)'">Analyze your own deal &rarr;</button>
        </div>
      </div>

      <!-- Results header strip (shows deal name + timestamp on load) -->
      <div id="results-header" style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;padding:0 2px;">
        <div style="font-size:11px;color:var(--text-muted);" id="results-ts"></div>
        <div style="display:flex;gap:6px;" id="results-actions"></div>
      </div>

      <!-- Verdict banner — centered hero layout (#86, #266) -->
      <div class="verdict-wrap" id="verdict-banner">
        <div id="verdict-name"></div>
        <div id="verdict-stamp"></div>
        <div id="verdict-reason-line"></div>
        <!-- Confidence ring + detail row -->
        <div style="display:flex;align-items:center;justify-content:center;gap:20px;margin-top:16px;flex-wrap:wrap;">
          <div class="conf-ring-wrap">
            <svg width="80" height="80" viewBox="0 0 80 80">
              <circle class="cr-track" cx="40" cy="40" r="33"/>
              <circle class="cr-fill" id="cr-arc" cx="40" cy="40" r="33" stroke-dasharray="207.3" stroke-dashoffset="207.3"/>
            </svg>
            <div class="cr-label" id="cr-label">0%</div>
          </div>
          <div style="max-width:340px;text-align:left;">
            <div id="verdict-rat" style="font-size:12px;color:var(--text-secondary);line-height:1.6;"></div>
            <div id="email-status" style="font-size:11px;color:var(--green);margin-top:4px;"></div>
          </div>
        </div>
        <div class="d-flex gap-2 flex-wrap mt-3" id="metric-chips" style="justify-content:center;"></div>
      </div>

      <!-- Summary bar (#98) -->
      <div class="sum-bar" id="sum-bar"></div>

      <!-- Deal summary card (#87) -->
      <div class="ce-card mb-3 s1" id="deal-card" style="display:none;">
        <div class="ce-card-hdr" style="padding:8px 14px;font-size:10px;font-weight:700;color:var(--text-secondary);letter-spacing:.8px;text-transform:uppercase;display:flex;align-items:center;justify-content:space-between;">
          <span>&#128196; Parsed Deal Summary</span>
          <button onclick="toggleAssumptionEditor()" id="edit-assump-btn" style="font-size:10px;padding:2px 8px;border:1px solid var(--border-default);background:none;color:var(--accent);border-radius:4px;cursor:pointer;transition:all var(--t);">&#9998; Edit Assumptions</button>
        </div>
        <div style="padding:12px 14px;"><div class="stat-grid" id="stat-grid"></div></div>
        <!-- Assumption editor (#117) -->
        <div id="assump-editor" style="display:none;padding:0 14px 14px;">
          <div style="font-size:11px;color:var(--text-secondary);margin-bottom:8px;border-top:1px solid var(--border-muted);padding-top:10px;">Edit parsed assumptions and re-run analysis:</div>
          <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;">
            <div><label style="font-size:10px;color:var(--text-secondary);font-weight:500;display:block;margin-bottom:2px;">Cap Rate (%)</label><input id="ae-cap" type="number" step="0.1" style="width:100%;background:var(--bg-canvas);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:5px 8px;font-size:12px;" placeholder="5.2"></div>
            <div><label style="font-size:10px;color:var(--text-secondary);font-weight:500;display:block;margin-bottom:2px;">Proj. IRR (%)</label><input id="ae-irr" type="number" step="0.1" style="width:100%;background:var(--bg-canvas);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:5px 8px;font-size:12px;" placeholder="18.2"></div>
            <div><label style="font-size:10px;color:var(--text-secondary);font-weight:500;display:block;margin-bottom:2px;">NOI ($)</label><input id="ae-noi" type="number" step="1000" style="width:100%;background:var(--bg-canvas);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:5px 8px;font-size:12px;" placeholder="962000"></div>
            <div><label style="font-size:10px;color:var(--text-secondary);font-weight:500;display:block;margin-bottom:2px;">Asking Price ($)</label><input id="ae-price" type="number" step="10000" style="width:100%;background:var(--bg-canvas);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:5px 8px;font-size:12px;" placeholder="18500000"></div>
            <div><label style="font-size:10px;color:var(--text-secondary);font-weight:500;display:block;margin-bottom:2px;">Exit Cap Rate (%)</label><input id="ae-exitcap" type="number" step="0.1" style="width:100%;background:var(--bg-canvas);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:5px 8px;font-size:12px;" placeholder="4.8"></div>
            <div><label style="font-size:10px;color:var(--text-secondary);font-weight:500;display:block;margin-bottom:2px;">Rent Growth (%/yr)</label><input id="ae-rg" type="number" step="0.1" style="width:100%;background:var(--bg-canvas);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:5px 8px;font-size:12px;" placeholder="4.5"></div>
          </div>
          <button onclick="reAnalyzeWithEdits()" style="margin-top:10px;padding:6px 14px;background:rgba(88,166,255,.08);border:1px solid var(--accent);color:var(--accent);border-radius:var(--r-sm);font-size:12px;cursor:pointer;font-weight:500;transition:all var(--t);">&#9654; Re-analyze with Changes</button>
          <span id="ae-status" style="font-size:11px;color:var(--text-secondary);margin-left:8px;"></span>
        </div>
      </div>

      <!-- #245: Devil's Advocate panel — adversarial failure analysis -->
      <div id="devil-advocate-panel" style="display:none;margin-bottom:12px;"></div>

      <!-- Market Rent Context (#171) — auto-populated from RentCast/static benchmarks -->
      <div id="rent-context-card" class="ce-card mb-3 s1" style="display:none;padding:12px 14px;"></div>

      <!-- ATTOM Property Data (#175) — last sale, assessed value, tax, comps -->
      <div id="attom-card" class="ce-card mb-3 s1" style="display:none;padding:12px 14px;"></div>

      <!-- Data source badge (#122) -->
      <div id="data-badge" style="display:none;margin-bottom:8px;font-size:11px;color:#484f58;">
        Market data: <span id="data-src-text"></span>
      </div>

      <!-- Tabs (#91, #121 executive summary) + content -->
      <div class="ce-card s2">
        <div class="ce-tabs" id="ce-tabs">
          <button class="ce-tab active" onclick="showTab('summary',this)">&#9889; Summary</button>
          <button class="ce-tab" onclick="showTab('advisors',this)">&#129504; Council <span class="tab-bdg" style="background:var(--accent-dim);color:var(--accent);" id="bdg-adv">5</span></button>
          <button class="ce-tab" onclick="showTab('memo',this)">&#128196; Memo</button>
          <button class="ce-tab" onclick="showTab('diligence',this)" id="tab-diligence-btn">&#128269; Diligence <span class="tab-bdg" id="bdg-diligence" style="display:none;"></span></button>
          <button class="ce-tab" onclick="showTab('data',this);loadComps();renderScenarioPlanner();" id="tab-data-btn">&#128202; Data</button>
          <div id="tab-indicator"></div>
        </div>
        <div class="tab-content-wrap">
          <!-- Summary tab -->
          <div id="tab-summary"><div id="ips-check-panel"></div><div id="sponsor-track-card" style="display:none;margin-bottom:14px;"></div><div id="comp-panel" style="display:none;"></div><div id="summary-content"></div></div>
          <!-- Council tab (was Advisors) -->
          <div id="tab-advisors" style="display:none;">
            <div id="advisor-radar-wrap" style="display:none;text-align:center;margin-bottom:16px;"></div>
            <div id="adv-content"></div>
          </div>
          <!-- Memo tab -->
          <div id="tab-memo" style="display:none;"><div class="memo-text" id="memo-content"></div></div>
          <!-- Diligence tab: Audit + Bias + Pre-Mortem + Macro (merged) -->
          <div id="tab-diligence" style="display:none;">
            <!-- Audit section -->
            <div class="dd-section-hdr">&#9989; Audit</div>
            <div id="audit-content"></div>
            <!-- Bias section -->
            <div class="dd-section-hdr" style="margin-top:20px;">&#128269; Assumption Bias</div>
            <div id="bias-killshot" style="display:none;margin-bottom:14px;"></div>
            <div id="bias-flags-structured" style="display:none;"></div>
            <div id="bias-version-timeline" style="display:none;margin-top:14px;padding-top:12px;border-top:1px solid var(--border-muted);"></div>
            <div id="bias-portfolio-note" style="display:none;margin-bottom:10px;"></div>
            <div id="geo-risk-panel" style="display:none;margin-top:14px;"></div>
            <div id="assumption-evidence-panel" style="display:none;margin-top:14px;"></div>
            <div id="override-audit-trail" style="display:none;margin-top:14px;padding-top:12px;border-top:1px solid var(--border-muted);"></div>
            <pre class="mono" id="bias-content" style="display:none;"></pre>
            <!-- Pre-Mortem section -->
            <div class="dd-section-hdr" style="margin-top:20px;">&#128128; Pre-Mortem</div>
            <pre class="mono" id="premortem-content"></pre>
            <!-- Macro section -->
            <div class="dd-section-hdr" style="margin-top:20px;">&#128200; Macro Context</div>
            <div class="memo-text" id="macro-content"></div>
          </div>
          <!-- Data tab: Sensitivity + Comps + Scenarios (merged) -->
          <div id="tab-data" style="display:none;">
            <!-- Stress/Sensitivity section -->
            <div class="dd-section-hdr">&#128202; Sensitivity Analysis</div>
            <div class="chart-wrap"><canvas id="stress-chart"></canvas></div>
            <details style="margin-top:10px;"><summary style="font-size:11px;color:var(--text-muted);cursor:pointer;">Show raw data table</summary><pre class="mono mt-2" id="stress-raw"></pre></details>
            <!-- Comps section -->
            <div class="dd-section-hdr" style="margin-top:24px;">&#128205; Rent Comps</div>
            <div id="comps-content">
              <div style="color:var(--text-muted);font-size:12px;padding:16px 0;text-align:center;">
                <span style="animation:spin 1s linear infinite;display:inline-block;">&#8635;</span> Loading rent comps...
              </div>
            </div>
            <!-- Scenarios section -->
            <div class="dd-section-hdr" style="margin-top:24px;">&#127919; Scenario Planner</div>
            <div id="scenarios-content" style="padding:4px 0;">
              <div style="color:var(--text-muted);font-size:12px;padding:24px 0;text-align:center;">Run a deal analysis first to use the Scenario Planner.</div>
            </div>
          </div>
          <!-- Keep old single-purpose tab divs hidden for backward-compat JS that targets them -->
          <div id="tab-stress"    style="display:none;"></div>
          <div id="tab-audit"     style="display:none;"></div>
          <div id="tab-bias"      style="display:none;"></div>
          <div id="tab-premortem" style="display:none;"></div>
          <div id="tab-macro"     style="display:none;"></div>
          <div id="tab-comps"     style="display:none;"></div>
          <div id="tab-scenarios" style="display:none;"></div>
        </div>
      </div>

      <!-- Action bar: 4 core actions -->
      <div class="action-bar s3">
        <button class="action-btn action-btn-primary" onclick="shareLink()" id="shareBtn">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M4 12v8a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-8"/><polyline points="16 6 12 2 8 6"/><line x1="12" y1="2" x2="12" y2="15"/></svg>
          Share
        </button>
        <button class="action-btn" onclick="addToPipeline()" id="pipeBtn">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/></svg>
          Pipeline
        </button>
        <button class="action-btn" onclick="downloadPDF()" id="pdfBtn">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
          Export
        </button>
        <button class="action-btn" onclick="reanalyzeReport()" id="reanalyzeBtn">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="M23 4v6h-6"/><path d="M1 20v-6h6"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
          Re-analyze
        </button>
        <!-- Hidden buttons kept for JS compatibility -->
        <button id="copyBtn"       style="display:none;" onclick="copyMemo()"></button>
        <button id="lpBtn"         style="display:none;" onclick="openLpModal()"></button>
        <button id="daBtn"         style="display:none;" onclick="runDevilAdvocate()"></button>
        <button id="djNoteOpenBtn" style="display:none;" onclick="djNoteOpen()"></button>
        <button id="djLogBtn"      style="display:none;" onclick="djToggleLogPanel()"></button>
      </div>
      <div id="share-msg" style="font-size:11px;color:#3fb950;margin-top:4px;display:none;"></div>
      <!-- #221: Decision Journal note input + log -->
      <div class="dj-note-input-wrap s3" id="dj-note-input-wrap">
        <div style="font-size:10px;color:var(--amber);font-family:var(--mono);margin-bottom:6px;" id="dj-note-label">Note for: Summary</div>
        <textarea class="dj-note-textarea" id="dj-note-textarea" placeholder="Add your decision rationale, concerns, or observations for this tab..."></textarea>
        <div style="display:flex;gap:6px;justify-content:flex-end;margin-top:6px;">
          <button class="btn-ghost" style="font-size:11px;padding:4px 10px;" onclick="djNoteClose()">Cancel</button>
          <button class="btn-ghost" style="font-size:11px;padding:4px 10px;color:var(--amber);border-color:rgba(232,160,32,.4);" onclick="djNoteSave()">Save Note</button>
        </div>
      </div>
      <div id="dj-log-panel" class="dj-log-panel s3">
        <div class="dj-log-header" onclick="djToggleLog()">
          <span>&#128221; Decision Log</span>
          <span id="dj-log-count" style="font-size:10px;background:rgba(232,160,32,.12);padding:2px 8px;border-radius:10px;">0 notes</span>
        </div>
        <div class="dj-log-body" id="dj-log-body">
          <div style="color:var(--text-muted);font-size:12px;">No notes yet.</div>
        </div>
      </div>

    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<!-- Safe JSON data island — avoids regex parse ambiguity in inline JS (#166) -->
<script type="application/json" id="ce-prefill-data">{{ _prefill_job|default('null')|safe }}</script>
<script>
// ── Demo OM (#83) ─────────────────────────────────────────────────────────
const DEMO_OM = `OFFERING MEMORANDUM - Sunset Ridge Apartments
Phoenix, Arizona | 124 Units | Asking Price: $18,500,000

PROPERTY OVERVIEW
Sunset Ridge Apartments is a 124-unit garden-style multifamily community in the Ahwatukee submarket of Phoenix, AZ. Built 1986, currently 91% occupied, average in-place rent $1,245/unit/month.

INVESTMENT HIGHLIGHTS
- Going-in Cap Rate: 5.2% on current NOI of $962,000
- Projected IRR: 18.2% over a 5-year hold period
- Value-add: 80-unit renovation at $8,500/door, pushing rents to $1,525/unit
- Rent growth assumption: 4.5% annually (market trailing 12mo: 1.5%)
- Exit cap rate assumption: 4.8% (compression from current 5.2%)

FINANCIAL SUMMARY
- Asking Price: $18,500,000 ($149,193/unit)
- Current NOI: $962,000 | Pro Forma NOI: $1,480,000
- Loan: $12,950,000 (70% LTV) at 6.85% IO for 3 years
- Debt matures Year 3, planned exit Year 5 (refinance required)
- Projected Equity Multiple: 2.4x

SPONSOR
Apex Capital Group - 12 years in Phoenix MSA, 2,400 units under management.

EXIT STRATEGY
Sell at $25,200,000 in Year 5 at 4.8% exit cap on stabilized NOI of $1,210,000.`;

// ── History — server-side (#112) ──────────────────────────────────────────
async function loadHist(params){
  try{
    let url='/api/history';
    if(params){const qs=new URLSearchParams(params).toString();if(qs)url+='?'+qs;}
    const r=await fetch(url);
    return r.ok ? await r.json() : [];
  }catch{return [];}
}
// ── History filter state (#195) ───────────────────────────────────────────
let _histFilters={q:'',verdict:'',days:''};
function _readHistFilters(){
  const q=document.getElementById('hist-search');
  const v=document.getElementById('hist-verdict');
  const d=document.getElementById('hist-days');
  if(q)_histFilters.q=q.value.trim();
  if(v)_histFilters.verdict=v.value;
  if(d)_histFilters.days=d.value;
}
function _saveHistFilters(){
  try{localStorage.setItem('ce_hist_filters',JSON.stringify(_histFilters));}catch(e){}
}
function _loadHistFilters(){
  try{
    const s=localStorage.getItem('ce_hist_filters');
    if(s)_histFilters=JSON.parse(s);
    const q=document.getElementById('hist-search');
    const v=document.getElementById('hist-verdict');
    const d=document.getElementById('hist-days');
    if(q&&_histFilters.q)q.value=_histFilters.q;
    if(v&&_histFilters.verdict)v.value=_histFilters.verdict;
    if(d&&_histFilters.days)d.value=_histFilters.days;
  }catch(e){}
}
let _compareSelected=new Set();
// ── Score sparkline helper (#187) ──────────────────────────────────────────
function _sparklineSVG(scores){
  // Renders a 28×14 inline SVG polyline from an array of score values.
  // Returns empty string if fewer than 2 points.
  if(!scores||scores.length<2)return '';
  const W=28,H=14,pad=1;
  const min=Math.min(...scores),max=Math.max(...scores);
  const range=Math.max(max-min,1);
  const pts=scores.map((s,i)=>{
    const x=pad+(i/(scores.length-1))*(W-2*pad);
    const y=H-pad-(s-min)/range*(H-2*pad);
    return x.toFixed(1)+','+y.toFixed(1);
  }).join(' ');
  // Color: last>first=green, last<first=red, flat=gray
  const trend=scores[scores.length-1]-scores[0];
  const col=trend>2?'#3fb950':trend<-2?'#f85149':'#8b949e';
  const tip=scores.map((s,i)=>'Run '+(i+1)+': '+s+'/100').join(', ');
  return '<svg width="'+W+'" height="'+H+'" viewBox="0 0 '+W+' '+H+'" style="flex-shrink:0;vertical-align:middle;" title="'+tip+'">'
    +'<polyline points="'+pts+'" fill="none" stroke="'+col+'" stroke-width="1.5" stroke-linejoin="round" stroke-linecap="round"/>'
    +'<circle cx="'+(scores.length>1?(pad+(1/(scores.length-1))*(W-2*pad)*( scores.length-1)).toFixed(1):W/2)+'" cy="'+(H-pad-(scores[scores.length-1]-min)/range*(H-2*pad)).toFixed(1)+'" r="2" fill="'+col+'"/>'
    +'</svg>';
}

async function renderHist(){
  _readHistFilters();
  _saveHistFilters();
  const params={};
  if(_histFilters.q)params.q=_histFilters.q;
  if(_histFilters.verdict&&_histFilters.verdict!=='all')params.verdict=_histFilters.verdict;
  if(_histFilters.days&&_histFilters.days!=='0')params.days=_histFilters.days;
  const h=await loadHist(params);
  const el=document.getElementById('history-list'),btn=document.getElementById('clearHistBtn');
  if(!h.length){
    const isFiltered=(_histFilters.q||(_histFilters.verdict&&_histFilters.verdict!=='all')||(_histFilters.days&&_histFilters.days!=='0'));
    if(isFiltered){
      el.innerHTML='<div style="font-size:12px;color:var(--text-muted);padding:6px 10px;">No results matching filters</div>';
    }else{
      // #260: Illustrated empty state — eye + document motif
      el.innerHTML='<div class="empty-state-wrap">'
        +'<svg width="80" height="80" viewBox="0 0 80 80" fill="none" stroke="var(--accent)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">'
        +'<rect x="18" y="12" width="44" height="56" rx="5"/>'
        +'<line x1="26" y1="26" x2="54" y2="26"/>'
        +'<line x1="26" y1="34" x2="54" y2="34"/>'
        +'<line x1="26" y1="42" x2="44" y2="42"/>'
        +'<circle cx="52" cy="56" r="10"/>'
        +'<circle cx="52" cy="56" r="4" stroke-width="1.5"/>'
        +'<line x1="59" y1="63" x2="64" y2="68" stroke-width="2"/>'
        +'</svg>'
        +'<div class="empty-state-title">No deals analyzed yet.</div>'
        +'<div class="empty-state-sub">Your analysis history will appear here.<br>Submit an OM to get started.</div>'
        +'<a href="#" onclick="newAnalysis();return false;" class="empty-state-cta">Analyze your first deal →</a>'
        +'</div>';
    }
    btn.style.display='none';_updateCmpBtn();return;
  }
  btn.style.display='block';
  el.innerHTML=h.map(x=>{
    const c=x.verdict==='GO'?'#3fb950':x.verdict==='NO-GO'?'#f85149':'#d29922';
    const dt=x.created_at?x.created_at.substring(0,10):'';
    const chk=_compareSelected.has(x.id)?'checked':'';
    const spark=_sparklineSVG(x.score_history||[]);
    const isActive=window._currentJobId===x.id?'active':'';
    return `<div class="hist-item ${isActive}" data-jid="${x.id}" style="display:flex;align-items:flex-start;gap:6px;">
      <input type="checkbox" ${chk} style="margin-top:3px;accent-color:var(--accent);cursor:pointer;flex-shrink:0;" onchange="toggleCmp('${x.id}',this.checked)" onclick="event.stopPropagation()">
      <div style="flex:1;cursor:pointer;" onclick="loadReport('${x.id}')">
        <div style="display:flex;align-items:center;gap:6px;"><span class="hist-dot" style="background:${c}"></span><span style="max-width:100px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:inline-block;">${esc(x.deal_name||'Deal')}</span>${spark}</div>
        <div style="color:#484f58;font-size:10px;margin-left:16px;">${x.verdict||'?'} &middot; ${dt}</div>
      </div>
    </div>`;
  }).join('');
  _updateCmpBtn();
}
/* ── Sidebar history DOM filter (#206) — no server round-trip ── */
function filterHistDom(q){
  const clr=document.getElementById('hist-search-clear');
  if(clr)clr.style.display=q?'block':'none';
  const term=q.trim().toLowerCase();
  const items=document.querySelectorAll('#history-list .hist-item');
  items.forEach(el=>{
    if(!term){
      el.style.opacity='1';el.style.pointerEvents='';
    } else {
      const text=(el.textContent||'').toLowerCase();
      const match=text.includes(term);
      el.style.opacity=match?'1':'0.22';
      el.style.pointerEvents=match?'':'none';
      el.style.transition='opacity .15s';
    }
  });
}
function toggleCmp(id,checked){checked?_compareSelected.add(id):_compareSelected.delete(id);if(_compareSelected.size>2){const first=_compareSelected.values().next().value;_compareSelected.delete(first);}renderHist();}
function _updateCmpBtn(){
  const btn=document.getElementById('cmp-launch-btn');
  if(btn)btn.style.display=_compareSelected.size===2?'block':'none';
  if(_compareSelected.size===2)cmpDrawerOpen();
  else if(_compareSelected.size<2)cmpDrawerClose();
}
function launchCompare(){
  const ids=[..._compareSelected];
  if(ids.length!==2)return;
  window.open('/compare?a='+ids[0]+'&b='+ids[1],'_blank');
}
function cmpDrawerClose(){
  document.getElementById('cmp-drawer').classList.remove('open');
}
async function cmpDrawerOpen(){
  const ids=[..._compareSelected];
  if(ids.length!==2)return;
  // Update full-comparison link
  const lnk=document.getElementById('cmp-full-link');
  if(lnk)lnk.href='/compare?a='+ids[0]+'&b='+ids[1];
  document.getElementById('cmp-drawer').classList.add('open');
  const body=document.getElementById('cmp-drawer-body');
  body.innerHTML='<div style="color:var(--text-muted);font-size:12px;text-align:center;padding:16px;">Loading...</div>';
  try{
    const [ra,rb]=await Promise.all([fetch('/status/'+ids[0]).then(r=>r.json()),fetch('/status/'+ids[1]).then(r=>r.json())]);
    body.innerHTML=cmpBuildGrid(ra,rb);
  }catch(e){body.innerHTML='<div style="color:var(--red);font-size:12px;padding:12px;">Error loading deal data</div>';}
}
function cmpBuildGrid(a,b){
  const da=a.deal||{},db=b.deal||{};
  const ma=a.memo||'',mb=b.memo||'';
  // Verdict
  function getVerdict(memo){
    const mu=memo.toUpperCase();
    if(mu.includes('NO-GO'))return{t:'NO-GO',c:'var(--red)',bc:'rgba(248,81,73,.3)'};
    if(/\\bGO\\b/.test(mu)&&!mu.includes('CONDITIONAL'))return{t:'GO',c:'var(--green)',bc:'rgba(63,185,80,.3)'};
    return{t:'COND',c:'var(--amber)',bc:'rgba(210,153,34,.3)'};
  }
  const va=getVerdict(ma),vb=getVerdict(mb);
  // Conf
  function getConf(memo){const cm=memo.match(/Confidence[^0-9]*([0-9]+)/);return cm?Math.min(parseInt(cm[1]),100):72;}
  const ca=getConf(ma),cb=getConf(mb);
  // Metrics comparison: higher is better for cap/irr/units, lower for price (all as numbers)
  function metricRow(label,valA,valB,hiIsBetter,fmt){
    const na=parseFloat(valA)||0,nb=parseFloat(valB)||0;
    const aWins=hiIsBetter?na>nb:na<nb&&na>0;
    const bWins=hiIsBetter?nb>na:nb<na&&nb>0;
    return '<div style="display:contents;">'
      +'<div class="cmp-label" style="grid-column:1/-1;">'+label+'</div>'
      +'<div class="cmp-cell'+(aWins?' cmp-winner':'')+'"><div class="cmp-cell-val">'+fmt(valA)+'</div></div>'
      +'<div class="cmp-cell'+(bWins?' cmp-winner':'')+'"><div class="cmp-cell-val">'+fmt(valB)+'</div></div>'
      +'</div>';
  }
  const fmtPct=v=>v?v+'%':'—';
  const fmtUSD=v=>v?'$'+Number(v).toLocaleString():'—';
  const fmtNum=v=>v||'—';
  const cols='grid-template-columns:1fr 1fr';
  return '<div style="display:grid;'+cols+';gap:8px;">'
    +'<div class="cmp-deal-header" style="border-bottom:1px solid rgba(255,255,255,.06);padding-bottom:8px;">'+esc(da.deal_name||'Deal A')+'<span class="cmp-verdict-badge" style="margin-left:6px;color:'+va.c+';border-color:'+va.bc+';">'+va.t+'</span></div>'
    +'<div class="cmp-deal-header" style="border-bottom:1px solid rgba(255,255,255,.06);padding-bottom:8px;">'+esc(db.deal_name||'Deal B')+'<span class="cmp-verdict-badge" style="margin-left:6px;color:'+vb.c+';border-color:'+vb.bc+';">'+vb.t+'</span></div>'
    +metricRow('Confidence Score',ca,cb,true,fmtNum)
    +metricRow('Cap Rate (%)',da.cap_rate,db.cap_rate,true,fmtPct)
    +metricRow('Projected IRR (%)',da.projected_irr,db.projected_irr,true,fmtPct)
    +metricRow('Asking Price',da.asking_price||da.price,db.asking_price||db.price,false,fmtUSD)
    +metricRow('Units',da.units,db.units,true,fmtNum)
    +'</div>';
}
function clearHistory(){_compareSelected.clear();renderHist();}
async function loadReport(jid){
  // Mark active sidebar item (#199)
  document.querySelectorAll('.hist-item').forEach(h=>h.classList.remove('active'));
  const clicked=document.querySelector('.hist-item[data-jid="'+jid+'"]');
  if(clicked)clicked.classList.add('active');
  const r=await fetch('/status/'+jid);
  const d=await r.json();
  if(d.status==='done'){renderResults(d);window._currentJobId=jid;}
}

// ── UI helpers ─────────────────────────────────────────────────────────────
/* ── Report mode chrome ── */
function _injectReportChrome(job){
  const deal=job.deal||{};
  const dealName=deal.deal_name||'Deal Analysis';
  const ts=new Date().toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'});
  const rp=document.getElementById('results-panel');
  if(!rp)return;
  // #226: Load branding from URL params (for LP viewers) or localStorage (for owner)
  const _urlP=new URLSearchParams(window.location.search);
  const _fnP=_urlP.get('fn')||''; const _loP=_urlP.get('lo')||''; const _acP=_urlP.get('ac')||''; const _hbP=_urlP.get('hb')==='1';
  const _gnP=_urlP.get('gn')?decodeURIComponent(_urlP.get('gn')):'';
  const _b=wlLoad();
  const _firmName=_fnP||_b.firm_name||'';
  const _logoUrl=_loP||_b.logo_url||'';
  const _accentC=_acP||_b.accent_color||'';
  const _hideBadge=_hbP||_b.hide_badge||false;
  const _gpNote=_gnP||_b.gp_note||'';
  // Apply accent color override to CSS variable
  if(_accentC){document.documentElement.style.setProperty('--accent',_accentC);document.documentElement.style.setProperty('--amber',_accentC);}
  // Header
  const hdr=document.createElement('div');
  hdr.className='report-header';
  let hdrLeft='<div>';
  if(_logoUrl){hdrLeft+='<img src="'+_logoUrl+'" alt="'+esc(_firmName)+'" id="rpt-logo-img" style="max-height:36px;max-width:160px;object-fit:contain;margin-bottom:8px;display:block;">';}
  hdrLeft+='<div class="report-deal-name">'+esc(dealName)+'</div>';
  const byLine=_firmName?_firmName+' Analysis &middot; Generated '+ts:'ClearEye Analysis &middot; Generated '+ts;
  hdrLeft+='<div class="report-meta">'+byLine+'</div></div>';
  const ctaHtml=_hideBadge?'':'<a href="/app" class="report-cta">&#9881; Run your own analysis &rarr;</a>';
  // #242: Export ODD Package button (owner-only — not shown in LP portal view)
  const jobIdForExport=window._currentJobId||window._reportJobId||'';
  const exportBtn=jobIdForExport&&!window._lpToken
    ?'<a href="/export/'+jobIdForExport+'" class="report-export-btn" title="Download branded IC memorandum PDF" download>&#8659; Export ODD Package</a>'
    :'';
  hdr.innerHTML=hdrLeft+'<div style="display:flex;flex-direction:column;gap:8px;align-items:flex-end;">'+exportBtn+ctaHtml+'</div>';
  rp.insertBefore(hdr,rp.firstChild);
  // GP Investment Perspective card (#236)
  if(_gpNote){
    const gpCard=document.createElement('div');
    gpCard.id='gp-note-card';
    const acCol=_accentC||'var(--accent)';
    gpCard.style.cssText='margin:0 0 18px;padding:14px 18px;background:rgba(232,160,32,.04);border:1px solid rgba(232,160,32,.18);border-left:3px solid '+acCol+';border-radius:0 8px 8px 0;';
    gpCard.innerHTML='<div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:'+acCol+';font-family:var(--mono);margin-bottom:6px;">'+(esc(_firmName)||'GP')+' Investment Perspective</div>'
      +'<div style="font-size:12px;color:var(--text-secondary);line-height:1.6;">'+esc(_gpNote)+'</div>';
    const firstChild=rp.querySelector('.s3,.s4,.report-header');
    if(firstChild&&firstChild.nextSibling){rp.insertBefore(gpCard,firstChild.nextSibling);}
    else{rp.insertBefore(gpCard,rp.children[1]||null);}
  }
  // Footer
  const ftr=document.createElement('div');
  ftr.className='report-footer';
  if(_hideBadge&&_firmName){
    ftr.innerHTML='<span style="font-size:11px;color:var(--text-muted);">'+esc(_firmName)+' &mdash; Investment Analysis</span><span>'+ts+'</span>';
  }else{
    // #273: Viral acquisition footer — turns every shared report into a lead
    const _utmUrl='/?utm_source=report&utm_medium=share&utm_campaign=viral';
    ftr.innerHTML='<span><span class="report-brand"><span class="report-brand-logo">&#128065; ClearEye</span></span></span>'
      +'<span style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;">'
      +'<span style="font-size:11px;color:var(--text-muted);">This analysis took 90 seconds.</span>'
      +'<a href="'+_utmUrl+'" style="display:inline-flex;align-items:center;gap:5px;padding:6px 13px;background:var(--accent);color:#fff;border-radius:6px;font-size:11px;font-weight:600;text-decoration:none;letter-spacing:.01em;">Try ClearEye free &rarr;</a>'
      +'</span>';
  }
  rp.appendChild(ftr);
  // LP portal referral CTA banner (#214) — visible when viewing a shared LP link
  if(window._lpToken){
    const lpCta=document.createElement('div');
    lpCta.id='lp-referral-cta';
    lpCta.style.cssText='background:var(--bg-surface);border-top:1px solid var(--border-muted);padding:14px 24px;display:flex;align-items:center;justify-content:space-between;font-size:11px;margin-top:0;';
    lpCta.innerHTML='<span style="font-family:var(--mono);color:rgba(240,237,232,.35);">Powered by ClearEye AI — Real Estate Investment Intelligence</span>'
      +'<a href="http://localhost:5052" style="font-family:var(--mono);font-size:11px;color:#e8a020;text-decoration:none;">Get AI deal analysis →</a>';
    rp.appendChild(lpCta);
  }
  // Adjust nav for report mode
  const tagline=document.querySelector('.ce-tagline');
  if(tagline)tagline.textContent='Shared Report';
  document.title='ClearEye Report — '+dealName;
  // #252: Inject deal timeline sidebar
  _injectTimelineSidebar(deal, job);
  // #221: Inject Decision Trail if notes exist for this job
  const jid=job.job_id||window._currentJobId||'';
  if(jid){
    const notes=djLoad(jid);
    if(notes.length){
      const trail=document.createElement('div');
      trail.className='dj-trail s3';
      const rows=notes.map(function(e){
        const d=new Date(e.ts);
        const fmt=d.toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'})+' '+d.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'});
        return '<div class="dj-log-entry"><div class="dj-log-tab">'+esc(e.label)+'</div><div class="dj-log-text">'+esc(e.text)+'</div><div class="dj-log-ts">'+fmt+'</div></div>';
      }).join('');
      trail.innerHTML='<div class="dj-trail-title">Decision Trail</div>'+rows;
      rp.insertBefore(trail,rp.querySelector('.report-footer'));
    }
  }
}

// ── Report timeline sidebar (#252) ──────────────────────────────────────────
function _injectTimelineSidebar(deal, job){
  const STAGES=['Screening','LOI','Due Diligence','Closed','Passed'];
  const jobId=job.job_id||window._currentJobId||'';
  const dealName=(deal&&deal.deal_name)||'';
  if(!jobId&&!dealName)return;

  function _buildSidebar(pDeal, timelineEvents){
    const stage=pDeal?pDeal.stage||'Screening':'';
    const created=pDeal?pDeal.created_at||'':'';
    const entered=pDeal?pDeal.stage_entered_at||'':'';
    const activeIdx=STAGES.indexOf(stage);

    // Days since creation
    let daysSince='';
    if(created){
      try{
        const diff=Date.now()-new Date(created).getTime();
        daysSince=Math.round(diff/86400000)+' d';
      }catch(e){}
    }
    // Days in current stage
    let daysInStage='';
    if(entered){
      try{
        const diff=Date.now()-new Date(entered).getTime();
        daysInStage=Math.round(diff/86400000)+' d in stage';
      }catch(e){}
    }

    // Build stage event map from timeline
    const stageTs={};
    (timelineEvents||[]).forEach(function(ev){
      if(ev.type==='status'&&ev.detail){
        const m=ev.detail.match(/→\\s*(.+)/) || ev.detail.match(/to\\s+(.+)/i);
        if(m){const s=m[1].trim();if(STAGES.includes(s))stageTs[s]=ev.ts;}
      }
      if(ev.type==='created'&&created)stageTs['Screening']=created;
    });
    if(created&&!stageTs['Screening'])stageTs['Screening']=created;

    function fmtDate(ts){
      if(!ts)return'';
      try{return new Date(ts).toLocaleDateString('en-US',{month:'short',day:'numeric'});}catch(e){return'';}
    }

    const stageItems=STAGES.map(function(s,i){
      let cls='rts-future';
      if(i<activeIdx)cls='rts-done';
      else if(i===activeIdx)cls='rts-active';
      const dot=cls==='rts-done'?'&#10003;':cls==='rts-active'?'&#9679;':'';
      const dateStr=fmtDate(stageTs[s]);
      const daysChip=(i===activeIdx&&daysInStage)
        ?'<div class="rts-stage-days">'+daysInStage+'</div>':
        (i<activeIdx&&dateStr?'':'');
      return '<li class="rts-stage '+cls+'">'
        +'<div class="rts-dot">'+dot+'</div>'
        +'<div class="rts-stage-info">'
        +'<div class="rts-stage-name">'+s+'</div>'
        +(dateStr?'<div class="rts-stage-date">'+dateStr+'</div>':'')
        +daysChip
        +'</div></li>';
    }).join('');

    const pipelineLink=pDeal
      ?'<a href="/pipeline" style="display:block;margin-top:16px;font-size:11px;color:var(--accent);font-family:var(--mono);text-decoration:none;">View in Pipeline &rarr;</a>'
      :'';

    const sidebar=document.createElement('div');
    sidebar.className='report-timeline-sidebar';
    sidebar.innerHTML=
      '<div class="rts-title">Deal Timeline</div>'
      +'<div class="rts-deal-name">'+esc(pDeal?pDeal.deal_name||dealName:dealName)+'</div>'
      +(daysSince?'<div class="rts-meta">'+daysSince+' since submission</div>':'')
      +'<ul class="rts-stages">'+stageItems+'</ul>'
      +pipelineLink;

    document.body.appendChild(sidebar);
    document.body.classList.add('report-has-timeline');

    // Mobile toggle button
    const tBtn=document.createElement('button');
    tBtn.className='rts-toggle-btn';
    tBtn.title='Toggle timeline';
    tBtn.textContent='TIMELINE';
    tBtn.addEventListener('click',function(){
      document.body.classList.toggle('report-timeline-open');
    });
    document.body.appendChild(tBtn);
  }

  // Fetch pipeline deals, match by job_id or deal_name
  fetch('/api/pipeline').then(function(r){return r.json();}).then(function(data){
    const deals=data.deals||[];
    let match=deals.find(function(d){return d.job_id&&d.job_id===jobId;});
    if(!match&&dealName){
      match=deals.find(function(d){return d.deal_name&&d.deal_name.toLowerCase()===dealName.toLowerCase();});
    }
    if(!match){
      // No pipeline match — show minimal sidebar with just analysis date
      _buildSidebar(null, []);
      return;
    }
    // Fetch timeline events
    fetch('/api/pipeline/'+match.id+'/timeline')
      .then(function(r){return r.json();})
      .then(function(tdata){_buildSidebar(match, tdata.events||[]);})
      .catch(function(){_buildSidebar(match,[]);});
  }).catch(function(){
    _buildSidebar(null,[]);
  });
}

/* ── Cmd+K Command Palette (#167) ── */
const KPAL_COMMANDS=[
  {section:'Navigate',icon:'&#127968;',name:'Home / New Analysis',desc:'Start a fresh deal analysis',action:()=>newAnalysis(),shortcut:'N'},
  {section:'Navigate',icon:'&#128202;',name:'Markets Heat Map',desc:'Browse MSA market data',action:()=>{window.location='/markets';}},
  {section:'Navigate',icon:'&#128269;',name:'Find Deals',desc:'Search and discover deals',action:()=>{window.location='/find-deals';}},
  {section:'Navigate',icon:'&#128203;',name:'Pipeline',desc:'Kanban deal tracker',action:()=>{window.location='/pipeline';}},
  {section:'Navigate',icon:'&#9878;',name:'Compare Deals',desc:'Side-by-side comparison',action:()=>{window.location='/compare';}},
  {section:'Navigate',icon:'&#128176;',name:'Pricing',desc:'View plans and upgrade',action:()=>{window.location='/pricing';}},
  {section:'Actions',icon:'&#9654;',name:'Analyze a URL',desc:'Paste a listing URL to analyze',action:()=>{kpalClose();setInputTab('url');document.getElementById('url_input')?.focus();}},
  {section:'Actions',icon:'&#128196;',name:'Upload PDF',desc:'Drop an OM PDF to analyze',action:()=>{kpalClose();setInputTab('pdf');}},
  {section:'Actions',icon:'&#127921;',name:'Load Demo Deal',desc:'Run example analysis',action:()=>{kpalClose();loadDemo();}},
  {section:'Actions',icon:'&#128279;',name:'Share Report',desc:'Create LP share link',action:()=>{kpalClose();shareLink();}},
  {section:'Actions',icon:'&#11015;',name:'Download PDF',desc:'Export current analysis',action:()=>{kpalClose();downloadPDF();}},
  {section:'Actions',icon:'&#128203;',name:'Add to Pipeline',desc:'Save deal to Kanban board',action:()=>{kpalClose();addToPipeline();}},
];
let _kpalIdx=0;
let _kpalFiltered=[];

function kpalOpen(){
  const bd=document.getElementById('kpal-backdrop');
  const inp=document.getElementById('kpal-input');
  bd.classList.add('open');
  inp.value='';
  kpalFilter();
  setTimeout(()=>inp.focus(),50);
}
function kpalClose(e){
  if(e&&e.target!==document.getElementById('kpal-backdrop'))return;
  document.getElementById('kpal-backdrop').classList.remove('open');
}
function kpalForceClose(){document.getElementById('kpal-backdrop').classList.remove('open');}
function kpalFilter(){
  const q=(document.getElementById('kpal-input').value||'').toLowerCase();
  _kpalFiltered=q?KPAL_COMMANDS.filter(c=>c.name.toLowerCase().includes(q)||c.desc.toLowerCase().includes(q)||(c.section&&c.section.toLowerCase().includes(q))):KPAL_COMMANDS;
  _kpalIdx=0;
  kpalRender();
}
function kpalRender(){
  const el=document.getElementById('kpal-results');
  if(!_kpalFiltered.length){el.innerHTML='';return;}
  let html='',lastSec='';
  _kpalFiltered.forEach((c,i)=>{
    if(c.section!==lastSec){html+='<div class="kpal-section">'+c.section+'</div>';lastSec=c.section;}
    const active=i===_kpalIdx?'active':'';
    const sc=c.shortcut?'<span class="kpal-shortcut">'+c.shortcut+'</span>':'';
    html+='<div class="kpal-item '+active+'" data-idx="'+i+'" onclick="kpalSelect('+i+')" onmouseenter="kpalHover('+i+')">'
      +'<div class="kpal-icon">'+c.icon+'</div>'
      +'<div class="kpal-label"><div class="kpal-name">'+c.name+'</div><div class="kpal-desc">'+c.desc+'</div></div>'
      +sc+'</div>';
  });
  el.innerHTML=html;
  kpalScrollActive();
}
function kpalHover(i){_kpalIdx=i;kpalRender();}
function kpalSelect(i){
  const c=_kpalFiltered[i];
  if(c){kpalForceClose();c.action();}
}
function kpalKey(e){
  if(e.key==='Escape'){kpalForceClose();return;}
  if(e.key==='ArrowDown'){e.preventDefault();_kpalIdx=Math.min(_kpalIdx+1,_kpalFiltered.length-1);kpalRender();kpalScrollActive();}
  else if(e.key==='ArrowUp'){e.preventDefault();_kpalIdx=Math.max(_kpalIdx-1,0);kpalRender();kpalScrollActive();}
  else if(e.key==='Enter'){e.preventDefault();kpalSelect(_kpalIdx);}
}
function kpalScrollActive(){
  const el=document.getElementById('kpal-results');
  const active=el.querySelector('.kpal-item.active');
  if(active)active.scrollIntoView({block:'nearest'});
}
// Global Cmd+K / Ctrl+K trigger
document.addEventListener('keydown',function(e){
  if((e.metaKey||e.ctrlKey)&&e.key==='k'){
    e.preventDefault();
    const bd=document.getElementById('kpal-backdrop');
    bd.classList.contains('open')?kpalForceClose():kpalOpen();
  }
  if(e.key==='Escape'){kpalForceClose();scClose();}
  // Cmd+/ or ? (when not in an input) → shortcut cheatsheet (#204)
  if((e.metaKey||e.ctrlKey)&&e.key==='/'){
    e.preventDefault();
    const sc=document.getElementById('sc-backdrop');
    if(sc)sc.classList.contains('open')?scClose():scOpen();
  }
  if(e.key==='?'&&!['INPUT','TEXTAREA'].includes(document.activeElement.tagName)){
    e.preventDefault();
    const sc=document.getElementById('sc-backdrop');
    if(sc)sc.classList.contains('open')?scClose():scOpen();
  }
});

/* ── Shortcut cheatsheet helpers (#204) ── */
function scOpen(){
  const bd=document.getElementById('sc-backdrop');
  if(bd){bd.classList.add('open');document.body.style.overflow='hidden';}
}
function scClose(){
  const bd=document.getElementById('sc-backdrop');
  if(bd){bd.classList.remove('open');document.body.style.overflow='';}
}
function scBackdropClick(e){
  if(e.target===document.getElementById('sc-backdrop'))scClose();
}

/* ── First-run Onboarding Banner (#170) ── */
(function(){
  const LS_KEY='ce_onboarding_dismissed';
  const banner=document.getElementById('onboarding-banner');
  if(!banner)return;
  // Show if never dismissed AND no prior analyses in history
  const dismissed=localStorage.getItem(LS_KEY);
  if(!dismissed){
    banner.classList.add('visible');
  }
})();
// Guided Onboarding Modal (#235) — show after 400ms if never analyzed
setTimeout(obCheck, 400);
function dismissOnboarding(){
  localStorage.setItem('ce_onboarding_dismissed','1');
  const b=document.getElementById('onboarding-banner');
  if(b){b.style.transition='opacity .3s ease,max-height .4s ease';b.style.opacity='0';setTimeout(()=>b.remove(),350);}
}

// ── Guided Onboarding Modal (#235) ────────────────────────────────────────
const _OB_KEY='ce_has_analyzed';
const _SAMPLE_OM=`OFFERING MEMORANDUM
Sunset Ridge Apartments
1847 E. Camelback Road, Phoenix, AZ 85016

PROPERTY OVERVIEW
Class B garden-style multifamily community, 150 units on 4.8 acres.
Built 1987, recently acquired for value-add repositioning.

INVESTMENT HIGHLIGHTS
- Strong Phoenix multifamily market with sustained population growth
- Significant renovation upside: 68 of 150 units unrenovated (avg $8,200/unit budget)
- Projected rent premium of $285/month on renovated units vs current
- Sponsor track record: 12 completed value-add transactions in Phoenix MSA

FINANCIALS
Asking Price: $12,200,000 ($81,333/unit)
Current NOI: $584,000
Pro-forma Year 1 NOI: $742,000
Pro-forma Cap Rate: 6.08% (in-place 4.79%)

CURRENT RENT SCHEDULE
Average in-place rent: $1,147/unit/month
Pro-forma stabilized rent: $1,312/unit/month
Projected rent growth: 5.2% annually (Years 1-5)
Vacancy assumption: 4.0% (sponsor states "submarket-low demand")

DEBT & RETURNS
Loan Amount: $8,540,000 (LTV: 70%)
Interest Rate: 6.25% fixed, 5-year term, 30-year amortization
Equity Required: $3,660,000 (includes acquisition + renovation)
Projected IRR: 19.4% (5-year hold)
Projected Equity Multiple: 2.11x
Exit Cap Rate Assumption: 5.0% (Year 5)

CAPITAL EXPENDITURE
Renovation Budget: $1,230,000 total ($8,200/unit x 150 units)
Capital Reserve: $75,000 ($500/unit annually)
Roof/HVAC/Plumbing deferred maintenance: "Addressed in renovation budget"

MARKET SUMMARY
Phoenix MSA multifamily fundamentals remain strong. New supply pipeline
of 8,400 units projected for delivery in 2025-2026 in the Camelback
corridor. Sponsor projects continued rent growth based on "historical
10-year trajectory" in Phoenix metro.`;

function obCheck(){
  if(localStorage.getItem(_OB_KEY))return;
  const ov=document.getElementById('ob-modal-overlay');
  if(ov)ov.style.display='flex';
}
function obClose(){
  const ov=document.getElementById('ob-modal-overlay');
  if(ov)ov.style.display='none';
}
function obLoadSample(){
  const ta=document.getElementById('om_input');
  if(ta){ta.value=_SAMPLE_OM;ta.focus();}
  obClose();
  setStatus('Sample OM loaded — click Analyze to see what the sponsor is hiding. ');
  // Scroll to the analyze button
  const btn=document.getElementById('analyzeBtn');
  if(btn){btn.scrollIntoView({behavior:'smooth',block:'center'});}
}
// Mark as analyzed after first successful result
const _origRenderResults=window.renderResults;
// Hook at call time rather than override (renderResults is defined later)
document.addEventListener('DOMContentLoaded',()=>{setTimeout(()=>{
  const _orig=window.renderResults;
  if(_orig)window.renderResults=function(){
    localStorage.setItem(_OB_KEY,'1');
    return _orig.apply(this,arguments);
  };
},200);});

/* ── Mobile nav + input toggle ── */
function toggleMobileNav(){
  const d=document.getElementById('mobile-nav-drawer');
  d.classList.toggle('open');
}
// Close mobile nav when clicking outside
document.addEventListener('click',function(e){
  const nav=document.getElementById('mobile-nav-drawer');
  if(nav&&nav.classList.contains('open')&&!nav.contains(e.target)&&!e.target.closest('.ce-ham')){
    nav.classList.remove('open');
  }
});
function toggleMobileInput(){
  const body=document.getElementById('mobile-input-body');
  const toggle=document.getElementById('mobile-input-toggle');
  const isCollapsed=body.style.display==='none';
  body.style.display=isCollapsed?'block':'none';
  toggle.classList.toggle('collapsed',!isCollapsed);
}

// ── #258: Mobile sidebar drawer ─────────────────────────────────────────────
function _sidebarToggle(){
  document.body.classList.toggle('sidebar-open');
  const fab=document.getElementById('sidebar-fab');
  if(fab)fab.setAttribute('aria-expanded',document.body.classList.contains('sidebar-open')?'true':'false');
}
function _sidebarClose(){
  document.body.classList.remove('sidebar-open');
  const fab=document.getElementById('sidebar-fab');
  if(fab)fab.setAttribute('aria-expanded','false');
}
// Close drawer when a history item is tapped on mobile
document.addEventListener('click',function(e){
  if(e.target.closest('.hist-item')&&window.innerWidth<=768)_sidebarClose();
},true);

function newAnalysis(){
  document.getElementById('om_input').value='';
  document.getElementById('email_input').value='';
  document.getElementById('results').style.display='none';
  document.getElementById('empty-state').style.display='block';
  document.getElementById('demoBtn').style.display='block';
  document.getElementById('analyzeBtn').disabled=false;
  document.getElementById('status-msg').textContent='';
  document.getElementById('prog').style.display='none';
  resetProg();
  document.getElementById('om_input').focus();
  // On mobile, expand the input panel automatically
  const body=document.getElementById('mobile-input-body');
  if(body&&body.style.display==='none'){
    body.style.display='block';
    const toggle=document.getElementById('mobile-input-toggle');
    if(toggle)toggle.classList.remove('collapsed');
  }
}

let _isDemoMode=false;
function loadDemo(){
  document.getElementById('om_input').value=DEMO_OM;
  document.getElementById('demoBtn').style.display='none';
  const btn=document.getElementById('analyzeBtn');
  btn.style.transition='transform .15s';btn.style.transform='scale(1.03)';
  setTimeout(()=>btn.style.transform='',300);
}
// #216: Auto-submit demo deal with banner
function loadDemoAndRun(){
  _isDemoMode=true;
  document.getElementById('om_input').value=DEMO_OM;
  document.getElementById('demoBtn').style.display='none';
  const banner=document.getElementById('sample-deal-banner');
  if(banner){banner.style.display='block';}
  // Slight delay so user sees the textarea fill before analysis starts
  setTimeout(()=>startAnalyze(),320);
}
function clearSampleAndFocus(){
  _isDemoMode=false;
  document.getElementById('om_input').value='';
  const banner=document.getElementById('sample-deal-banner');
  if(banner){banner.style.display='none';}
  const demoBtn=document.getElementById('demoBtn');
  if(demoBtn){demoBtn.style.display='block';}
  document.getElementById('om_input').focus();
  // Clear results if showing
  const resPanel=document.getElementById('results');
  if(resPanel&&resPanel.style.display!=='none'){
    document.getElementById('initial-placeholder') && (document.getElementById('initial-placeholder').style.display='flex');
    resPanel.style.display='none';
  }
}

// LP section tracking (#144)
let _lpCurrentSection=null;
let _lpSectionEnterTime=null;
function _lpTrackSection(name){
  if(!window._lpToken)return;
  const now=Date.now();
  if(_lpCurrentSection&&_lpCurrentSection!==name&&_lpSectionEnterTime){
    const dur=Math.round((now-_lpSectionEnterTime)/100)/10;
    fetch('/api/lp/'+window._lpToken+'/event',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({event_type:'section_exit',section:_lpCurrentSection,duration_s:dur})}).catch(()=>{});
  }
  _lpCurrentSection=name;
  _lpSectionEnterTime=now;
  fetch('/api/lp/'+window._lpToken+'/event',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({event_type:'section_enter',section:name,duration_s:0})}).catch(()=>{});
}

function showTab(name,el){
  document.querySelectorAll('[id^="tab-"]').forEach(t=>t.style.display='none');
  document.querySelectorAll('.ce-tab').forEach(t=>t.classList.remove('active'));
  document.getElementById('tab-'+name).style.display='block';
  el.classList.add('active');
  _djCurrentTab=name; // #221: track active tab for decision journal
  // Slide indicator (#199)
  const ind=document.getElementById('tab-indicator');
  if(ind){
    const tabs=document.getElementById('ce-tabs');
    const tabsRect=tabs.getBoundingClientRect();
    const elRect=el.getBoundingClientRect();
    ind.style.left=(elRect.left-tabsRect.left+tabs.scrollLeft)+'px';
    ind.style.width=elRect.width+'px';
  }
  _lpTrackSection(name);
  return false;
}
function _initTabIndicator(){
  const active=document.querySelector('.ce-tab.active');
  if(active)showTab('summary',active);
}

function setStatus(m){document.getElementById('status-msg').textContent=m;}

function handleKey(e){if((e.ctrlKey||e.metaKey)&&e.key==='Enter'){e.preventDefault();startAnalyze();}}

// ── Progress (#85, redesigned #162) ─────────────────────────────────────────
const PROG_MAP={queued:0,parsing:1,stress_test:2,council:3,chairman:4,sending_email:4,done:5};
const ADV_CHIPS=['&#128059; Bear Case','&#9878; Tax','&#127961; Market','&#129504; Bias','&#128682; Exit'];
let _advTimer=null;
function setProg(st){
  const lvl=PROG_MAP[st]||0;
  [0,1,2,3].forEach(i=>{
    const ind=document.getElementById('pi'+i);
    const step=document.getElementById('pstep'+i);
    const lbl=step&&step.querySelector('.ps-label');
    if(!ind)return;
    if(i<lvl){
      ind.className='ps-indicator pi-done';
      ind.innerHTML='<span>&#10003;</span>';
      step&&step.classList.add('ps-done');
      step&&step.classList.remove('ps-active');
      if(lbl)lbl.classList.remove('idle');
    } else if(i===lvl){
      ind.className='ps-indicator pi-run';
      ind.innerHTML='<span class="ps-num">'+(i+1)+'</span>';
      step&&step.classList.add('ps-active');
      step&&step.classList.remove('ps-done');
      if(lbl)lbl.classList.remove('idle');
    } else {
      ind.className='ps-indicator pi-idle';
      ind.innerHTML='<span class="ps-num">'+(i+1)+'</span>';
      step&&step.classList.remove('ps-active','ps-done');
      if(lbl)lbl.classList.add('idle');
    }
  });
  if(st==='council')_startAdvChips();
}
function _startAdvChips(){
  const el=document.getElementById('advisor-substeps');
  if(!el)return;
  el.innerHTML='';
  let i=0;
  if(_advTimer)clearInterval(_advTimer);
  _advTimer=setInterval(()=>{
    if(i>=ADV_CHIPS.length){clearInterval(_advTimer);return;}
    if(i>0){const prev=el.querySelectorAll('.adv-step-chip')[i-1];if(prev)prev.className='adv-step-chip done';}
    const c=document.createElement('span');
    c.className='adv-step-chip';
    c.innerHTML=ADV_CHIPS[i];
    el.appendChild(c);
    i++;
  },11000);
}
function resetProg(){
  if(_advTimer){clearInterval(_advTimer);_advTimer=null;}
  [0,1,2,3].forEach(i=>{
    const ind=document.getElementById('pi'+i);
    const step=document.getElementById('pstep'+i);
    const lbl=step&&step.querySelector('.ps-label');
    if(ind){ind.className='ps-indicator pi-idle';ind.innerHTML='<span class="ps-num">'+(i+1)+'</span>';}
    step&&step.classList.remove('ps-active','ps-done');
    if(lbl)lbl.classList.add('idle');
  });
  const el=document.getElementById('advisor-substeps');
  if(el)el.innerHTML='Bear Case &middot; Tax &middot; Market &middot; Bias &middot; Exit';
}

// ── Input tab switching (#137) ────────────────────────────────────────────
function setInputTab(tab){
  // #268: simplified — toggle secondary panels, highlight alt links
  ['url','pdf'].forEach(function(t){
    const panel=document.getElementById(t+'-panel');
    if(panel)panel.style.display=(t===tab)?'block':'none';
  });
  // Highlight active alt link
  ['tab-url','tab-pdf'].forEach(function(id){
    const el=document.getElementById(id);
    if(!el)return;
    const isActive=(id==='tab-'+tab);
    el.style.color=isActive?'var(--accent)':'var(--text-muted)';
    el.style.fontWeight=isActive?'600':'400';
  });
  if(tab==='text'){document.getElementById('om_input')?.focus();}
}

// ── URL Listing Fetch (#137) ───────────────────────────────────────────────
async function fetchListingUrl(){
  const url=document.getElementById('listing-url').value.trim();
  if(!url){document.getElementById('url-status').textContent='Please enter a URL first.';return;}
  const btn=document.getElementById('fetch-url-btn');
  btn.disabled=true;btn.textContent='Fetching...';
  document.getElementById('url-status').textContent='Fetching listing data...';
  document.getElementById('url-preview').style.display='none';
  try{
    const r=await fetch('/api/fetch-url',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({url})
    });
    const d=await r.json();
    if(d.error&&!d.om_text){
      document.getElementById('url-status').textContent='Error: '+d.error;
      btn.disabled=false;btn.textContent='Fetch →';
      return;
    }
    // Populate textarea
    if(d.om_text){
      document.getElementById('om_input').value=d.om_text;
      document.getElementById('demoBtn').style.display='none';
      checkInput();
    }
    // Show preview card
    const previewEl=document.getElementById('url-preview');
    const fields=[];
    if(d.deal_name)fields.push('<strong style="color:#e6edf3;">'+escH(d.deal_name)+'</strong>');
    if(d.address)fields.push('&#128205; '+escH(d.address));
    if(d.property_type)fields.push('&#127968; '+escH(d.property_type));
    if(d.units)fields.push('&#127968; '+d.units+' units');
    if(d.asking_price){const p=parseFloat(d.asking_price);fields.push('&#128178; $'+p.toLocaleString());}
    if(d.cap_rate)fields.push('&#128200; '+parseFloat(d.cap_rate).toFixed(1)+'% cap');
    if(d.noi){const n=parseFloat(d.noi);fields.push('&#128181; NOI $'+n.toLocaleString());}
    previewEl.innerHTML='<div style="color:#3fb950;margin-bottom:4px;">&#10003; Extracted from listing:</div>'+fields.join(' &nbsp;&#xb7;&nbsp; ');
    previewEl.style.display='block';
    // Status
    const sourced=d.deal_name?('&ldquo;'+escH(d.deal_name.substring(0,50))+'&rdquo;'):'listing';
    document.getElementById('url-status').innerHTML='&#10003; Populated from '+sourced+'. Review &amp; edit below, then click Analyze.';
    // Pulse analyze button
    const ab=document.getElementById('analyzeBtn');
    ab.style.transform='scale(1.03)';setTimeout(()=>ab.style.transform='',300);
    // Switch to text tab to show the populated textarea
    setTimeout(()=>setInputTab('text'),400);
  }catch(e){
    document.getElementById('url-status').textContent='Network error: '+e.message;
  }
  btn.disabled=false;btn.textContent='Fetch →';
}
function escH(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}

// ── PDF Upload (#111) ──────────────────────────────────────────────────────
function handleDrop(e){
  e.preventDefault();
  document.getElementById('drop-zone').style.borderColor='#30363d';
  const f=e.dataTransfer.files[0];
  if(f&&f.type==='application/pdf')uploadPDF(f);
  else setPdfStatus('Only PDF files supported');
}
async function uploadPDF(file){
  if(!file)return;
  setPdfStatus('Extracting text from '+file.name+'...');
  const fd=new FormData();fd.append('pdf',file);
  try{
    const r=await fetch('/upload',{method:'POST',body:fd});
    const d=await r.json();
    if(d.error){setPdfStatus('Error: '+d.error);return;}
    document.getElementById('om_input').value=d.text;
    document.getElementById('demoBtn').style.display='none';
    setPdfStatus('Extracted '+d.pages+' pages from '+file.name);
    checkInput();
    // pulse analyze
    const btn=document.getElementById('analyzeBtn');
    btn.style.transform='scale(1.03)';setTimeout(()=>btn.style.transform='',300);
  }catch(e){setPdfStatus('Upload failed: '+e.message);}
}
function setPdfStatus(m){document.getElementById('pdf-status').textContent=m;}

// ── Input validation warnings (#122) ──────────────────────────────────────
function checkInput(){
  const txt=document.getElementById('om_input').value;
  const missing=[];
  if(!/price|asking|[$][0-9]/i.test(txt))missing.push('asking price');
  if(!/cap rate|cap:/i.test(txt))missing.push('cap rate');
  if(!/noi|net operating/i.test(txt))missing.push('NOI');
  const warn=document.getElementById('missing-warn');
  if(missing.length&&txt.length>80){
    warn.style.display='block';
    warn.textContent='Missing key data: '+missing.join(', ')+' — analysis may be incomplete';
  } else {warn.style.display='none';}
}

// ── Quick Kill Pre-Screen (#228) ──────────────────────────────────────────
async function quickScan(){
  const om=(document.getElementById('om_input').value||'').trim();
  if(!om){setStatus('Paste an OM first.');return;}
  const btn=document.getElementById('quickScanBtn');
  const res=document.getElementById('qs-result');
  if(btn)btn.disabled=true;
  if(res){res.style.display='block';res.innerHTML='<span style="color:var(--text-muted);">&#x26A1; Scanning deal&#x2026;</span>';}
  try{
    const r=await fetch('/api/quick-scan',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({om_text:om})});
    const d=await r.json();
    if(d.error){if(res)res.innerHTML='<span style="color:#f85149;">Error: '+esc(d.error)+'</span>';return;}
    const isPass=d.recommendation==='HARD PASS';
    const borderCol=isPass?'rgba(248,81,73,.3)':'rgba(63,185,80,.3)';
    const bgCol=isPass?'rgba(248,81,73,.06)':'rgba(63,185,80,.06)';
    const badgeCol=isPass?'#f85149':'#3fb950';
    const flagsHtml=d.deal_breakers&&d.deal_breakers.length?'<ul style="margin:6px 0 0 0;padding-left:16px;color:var(--text-secondary);font-size:11px;">'+d.deal_breakers.map(f=>'<li>'+esc(f)+'</li>').join('')+'</ul>':'';
    const actionBtn=isPass?'<button onclick="_qsLogPass()" style="margin-top:8px;padding:4px 10px;font-size:11px;background:rgba(248,81,73,.12);border:1px solid rgba(248,81,73,.35);color:#f85149;border-radius:4px;cursor:pointer;">Log to Pipeline as Passed</button>':'<button onclick="startAnalyze()" style="margin-top:8px;padding:4px 10px;font-size:11px;background:rgba(63,185,80,.12);border:1px solid rgba(63,185,80,.35);color:#3fb950;border-radius:4px;cursor:pointer;">&#x25BA; Run Full Analysis</button>';
    if(res)res.innerHTML='<div style="border:1px solid '+borderCol+';background:'+bgCol+';border-radius:7px;padding:10px 12px;">'
      +'<div style="font-family:var(--mono);font-size:12px;font-weight:700;color:'+badgeCol+';margin-bottom:4px;">'+(isPass?'&#x26D4; HARD PASS':'&#x2713; FULL ANALYSIS RECOMMENDED')+'</div>'
      +'<div style="font-size:11px;color:var(--text-secondary);">'+esc(d.reason)+'</div>'
      +(d.irr_check?'<div style="font-size:11px;color:var(--text-muted);margin-top:4px;font-style:italic;">'+esc(d.irr_check)+'</div>':'')
      +flagsHtml+actionBtn+'</div>';
    if(!isPass)setTimeout(()=>startAnalyze(),600);
  }catch(e){if(res)res.innerHTML='<span style="color:#f85149;">Error: '+e.message+'</span>';}
  finally{if(btn)btn.disabled=false;}
}
function _qsLogPass(){
  const om=(document.getElementById('om_input').value||'').trim();
  const name=om.split('\\n').find(l=>l.length>10)||'Untitled Deal';
  fetch('/api/pipeline',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({deal_name:name.slice(0,60),stage:'Passed',notes:'Quick Kill pre-screen: Hard Pass'})})
    .then(()=>setStatus('Logged to pipeline as Passed.'));
}

// ── Quick-scan deal-breaker badges on paste (#253) ─────────────────────────
(function(){
  let _qsTimer=null;
  let _qsLastText='';
  let _qsRunning=false;

  window._qsSchedule=function(){
    if(_qsTimer)clearTimeout(_qsTimer);
    const el=document.getElementById('om_input');
    if(!el)return;
    const txt=(el.value||'').trim();
    if(txt.length<120){_qsClear();return;}  // too short to bother
    _qsTimer=setTimeout(function(){_qsRun(txt);},1500);
  };

  window._qsClear=function(){
    const bd=document.getElementById('qs-badges');
    if(bd){bd.style.display='none';bd.innerHTML='';}
    _qsLastText='';
  };

  async function _qsRun(txt){
    if(_qsRunning||txt===_qsLastText)return;
    _qsLastText=txt;
    _qsRunning=true;
    const bd=document.getElementById('qs-badges');
    if(!bd)return;
    bd.style.display='block';
    bd.innerHTML='<span class="qs-spinner"></span> <span style="font-size:11px;color:var(--text-muted);vertical-align:middle;">Pre-screening&hellip;</span>';
    try{
      const r=await fetch('/api/quick-scan',{
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body:JSON.stringify({om_text:txt.slice(0,3000)})
      });
      const d=await r.json();
      if(d.error){bd.style.display='none';return;}
      const isPass=d.recommendation==='HARD PASS';
      const verdictCls=isPass?'qs-verdict qs-verdict-fail':'qs-verdict qs-verdict-pass';
      const verdictIcon=isPass?'&#9940;':'&#10003;';
      const verdictLabel=isPass?'HARD PASS':'Pre-screen: clear';
      let html='<div class="'+verdictCls+'">'+verdictIcon+' '+verdictLabel+'</div>';
      if(isPass&&d.deal_breakers&&d.deal_breakers.length){
        html+='<div>';
        d.deal_breakers.slice(0,3).forEach(function(f){
          html+='<span class="qs-chip">&#9888; '+esc(f)+'</span>';
        });
        html+='</div>';
      }
      if(d.irr_check){html+='<div class="qs-irr">'+esc(d.irr_check)+'</div>';}
      bd.innerHTML=html;
    }catch(e){
      bd.style.display='none';
    }finally{
      _qsRunning=false;
    }
  }
})();

// ── Analysis with SSE (#114) ───────────────────────────────────────────────
let pollTimer=null, sseSource=null;
window._currentJobId=null;

async function startAnalyze(){
  const txt=document.getElementById('om_input').value.trim();
  if(!txt){setStatus('Please paste an offering memorandum first.');return;}
  const email=document.getElementById('email_input').value.trim();
  // #253: Clear quick-scan badges when full analysis starts
  window._qsClear&&window._qsClear();
  document.getElementById('analyzeBtn').disabled=true;
  document.getElementById('results').style.display='none';
  document.getElementById('empty-state').style.display='none';
  document.getElementById('results-skeleton').style.display='block';
  // Auto-dismiss onboarding on first analysis (#170)
  if(document.getElementById('onboarding-banner'))dismissOnboarding();
  document.getElementById('prog').style.display='flex';
  setStatus('Queuing your deal for analysis...');resetProg();setProg('parsing');
  try{
    const r=await fetch('/analyze',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({om_text:txt,email:email})});
    const {job_id,error}=await r.json();
    if(error){
      document.getElementById('analyzeBtn').disabled=false;
      document.getElementById('prog').style.display='none';
      document.getElementById('results-skeleton').style.display='none';
      // #274: Show upgrade prompt when quota is exceeded
      if(error.toLowerCase().includes('quota')||error.toLowerCase().includes('limit')){
        document.getElementById('empty-state').style.display='block';
        document.getElementById('empty-state').innerHTML=
          '<div style="max-width:420px;margin:0 auto;padding:32px 20px;text-align:center;">'
          +'<div style="font-size:2rem;margin-bottom:16px;">&#128202;</div>'
          +'<div style="font-family:var(--font-display);font-style:italic;font-size:1.4rem;font-weight:400;margin-bottom:10px;color:var(--text-primary);">You\'ve used all your free analyses.</div>'
          +'<div style="font-size:13px;color:var(--text-secondary);margin-bottom:20px;line-height:1.65;">ClearEye Professional unlocks unlimited analyses, LP sharing, deal alerts, and the full pipeline board — for teams that look at deals seriously.</div>'
          +'<a href="/pricing" style="display:inline-flex;align-items:center;gap:6px;padding:11px 24px;background:var(--accent);color:#fff;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none;">See pricing — start free &rarr;</a>'
          +'<div style="margin-top:14px;font-size:11px;color:var(--text-muted);">Or wait until next month for your quota to reset.</div>'
          +'</div>';
      }else{
        setStatus('Error: '+error);
        document.getElementById('empty-state').style.display='block';
      }
      return;
    }
    window._currentJobId=job_id;
    // Try SSE first, fall back to polling
    if(typeof EventSource!=='undefined'){
      if(sseSource)sseSource.close();
      sseSource=new EventSource('/stream/'+job_id);
      sseSource.addEventListener('status',e=>{setStatus(SMSG[e.data]||e.data);setProg(e.data);});
      sseSource.addEventListener('done',()=>{
        sseSource.close();
        fetch('/status/'+job_id).then(r=>r.json()).then(d=>{
          renderResults(d);document.getElementById('analyzeBtn').disabled=false;
          document.getElementById('prog').style.display='none';renderHist();
        });
      });
      sseSource.addEventListener('error',e=>{
        sseSource.close();setStatus('Error occurred');
        document.getElementById('analyzeBtn').disabled=false;
        document.getElementById('prog').style.display='none';
        document.getElementById('results-skeleton').style.display='none';
        document.getElementById('empty-state').style.display='block';
      });
    } else {
      pollTimer=setInterval(()=>poll(job_id),2500);
    }
  }catch(e){setStatus('Network error');document.getElementById('analyzeBtn').disabled=false;}
}

// #270: Narrative status messages — tell the story while waiting
const SMSG={
  queued:       'Reading the offering memorandum...',
  parsing:      'Extracting deal terms, cap rate, and unit economics...',
  enriching:    'Pulling current market comps and macro benchmarks...',
  stress_test:  'Running IRR sensitivity across 9 rate + growth scenarios...',
  council:      'Marcus, Diana, Bobby, PropTech, and GTM are deliberating...',
  chairman:     'Chairman is synthesizing the council consensus...',
  sending_email:'Preparing your investment memo...',
  done:         'Analysis complete — here\'s your verdict.'
};

async function poll(jid){
  try{
    const r=await fetch('/status/'+jid);
    const d=await r.json();
    setStatus(SMSG[d.status]||d.status);
    setProg(d.status);
    if(d.status==='done'){
      clearInterval(pollTimer);renderResults(d);
      document.getElementById('analyzeBtn').disabled=false;
      document.getElementById('prog').style.display='none';renderHist();
    } else if(d.status==='error'){
      clearInterval(pollTimer);setStatus('Error: '+d.message);
      document.getElementById('analyzeBtn').disabled=false;
      document.getElementById('prog').style.display='none';
      document.getElementById('results-skeleton').style.display='none';
      document.getElementById('empty-state').style.display='block';
    }
  }catch(e){setStatus('Polling...');}
}

// ── Advisor score extraction (#177) — global so renderSummary can also use it ──
function extractAdvisorScore(name,text){
  const t=String(text);
  let m=t.match(/score[:\\s]+([0-9]+)\\s*\\/\\s*100/i);
  if(m)return parseInt(m[1]);
  m=t.match(/\\b([0-9]{1,2}|100)\\s*\\/\\s*100\\b/);
  if(m)return parseInt(m[1]);
  m=t.match(/\\b([0-9]+)%\\s*(?:confidence|score|rating)/i);
  if(m)return Math.min(100,parseInt(m[1]));
  const gm=t.match(/grade[:\\s]+([A-F][+-]?)/i);
  if(gm){const G={'A+':97,'A':93,'A-':90,'B+':87,'B':83,'B-':80,'C+':77,'C':73,'C-':70,'D+':67,'D':63,'D-':60,'F':40};return G[gm[1].toUpperCase()]||70;}
  const lo=t.toLowerCase();
  const bull=['strong','excellent','favorable','outperform','robust','compelling','attractive','positive'].filter(function(w){return lo.includes(w);}).length;
  const bear=['weak','poor','concern','overpriced','unfavorable','caution','warning','avoid','decline','significant risk'].filter(function(w){return lo.includes(w);}).length;
  const isNogo=/no[\\s-]go|pass|reject|decline/.test(lo);
  if(isNogo)return Math.max(20,40+bull*3-bear*4);
  return Math.min(92,Math.max(38,65+bull*5-bear*5));
}
// #246: Enhanced score extractor — returns score + breakdown components
function extractAdvisorScoreEx(name,text){
  const t=String(text);
  const lo=t.toLowerCase();
  const BULL_WORDS=['strong','excellent','favorable','outperform','robust','compelling','attractive','positive'];
  const BEAR_WORDS=['weak','poor','concern','overpriced','unfavorable','caution','warning','avoid','decline','significant risk'];
  const bullHits=BULL_WORDS.filter(function(w){return lo.includes(w);});
  const bearHits=BEAR_WORDS.filter(function(w){return lo.includes(w);});
  const isNogo=/no[\\s-]go|pass|reject|decline/.test(lo);
  let score=0,method='sentiment',driver='';
  let m=t.match(/score[:\\s]+([0-9]+)\\s*\\/\\s*100/i);
  if(m){score=parseInt(m[1]);method='explicit';driver='Stated '+score+'/100 in text';}
  if(!score){m=t.match(/\\b([0-9]{1,2}|100)\\s*\\/\\s*100\\b/);if(m){score=parseInt(m[1]);method='explicit';driver='Score '+score+'/100 found in output';}}
  if(!score){m=t.match(/\\b([0-9]+)%\\s*(?:confidence|score|rating)/i);if(m){score=Math.min(100,parseInt(m[1]));method='explicit';driver=score+'% confidence stated';}}
  if(!score){const gm=t.match(/grade[:\\s]+([A-F][+-]?)/i);if(gm){const G={'A+':97,'A':93,'A-':90,'B+':87,'B':83,'B-':80,'C+':77,'C':73,'C-':70,'D+':67,'D':63,'D-':60,'F':40};score=G[gm[1].toUpperCase()]||70;method='grade';driver='Grade '+gm[1].toUpperCase()+' → '+score+'/100';}}
  if(!score){method='sentiment';if(isNogo){score=Math.max(20,40+bullHits.length*3-bearHits.length*4);driver='NO-GO signal detected';}else{score=Math.min(92,Math.max(38,65+bullHits.length*5-bearHits.length*5));driver=bullHits.length>bearHits.length?bullHits.length+' bullish signal(s): '+bullHits.slice(0,3).join(', '):bearHits.length>0?bearHits.length+' bearish signal(s): '+bearHits.slice(0,3).join(', '):'Neutral — no strong signals';}}
  return {score:score,method:method,driver:driver,bull:bullHits,bear:bearHits,isNogo:isNogo,name:name};
}

// ── Render results ─────────────────────────────────────────────────────────
function renderResults(data){
  document.getElementById('results-skeleton').style.display='none';
  // #216: Show demo mode CTA banner if demo was run
  const demoBanner=document.getElementById('demo-results-banner');
  if(demoBanner) demoBanner.style.display=_isDemoMode?'block':'none';
  // Store current deal for assumption editor (#117)
  _currentDeal=data.deal||{};
  const memo=data.memo||'';
  // Verdict
  let vt='CONDITIONAL',vc='vs-cond';
  const mu=memo.toUpperCase();
  if(mu.includes('NO-GO')){vt='NO-GO';vc='vs-nogo';}
  else if(/\\bGO\\b/.test(mu)&&!mu.includes('CONDITIONAL')){vt='GO';vc='vs-go';}
  document.getElementById('verdict-stamp').innerHTML=`<div class="verdict-stamp ${vc}">${vt}</div>`;
  document.getElementById('verdict-name').textContent=data.deal?.deal_name||'Deal Analysis';
  // #266: Populate reason line — extract REASON or first key sentence from memo
  (function(){
    const rl=document.getElementById('verdict-reason-line');
    if(!rl)return;
    const rMatch=memo.match(/REASON[:\\s]+([^\\n]+)/i)||memo.match(/RECOMMENDATION[:\\s]+([^\\n.]+)/i);
    const killMatch=memo.match(/KILL SHOT[:\\s\\n]+([^\\n]+)/i);
    const txt=(rMatch&&rMatch[1].trim())||(killMatch&&killMatch[1].trim())||'';
    rl.textContent=txt.length>10?txt.slice(0,160)+(txt.length>160?'…':''):'';
  })();
  // Color wash on banner
  const vbEl=document.getElementById('verdict-banner');
  vbEl.classList.remove('vb-go','vb-nogo','vb-cond');
  if(vc==='vs-go')vbEl.classList.add('vb-go');
  else if(vc==='vs-nogo')vbEl.classList.add('vb-nogo');
  else vbEl.classList.add('vb-cond');

  // Report mode: inject verdict watermark text (#198)
  let _wmEl=document.getElementById('verdict-watermark-bg');
  if(!_wmEl){_wmEl=document.createElement('div');_wmEl.id='verdict-watermark-bg';_wmEl.className='verdict-watermark';vbEl.prepend(_wmEl);}
  _wmEl.textContent=vt;

  // Confidence ring (r=33, circ=207.3)
  const cm=memo.match(/Confidence[^0-9]*([0-9]+)/);
  const conf=cm?Math.min(parseInt(cm[1]),100):72;
  const circ=2*Math.PI*33;
  // Arc color matches verdict
  const arcColor=vc==='vs-go'?'var(--green)':vc==='vs-nogo'?'var(--red)':'var(--amber)';
  document.getElementById('cr-arc').style.stroke=arcColor;
  setTimeout(()=>{
    document.getElementById('cr-arc').style.strokeDashoffset=circ*(1-conf/100);
    document.getElementById('cr-label').textContent=conf+'%';
    document.getElementById('cr-label').style.color=arcColor;
  },120);

  const rm=memo.match(/Rationale[^a-z]+(.+?)(\\n|$)/);
  document.getElementById('verdict-rat').textContent=rm?.[1]?.trim()||'';
  const er=data.email_result;
  if(er?.sent)document.getElementById('email-status').textContent='Sent to '+er.email;
  else if(er?.path)document.getElementById('email-status').textContent='HTML saved to outputs/';

  // #269: Metrics scorecard grid — value + market benchmark indicator
  const deal=data.deal||{};
  function _metricColor(label,val){
    const n=parseFloat((val||'').replace(/[^0-9.-]/g,''));
    if(isNaN(n)||!val||val==='—')return '';
    if(label==='Cap Rate')return n>=5.5?'var(--green)':n>=4?'var(--amber)':'var(--red)';
    if(label==='Proj. IRR')return n>=16?'var(--green)':n>=11?'var(--amber)':'var(--red)';
    if(label==='LTV')return n<=65?'var(--green)':n<=75?'var(--amber)':'var(--red)';
    return 'var(--accent)';
  }
  function _metricArrow(label,val){
    // Simple above/at/below indicator vs typical CRE benchmarks
    const n=parseFloat((val||'').replace(/[^0-9.-]/g,''));
    if(isNaN(n))return '';
    if(label==='Cap Rate'){if(n>=5.5)return '&#9650;';if(n>=4)return '&#9679;';return '&#9660;';}
    if(label==='Proj. IRR'){if(n>=16)return '&#9650;';if(n>=11)return '&#9679;';return '&#9660;';}
    if(label==='LTV'){if(n<=65)return '&#9650;';if(n<=75)return '&#9679;';return '&#9660;';}
    return '';
  }
  const tpFit=tpScore(deal);
  const metricRows=[
    ['Ask Price',   deal.asking_price?'$'+Number(deal.asking_price).toLocaleString():'—', ''],
    ['Cap Rate',    deal.cap_rate?deal.cap_rate+'%':'—', 'Cap Rate'],
    ['Proj. IRR',   deal.projected_irr?deal.projected_irr+'%':'—', 'Proj. IRR'],
    ['Units',       deal.units||'—', ''],
    ['Hold',        deal.hold_period?deal.hold_period+' yr':'—', ''],
    ['LTV',         deal.ltv?deal.ltv+'%':'—', 'LTV'],
    ['Price/Unit',  deal.asking_price&&deal.units?'$'+Math.round(deal.asking_price/deal.units).toLocaleString():'—', ''],
    ...(tpFit!==null?[['Thesis Fit', tpFit+'%', 'Proj. IRR']]:[] )
  ].filter(([,v])=>v&&v!=='—');
  document.getElementById('metric-chips').innerHTML=metricRows.map(([label,val,colorKey])=>{
    const col=colorKey?_metricColor(colorKey,val):'var(--text-primary)';
    const arrow=colorKey?_metricArrow(colorKey,val):'';
    const arrowCol=arrow==='&#9650;'?'var(--green)':arrow==='&#9660;'?'var(--red)':'var(--text-muted)';
    return '<div class="metric-chip-v2">'
      +'<span class="mc-label-v2">'+label+'</span>'
      +'<span class="mc-val-v2" style="color:'+col+'">'+val
      +(arrow?'<span style="font-size:8px;color:'+arrowCol+';margin-left:3px;">'+arrow+'</span>':'')
      +'</span></div>';
  }).join('');

  // Summary bar (#98)
  const auditRed=(data.validation_report||'').split('\\n').filter(l=>l.includes('RED FLAG')||l.includes('[XX]')||l.includes('\\u2717')).length;
  const biasF=(data.bias_report||'').split('\\n').filter(l=>l.includes('DETECTED')||l.includes('[!]')||l.includes('HIGH')).length;
  const grade=(data.validation_report||'').match(/Grade[:\\s]+([A-F][+-]?)/)?.[1];
  const sb=[];
  const _diligenceTab=()=>document.querySelector('[onclick*="showTab(\'diligence\'"]')||document.getElementById('tab-diligence-btn');
  if(auditRed>0)sb.push(`<span class="sum-chip sc-red" onclick="showTab('diligence',_diligenceTab()||this)">Audit: ${auditRed} red flags</span>`);
  if(biasF>0)sb.push(`<span class="sum-chip sc-amber" onclick="showTab('diligence',_diligenceTab()||this)">Bias: ${biasF} flags</span>`);
  if(grade){const gc=grade[0]==='A'||grade[0]==='B'?'sc-green':grade[0]==='D'||grade[0]==='F'?'sc-red':'sc-amber';sb.push(`<span class="sum-chip ${gc}">Audit Grade: ${grade}</span>`);}
  document.getElementById('sum-bar').innerHTML=sb.join('');
  // Update Diligence tab badge (combined audit+bias count)
  const totalDiligenceFlags=(auditRed||0)+(biasF||0);
  const bdgD=document.getElementById('bdg-diligence');
  if(bdgD&&totalDiligenceFlags>0){bdgD.textContent=totalDiligenceFlags;bdgD.style.display='inline';}
  if(auditRed){const b=document.getElementById('bdg-audit');if(b){b.textContent=auditRed;b.style.display='inline';}}
  if(biasF){const b=document.getElementById('bdg-bias');if(b){b.textContent=biasF;b.style.display='inline';}}

  // Kill Shot Summary — extract from Chairman memo (#231)
  (function(){
    const ksEl=document.getElementById('ips-check-panel');
    if(!ksEl)return;
    const memoText=data.memo||'';
    const ksMatch=memoText.match(/##[\\s]*KILL[\\s]*SHOT[\\s]*\\n([\\s\\S]+?)(?=\\n##|$)/i);
    const ksTxt=ksMatch?ksMatch[1].trim().replace(/^[-*\\u2022]\\s*/,''):'';
    if(ksTxt&&ksTxt.length>10){
      const ksDiv=document.createElement('div');
      ksDiv.id='kill-shot-summary';
      ksDiv.style.cssText='background:rgba(248,81,73,.06);border:1px solid rgba(248,81,73,.25);border-left:3px solid #f85149;border-radius:7px;padding:10px 14px;margin-bottom:10px;';
      ksDiv.innerHTML='<div style="font-family:var(--mono);font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:#f85149;margin-bottom:5px;">&#9888; Kill Shot — Highest Risk Flag</div>'
        +'<div style="font-size:12px;color:var(--text-secondary);line-height:1.5;">'+esc(ksTxt)+'</div>';
      const existing=document.getElementById('kill-shot-summary');
      if(existing)existing.remove();
      ksEl.prepend(ksDiv);
    }
  })();

  // IPS Compliance Check (#229)
  ipsRender(deal);

  // Deal summary card (#87)
  const stats=[['Name',deal.deal_name],['Type',deal.property_type],['Market',deal.market],['Units',deal.units],['Asking Price',deal.asking_price?'$'+Number(deal.asking_price).toLocaleString():null],['Price/Unit',deal.asking_price&&deal.units?'$'+Math.round(deal.asking_price/deal.units).toLocaleString():null],['Cap Rate',deal.cap_rate?deal.cap_rate+'%':null],['NOI',deal.noi?'$'+Number(deal.noi).toLocaleString():null],['Sponsor',deal.sponsor],['Hold',deal.hold_period?deal.hold_period+' yr':null],['LTV',deal.ltv?deal.ltv+'%':null],['Proj. IRR',deal.projected_irr?deal.projected_irr+'%':null]];
  const KEY_STAT_METRICS=new Set(['Cap Rate','Proj. IRR','LTV','Price/Unit','NOI','Asking Price']);
  function statValClass(lbl,val){
    const n=parseFloat((val||'').replace(/[^0-9.-]/g,''));
    if(isNaN(n)||!val||val==='—')return '';
    if(lbl==='Cap Rate') return n>=5.5?'stat-vpos':n>=4?'stat-vcau':'stat-vneg';
    if(lbl==='Proj. IRR') return n>=16?'stat-vpos':n>=11?'stat-vcau':'stat-vneg';
    if(lbl==='LTV') return n<=65?'stat-vpos':n<=75?'stat-vcau':'stat-vneg';
    if(lbl==='Asking Price'||lbl==='Price/Unit'||lbl==='NOI') return 'stat-vacc';
    return '';
  }
  document.getElementById('stat-grid').innerHTML=stats.map(([l,v])=>{
    const isKey=KEY_STAT_METRICS.has(l);
    const vc=statValClass(l,v||'');
    const boxCls=isKey?'stat-box stat-key':'stat-box';
    const valCls='stat-val'+(isKey?' stat-hero':'')+(vc?' '+vc:'');
    return `<div class="${boxCls}"><div class="stat-lbl">${l}</div><div class="${valCls}">${v||'&mdash;'}</div></div>`;
  }).join('');
  document.getElementById('deal-card').style.display='block';

  // ── Market Rent Context (#171) — auto-injected below stat grid ──────────
  const rc=data.rent_context||deal.live_market_data||{};
  const rcEl=document.getElementById('rent-context-card');
  if(rcEl&&(rc.avg_rent||rc.avg_cap_rate||rc.vacancy_rate)){
    const isLive=rc._source==='rentcast_live';
    const srcDot=isLive
      ?'<span style="width:7px;height:7px;border-radius:50%;background:var(--green);display:inline-block;box-shadow:0 0 4px var(--green);margin-right:4px;"></span>Live'
      :'<span style="width:7px;height:7px;border-radius:50%;background:var(--amber);display:inline-block;margin-right:4px;"></span>Static';
    const dealCapRate=parseFloat((deal.cap_rate||'0').toString().replace(/[^0-9.-]/g,''));
    const mktCapRate=rc.avg_cap_rate||0;
    const capSpread=dealCapRate&&mktCapRate?+(dealCapRate-mktCapRate).toFixed(2):null;
    const spreadColor=capSpread===null?'':capSpread>=0.5?'color:var(--green)':capSpread<0?'color:var(--red)':'color:var(--amber)';
    const spreadTxt=capSpread!==null?(capSpread>=0?'+':'')+capSpread+'% vs mkt':'';
    const rentGrowthColor=(rc.rent_growth_1yr||0)>2.5?'color:var(--green)':(rc.rent_growth_1yr||0)<0?'color:var(--red)':'color:var(--amber)';
    const vacColor=(rc.vacancy_rate||7)<6?'color:var(--green)':(rc.vacancy_rate||7)>9?'color:var(--red)':'color:var(--amber)';
    rcEl.style.display='block';
    rcEl.innerHTML=`
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
      <span style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);">&#127758; Market Rent Context &mdash; ${esc(rc.market||deal.market||'')}</span>
      <span style="font-size:9px;font-weight:600;color:var(--text-muted);display:flex;align-items:center;">${srcDot}</span>
    </div>
    <div style="display:flex;gap:10px;flex-wrap:wrap;">
      ${rc.avg_rent?`<div class="rc-stat"><div class="rc-val">$${Math.round(rc.avg_rent).toLocaleString()}</div><div class="rc-lbl">Avg Market Rent</div></div>`:''}
      ${rc.avg_cap_rate?`<div class="rc-stat"><div class="rc-val ${spreadColor}">${mktCapRate.toFixed(1)}%${spreadTxt?`<span style="font-size:10px;margin-left:4px;${spreadColor}">${spreadTxt}</span>`:''}</div><div class="rc-lbl">Market Cap Rate</div></div>`:''}
      ${rc.rent_growth_1yr!=null?`<div class="rc-stat"><div class="rc-val" style="${rentGrowthColor}">${rc.rent_growth_1yr>0?'+':''}${rc.rent_growth_1yr.toFixed(1)}% YoY</div><div class="rc-lbl">Rent Growth</div></div>`:''}
      ${rc.vacancy_rate!=null?`<div class="rc-stat"><div class="rc-val" style="${vacColor}">${rc.vacancy_rate.toFixed(1)}%</div><div class="rc-lbl">Vacancy Rate</div></div>`:''}
    </div>`;
  }

  // ATTOM Property Data (#175)
  const attom=data.attom_data||{};
  const attomEl=document.getElementById('attom-card');
  if(attomEl&&(attom.last_sale_price||attom.assessed_value||attom.year_built||attom.tax_amount)){
    const fmtUSD=v=>v?'$'+Number(v).toLocaleString():'—';
    const isLive=attom._source==='attom_live';
    const liveDot=isLive
      ?'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);margin-right:5px;vertical-align:middle;box-shadow:0 0 4px var(--green);"></span>'
      :'<span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--text-muted);margin-right:5px;vertical-align:middle;"></span>';
    const comps=Array.isArray(attom.comp_sales)?attom.comp_sales:[];
    const compsAvgPPU=comps.length>0?(()=>{const ppus=comps.filter(c=>c.price_per_unit).map(c=>c.price_per_unit);return ppus.length?Math.round(ppus.reduce((a,b)=>a+b,0)/ppus.length):null;})():null;
    attomEl.style.display='block';
    attomEl.innerHTML=
      '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">'
      +'<span style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);">'+liveDot+'ATTOM Property Data</span>'
      +'<span style="font-size:10px;color:var(--text-muted);">'+comps.length+' comps found</span>'
      +'</div>'
      +'<div style="display:flex;gap:8px;flex-wrap:wrap;">'
      +(attom.last_sale_price?'<div class="rc-stat"><div class="rc-val">'+fmtUSD(attom.last_sale_price)+'</div><div class="rc-lbl">Last Sale'+(attom.last_sale_date?' '+(String(attom.last_sale_date).substring(0,4)):'')+'</div></div>':'')
      +(attom.assessed_value?'<div class="rc-stat"><div class="rc-val">'+fmtUSD(attom.assessed_value)+'</div><div class="rc-lbl">Assessed Value</div></div>':'')
      +(attom.tax_amount?'<div class="rc-stat"><div class="rc-val">'+fmtUSD(attom.tax_amount)+'</div><div class="rc-lbl">Ann. Tax</div></div>':'')
      +(attom.year_built?'<div class="rc-stat"><div class="rc-val">'+attom.year_built+'</div><div class="rc-lbl">Year Built</div></div>':'')
      +(compsAvgPPU?'<div class="rc-stat"><div class="rc-val">'+fmtUSD(compsAvgPPU)+'/unit</div><div class="rc-lbl">Comps Avg PPU</div></div>':'')
      +'</div>';
  }

  // Data source badge (#122)
  if(data.data_source){
    document.getElementById('data-src-text').textContent=data.data_source;
    document.getElementById('data-badge').style.display='block';
  }

  // Executive Summary tab (#121)
  renderSummary(data, vc, conf, vt);

  // Memo — IC format rendering (#231/#240 premium report mode)
  (function(){
    const mc=document.getElementById('memo-content');
    if(!mc)return;
    const sections=memo.split(/\\n(?=##[\\s])/);
    if(sections.length<=1){mc.textContent=memo;return;}
    const rptMode=!!window._reportMode;
    let html='';
    // Extract verdict for report mode card
    let verdictHtml='';
    if(rptMode){
      const vMatch=memo.match(/##[\\s]*GO[\\s/]*NO-GO[\\s\\S]*?\\n([\\s\\S]+?)(?=\\n##|$)/i);
      if(vMatch){
        const vBlock=vMatch[1];
        const isGo=/^\\s*GO\\b/i.test(vBlock)&&!/NO-GO/i.test(vBlock.split('\\n')[0]);
        const isNogo=/NO-GO/i.test(vBlock.split('\\n')[0]);
        const isCond=/CONDITIONAL/i.test(vBlock.split('\\n')[0]);
        const confMatch=vBlock.match(/Confidence:[\\s]*([\\d]+)%?/i);
        const conf=confMatch?confMatch[1]:'—';
        const ratioMatch=vBlock.match(/Rationale:[\\s]*([^\\n]+(?:\\n(?!\\n)[^\\n]+)*)/i);
        const rationale=ratioMatch?ratioMatch[1].trim():'';
        const vCls=isNogo?'nogo':isCond?'conditional':'';
        const vColor=isNogo?'#f85149':isCond?'var(--amber)':'#3fb950';
        const vLabel=isNogo?'NO-GO':isCond?'CONDITIONAL GO':'GO';
        verdictHtml='<div class="rm-verdict-card '+vCls+'" style="border-color:'+vColor+'40;">'
          +'<div><div class="rm-verdict-label">Chairman Verdict</div><div class="rm-verdict-stamp" style="color:'+vColor+';">'+esc(vLabel)+'</div>'+(rationale?'<div style="font-size:11.5px;color:var(--text-secondary);margin-top:6px;line-height:1.5;max-width:480px;">'+esc(rationale)+'</div>':'')+'</div>'
          +'<div style="text-align:right;"><div class="rm-verdict-label">Confidence</div><div class="rm-verdict-conf" style="color:'+vColor+';">'+esc(conf)+'<span style="font-size:1rem;">%</span></div></div>'
          +'</div>';
      }
    }
    sections.forEach(function(sec){
      const hdrMatch=sec.match(/^##[\\s]*(.+?)[\\s]*\\n([\\s\\S]*)/);
      if(!hdrMatch){html+='<div style="white-space:pre-wrap;font-size:12.5px;line-height:1.7;color:var(--text-secondary);">'+esc(sec)+'</div>';return;}
      const hdr=hdrMatch[1].trim();
      const body=hdrMatch[2].trim();
      const isKS=hdr.toUpperCase().includes('KILL SHOT');
      const isMust=hdr.toUpperCase().includes('WHAT MUST');
      const isKC=hdr.toUpperCase().includes('KILL CRITER');
      const isDiss=hdr.toUpperCase().includes('DISSENTING');
      const isDDQ=hdr.toUpperCase().includes('DUE DILIGENCE');
      const isVerdict=hdr.toUpperCase().includes('GO') && hdr.toUpperCase().includes('NO-GO');
      if(rptMode){
        // Premium report layout
        if(isKS){
          html+='<div class="rm-kill-shot"><div class="rm-kill-label">&#9888; Kill Shot</div><div class="rm-kill-text">'+esc(body)+'</div></div>';
          return;
        }
        if(isVerdict){
          html+=verdictHtml;return;
        }
        if(isDDQ){
          const lines=body.split('\\n').filter(l=>l.trim().match(/^[\\d\\*\\-]/));
          const items=lines.map(function(l,i){
            const txt=l.replace(/^[\\d\\-\\*\\.]+[\\s]*/,'').trim();
            return txt?'<div class="rm-dd-item"><span class="rm-dd-num">'+(i+1)+'.</span><span class="rm-dd-text">'+esc(txt)+'</span></div>':'';
          }).join('');
          const barColor='var(--accent)';
          html+='<div class="rm-section"><div class="rm-section-hdr"><div class="rm-section-bar" style="background:'+barColor+';min-height:20px;"></div><div class="rm-section-title">'+esc(hdr)+'</div></div>'
            +'<div style="padding:4px 0;">'+items+'</div></div>';
          return;
        }
        const barColor=isMust?'var(--accent)':isDiss?'#58a6ff':isKC?'var(--amber)':'rgba(255,255,255,.12)';
        html+='<div class="rm-section"><div class="rm-section-hdr"><div class="rm-section-bar" style="background:'+barColor+';min-height:20px;"></div><div class="rm-section-title">'+esc(hdr)+'</div></div>'
          +'<div class="rm-body">'+esc(body)+'</div></div>';
      } else {
        const hdrBorder=isKS?'#f85149':isMust?'var(--accent)':isKC?'var(--amber)':isDiss?'#58a6ff':'rgba(255,255,255,.15)';
        const hdrColor=isKS?'#f85149':isMust?'var(--accent)':isKC?'var(--amber)':isDiss?'#58a6ff':'var(--text-muted)';
        html+='<div style="margin-bottom:18px;">'
          +'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:'+hdrColor+';font-family:var(--mono);padding-bottom:6px;border-bottom:1px solid '+hdrBorder+';margin-bottom:8px;">'+esc(hdr)+'</div>'
          +'<div style="white-space:pre-wrap;font-size:12.5px;line-height:1.7;color:var(--text-secondary);">'+esc(body)+'</div>'
          +'</div>';
      }
    });
    mc.innerHTML=html;
    // Populate share bar deal name
    if(rptMode){const rsb=document.getElementById('rsb-deal-name');if(rsb)rsb.textContent=data.deal?.deal_name||'Deal Report';}
  })();

  // Advisors (#89, #177 score badges)
  const AMETA={bear:{icon:'&#128059;',cls:'adv-bear'},tax:{icon:'&#9878;',cls:'adv-tax'},market:{icon:'&#127961;',cls:'adv-market'},bias:{icon:'&#129504;',cls:'adv-bias'},exit:{icon:'&#128682;',cls:'adv-exit'}};

  const aEl=document.getElementById('adv-content');aEl.innerHTML='';
  const _advScores=[];
  const _advBreakdowns=[]; // #246: per-advisor score breakdown
  Object.entries(data.advisors||{}).forEach(function([name,text],i){
    const key=Object.keys(AMETA).find(function(k){return name.toLowerCase().includes(k);});
    const meta=key?AMETA[key]:{icon:'&#128100;',cls:''};
    const lines=String(text).split('\\n');
    const prev=lines.slice(0,3).join('\\n'),rest=lines.slice(3).join('\\n');
    const rid='ar'+i;
    const isNogo=String(text).slice(0,200).toLowerCase().match(/no.go|caution|avoid|decline|pass|reject/);
    const chipC=isNogo?'var(--red)':'var(--green)';
    const chipL=isNogo?'NO-GO':'GO';
    // Score badge (#177, enhanced #246)
    const scoreEx=extractAdvisorScoreEx(name,text);
    _advScores.push(scoreEx.score);
    _advBreakdowns.push(scoreEx);
    const scoreC=scoreEx.score>=70?'var(--green)':scoreEx.score>=50?'var(--amber)':'var(--red)';
    const scoreBadge='<span style="margin-left:auto;font-size:11px;font-weight:700;color:'+scoreC+';padding:1px 7px;border-radius:4px;border:1px solid '+scoreC+';opacity:.85;letter-spacing:.02em;">'+scoreEx.score+'</span>';
    // Editorial divider in report mode (#198)
    if(window._reportMode){
      const div=document.createElement('div');
      div.className='report-adv-divider';
      div.textContent='Advisor '+(i+1)+' of '+Object.keys(data.advisors||{}).length+' — '+esc(name);
      aEl.appendChild(div);
    }
    // #267: Extract key finding — first sentence containing a risk keyword or the first sentence
    const allText=String(text).trim();
    const sentences=allText.split(/(?<=[.!?])\\s+/);
    const riskKw=/risk|concern|flag|caution|warn|issue|problem|gap|miss|weak|inflat|optimis|understat|no.go|deal.break/i;
    const keyFinding=(sentences.find(s=>riskKw.test(s))||sentences[0]||'').trim().slice(0,140);
    const rid='ar'+i;
    const d=document.createElement('div');
    d.className='adv-card '+meta.cls+' s'+Math.min(i+1,5);
    const advHdr='<div class="adv-hdr">'
      +'<div class="adv-icon">'+meta.icon+'</div>'
      +'<div style="flex:1;min-width:0;">'
      +'<span class="adv-name" style="font-family:var(--font-display);font-style:italic;font-size:1rem;font-weight:400;letter-spacing:-0.01em;">'+esc(name)+'</span>'
      +(keyFinding?'<div class="adv-key-finding">'+esc(keyFinding+(keyFinding.length>=140?'…':''))+'</div>':'')
      +'</div>'
      +'<div style="display:flex;flex-direction:column;align-items:flex-end;gap:4px;flex-shrink:0;">'
      +scoreBadge
      +'<span class="adv-verdict-chip" style="color:'+chipC+';font-size:9px;">'+chipL+'</span>'
      +'</div>'
      +'</div>';
    const advBody=allText?'<div class="adv-body adv-body-collapsed" id="adv-body-'+rid+'">'
      +'<div class="adv-text">'+esc(allText)+'</div>'
      +'</div>'
      +'<div class="adv-more" onclick="togAdv(\'adv-body-'+rid+'\',this)">&#9660; Full analysis</div>':'';
    d.innerHTML=advHdr+advBody;
    aEl.appendChild(d);
  });
  // Store consensus on data for renderSummary (#177, #246)
  data._advScores=_advScores;
  data._advBreakdowns=_advBreakdowns;
  data._advConsensus=_advScores.length?Math.round(_advScores.reduce(function(a,b){return a+b;},0)/_advScores.length):null;

  // Radar chart (#192)
  if(_advScores.length>=2){
    const advNames=Object.keys(data.advisors||{});
    const advMap={};
    advNames.forEach(function(n,i){advMap[n]=_advScores[i]||0;});
    renderAdvisorRadar(advMap, data._advConsensus);
  }

  // Chart (#88)
  renderChart(data.stress_table||'');
  document.getElementById('stress-raw').textContent=data.stress_table||'(No data)';

  // Audit (#90)
  renderAudit(data.validation_report||'');
  renderBias(data.bias_report||'', data);
  document.getElementById('premortem-content').textContent=data.premortem_report||'(No pre-mortem data)';
  document.getElementById('macro-content').textContent=data.macro_brief||'(No macro brief)';

  // History (re-fetch from server after analysis)
  if(typeof renderHist==='function')renderHist();

  // Results header strip — timestamp + deal name
  const tsEl=document.getElementById('results-ts');
  if(tsEl){
    const now=new Date();
    const ts=now.toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
    const dn=deal.deal_name?` &middot; ${esc(deal.deal_name)}`:'';
    tsEl.innerHTML=`Analysis completed at ${ts}${dn}`;
  }

  document.getElementById('results').style.display='block';
  document.getElementById('empty-state').style.display='none';
  // #256: Staggered reveal — replay ceReveal on every results-panel direct child
  (function(){
    const rp=document.getElementById('results-panel');
    if(!rp)return;
    const children=Array.from(rp.children);
    children.forEach(function(el,i){
      el.classList.remove('ce-reveal');
      void el.offsetWidth; // force reflow so animation restarts
      el.style.animationDelay=i*55+'ms';
      el.classList.add('ce-reveal');
    });
  })();
  // Init tab indicator position (#199)
  setTimeout(_initTabIndicator, 80);
  // Delta comparison after assumption override (#215)
  if(_overrideBaseline) setTimeout(()=>_renderDeltaPanel(data), 100);
  // #219: Show Kill Sheet FAB
  const ksFab=document.getElementById('kill-sheet-fab');
  if(ksFab&&!window._reportMode)ksFab.style.display='block';
  // #221: Load decision journal for this job
  const jid=data.job_id||window._currentJobId||'';
  if(jid) djRenderLog(jid);
}

// ── Chart.js (#88) ─────────────────────────────────────────────────────────
let sCh=null;
function renderChart(raw){
  const lines=raw.split('\\n').filter(l=>l.includes('%')&&l.includes('|'));
  const labels=[],irrs=[],colors=[];
  lines.forEach(line=>{
    const parts=line.split('|').map(s=>s.trim());
    if(parts.length<2)return;
    const label=(parts[0]||parts[1]||'').substring(0,28);
    const m=line.match(/(-?[0-9]+\\.?[0-9]*)\\s*%/g);
    if(!m)return;
    const irr=parseFloat(m[m.length-1]);
    labels.push(label);irrs.push(irr);colors.push(irr>=8?'#3fb950':'#f85149');
  });
  if(!labels.length)return;
  if(sCh)sCh.destroy();
  const ctx=document.getElementById('stress-chart')?.getContext('2d');
  if(!ctx)return;
  sCh=new Chart(ctx,{type:'bar',data:{labels,datasets:[{data:irrs,backgroundColor:colors,borderRadius:4,barThickness:22}]},options:{indexAxis:'y',responsive:true,maintainAspectRatio:false,plugins:{legend:{display:false},tooltip:{callbacks:{label:c=>` IRR: ${c.raw}%`}}},scales:{x:{ticks:{color:'#8b949e',callback:v=>v+'%'},grid:{color:'#21262d'},title:{display:true,text:'IRR %',color:'#8b949e'}},y:{ticks:{color:'#8b949e',font:{size:11}},grid:{color:'#21262d'}}},animation:{duration:700,easing:'easeOutQuart'}}});
}

// ── Advisor Radar Chart (#192) ────────────────────────────────────────────
function renderAdvisorRadar(advMap, consensus){
  const wrap=document.getElementById('advisor-radar-wrap');
  if(!wrap)return;
  const entries=Object.entries(advMap);
  if(entries.length<2){wrap.style.display='none';return;}
  const W=300,H=300,cx=W/2,cy=H/2,R=110,rPad=20;
  const n=entries.length;
  const labels=entries.map(function(e){
    const nm=e[0];
    if(nm.toLowerCase().includes('bear'))return 'Bear';
    if(nm.toLowerCase().includes('tax'))return 'Tax';
    if(nm.toLowerCase().includes('market'))return 'Market';
    if(nm.toLowerCase().includes('bias'))return 'Bias';
    if(nm.toLowerCase().includes('exit'))return 'Exit';
    return nm.split(' ')[0];
  });
  const scores=entries.map(function(e){return Math.max(0,Math.min(100,e[1]||0));});
  const verdictColor=(consensus||0)>=70?'#3fb950':(consensus||0)>=50?'#d29922':'#f85149';
  // Compute vertex positions
  function pt(idx,val){
    const angle=(Math.PI*2*idx/n)-Math.PI/2;
    const r=rPad+((R-rPad)*val/100);
    return [cx+r*Math.cos(angle), cy+r*Math.sin(angle)];
  }
  function axPt(idx,rr){
    const angle=(Math.PI*2*idx/n)-Math.PI/2;
    return [cx+rr*Math.cos(angle), cy+rr*Math.sin(angle)];
  }
  // Build SVG
  let svgParts=[];
  // Grid rings
  [25,50,75,100].forEach(function(pct){
    const rr=rPad+(R-rPad)*pct/100;
    let pts=[];
    for(let i=0;i<n;i++){const p=axPt(i,rr);pts.push(p[0]+','+p[1]);}
    svgParts.push('<polygon points="'+pts.join(' ')+'" fill="none" stroke="#21262d" stroke-width="1"/>');
  });
  // Axis lines
  for(let i=0;i<n;i++){
    const p=axPt(i,R);
    svgParts.push('<line x1="'+cx+'" y1="'+cy+'" x2="'+p[0]+'" y2="'+p[1]+'" stroke="#30363d" stroke-width="1"/>');
  }
  // Data polygon
  const dataPts=scores.map(function(s,i){const p=pt(i,s);return p[0]+','+p[1];}).join(' ');
  svgParts.push('<polygon points="'+dataPts+'" fill="'+verdictColor+'" fill-opacity="0.18" stroke="'+verdictColor+'" stroke-width="2"/>');
  // Dots + score labels
  scores.forEach(function(s,i){
    const p=pt(i,s);
    svgParts.push('<circle cx="'+p[0]+'" cy="'+p[1]+'" r="4" fill="'+verdictColor+'"/>');
  });
  // Axis labels
  labels.forEach(function(lbl,i){
    const p=axPt(i,R+16);
    svgParts.push('<text x="'+p[0]+'" y="'+p[1]+'" text-anchor="middle" dominant-baseline="middle" font-size="10" fill="#8b949e" font-family="system-ui,sans-serif">'+lbl+'</text>');
  });
  // Score values at vertex
  scores.forEach(function(s,i){
    const p=pt(i,s);
    const off=axPt(i,1);
    const dx=(p[0]-cx)*0.18, dy=(p[1]-cy)*0.18;
    svgParts.push('<text x="'+(p[0]+dx)+'" y="'+(p[1]+dy)+'" text-anchor="middle" dominant-baseline="middle" font-size="9" fill="'+verdictColor+'" font-weight="700">'+s+'</text>');
  });
  // Center label
  const cLabel=(consensus||0)>=70?'Bullish':(consensus||0)>=50?'Cautious':'Bearish';
  svgParts.push('<text x="'+cx+'" y="'+(cy-6)+'" text-anchor="middle" font-size="13" font-weight="800" fill="'+verdictColor+'">'+Math.round(consensus||0)+'</text>');
  svgParts.push('<text x="'+cx+'" y="'+(cy+9)+'" text-anchor="middle" font-size="9" fill="#8b949e">'+cLabel+'</text>');
  wrap.innerHTML='<svg width="'+W+'" height="'+H+'" viewBox="0 0 '+W+' '+H+'" style="max-width:300px;">'+svgParts.join('')+'</svg>';
  wrap.style.display='block';
}

// ── Audit flags (#90) ─────────────────────────────────────────────────────
function renderAudit(raw){
  const el=document.getElementById('audit-content');
  const jid=window._currentJobId||'';
  const lines=raw.split('\\n');
  let html='',hdr=true;
  lines.forEach(line=>{
    if(!line.trim())return;
    const isRed=line.includes('RED FLAG')||line.includes('[XX]')||line.includes('\\u2717');
    const isWarn=line.includes('OPTIMISTIC')||line.includes('[!!]')||line.includes('\\u26a0')||line.includes('WARN');
    const isOk=line.includes('VALIDATED')||line.includes('[OK]')||line.includes('\\u2713');
    if(isRed||isWarn||isOk){
      hdr=false;
      const fc=isRed?'fc-red':isWarn?'fc-warn':'fc-ok';
      const bc=isRed?'fb-red':isWarn?'fb-warn':'fb-ok';
      const bl=isRed?'&#10007; RED FLAG':isWarn?'&#9888; OPTIMISTIC':'&#10003; OK';
      const flagTxt=esc(line.replace(/[[]XX[]]|[[]OK[]]|[[]!![]]|RED FLAG|VALIDATED|OPTIMISTIC|[✓✗⚠]/g,'').trim());
      const rawFlagTxt=line.replace(/[[]XX[]]|[[]OK[]]|[[]!![]]|RED FLAG|VALIDATED|OPTIMISTIC|[✓✗⚠]/g,'').trim().slice(0,120);
      const overrideBtn=(isRed||isWarn)?`<button onclick="_oaShowInput(this,'${jid}','Audit',${JSON.stringify(rawFlagTxt)})" style="margin-left:auto;padding:2px 8px;background:rgba(88,166,255,.08);border:1px solid rgba(88,166,255,.2);color:#58a6ff;border-radius:3px;font-size:10px;cursor:pointer;flex-shrink:0;">Override</button>`:'';
      html+=`<div class="flag-card ${fc}" data-oa-wrap="1" style="flex-wrap:wrap;"><span class="fb ${bc}">${bl}</span><span style="font-size:12px;color:#c9d1d9;flex:1;">${flagTxt}</span>${overrideBtn}</div>`;
    } else if(hdr){
      html+=`<div style="font-size:12px;color:#8b949e;margin-bottom:6px;">${esc(line)}</div>`;
    }
  });
  el.innerHTML=(html||`<pre class="mono">${esc(raw)}</pre>`)+'<div id="oa-log-audit"></div>';
  _oaRenderLog(jid,'oa-log-audit');
}

/* ── Bias tab: Kill Shot + structured flags (#213) ── */
function renderBias(raw, data){
  const ks=document.getElementById('bias-killshot');
  const sf=document.getElementById('bias-flags-structured');
  const pn=document.getElementById('bias-portfolio-note');
  const pre=document.getElementById('bias-content');
  if(!raw||!ks){if(pre){pre.textContent=raw||'(No bias data)';pre.style.display='block';}return;}

  // Parse lines for HIGH/DETECTED/[!]/MEDIUM/LOW flags
  const lines=raw.split('\\n').map(l=>l.trim()).filter(Boolean);
  const flags=[];
  lines.forEach(line=>{
    const isHigh=/(HIGH|DETECTED|\\[!\\]|WARNING|CRITICAL)/i.test(line);
    const isMed=/(MEDIUM|POSSIBLE|POTENTIAL|MODERATE)/i.test(line);
    const isLow=/(LOW|MINOR|MINIMAL)/i.test(line);
    if(isHigh||isMed||isLow){
      flags.push({text:line.replace(/^[\\[!\\]HIGH|MEDIUM|LOW|DETECTED|WARNING|CRITICAL:]+/i,'').replace(/^[-:•→]+/,'').trim(),
        level:isHigh?'HIGH':isMed?'MEDIUM':'LOW'});
    }
  });
  // #218: Store parsed flags for override audit gate
  _currentBiasFlags=flags.slice();

  // Kill Shot — highest severity flag in plain English
  const topFlag=flags.find(f=>f.level==='HIGH')||flags[0];
  if(topFlag&&ks){
    ks.style.display='block';
    ks.innerHTML=`<div style="background:rgba(248,81,73,.07);border:1px solid rgba(248,81,73,.3);border-left:4px solid #f85149;border-radius:8px;padding:14px 16px;">
      <div style="font-family:var(--mono);font-size:9px;letter-spacing:.12em;color:#f85149;text-transform:uppercase;margin-bottom:6px;">&#9888; Kill Shot — Highest Risk Flag</div>
      <div style="font-size:13px;color:#f0ede8;line-height:1.5;">${esc(topFlag.text)}</div>
    </div>`;
  }

  // Structured flag cards with Override button (#232)
  const jid=window._currentJobId||'';
  if(sf&&flags.length){
    sf.style.display='block';
    const colorMap={HIGH:['rgba(248,81,73,.06)','rgba(248,81,73,.3)','#f85149'],
                    MEDIUM:['rgba(232,160,32,.06)','rgba(232,160,32,.25)','#e8a020'],
                    LOW:['rgba(63,185,80,.06)','rgba(63,185,80,.2)','#3fb950']};
    sf.innerHTML='<div style="font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);opacity:.7;margin:12px 0 8px;">All Bias Flags</div>'+
      flags.map(f=>{const [bg,bdr,fc]=colorMap[f.level];
        const rawFlagTxt=f.text.slice(0,120);
        const overrideBtn=f.level!=='LOW'?`<button onclick="_oaShowInput(this,'${jid}','Bias',${JSON.stringify(rawFlagTxt)})" style="margin-left:auto;padding:2px 8px;background:rgba(88,166,255,.08);border:1px solid rgba(88,166,255,.2);color:#58a6ff;border-radius:3px;font-size:10px;cursor:pointer;flex-shrink:0;">Override</button>`:'';
        return `<div style="background:${bg};border:1px solid ${bdr};border-radius:7px;padding:10px 13px;margin-bottom:6px;display:flex;align-items:flex-start;gap:10px;flex-wrap:wrap;" data-oa-wrap="1">
          <span style="font-family:var(--mono);font-size:9px;font-weight:700;color:${fc};flex-shrink:0;margin-top:2px;">${f.level}</span>
          <span style="font-size:12px;color:var(--text-secondary);line-height:1.5;flex:1;">${esc(f.text)}</span>
          ${overrideBtn}
        </div>`;}).join('')
      +'<div id="oa-log-bias"></div>';
    _oaRenderLog(jid,'oa-log-bias');
  }

  // Portfolio pattern note — check localStorage history for similar flags
  try{
    const hist=JSON.parse(localStorage.getItem('ce_history')||'[]');
    const similarDeals=hist.filter(h=>h.id!==window._currentJobId&&h.bias&&flags.some(f=>h.bias.includes(f.text.slice(0,30))));
    if(similarDeals.length&&pn){
      pn.style.display='block';
      pn.innerHTML=`<div style="background:rgba(232,160,32,.05);border:1px solid rgba(232,160,32,.18);border-radius:7px;padding:10px 13px;font-size:12px;color:var(--text-muted);">
        <span style="color:var(--accent);font-weight:600;">&#9888; Portfolio Pattern:</span> Similar bias flags appeared in ${similarDeals.length} other deal${similarDeals.length>1?'s':''} in your history. This may indicate a systematic assumption drift.
      </div>`;
    }
  }catch(_){}

  // Fallback raw text hidden (available via details)
  if(pre){pre.textContent=raw;pre.style.display='none';}

  // If no flags parsed, fall back to raw
  if(!flags.length&&pre){pre.style.display='block';}

  // #243: Trigger Geography Risk panel if geo extrapolation flag detected
  const geoTrigger=/geographic.*(extrapolat|bias|assumption|confiden|unknown)|sun.belt.*risk|market.*not.*verified|location.*bias|msa.*unfamiliar/i;
  if(geoTrigger.test(raw)){
    renderGeographyRisk((data&&data.deal)||{});
  }
  // #244: Render assumption evidence panel for flagged assumptions
  renderAssumptionEvidence(raw,(data&&data.deal)||{});

  // #254: Save flag snapshot to localStorage and render version timeline
  _bvtSaveAndRender(flags);
}

// ── Bias version timeline (#254) ────────────────────────────────────────────
function _bvtSaveAndRender(flags){
  const jid=window._currentJobId||'';
  if(!jid||!flags)return;
  const storageKey='ce_analysis_history_'+jid;
  // Load existing versions
  let versions=[];
  try{versions=JSON.parse(localStorage.getItem(storageKey)||'[]');}catch(e){}
  // Count by severity
  const high=flags.filter(function(f){return f.level==='HIGH';}).length;
  const med =flags.filter(function(f){return f.level==='MEDIUM';}).length;
  const low =flags.filter(function(f){return f.level==='LOW';}).length;
  const total=flags.length;
  const ts=new Date().toISOString();
  // Append new version
  versions.push({ts:ts,high:high,med:med,low:low,total:total});
  // Keep max 5 versions
  if(versions.length>5)versions=versions.slice(versions.length-5);
  try{localStorage.setItem(storageKey,JSON.stringify(versions));}catch(e){}
  // Only show timeline if more than 1 version
  const el=document.getElementById('bias-version-timeline');
  if(!el||versions.length<2)return;
  el.style.display='block';
  const rows=versions.map(function(v,i){
    const isCurrent=i===versions.length-1;
    const d=new Date(v.ts);
    const dateStr=d.toLocaleDateString('en-US',{month:'short',day:'numeric'})+' '+d.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'});
    // Dot strip
    let dots='';
    for(let h=0;h<v.high;h++)dots+='<span class="bvt-dot bvt-dot-high" title="HIGH"></span>';
    for(let m=0;m<v.med;m++)dots+='<span class="bvt-dot bvt-dot-med" title="MEDIUM"></span>';
    for(let l=0;l<v.low;l++)dots+='<span class="bvt-dot bvt-dot-low" title="LOW"></span>';
    const breakdown=(v.high?'<span class="bvt-label-high">'+v.high+' H </span>':'')
      +(v.med?'<span class="bvt-label-med">'+v.med+' M </span>':'')
      +(v.low?'<span class="bvt-label-low">'+v.low+' L</span>':'');
    return '<div class="bvt-row'+(isCurrent?' bvt-current':'')+'">'+
      '<div class="bvt-ver">v'+(i+1)+'</div>'+
      '<div class="bvt-date">'+dateStr+'</div>'+
      '<div class="bvt-count">'+v.total+' flags</div>'+
      '<div style="display:flex;align-items:center;gap:6px;flex:1;">'+
        '<div class="bvt-dots">'+dots+'</div>'+
        '<div>'+breakdown+'</div>'+
      '</div>'+
    '</div>';
  }).join('');
  el.innerHTML='<div class="bvt-title">Flag Severity Across Re-analyses</div>'+rows;
}

// ── Geography Risk Module (#243) ─────────────────────────────────────────
// MSA profiles for top 30 US real estate markets
const _MSA_PROFILES={
  'Phoenix':     {state:'AZ',pop:[4.8,5.1,4.3,4.9,5.2],employers:['Intel','Honeywell','Banner Health'],supply_pct:4.1,abs_months:3.8,vacancy:5.2,rent_growth:4.8,cap_rate:5.1,irr_med:16},
  'Dallas':      {state:'TX',pop:[6.3,6.7,4.1,5.8,5.5],employers:['AT&T','American Airlines','Bank of America'],supply_pct:5.2,abs_months:5.1,vacancy:7.1,rent_growth:3.2,cap_rate:4.8,irr_med:15},
  'Austin':      {state:'TX',pop:[8.1,3.2,5.4,7.1,3.9],employers:['Dell','Apple','Samsung'],supply_pct:7.8,abs_months:7.2,vacancy:9.4,rent_growth:1.1,cap_rate:4.4,irr_med:13},
  'Denver':      {state:'CO',pop:[2.1,1.4,2.8,3.1,2.6],employers:['Lockheed Martin','Centura Health','DaVita'],supply_pct:3.9,abs_months:4.8,vacancy:7.8,rent_growth:2.1,cap_rate:4.7,irr_med:14},
  'Atlanta':     {state:'GA',pop:[3.2,2.8,3.5,4.1,4.3],employers:['Delta Air Lines','Cox Enterprises','Home Depot'],supply_pct:4.4,abs_months:4.2,vacancy:6.8,rent_growth:3.4,cap_rate:5.0,irr_med:15},
  'Miami':       {state:'FL',pop:[1.8,2.1,3.4,3.8,2.9],employers:['Baptist Health','Carnival Corp','AmeriHealth'],supply_pct:5.1,abs_months:5.8,vacancy:5.1,rent_growth:3.8,cap_rate:4.5,irr_med:14},
  'Tampa':       {state:'FL',pop:[2.4,2.9,3.8,4.2,3.6],employers:['WellCare','Raymond James','Publix'],supply_pct:4.8,abs_months:4.1,vacancy:5.6,rent_growth:4.2,cap_rate:5.2,irr_med:16},
  'Orlando':     {state:'FL',pop:[3.1,0.8,3.2,4.4,4.1],employers:['Walt Disney','UCF','AdventHealth'],supply_pct:5.6,abs_months:4.7,vacancy:6.1,rent_growth:3.9,cap_rate:5.0,irr_med:15},
  'Charlotte':   {state:'NC',pop:[3.2,3.4,3.9,4.6,4.8],employers:['Bank of America','Wells Fargo','Atrium Health'],supply_pct:4.1,abs_months:3.9,vacancy:6.4,rent_growth:4.1,cap_rate:5.1,irr_med:16},
  'Raleigh':     {state:'NC',pop:[3.8,3.1,4.2,5.1,4.7],employers:['IBM','Lenovo','WakeMed'],supply_pct:5.3,abs_months:4.4,vacancy:7.2,rent_growth:3.1,cap_rate:4.6,irr_med:14},
  'Nashville':   {state:'TN',pop:[2.9,2.1,3.4,4.1,3.8],employers:['HCA Healthcare','Bridgestone','Nissan'],supply_pct:5.4,abs_months:5.2,vacancy:7.9,rent_growth:2.8,cap_rate:4.8,irr_med:14},
  'Las Vegas':   {state:'NV',pop:[2.1,0.3,3.1,3.8,2.9],employers:['MGM Resorts','Station Casinos','UNLV Health'],supply_pct:3.2,abs_months:3.1,vacancy:5.8,rent_growth:3.6,cap_rate:5.3,irr_med:15},
  'Salt Lake City':{state:'UT',pop:[2.6,2.8,2.9,3.1,2.4],employers:['Goldman Sachs','Adobe','Qualtrics'],supply_pct:3.9,abs_months:4.1,vacancy:6.8,rent_growth:2.4,cap_rate:4.9,irr_med:14},
  'Boise':       {state:'ID',pop:[4.1,4.8,6.2,4.1,2.9],employers:['HP Inc.','Micron','St. Lukes Health'],supply_pct:4.8,abs_months:5.9,vacancy:8.2,rent_growth:1.8,cap_rate:5.0,irr_med:13},
  'Seattle':     {state:'WA',pop:[1.4,0.9,1.2,1.8,1.6],employers:['Amazon','Microsoft','Boeing'],supply_pct:3.8,abs_months:6.1,vacancy:8.1,rent_growth:2.1,cap_rate:4.1,irr_med:12},
  'San Antonio': {state:'TX',pop:[2.8,2.1,3.1,3.4,3.2],employers:['USAA','Valero Energy','H-E-B'],supply_pct:4.2,abs_months:4.8,vacancy:6.2,rent_growth:3.1,cap_rate:5.4,irr_med:15},
  'Jacksonville':{state:'FL',pop:[2.1,1.8,2.9,3.4,3.1],employers:['Florida Blue','Fidelity','Mayo Clinic'],supply_pct:3.8,abs_months:3.6,vacancy:5.9,rent_growth:3.8,cap_rate:5.3,irr_med:16},
  'Indianapolis':{state:'IN',pop:[0.8,0.4,1.1,1.4,1.2],employers:['Eli Lilly','Roche','Anthem'],supply_pct:2.8,abs_months:3.4,vacancy:6.1,rent_growth:2.8,cap_rate:5.6,irr_med:15},
  'Columbus':    {state:'OH',pop:[1.2,0.9,1.6,1.8,1.7],employers:['Ohio State U.','JPMorgan Chase','Nationwide'],supply_pct:2.9,abs_months:3.1,vacancy:5.8,rent_growth:3.2,cap_rate:5.5,irr_med:15},
  'Kansas City': {state:'MO',pop:[0.6,0.4,0.9,1.1,1.0],employers:['Cerner','H&R Block','Garmin'],supply_pct:2.4,abs_months:3.8,vacancy:5.4,rent_growth:3.4,cap_rate:5.8,irr_med:16},
  'Memphis':     {state:'TN',pop:[0.2,0.1,0.4,0.5,0.4],employers:['FedEx','AutoZone','International Paper'],supply_pct:2.1,abs_months:4.2,vacancy:7.2,rent_growth:2.1,cap_rate:6.2,irr_med:16},
  'Sacramento':  {state:'CA',pop:[0.8,0.4,0.9,1.1,0.9],employers:['CalSTRS','Intel','Sutter Health'],supply_pct:2.2,abs_months:4.8,vacancy:5.6,rent_growth:2.6,cap_rate:4.8,irr_med:13},
  'Riverside':   {state:'CA',pop:[1.1,0.6,1.4,1.8,1.6],employers:['Amazon','UCR','Loma Linda Health'],supply_pct:2.8,abs_months:3.2,vacancy:4.8,rent_growth:3.1,cap_rate:4.9,irr_med:14},
  'Tucson':      {state:'AZ',pop:[1.2,0.8,1.4,1.8,1.6],employers:['University of Arizona','Raytheon','Banner Health'],supply_pct:2.4,abs_months:3.6,vacancy:5.4,rent_growth:3.8,cap_rate:5.6,irr_med:15},
  'Albuquerque': {state:'NM',pop:[0.4,0.2,0.6,0.8,0.7],employers:['Intel','Sandia National Labs','Lovelace Health'],supply_pct:1.8,abs_months:4.1,vacancy:6.8,rent_growth:2.4,cap_rate:6.0,irr_med:14},
  'El Paso':     {state:'TX',pop:[0.9,0.3,0.8,1.1,1.0],employers:['Fort Bliss','UTEP','WellMed'],supply_pct:2.2,abs_months:4.4,vacancy:5.2,rent_growth:3.4,cap_rate:5.9,irr_med:15},
  'Oklahoma City':{state:'OK',pop:[1.1,0.6,1.2,1.4,1.3],employers:['Chesapeake Energy','INTEGRIS Health','Hobby Lobby'],supply_pct:2.6,abs_months:4.8,vacancy:6.1,rent_growth:2.8,cap_rate:6.1,irr_med:15},
  'Portland':    {state:'OR',pop:[0.6,0.2,0.8,0.9,0.7],employers:['Nike','Intel','Providence Health'],supply_pct:3.1,abs_months:5.8,vacancy:7.4,rent_growth:1.6,cap_rate:4.6,irr_med:12},
  'Minneapolis': {state:'MN',pop:[0.4,0.1,0.6,0.8,0.7],employers:['UnitedHealth','Target','3M'],supply_pct:3.2,abs_months:5.4,vacancy:6.8,rent_growth:1.9,cap_rate:4.9,irr_med:13},
  'St. Louis':   {state:'MO',pop:[0.1,-0.1,0.3,0.4,0.3],employers:['Edward Jones','Centene','Anheuser-Busch'],supply_pct:1.9,abs_months:4.6,vacancy:6.2,rent_growth:2.4,cap_rate:5.7,irr_med:14}
};
// National averages for comparison
const _MSA_NATIONAL={vacancy:6.6,rent_growth:2.9,cap_rate:5.3,pop_avg:1.9,supply_pct:3.4,irr_med:14};

// #250: Comparable closed deal database (hardcoded CoStar/RealPage 2024 data, vintage-labeled)
const _COMP_DATABASE={
  'Phoenix':[
    {name:'Sunbelt Class B, 180u',cap:5.3,ppu:198000,irr:17.2,units:180,vtg:'Q2 2024',src:'CoStar'},
    {name:'Garden-style, East Valley',cap:5.1,ppu:215000,irr:16.8,units:240,vtg:'Q3 2024',src:'CoStar'},
    {name:'Value-add, Chandler',cap:4.9,ppu:228000,irr:18.1,units:132,vtg:'Q1 2024',src:'RealPage'},
    {name:'Core plus, Scottsdale adj',cap:5.6,ppu:185000,irr:15.4,units:96,vtg:'Q4 2023',src:'CBRE'}
  ],
  'Dallas':[
    {name:'Dallas Suburbs Class B',cap:5.2,ppu:175000,irr:15.8,units:210,vtg:'Q3 2024',src:'CoStar'},
    {name:'Value-add, Fort Worth',cap:5.5,ppu:162000,irr:16.9,units:144,vtg:'Q2 2024',src:'RealPage'},
    {name:'Garden-style, Frisco',cap:4.8,ppu:238000,irr:15.1,units:300,vtg:'Q1 2024',src:'CBRE'},
    {name:'Core, Plano submarket',cap:5.0,ppu:210000,irr:14.8,units:168,vtg:'Q4 2023',src:'CoStar'}
  ],
  'Atlanta':[
    {name:'Sunbelt Value-add, 160u',cap:5.4,ppu:168000,irr:16.5,units:160,vtg:'Q2 2024',src:'CoStar'},
    {name:'Class B, Buckhead adj',cap:5.1,ppu:192000,irr:15.9,units:112,vtg:'Q3 2024',src:'JLL'},
    {name:'Garden style, Alpharetta',cap:5.7,ppu:155000,irr:17.3,units:224,vtg:'Q1 2024',src:'RealPage'}
  ],
  'Denver':[
    {name:'Colorado Class B, 148u',cap:5.0,ppu:258000,irr:14.6,units:148,vtg:'Q2 2024',src:'CoStar'},
    {name:'Value-add, Aurora',cap:5.3,ppu:235000,irr:15.8,units:96,vtg:'Q3 2024',src:'CBRE'},
    {name:'Garden, Colorado Springs',cap:5.8,ppu:195000,irr:16.4,units:192,vtg:'Q1 2024',src:'RealPage'}
  ],
  'Austin':[
    {name:'Austin Metro Class B',cap:5.5,ppu:210000,irr:16.2,units:200,vtg:'Q2 2024',src:'CoStar'},
    {name:'Value-add, Round Rock',cap:5.8,ppu:185000,irr:17.1,units:136,vtg:'Q3 2024',src:'RealPage'},
    {name:'Garden style, Pflugerville',cap:6.0,ppu:172000,irr:17.8,units:176,vtg:'Q1 2024',src:'CBRE'}
  ],
  'Nashville':[
    {name:'Nashville Suburbs, 120u',cap:5.2,ppu:220000,irr:16.0,units:120,vtg:'Q2 2024',src:'CoStar'},
    {name:'Class B, Murfreesboro',cap:5.6,ppu:196000,irr:16.9,units:168,vtg:'Q3 2024',src:'JLL'},
    {name:'Value-add, Brentwood adj',cap:4.9,ppu:248000,irr:15.3,units:88,vtg:'Q1 2024',src:'RealPage'}
  ],
  'Tampa':[
    {name:'Tampa Bay Class B, 200u',cap:5.3,ppu:188000,irr:17.0,units:200,vtg:'Q2 2024',src:'CoStar'},
    {name:'Garden, St. Petersburg',cap:5.6,ppu:172000,irr:17.8,units:144,vtg:'Q3 2024',src:'CBRE'},
    {name:'Value-add, Clearwater',cap:5.9,ppu:158000,irr:18.4,units:112,vtg:'Q1 2024',src:'RealPage'}
  ],
  'Charlotte':[
    {name:'Charlotte Suburbs, 156u',cap:5.4,ppu:178000,irr:16.3,units:156,vtg:'Q2 2024',src:'CoStar'},
    {name:'Class B, Concord',cap:5.7,ppu:162000,irr:17.2,units:128,vtg:'Q3 2024',src:'JLL'},
    {name:'Garden style, Rock Hill',cap:6.0,ppu:148000,irr:17.9,units:184,vtg:'Q1 2024',src:'RealPage'}
  ],
  'Houston':[
    {name:'Houston Class B, 224u',cap:5.6,ppu:152000,irr:16.8,units:224,vtg:'Q2 2024',src:'CoStar'},
    {name:'Value-add, Sugar Land',cap:5.4,ppu:168000,irr:16.1,units:160,vtg:'Q3 2024',src:'CBRE'},
    {name:'Garden, Katy submarket',cap:5.9,ppu:142000,irr:17.5,units:192,vtg:'Q1 2024',src:'RealPage'}
  ],
  'Orlando':[
    {name:'Central FL Class B, 176u',cap:5.3,ppu:195000,irr:17.1,units:176,vtg:'Q2 2024',src:'CoStar'},
    {name:'Value-add, Kissimmee',cap:5.7,ppu:175000,irr:17.9,units:120,vtg:'Q3 2024',src:'JLL'},
    {name:'Garden, Lake Nona adj',cap:5.0,ppu:218000,irr:16.2,units:240,vtg:'Q1 2024',src:'RealPage'}
  ]
};
const _COMP_NATIONAL_MED={cap:5.3,ppu:185000,irr:15.8};

// Render comparable deal benchmarks panel (#250)
function renderCompPanel(deal){
  const panel=document.getElementById('comp-panel');
  if(!panel||!deal)return;
  const _msaMatch=_matchMSA(deal);
  const msaKey=_msaMatch?_msaMatch.name:null;
  const comps=msaKey&&_COMP_DATABASE[msaKey]?_COMP_DATABASE[msaKey]:null;
  if(!comps){panel.style.display='none';return;}
  const dealCap=parseFloat(deal.cap_rate)||null;
  const compCapMed=(comps.reduce(function(s,c){return s+c.cap;},0)/comps.length);
  const capDelta=dealCap!=null?(dealCap-compCapMed):null;
  const deltaStr=capDelta!=null?(capDelta>=0?'+':'')+capDelta.toFixed(1)+'% vs comp median':null;
  const deltaColor=capDelta!=null?(capDelta>0.3?'#3fb950':capDelta<-0.3?'#f85149':'var(--amber)'):'var(--text-muted)';
  const rows=comps.map(function(c){
    const ppuFmt='$'+(c.ppu/1000).toFixed(0)+'K';
    return '<tr style="border-bottom:1px solid rgba(255,255,255,.04);">'
      +'<td style="padding:5px 8px;font-size:11px;color:var(--text-secondary);">'+esc(c.name)+'</td>'
      +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px;color:var(--text-primary);text-align:right;">'+c.cap.toFixed(1)+'%</td>'
      +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px;color:var(--text-primary);text-align:right;">'+ppuFmt+'</td>'
      +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px;color:var(--accent);text-align:right;">'+c.irr.toFixed(1)+'%</td>'
      +'<td style="padding:5px 8px;font-size:9px;color:var(--text-muted);text-align:right;">'+esc(c.vtg)+' '+esc(c.src)+'</td>'
      +'</tr>';
  }).join('');
  const medRow='<tr style="background:rgba(232,160,32,.04);border-top:1px solid rgba(232,160,32,.15);">'
    +'<td style="padding:5px 8px;font-family:var(--mono);font-size:9px;font-weight:700;color:var(--accent);letter-spacing:.05em;">COMP MEDIAN</td>'
    +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px;font-weight:700;color:var(--accent);text-align:right;">'+compCapMed.toFixed(1)+'%</td>'
    +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px;font-weight:700;color:var(--accent);text-align:right;">$'+(comps.reduce(function(s,c){return s+c.ppu;},0)/comps.length/1000).toFixed(0)+'K</td>'
    +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px;font-weight:700;color:var(--accent);text-align:right;">'+(comps.reduce(function(s,c){return s+c.irr;},0)/comps.length).toFixed(1)+'%</td>'
    +'<td></td>'
    +'</tr>';
  panel.style.display='block';
  panel.innerHTML='<div style="background:var(--bg-elevated);border:1px solid var(--border-muted);border-radius:8px;padding:11px 14px;margin-bottom:14px;">'
    +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:9px;">'
    +'<span style="font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);">&#128200; Market Comps &#8212; '+esc(msaKey)+' 2024</span>'
    +(deltaStr?'<span style="font-family:var(--mono);font-size:11px;font-weight:700;color:'+deltaColor+';">'+esc(deltaStr)+'</span>':'')
    +'</div>'
    +'<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;">'
    +'<thead><tr style="border-bottom:1px solid rgba(255,255,255,.08);">'
    +'<th style="padding:4px 8px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);font-weight:400;text-align:left;">Deal</th>'
    +'<th style="padding:4px 8px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);font-weight:400;text-align:right;">Cap Rate</th>'
    +'<th style="padding:4px 8px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);font-weight:400;text-align:right;">PPU</th>'
    +'<th style="padding:4px 8px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);font-weight:400;text-align:right;">IRR</th>'
    +'<th style="padding:4px 8px;font-size:9px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);font-weight:400;text-align:right;">Source</th>'
    +'</tr></thead>'
    +'<tbody>'+rows+medRow+'</tbody>'
    +'</table></div>'
    +(dealCap!=null?'<div style="font-size:10px;color:var(--text-muted);margin-top:7px;font-family:var(--mono);">This deal cap: <strong style="color:'+deltaColor+';">'+dealCap.toFixed(1)+'%</strong> &#8212; '+esc(deltaStr)+'</div>':'')
    +'<div style="font-size:10px;color:var(--text-muted);margin-top:4px;font-style:italic;">Representative closed comps, 2024. Verify with current market data before investment decisions.</div>'
    +'</div>';
}

function _matchMSA(deal){
  const loc=(deal.address||deal.market||deal.location||'').toLowerCase();
  for(const [name,data] of Object.entries(_MSA_PROFILES)){
    if(loc.includes(name.toLowerCase())||loc.includes(data.state.toLowerCase()+' ')){
      return {name,data};
    }
  }
  // Try state abbreviation match
  const stateMatch=loc.match(/,\\s*([A-Z]{2})\\b/i);
  if(stateMatch){
    const st=stateMatch[1].toUpperCase();
    for(const [name,data] of Object.entries(_MSA_PROFILES)){
      if(data.state===st) return {name,data};
    }
  }
  return null;
}

function _sparklineSVG(values, color='#e8a020'){
  const w=80,h=28,pad=2;
  const mn=Math.min(...values),mx=Math.max(...values);
  const range=mx-mn||1;
  const pts=values.map((v,i)=>{
    const x=pad+(i/(values.length-1))*(w-2*pad);
    const y=h-pad-((v-mn)/range)*(h-2*pad);
    return [x,y];
  });
  const poly=pts.map(p=>p.join(',')).join(' ');
  const area='M'+pts[0][0]+','+h+' '+pts.map(p=>p.join(',')).join(' ')+' '+pts[pts.length-1][0]+','+h+' Z';
  return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" xmlns="http://www.w3.org/2000/svg" style="display:block">
    <defs><linearGradient id="spg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="${color}" stop-opacity=".18"/><stop offset="100%" stop-color="${color}" stop-opacity="0"/></linearGradient></defs>
    <path d="${area}" fill="url(#spg)"/>
    <polyline points="${poly}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
    <circle cx="${pts[pts.length-1][0]}" cy="${pts[pts.length-1][1]}" r="2" fill="${color}"/>
  </svg>`;
}

function renderGeographyRisk(deal){
  const panel=document.getElementById('geo-risk-panel');
  if(!panel)return;
  const match=_matchMSA(deal);
  const years=['2019','2020','2021','2022','2023'];
  const nat=_MSA_NATIONAL;

  // Build header
  let html=`<div style="border:1px solid rgba(232,160,32,.2);border-radius:10px;overflow:hidden;background:rgba(232,160,32,.03);">
    <div style="padding:12px 16px;border-bottom:1px solid rgba(232,160,32,.12);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
      <div>
        <div style="font-family:var(--mono);font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:var(--accent);margin-bottom:3px;">&#9651; Geography Risk Module · #243</div>
        <div style="font-size:13px;font-weight:600;color:var(--text-primary);">${match?esc(match.name)+', '+esc(match.data.state):'Market Not Identified'}</div>
      </div>
      <div style="font-family:var(--mono);font-size:9px;color:var(--text-muted);padding:3px 8px;border:1px solid rgba(232,160,32,.2);border-radius:4px;">GEO EXTRAPOLATION FLAG TRIGGERED</div>
    </div>`;

  if(!match){
    html+=`<div style="padding:16px;font-size:12px;color:var(--text-muted);">Market not matched to top-50 MSA database. Verify address in deal details.</div></div>`;
    panel.innerHTML=html;
    panel.style.display='block';
    return;
  }

  const m=match.data;
  const popTrend=m.pop[m.pop.length-1];
  const popColor=popTrend>nat.pop_avg?'#3fb950':'#f85149';
  const vacColor=m.vacancy<nat.vacancy?'#3fb950':'#f85149';
  const rentColor=m.rent_growth>nat.rent_growth?'#3fb950':'#e8a020';
  const capColor=m.cap_rate>nat.cap_rate?'#3fb950':'#e8a020';

  // Population trend sparkline
  html+=`<div style="padding:14px 16px;display:grid;grid-template-columns:1fr 1fr;gap:12px;border-bottom:1px solid rgba(255,255,255,.05);">
    <div>
      <div style="font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);margin-bottom:8px;">Population Growth (YoY %)</div>
      <div style="display:flex;align-items:flex-end;gap:10px;">
        ${_sparklineSVG(m.pop,popColor)}
        <div>
          ${years.map((y,i)=>`<div style="font-size:9px;font-family:var(--mono);color:${m.pop[i]>nat.pop_avg?'#3fb950':'var(--text-muted)'};line-height:1.7;">${y}: <b style="color:${m.pop[i]>nat.pop_avg?'#3fb950':'#f0ede8'}">+${m.pop[i].toFixed(1)}%</b></div>`).join('')}
        </div>
      </div>
    </div>
    <div>
      <div style="font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);margin-bottom:8px;">Top 3 Employers</div>
      ${m.employers.map((e,i)=>`<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
        <div style="width:18px;height:18px;border-radius:3px;background:rgba(232,160,32,.${i===0?'15':i===1?'10':'07'});display:flex;align-items:center;justify-content:center;font-size:9px;font-weight:700;color:var(--accent);font-family:var(--mono);">${i+1}</div>
        <div style="font-size:11.5px;color:var(--text-secondary);">${esc(e)}</div>
      </div>`).join('')}
    </div>
  </div>`;

  // Supply & absorption + market metrics
  const supplyColor=m.supply_pct>5?'#f85149':m.supply_pct>3.5?'#e8a020':'#3fb950';
  html+=`<div style="padding:14px 16px;border-bottom:1px solid rgba(255,255,255,.05);">
    <div style="font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);margin-bottom:10px;">Supply Pipeline vs Absorption</div>
    <div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;">
      <div style="flex:1;min-width:180px;">
        <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text-muted);margin-bottom:4px;font-family:var(--mono);">
          <span>Pipeline (% of stock)</span><span style="color:${supplyColor};">${m.supply_pct.toFixed(1)}%</span>
        </div>
        <div style="height:5px;background:rgba(255,255,255,.07);border-radius:3px;overflow:hidden;">
          <div style="height:100%;width:${Math.min(m.supply_pct/10*100,100)}%;background:${supplyColor};border-radius:3px;transition:width .5s ease;"></div>
        </div>
        <div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text-muted);margin-top:6px;font-family:var(--mono);">
          <span>Absorption runway</span><span style="color:${m.abs_months>5.5?'#f85149':'#3fb950'};">${m.abs_months.toFixed(1)} mo.</span>
        </div>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px;flex-shrink:0;">
        ${[
          ['Vacancy',m.vacancy+'%',vacColor,nat.vacancy+'%'],
          ['Rent Growth','+'+m.rent_growth+'%',rentColor,'+'+nat.rent_growth+'%'],
          ['Cap Rate',m.cap_rate+'%',capColor,nat.cap_rate+'%'],
          ['Median IRR',m.irr_med+'%','var(--accent)',nat.irr_med+'%']
        ].map(([label,val,col,natVal])=>`<div style="background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:6px;padding:6px 10px;">
          <div style="font-family:var(--mono);font-size:8px;color:var(--text-muted);margin-bottom:2px;">${label}</div>
          <div style="font-size:13px;font-weight:700;color:${col};">${val}</div>
          <div style="font-size:9px;color:var(--text-muted);">vs ${natVal} natl</div>
        </div>`).join('')}
      </div>
    </div>
  </div>`;

  // Divergence call-out — vs home-market heuristic
  const divergences=[];
  if(Math.abs(m.vacancy-nat.vacancy)>1.5)divergences.push({label:'Vacancy',msa:m.vacancy+'%',nat:nat.vacancy+'%',dir:m.vacancy>nat.vacancy?'higher':'lower'});
  if(Math.abs(m.rent_growth-nat.rent_growth)>1.0)divergences.push({label:'Rent Growth',msa:'+'+m.rent_growth+'%',nat:'+'+nat.rent_growth+'%',dir:m.rent_growth>nat.rent_growth?'stronger':'weaker'});
  if(Math.abs(m.cap_rate-nat.cap_rate)>0.5)divergences.push({label:'Cap Rate',msa:m.cap_rate+'%',nat:nat.cap_rate+'%',dir:m.cap_rate>nat.cap_rate?'higher':'lower'});
  if(divergences.length){
    html+=`<div style="padding:12px 16px;background:rgba(248,81,73,.04);">
      <div style="font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.07em;color:#f85149;margin-bottom:8px;">&#9888; Key Divergences vs National Average</div>
      <div style="display:flex;flex-wrap:wrap;gap:8px;">
        ${divergences.slice(0,3).map(d=>`<div style="padding:4px 10px;background:rgba(248,81,73,.06);border:1px solid rgba(248,81,73,.2);border-radius:4px;font-size:11px;color:var(--text-secondary);font-family:var(--mono);">
          <b style="color:#f85149;">${esc(d.label)}</b> is ${esc(d.dir)} here: <b>${esc(d.msa)}</b> vs ${esc(d.nat)} nationally
        </div>`).join('')}
      </div>
    </div>`;
  }

  html+='</div>';
  panel.innerHTML=html;
  panel.style.display='block';
}

// ── Assumption Evidence Panel (#244) ─────────────────────────────────────
// Hardcoded published benchmarks per assumption category (vintage-labeled)
const _ASSUMPTION_BENCHMARKS={
  rent_growth:{
    label:'Rent Growth',icon:'&#128200;',
    triggers:/rent.growth|rent.*assump|rent.*project|income.*growth|rental.*rate.*increas/i,
    sources:[
      {src:'CoStar Q3 2024',val:'2.1%',note:'Multifamily effective rent growth, national avg'},
      {src:'CBRE Research Q4 2024',val:'1.8–3.2%',note:'Sunbelt markets, class B multifamily'},
      {src:'RealPage Analytics Q4 2024',val:'0.6%',note:'12-mo rent change, top 50 metros'}
    ]
  },
  cap_rate:{
    label:'Cap Rate / Exit Cap',icon:'&#127919;',
    triggers:/exit.cap|cap.rate|terminal.cap|reversion|exit.*yield/i,
    sources:[
      {src:'NCREIF NPI Q3 2024',val:'5.4%',note:'Core multifamily cap rates, institutional grade'},
      {src:'CBRE Cap Rate Survey H2 2024',val:'5.1–5.8%',note:'Class A–B suburban apartments'},
      {src:'JLL Capital Markets Q4 2024',val:'5.6–6.2%',note:'Comparable exit cap rate range'}
    ]
  },
  vacancy:{
    label:'Vacancy / Occupancy',icon:'&#127970;',
    triggers:/vacanc|occupanc|stabiliz.*occupanc|absorption|lease.up/i,
    sources:[
      {src:'CoStar National Q3 2024',val:'7.8%',note:'Multifamily vacancy rate, national avg'},
      {src:'RealPage Q4 2024',val:'94.2%',note:'Physical occupancy, stabilized assets'},
      {src:'CBRE Viewpoint 2025',val:'6.5–8.5%',note:'Supply-impacted markets near-term range'}
    ]
  },
  exit_timing:{
    label:'Hold Period / Exit Timing',icon:'&#9201;',
    triggers:/hold.period|exit.*timing|year.*hold|dispose|reversion.*year|5.year|7.year/i,
    sources:[
      {src:'NCREIF ODCE Fund Avg 2024',val:'7.2 yrs',note:'Average core fund hold period'},
      {src:'CBRE Investor Survey 2024',val:'5–7 yrs',note:'Target hold period, value-add CRE'},
      {src:'Green Street Advisors 2024',val:'+60–120bps',note:'Exit cap premium for >7yr hold in rising-rate environment'}
    ]
  },
  noi_expenses:{
    label:'NOI / Operating Expenses',icon:'&#128176;',
    triggers:/operating.expense|\\bNOI\\b|net.operating|opex|expense.ratio|insurance.*increas|tax.*burden/i,
    sources:[
      {src:'IREM Income/Expense 2024',val:'37–42%',note:'OpEx ratio range, multifamily (excl debt service)'},
      {src:'NMHC Apartment Survey 2024',val:'$8,400/unit',note:'Median operating expenses per unit, class B'},
      {src:'CoStar OpEx Index Q3 2024',val:'+4.1% YoY',note:'Operating cost inflation — insurance & taxes'}
    ]
  }
};

function renderAssumptionEvidence(raw,deal){
  const panel=document.getElementById('assumption-evidence-panel');
  if(!panel||!raw){if(panel)panel.style.display='none';return;}
  const matched=Object.values(_ASSUMPTION_BENCHMARKS).filter(def=>def.triggers.test(raw));
  if(!matched.length){panel.style.display='none';return;}
  const cardHtml=matched.map(cat=>{
    const rows=cat.sources.map(s=>`<div style="display:flex;align-items:flex-start;gap:10px;padding:7px 0;border-bottom:1px solid rgba(255,255,255,.04);">
        <div style="flex:1;min-width:0;">
          <div style="font-family:var(--mono);font-size:10px;color:#58a6ff;margin-bottom:2px;">${esc(s.src)}</div>
          <div style="font-size:11px;color:var(--text-secondary);line-height:1.4;">${esc(s.note)}</div>
        </div>
        <div style="font-family:var(--mono);font-size:14px;font-weight:700;color:var(--accent);white-space:nowrap;flex-shrink:0;">${esc(s.val)}</div>
      </div>`).join('');
    return `<details style="margin-bottom:8px;" open>
      <summary style="cursor:pointer;list-style:none;display:flex;align-items:center;gap:8px;padding:9px 13px;background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.08);border-radius:7px;user-select:none;">
        <span style="font-size:14px;">${cat.icon}</span>
        <span style="font-family:var(--mono);font-size:11px;font-weight:700;color:var(--text-primary);">${esc(cat.label)}</span>
        <span style="margin-left:auto;font-size:9px;color:var(--text-muted);font-family:var(--mono);letter-spacing:.05em;">&#9660; ${cat.sources.length} sources</span>
      </summary>
      <div style="padding:8px 13px 4px;border:1px solid rgba(255,255,255,.06);border-top:none;border-radius:0 0 7px 7px;background:rgba(255,255,255,.015);">
        ${rows}
      </div>
    </details>`;
  }).join('');
  panel.style.display='block';
  panel.innerHTML=`<div style="border-top:1px solid rgba(255,255,255,.07);padding-top:14px;">
    <div style="font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);opacity:.7;margin-bottom:10px;">&#128196; Assumption Evidence &#8212; External Benchmarks</div>
    ${cardHtml}
    <div style="font-size:10px;color:var(--text-muted);margin-top:8px;font-style:italic;">Representative published benchmarks. Verify with current market data before investment decisions.</div>
  </div>`;
}

// ── Override Audit Trail (#232) ───────────────────────────────────────────
const _OA_KEY='ce_override_audit';
function _oaLoad(){try{return JSON.parse(localStorage.getItem(_OA_KEY)||'[]')}catch{return [];}}
function _oaSave(entries){try{localStorage.setItem(_OA_KEY,JSON.stringify(entries));}catch{}}
function _oaRecord(jid,source,flagText,reason){
  const entries=_oaLoad();
  entries.unshift({jid,source,flag_text:flagText,override_reason:reason,timestamp:new Date().toISOString()});
  _oaSave(entries.slice(0,200));
}
function _oaDeleteEntry(idx){const e=_oaLoad();e.splice(idx,1);_oaSave(e);}
function _oaRenderLog(jid,containerId){
  const el=document.getElementById(containerId);if(!el)return;
  const entries=_oaLoad().filter(e=>e.jid===jid);
  if(!entries.length){el.innerHTML='';return;}
  el.innerHTML='<div style="margin-top:16px;border-top:1px solid rgba(255,255,255,.06);padding-top:12px;">'
    +'<div style="font-family:var(--mono);font-size:9px;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin-bottom:8px;">Override Log</div>'
    +entries.map((e,i)=>'<div style="background:rgba(88,166,255,.05);border:1px solid rgba(88,166,255,.15);border-radius:6px;padding:8px 12px;margin-bottom:6px;font-size:11px;">'
      +'<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:8px;">'
      +'<div><span style="font-family:var(--mono);color:#58a6ff;font-size:10px;">OVERRIDE &#x2013; '+esc(e.source)+'</span><br>'
      +'<span style="color:var(--text-muted);font-size:10px;font-style:italic;">Flag: '+esc((e.flag_text||'').slice(0,80))+'</span><br>'
      +'<span style="color:var(--text-secondary);line-height:1.4;">'+esc(e.override_reason)+'</span></div>'
      +'<button onclick="_oaDeleteEntry('+i+');_oaRenderLog(\\''+jid+'\\',\\'oa-log-bias\\');_oaRenderLog(\\''+jid+'\\',\\'oa-log-audit\\');" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:11px;flex-shrink:0;" title="Delete">&#x2715;</button>'
      +'</div>'
      +'<div style="font-family:var(--mono);font-size:9px;color:var(--text-muted);margin-top:3px;">'+esc((e.timestamp||'').slice(0,16).replace('T',' '))+'</div>'
      +'</div>').join('')
    +'<button onclick="_oaExport(\\''+jid+'\\')" style="margin-top:8px;padding:5px 12px;background:rgba(88,166,255,.08);border:1px solid rgba(88,166,255,.2);color:#58a6ff;border-radius:4px;font-size:11px;cursor:pointer;">&#128196; Export Decision Audit Certificate</button>'
    +'</div>';
}
function _oaShowInput(btnEl,jid,source,flagText){
  const container=btnEl.closest('[data-oa-wrap]');
  if(!container)return;
  const existing=container.querySelector('.oa-input-wrap');
  if(existing){existing.remove();return;}
  const wrap=document.createElement('div');
  wrap.className='oa-input-wrap';
  wrap.style.cssText='margin-top:6px;';
  wrap.innerHTML='<input type="text" placeholder="Your override rationale (required)" style="width:100%;background:rgba(255,255,255,.04);border:1px solid rgba(88,166,255,.3);border-radius:4px;padding:6px 9px;font-size:11px;color:var(--text-primary);outline:none;" maxlength="200">'
    +'<div style="display:flex;gap:6px;margin-top:5px;">'
    +`<button onclick="_oaSaveInput(this,'${jid}','${source}')" style="padding:4px 10px;background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.3);color:#58a6ff;border-radius:3px;font-size:11px;cursor:pointer;">Save Override</button>`
    +`<button onclick="this.closest('.oa-input-wrap').remove()" style="padding:4px 8px;background:none;border:1px solid var(--border-default);color:var(--text-muted);border-radius:3px;font-size:11px;cursor:pointer;">Cancel</button>`
    +'</div>';
  wrap.dataset.flagText=flagText;
  container.appendChild(wrap);
  wrap.querySelector('input').focus();
}
function _oaSaveInput(btnEl,jid,source){
  const wrap=btnEl.closest('.oa-input-wrap');
  const reason=(wrap.querySelector('input').value||'').trim();
  if(!reason){wrap.querySelector('input').style.borderColor='rgba(248,81,73,.4)';return;}
  _oaRecord(jid,source,wrap.dataset.flagText||'',reason);
  wrap.remove();
  _oaRenderLog(jid,'oa-log-bias');
  _oaRenderLog(jid,'oa-log-audit');
}
function _oaExport(jid){
  const entries=_oaLoad().filter(e=>e.jid===jid);
  const job=JOBS?.[jid]||{};
  const dealName=(job.deal||{}).deal_name||'Deal Analysis';
  const memo=job.memo||'';
  const mu=memo.toUpperCase();
  const verdict=mu.includes('NO-GO')?'NO-GO':/\\bGO\\b/.test(mu)&&!mu.includes('CONDITIONAL')?'GO':'CONDITIONAL';
  const now=new Date().toLocaleString();
  let txt='CLEAREYE DECISION AUDIT CERTIFICATE\\n';
  txt+='='.repeat(50)+'\\n';
  txt+='Deal: '+dealName+'\\nJob ID: '+jid+'\\nVerdict: '+verdict+'\\nGenerated: '+now+'\\n\\n';
  txt+='OVERRIDE LOG ('+entries.length+' entries)\\n'+'-'.repeat(40)+'\\n';
  entries.forEach((e,i)=>{
    txt+=(i+1)+'. Source: '+e.source+'\\n   Flag: '+(e.flag_text||'').slice(0,120)+'\\n   Override Rationale: '+e.override_reason+'\\n   Timestamp: '+(e.timestamp||'').slice(0,16)+'\\n\\n';
  });
  if(!entries.length)txt+='No overrides recorded for this deal.\\n';
  txt+='\\n'+'-'.repeat(50)+'\\nThis certificate documents the GP\\'s override decisions against AI flags.\\nClearEye AI Real Estate Intelligence Platform.\\n';
  const blob=new Blob([txt],{type:'text/plain'});
  const a=document.createElement('a');
  a.href=URL.createObjectURL(blob);
  a.download='ClearEye_AuditCert_'+jid+'.txt';
  a.click();
}

// ── Executive Summary (#121) ───────────────────────────────────────────────
function renderSummary(data, verdictCls, conf, verdictText){
  const memo=data.memo||'';
  const val=data.validation_report||'';
  const advisors=data.advisors||{};

  // Extract top 3 risks from Bear advisor
  const bearText=Object.entries(advisors).find(([k])=>k.toLowerCase().includes('bear'))?.[1]||'';
  const riskLines=bearText.split('\\n').filter(l=>l.trim().length>30).slice(0,3);

  // Extract top 3 audit flags
  const flagLines=val.split('\\n').filter(l=>l.includes('RED FLAG')||l.includes('[XX]')).slice(0,3);

  // Extract DDQs from memo
  const ddqMatch=memo.match(/due diligence[^\\n]*[\\s\\S]*?(?=\\n\\n|$)/i);
  const ddqs=memo.split('\\n').filter(l=>l.match(/^[0-9]+\\.\\s|^[\\-\\*]\\s.*\\?/)).slice(0,4);

  const riskHtml=riskLines.length
    ?riskLines.map(l=>`<div class="risk-card">${esc(l.substring(0,160))}</div>`).join('')
    :'<div class="sum-empty">No critical risks identified</div>';
  const flagHtml=flagLines.length
    ?flagLines.map(l=>`<div class="flag-item">${esc(l.replace(/RED FLAG|\\[XX\\]|[✗]/g,'').trim().substring(0,160))}</div>`).join('')
    :'<div class="sum-ok">&#10003; No red flags</div>';
  const ddqHtml=ddqs.length
    ?ddqs.map(l=>`<div class="ddq-item">&#63; ${esc(l.replace(/^[0-9]+\\.\\s|^[-*]\\s/,'').trim().substring(0,160))}</div>`).join('')
    :'<div class="sum-empty">See full memo for due diligence questions</div>';
  // Advisor consensus bar (#177, #246 score explainability)
  const consensus=data._advConsensus;
  const scores=data._advScores||[];
  const breakdowns=data._advBreakdowns||[];
  let consensusHtml='';
  if(consensus!=null&&scores.length>0){
    const cColor=consensus>=70?'var(--green)':consensus>=50?'var(--amber)':'var(--red)';
    const cLabel=consensus>=70?'Bullish':consensus>=50?'Cautious':'Bearish';
    // #246: score driver summary — dominant signal across all advisors
    const nogoCount=breakdowns.filter(function(b){return b.isNogo;}).length;
    const highCount=scores.filter(function(s){return s>=70;}).length;
    const lowCount=scores.filter(function(s){return s<50;}).length;
    const driver246=nogoCount>0?nogoCount+' NO-GO advisor(s)':highCount>=Math.ceil(scores.length/2)?highCount+' of '+scores.length+' advisors bullish':lowCount>=Math.ceil(scores.length/2)?lowCount+' of '+scores.length+' advisors cautious':'Mixed signals across advisors';
    // #246: breakdown rows for popover
    const bdRows=breakdowns.map(function(b){
      const sc=b.score>=70?'var(--green)':b.score>=50?'var(--amber)':'var(--red)';
      const methodIcon=b.method==='explicit'?'&#9654;':b.method==='grade'?'&#9650;':'&#126;';
      return '<div style="display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.04);">'
        +'<span style="font-size:11px;color:var(--text-secondary);flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">'+esc(b.name.split(/[:(]/)[0].trim())+'</span>'
        +'<span style="font-family:var(--mono);font-size:12px;font-weight:700;color:'+sc+';flex-shrink:0;">'+b.score+'</span>'
        +'<span style="font-size:9px;color:var(--text-muted);flex-shrink:0;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="'+esc(b.driver)+'">'+methodIcon+' '+esc(b.driver.slice(0,40))+'</span>'
        +'</div>';
    }).join('');
    consensusHtml='<div style="background:var(--bg-elevated);border:1px solid var(--border-muted);border-radius:8px;padding:10px 14px;margin-bottom:14px;position:relative;">'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">'
      +'<span style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text-muted);">&#127775; Advisor Consensus</span>'
      +'<div style="display:flex;align-items:center;gap:6px;">'
      +'<span style="font-size:16px;font-weight:700;color:'+cColor+';">'+consensus+'<span style="font-size:10px;color:var(--text-muted);font-weight:400;">/100</span></span>'
      +'<button id="score-explain-btn" onclick="toggleScorePopover()" title="Score breakdown" style="background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.25);color:#58a6ff;border-radius:50%;width:18px;height:18px;font-size:10px;cursor:pointer;display:flex;align-items:center;justify-content:center;font-weight:700;flex-shrink:0;padding:0;">i</button>'
      +'</div>'
      +'</div>'
      +'<div style="height:6px;background:var(--bg-canvas);border-radius:3px;overflow:hidden;">'
      +'<div style="height:100%;width:'+consensus+'%;background:'+cColor+';border-radius:3px;transition:width .6s ease;"></div>'
      +'</div>'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-top:8px;">'
      +'<div style="display:flex;gap:6px;flex-wrap:wrap;">'
      +scores.map(function(s,idx){const sc=s>=70?'var(--green)':s>=50?'var(--amber)':'var(--red)';return '<span style="font-size:10px;padding:1px 6px;border-radius:3px;border:1px solid '+sc+';color:'+sc+';">'+s+'</span>';}).join('')
      +'</div>'
      +'<span style="font-size:10px;color:var(--text-muted);font-style:italic;">'+esc(driver246)+'</span>'
      +'</div>'
      // #246: Score breakdown popover
      +'<div id="score-popover" style="display:none;position:absolute;top:calc(100% + 6px);right:0;z-index:200;background:var(--bg-elevated);border:1px solid rgba(88,166,255,.25);border-radius:8px;padding:12px 14px;min-width:280px;box-shadow:0 8px 24px rgba(0,0,0,.4);">'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px;">'
      +'<span style="font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:#58a6ff;">Score Breakdown</span>'
      +'<button onclick="toggleScorePopover()" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:13px;line-height:1;padding:0;">&#215;</button>'
      +'</div>'
      +bdRows
      +'<div style="padding-top:8px;margin-top:4px;border-top:1px solid rgba(255,255,255,.07);display:flex;align-items:center;justify-content:space-between;">'
      +'<span style="font-size:11px;color:var(--text-muted);">Consensus (avg)</span>'
      +'<span style="font-family:var(--mono);font-size:14px;font-weight:700;color:'+cColor+';">'+consensus+' &#8212; '+cLabel+'</span>'
      +'</div>'
      +'<div style="font-size:10px;color:var(--text-muted);margin-top:6px;font-style:italic;">'+esc(driver246)+'</div>'
      +'</div>'
      +'</div>';
  }
  document.getElementById('summary-content').innerHTML=consensusHtml
    +'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px;">'
    +'<div><div class="sum-section-label">&#128059; Bear Case Risks</div>'+riskHtml+'</div>'
    +'<div><div class="sum-section-label">&#9888; Assumption Audit Flags</div>'+flagHtml+'</div>'
    +'</div>'
    +'<div><div class="sum-section-label">&#10067; Due Diligence Questions</div>'+ddqHtml+'</div>';
  // #248: Auto-load sponsor track record after summary renders
  const jid248=window._currentJobId||window._reportJobId;
  if(jid248) loadSponsorScore(jid248);
  // #250: Render comparable deal benchmarks
  renderCompPanel(data.deal||{});
}

// #248: Sponsor Track Record — load and render GP quality card
async function loadSponsorScore(jid){
  const card=document.getElementById('sponsor-track-card');
  if(!card||!jid)return;
  card.style.display='block';
  card.innerHTML='<div style="background:var(--bg-elevated);border:1px solid var(--border-muted);border-radius:8px;padding:10px 14px;display:flex;align-items:center;gap:8px;">'
    +'<span style="animation:spin 1s linear infinite;display:inline-block;font-size:12px;color:var(--accent);">&#8635;</span>'
    +'<span style="font-size:11px;color:var(--text-muted);">Analyzing sponsor track record&#8230;</span></div>';
  try{
    const res=await fetch('/api/sponsor_score/'+jid,{method:'POST',headers:{'Content-Type':'application/json'}});
    const d=await res.json();
    if(d.error){card.style.display='none';return;}
    const score=d.track_record_score||0;
    const verdict=d.verdict||'UNVERIFIED';
    const vColor=verdict==='STRONG'?'var(--green)':verdict==='ADEQUATE'?'var(--amber)':'var(--red)';
    const vBg=verdict==='STRONG'?'rgba(63,185,80,.07)':verdict==='ADEQUATE'?'rgba(232,160,32,.07)':'rgba(248,81,73,.06)';
    const vBdr=verdict==='STRONG'?'rgba(63,185,80,.25)':verdict==='ADEQUATE'?'rgba(232,160,32,.25)':'rgba(248,81,73,.25)';
    const fields=[
      {label:'Operator',val:d.operator_name||'Not disclosed'},
      {label:'Years Active',val:d.years_active!=null?d.years_active+' yrs':'Not disclosed'},
      {label:'Prior Deals',val:d.deals_mentioned!=null?d.deals_mentioned+' mentioned':'Not disclosed'},
      {label:'Claimed IRR',val:d.claimed_irr||'Not disclosed'},
      {label:'AUM',val:d.aum_mentioned||'Not disclosed'},
    ];
    const fieldHtml=fields.map(function(f){return '<div style="min-width:0;">'
      +'<div style="font-family:var(--mono);font-size:9px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;margin-bottom:2px;">'+esc(f.label)+'</div>'
      +'<div style="font-size:11px;color:var(--text-secondary);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="'+esc(f.val)+'">'+esc(f.val)+'</div>'
      +'</div>';}).join('');
    card.innerHTML='<div style="background:'+vBg+';border:1px solid '+vBdr+';border-radius:8px;padding:11px 14px;">'
      +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:9px;">'
      +'<span style="font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:var(--text-muted);">&#128203; Sponsor Quality</span>'
      +'<div style="display:flex;align-items:center;gap:8px;">'
      +'<span style="font-family:var(--mono);font-size:14px;font-weight:700;color:'+vColor+';">'+score+'<span style="font-size:9px;font-weight:400;color:var(--text-muted);">/100</span></span>'
      +'<span style="font-family:var(--mono);font-size:9px;font-weight:700;color:'+vColor+';padding:2px 7px;border:1px solid '+vBdr+';border-radius:4px;letter-spacing:.06em;">'+esc(verdict)+'</span>'
      +'</div>'
      +'</div>'
      +'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:9px;">'+fieldHtml+'</div>'
      +(d.rationale?'<div style="font-size:11px;color:var(--text-secondary);line-height:1.5;border-top:1px solid rgba(255,255,255,.06);padding-top:7px;margin-top:2px;">'+esc(d.rationale)+'</div>':'')
      +'</div>';
  }catch(err){card.style.display='none';}
}

// #246: Toggle score breakdown popover
function toggleScorePopover(){
  const p=document.getElementById('score-popover');
  if(!p)return;
  p.style.display=p.style.display==='none'?'block':'none';
  // Close on outside click
  if(p.style.display==='block'){
    setTimeout(function(){
      document.addEventListener('click',function _sp(e){
        if(!p.contains(e.target)&&e.target.id!=='score-explain-btn'){p.style.display='none';document.removeEventListener('click',_sp);}
      });
    },0);
  }
}

// ── Shareable link (#116) ──────────────────────────────────────────────────
// #245: Devil's Advocate — adversarial failure analysis
async function runDevilAdvocate(){
  const jid=window._currentJobId||window._reportJobId;
  const panel=document.getElementById('devil-advocate-panel');
  const btn=document.getElementById('daBtn');
  if(!panel)return;
  // Toggle off if already showing
  if(panel.style.display!=='none'&&panel.innerHTML){panel.style.display='none';if(btn)btn.textContent="&#9760; Devil's Advocate";return;}
  if(!jid){panel.style.display='block';panel.innerHTML='<div style="color:var(--text-muted);font-size:12px;padding:10px 0;">Run an analysis first.</div>';return;}
  // Loading state
  panel.style.display='block';
  panel.innerHTML='<div style="display:flex;align-items:center;gap:10px;padding:14px 16px;background:rgba(248,81,73,.04);border:1px solid rgba(248,81,73,.2);border-radius:8px;">'
    +'<span style="animation:spin 1s linear infinite;display:inline-block;font-size:14px;">&#8635;</span>'
    +'<span style="font-size:12px;color:var(--text-secondary);">Generating adversarial analysis&#8230;</span></div>';
  if(btn){btn.disabled=true;btn.textContent='&#8635; Thinking...';}
  try{
    const res=await fetch('/api/devil_advocate/'+jid,{method:'POST',headers:{'Content-Type':'application/json'}});
    const d=await res.json();
    if(d.error){panel.innerHTML='<div style="color:#f85149;font-size:12px;padding:10px;">Error: '+esc(d.error)+'</div>';return;}
    const modes=d.failure_modes||[];
    const modeColors=['rgba(248,81,73,.06)','rgba(232,160,32,.05)','rgba(88,166,255,.05)'];
    const modeBorders=['rgba(248,81,73,.3)','rgba(232,160,32,.25)','rgba(88,166,255,.2)'];
    const modeAccents=['#f85149','#e8a020','#58a6ff'];
    const cardsHtml=modes.map((m,i)=>`<div style="background:${modeColors[i]};border:1px solid ${modeBorders[i]};border-left:3px solid ${modeAccents[i]};border-radius:7px;padding:12px 14px;margin-bottom:8px;">
      <div style="font-family:var(--mono);font-size:9px;font-weight:700;color:${modeAccents[i]};text-transform:uppercase;letter-spacing:.1em;margin-bottom:5px;">Failure Mode ${i+1}</div>
      <div style="font-size:12px;font-weight:700;color:var(--text-primary);margin-bottom:5px;">${esc(m.title)}</div>
      <div style="font-size:12px;color:var(--text-secondary);line-height:1.55;">${esc(m.body)}</div>
    </div>`).join('');
    panel.innerHTML=`<div style="background:rgba(248,81,73,.03);border:1px solid rgba(248,81,73,.2);border-radius:10px;padding:14px 16px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px;">
        <div style="font-family:var(--mono);font-size:10px;font-weight:700;color:#f85149;letter-spacing:.08em;text-transform:uppercase;">&#9760; Devil's Advocate &#8212; Adversarial Failure Analysis</div>
        <button onclick="document.getElementById('devil-advocate-panel').style.display='none';document.getElementById('daBtn').textContent='&#9760; Devil\\'s Advocate';" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:14px;line-height:1;" title="Close">&#215;</button>
      </div>
      ${cardsHtml}
      <div style="font-size:10px;color:var(--text-muted);margin-top:8px;font-style:italic;">AI-generated adversarial scenarios. Validate assumptions with primary diligence before investment decisions.</div>
    </div>`;
  }catch(err){
    panel.innerHTML='<div style="color:#f85149;font-size:12px;padding:10px;">Request failed: '+esc(String(err))+'</div>';
  }finally{
    if(btn){btn.disabled=false;btn.textContent="&#9760; Devil's Advocate";}
  }
}

function shareLink(){
  const jid=window._currentJobId;
  if(!jid){showToast('No analysis to share yet','error');return;}
  const url=window.location.origin+'/report/'+jid;
  navigator.clipboard.writeText(url).then(()=>{
    showToast('Share link copied to clipboard','success');
    const msg=document.getElementById('share-msg');
    if(msg){msg.style.display='block';msg.textContent='Link copied: '+url;setTimeout(()=>msg.style.display='none',4000);}
  });
}

// ── PDF download (#115) ────────────────────────────────────────────────────
function downloadPDF(){
  const jid=window._currentJobId;
  if(!jid){alert('No analysis to download yet');return;}
  window.open('/export/'+jid,'_blank');
}

// ── Re-analyze with diff view (#190) ──────────────────────────────────────
async function reanalyzeReport(){
  const jid=window._currentJobId;
  if(!jid){alert('No saved analysis to re-run. Submit a deal first.');return;}
  const btn=document.getElementById('reanalyzeBtn');
  if(btn){btn.disabled=true;btn.textContent='Re-analyzing...';}
  try{
    const r=await fetch('/api/reanalyze/'+jid,{method:'POST'});
    const d=await r.json();
    if(!d.ok){
      alert(d.error||'Re-analyze failed');
      if(btn){btn.disabled=false;btn.textContent='↺ Re-analyze';}
      return;
    }
    const newJid=d.new_job_id;
    // Show diff panel immediately in pending state
    _showDiffPanel(jid,newJid,null);
    // Poll for completion then load diff
    let tries=0;
    const pollId=setInterval(async()=>{
      tries++;
      if(tries>60){clearInterval(pollId);return;}
      try{
        const sr=await fetch('/status/'+newJid);
        const sd=await sr.json();
        if(sd.status==='done'){
          clearInterval(pollId);
          const dr=await fetch('/api/diff/'+jid+'/'+newJid);
          const diff=await dr.json();
          _showDiffPanel(jid,newJid,diff);
          if(btn){btn.disabled=false;btn.textContent='↺ Re-analyze';}
        }else if(sd.status==='error'){
          clearInterval(pollId);
          document.getElementById('diff-panel-body').innerHTML='<div style="color:#f85149;font-size:13px;">Re-analysis failed: '+(sd.message||'unknown error')+'</div>';
          if(btn){btn.disabled=false;btn.textContent='↺ Re-analyze';}
        }
      }catch{}
    },3000);
  }catch(e){
    alert('Error: '+e);
    if(btn){btn.disabled=false;btn.textContent='↺ Re-analyze';}
  }
}

function _showDiffPanel(oldJid,newJid,diff){
  let panel=document.getElementById('diff-panel');
  if(!panel){
    panel=document.createElement('div');
    panel.id='diff-panel';
    panel.style.cssText='margin-top:16px;padding:16px;background:rgba(210,153,34,.06);border:1px solid rgba(210,153,34,.25);border-radius:8px;font-size:12px;';
    const resultPanel=document.getElementById('ce-results-inner')||document.querySelector('.ce-results');
    if(resultPanel)resultPanel.appendChild(panel);
  }
  if(!diff){
    panel.innerHTML='<div style="color:#d29922;font-size:12px;">&#8635; Re-analysis running... <span id="diff-spin" style="animation:spin 1s linear infinite;display:inline-block;">&#8635;</span></div>'
      +'<div style="font-size:11px;color:#8b949e;margin-top:4px;">Diff will appear here when complete. <a href="/report/'+newJid+'" target="_blank" style="color:#58a6ff;">View new report &rarr;</a></div>';
    return;
  }
  // Build diff rows
  function _deltaHtml(d){
    if(d===null||d===undefined)return '<span style="color:#8b949e;">—</span>';
    const col=d>0?'#3fb950':d<0?'#f85149':'#8b949e';
    const sign=d>0?'+':'';
    return '<span style="color:'+col+';font-weight:600;">'+sign+d+'</span>';
  }
  function _verdictHtml(v,changed){
    if(!v)return '—';
    const col=v==='GO'?'#3fb950':v.includes('NO')?'#f85149':'#d29922';
    return '<span style="color:'+col+';font-weight:700;">'+v+'</span>'+(changed?' <span style="font-size:10px;background:rgba(248,81,73,.12);color:#f85149;padding:1px 5px;border-radius:3px;">changed</span>':'');
  }
  const rows=[
    ['Verdict', _verdictHtml(diff.verdict.old,false)+' → '+_verdictHtml(diff.verdict.new,diff.verdict.changed)],
    ['Council Score', (diff.council_score.old||'—')+'/100 → '+(diff.council_score.new||'—')+'/100 '+_deltaHtml(diff.council_score.delta)],
    ['Cap Rate',      (diff.cap_rate.old||'—')+'% → '+(diff.cap_rate.new||'—')+'% '+_deltaHtml(diff.cap_rate.delta)],
    ['Asking Price',  (diff.asking_price.old?'$'+parseFloat(diff.asking_price.old).toLocaleString():'—')+' → '+(diff.asking_price.new?'$'+parseFloat(diff.asking_price.new).toLocaleString():'—')],
  ];
  panel.innerHTML='<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">'
    +'<span style="font-weight:700;color:#d29922;font-size:13px;">&#128101; Re-analysis Diff</span>'
    +'<a href="/report/'+newJid+'" target="_blank" style="font-size:11px;color:#58a6ff;">View new report &rarr;</a>'
    +'</div>'
    +'<table style="width:100%;border-collapse:collapse;">'
    +rows.map(r=>'<tr><td style="color:#8b949e;padding:3px 10px 3px 0;min-width:100px;">'+r[0]+'</td><td>'+r[1]+'</td></tr>').join('')
    +'</table>';
}

// ── Print / Save as PDF (#181) ─────────────────────────────────────────────
function printReport(){
  const jid=window._currentJobId;
  if(jid){
    // Prefer WeasyPrint PDF; fall back to browser print dialog
    fetch('/export/'+jid,{method:'HEAD'})
      .then(r=>{ if(r.ok){window.open('/export/'+jid,'_blank');} else{window.print();} })
      .catch(()=>window.print());
  } else {
    window.print();
  }
}

/* ── IC Memo download (#211) ── */
function downloadICMemo(){
  const jid=window._currentJobId;
  if(!jid){alert('No analysis loaded.');return;}
  const fab=document.getElementById('ic-memo-fab');
  const orig=fab?fab.innerHTML:'';
  if(fab){fab.innerHTML='&#9203; Generating…';fab.disabled=true;}
  // Navigate directly — endpoint streams HTML/PDF as attachment
  window.location.href='/api/ic-memo/'+jid;
  setTimeout(()=>{if(fab){fab.innerHTML=orig;fab.disabled=false;}},2500);
}

// #219: Kill Sheet download
function downloadKillSheet(){
  const jid=window._currentJobId;
  if(!jid){alert('No analysis loaded.');return;}
  const btn=document.getElementById('kill-sheet-fab');
  const orig=btn?btn.innerHTML:'';
  if(btn){btn.innerHTML='&#9203; Generating…';btn.disabled=true;}
  window.location.href='/api/kill-sheet/'+jid;
  setTimeout(()=>{if(btn){btn.innerHTML=orig;btn.disabled=false;}},2500);
}

// ── Assumption Editor (#117) ───────────────────────────────────────────────
let _currentDeal={};
function toggleAssumptionEditor(){
  const ed=document.getElementById('assump-editor');
  if(ed.style.display==='none'){
    // Pre-fill with current parsed values
    const d=_currentDeal;
    if(d.cap_rate)document.getElementById('ae-cap').value=d.cap_rate;
    if(d.projected_irr)document.getElementById('ae-irr').value=d.projected_irr;
    if(d.noi)document.getElementById('ae-noi').value=d.noi;
    if(d.asking_price)document.getElementById('ae-price').value=d.asking_price;
    if(d.exit_cap_rate)document.getElementById('ae-exitcap').value=d.exit_cap_rate;
    if(d.rent_growth)document.getElementById('ae-rg').value=d.rent_growth;
    ed.style.display='block';
    document.getElementById('edit-assump-btn').textContent='\\u2715 Close Editor';
  } else {
    ed.style.display='none';
    document.getElementById('edit-assump-btn').textContent='\\u270e Edit Assumptions';
  }
}
/* ── Assumption override + delta re-analysis (#215) ── */
let _overrideBaseline=null; // snapshot before re-analysis for delta comparison
let _activeOverrides={};    // what the user changed
let _currentBiasFlags=[];   // parsed HIGH bias flags from last renderBias() call (#218)
let _overrideAuditLog=[];   // append-only log of override rationales (#218)

// ── #218: Override Audit Modal ──────────────────────────────────────────
function oaOpen(topFlag){
  const backdrop=document.getElementById('oa-backdrop');
  const preview=document.getElementById('oa-flag-preview');
  const ta=document.getElementById('oa-rationale');
  if(!backdrop||!preview)return false;
  preview.textContent=topFlag?'ClearEye flagged: "'+topFlag.text.slice(0,120)+'"':'A high-severity risk flag was detected in this analysis.';
  ta.value='';
  document.getElementById('oa-char-hint').textContent='Minimum 20 characters required';
  document.getElementById('oa-confirm-btn').disabled=true;
  document.getElementById('oa-confirm-btn').style.opacity='0.4';
  backdrop.style.display='flex';
  ta.focus();
  return true;
}
function oaClose(){
  const backdrop=document.getElementById('oa-backdrop');
  if(backdrop)backdrop.style.display='none';
  _pendingOaCallback=null;
}
function oaCheckLength(){
  const ta=document.getElementById('oa-rationale');
  const btn=document.getElementById('oa-confirm-btn');
  const hint=document.getElementById('oa-char-hint');
  const len=(ta.value||'').trim().length;
  const ok=len>=20;
  btn.disabled=!ok;
  btn.style.opacity=ok?'1':'0.4';
  hint.textContent=ok?'&#10003; Rationale logged':'Minimum 20 characters required ('+(len)+'/20)';
  hint.style.color=ok?'#3fb950':'var(--text-muted)';
}
let _pendingOaCallback=null;
function oaConfirm(){
  const ta=document.getElementById('oa-rationale');
  const rationale=(ta.value||'').trim();
  if(rationale.length<20)return;
  const topFlag=_currentBiasFlags.find(f=>f.level==='HIGH');
  const entry={
    ts: new Date().toISOString(),
    jobId: window._currentJobId||'',
    dealName: (_currentDeal&&_currentDeal.deal_name)||'',
    flagText: topFlag?topFlag.text:'(bias flag)',
    overrides: Object.assign({},_activeOverrides),
    rationale: rationale
  };
  _overrideAuditLog.push(entry);
  // Persist to localStorage
  try{
    const stored=JSON.parse(localStorage.getItem('ce_override_audit')||'[]');
    stored.push(entry);
    localStorage.setItem('ce_override_audit',JSON.stringify(stored.slice(-50)));
  }catch(e){}
  oaClose();
  _renderOverrideAuditTrail();
  if(_pendingOaCallback){_pendingOaCallback();_pendingOaCallback=null;}
}
function _renderOverrideAuditTrail(){
  const el=document.getElementById('override-audit-trail');
  if(!el||!_overrideAuditLog.length)return;
  el.style.display='block';
  el.innerHTML='<div style="font-family:var(--mono);font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:var(--accent);opacity:.7;margin-bottom:8px;">Override Audit Trail (This Session)</div>'
    +_overrideAuditLog.map(e=>{
      const dt=new Date(e.ts).toLocaleString(undefined,{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
      return '<div style="background:rgba(232,160,32,.04);border:1px solid rgba(232,160,32,.15);border-radius:6px;padding:10px 12px;margin-bottom:6px;">'
        +'<div style="font-size:10px;color:var(--text-muted);margin-bottom:4px;font-family:var(--mono);">'+esc(dt)+' · '+esc(e.dealName||'Deal')+'</div>'
        +'<div style="font-size:11px;color:var(--text-secondary);margin-bottom:4px;">&#9888; Flag: <em>'+esc((e.flagText||'').slice(0,80))+'</em></div>'
        +'<div style="font-size:11px;color:var(--text-primary);line-height:1.4;">Rationale: '+esc(e.rationale.slice(0,200))+'</div>'
        +'</div>';
    }).join('');
}

async function reAnalyzeWithEdits(){
  const om=document.getElementById('om_input').value.trim();
  if(!om){document.getElementById('ae-status').textContent='No OM text to re-analyze.';return;}
  // #218: If there are HIGH-severity bias flags, require override rationale before proceeding
  const highFlags=_currentBiasFlags.filter(f=>f.level==='HIGH');
  if(highFlags.length){
    _pendingOaCallback=()=>_doReAnalyze(om);
    if(oaOpen(highFlags[0])) return; // modal shown; _doReAnalyze called after confirmation
  }
  _doReAnalyze(om);
}
async function _doReAnalyze(om){
  // Snapshot current result as baseline for delta (#215)
  if(window._currentJobId&&Object.keys(_currentDeal).length){
    _overrideBaseline={
      jobId:   window._currentJobId,
      deal:    JSON.parse(JSON.stringify(_currentDeal)),
      verdict: document.getElementById('verdict-stamp')?.textContent?.trim()||'',
      conf:    parseFloat(document.getElementById('conf-pct')?.textContent)||0,
    };
  }
  // Build override annotations appended to OM text
  const overrides=[];
  _activeOverrides={};
  const cap=document.getElementById('ae-cap').value;
  const irr=document.getElementById('ae-irr').value;
  const noi=document.getElementById('ae-noi').value;
  const price=document.getElementById('ae-price').value;
  const exitcap=document.getElementById('ae-exitcap').value;
  const rg=document.getElementById('ae-rg').value;
  if(cap){overrides.push('ANALYST OVERRIDE — Cap Rate: '+cap+'%');_activeOverrides['Cap Rate']=cap+'%';}
  if(irr){overrides.push('ANALYST OVERRIDE — Projected IRR: '+irr+'%');_activeOverrides['Projected IRR']=irr+'%';}
  if(noi){overrides.push('ANALYST OVERRIDE — NOI: $'+noi);_activeOverrides['NOI']='$'+noi;}
  if(price){overrides.push('ANALYST OVERRIDE — Asking Price: $'+price);_activeOverrides['Asking Price']='$'+price;}
  if(exitcap){overrides.push('ANALYST OVERRIDE — Exit Cap Rate: '+exitcap+'%');_activeOverrides['Exit Cap Rate']=exitcap+'%';}
  if(rg){overrides.push('ANALYST OVERRIDE — Annual Rent Growth: '+rg+'%');_activeOverrides['Rent Growth']=rg+'%';}
  const amendedOM=om+(overrides.length?'\\n\\n--- ANALYST OVERRIDES ---\\n'+overrides.join('\\n'):'');
  document.getElementById('ae-status').textContent='Re-analyzing...';
  document.getElementById('assump-editor').style.display='none';
  // Re-submit via normal analyze flow
  document.getElementById('om_input').value=amendedOM;
  await startAnalyze();
  document.getElementById('ae-status').textContent='';
}

function _renderDeltaPanel(newData){
  // Called from renderResults when _overrideBaseline is set
  if(!_overrideBaseline)return;
  const existing=document.getElementById('delta-panel');
  if(existing)existing.remove();

  const bl=_overrideBaseline;
  const newConf=parseFloat(document.getElementById('conf-pct')?.textContent)||0;
  const newVerdict=document.getElementById('verdict-stamp')?.textContent?.trim()||'';
  const confDelta=newConf-bl.conf;
  const confSign=confDelta>0?'+':'';
  const confColor=confDelta>0?'#3fb950':confDelta<0?'#f85149':'#8b949e';
  const verdictChanged=newVerdict!==bl.verdict;

  const overrideChips=Object.entries(_activeOverrides).map(([k,v])=>
    `<span style="font-family:var(--mono);font-size:10px;background:rgba(232,160,32,.08);border:1px solid rgba(232,160,32,.2);color:var(--accent);border-radius:4px;padding:2px 7px;">${k}: ${v}</span>`
  ).join(' ');

  const panel=document.createElement('div');
  panel.id='delta-panel';
  panel.style.cssText='margin:0 0 14px;padding:14px 16px;background:rgba(232,160,32,.04);border:1px solid rgba(232,160,32,.18);border-radius:10px;';
  panel.innerHTML=`
    <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
      <span style="font-family:var(--mono);font-size:9px;letter-spacing:.1em;text-transform:uppercase;color:var(--accent);">&#916; Delta Re-analysis — Assumption Override</span>
      <button onclick="document.getElementById('delta-panel').remove();_overrideBaseline=null;" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:13px;">&#215;</button>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px;">${overrideChips}</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">
      <div style="background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:7px;padding:10px 12px;">
        <div style="font-family:var(--mono);font-size:9px;color:#8b949e;margin-bottom:4px;">ORIGINAL VERDICT</div>
        <div style="font-size:14px;font-weight:700;color:${bl.verdict.includes('NO-GO')?'#f85149':bl.verdict==='GO'?'#3fb950':'#d29922'}">${bl.verdict||'—'}</div>
        <div style="font-family:var(--mono);font-size:11px;color:#8b949e;margin-top:2px;">${bl.conf}% confidence</div>
      </div>
      <div style="background:rgba(255,255,255,.03);border:1px solid rgba(255,255,255,.06);border-radius:7px;padding:10px 12px;">
        <div style="font-family:var(--mono);font-size:9px;color:#8b949e;margin-bottom:4px;">OVERRIDE VERDICT</div>
        <div style="font-size:14px;font-weight:700;color:${newVerdict.includes('NO-GO')?'#f85149':newVerdict==='GO'?'#3fb950':'#d29922'}">${newVerdict||'—'}</div>
        <div style="font-family:var(--mono);font-size:11px;color:${confColor};margin-top:2px;">${newConf}% confidence <span style="font-weight:700">(${confSign}${confDelta.toFixed(0)})</span></div>
      </div>
    </div>
    ${verdictChanged?'<div style="margin-top:8px;font-size:12px;color:#f85149;font-weight:500;">&#9888; Verdict changed with your overrides — review advisor analyses carefully.</div>':'<div style="margin-top:8px;font-size:12px;color:var(--text-muted);">&#10003; Verdict held with your overrides.</div>'}
  `;
  // Insert before verdict banner
  const vb=document.getElementById('verdict-banner');
  if(vb)vb.parentNode.insertBefore(panel,vb);
  // Clear baseline after one render
  _overrideBaseline=null;
}

// ── Misc ──────────────────────────────────────────────────────────────────
function togAdv(rid, btn){
  const r=document.getElementById(rid);
  if(!r)return;
  const isCollapsed=r.classList.contains('adv-body-collapsed');
  if(isCollapsed){
    r.classList.remove('adv-body-collapsed');
    if(btn)btn.innerHTML='&#9650; Show less';
  }else{
    r.classList.add('adv-body-collapsed');
    if(btn)btn.innerHTML='&#9660; Full analysis';
  }
}
// ── #264: Toast notification system ─────────────────────────────────────────
function showToast(msg, type){
  type = type || 'info';
  const container = document.getElementById('toast-container');
  if(!container) return;
  const icons = {success:'&#10003;', error:'&#9888;', info:'&#128202;'};
  const el = document.createElement('div');
  el.className = 'toast-msg toast-' + type;
  el.innerHTML = '<span class="toast-icon">' + (icons[type]||icons.info) + '</span><span>' + msg + '</span>';
  container.appendChild(el);
  const dismiss = function(){
    el.classList.add('toast-out');
    setTimeout(function(){if(el.parentNode)el.parentNode.removeChild(el);}, 260);
  };
  setTimeout(dismiss, 3500);
  el.addEventListener('click', dismiss);
}

function copyMemo(){
  navigator.clipboard.writeText(document.getElementById('memo-content').textContent).then(()=>{
    showToast('Memo copied to clipboard', 'success');
    const b=document.getElementById('copyBtn');b.textContent='Copied!';setTimeout(()=>b.textContent='&#128203; Copy Memo',2000);
  });
}
// ── Report share bar copy (#240) ──────────────────────────────────────────
function rsbCopyLink(){
  const url=window.location.href;
  navigator.clipboard.writeText(url).then(()=>{
    const b=document.getElementById('rsb-copy-btn');
    if(b){b.textContent='&#10003; Copied!';setTimeout(()=>{b.innerHTML='&#128279; Copy Link';},2200);}
  });
}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

// ── Comps Tab (#135) ──────────────────────────────────────────────────────
async function loadComps(){
  const jid=window._currentJobId;
  if(!jid){
    document.getElementById('comps-content').innerHTML='<div style="color:#f85149;font-size:12px;padding:16px;text-align:center;">No active analysis. Run a deal first.</div>';
    return;
  }
  document.getElementById('comps-content').innerHTML='<div style="color:#8b949e;font-size:12px;padding:16px;text-align:center;"><span style="animation:spin 1s linear infinite;display:inline-block;">&#8635;</span> Fetching 1-mile rent comps...</div>';
  try{
    const r=await fetch('/api/comps/'+jid);
    const d=await r.json();
    if(d.error){document.getElementById('comps-content').innerHTML='<div style="color:#f85149;font-size:12px;padding:16px;">'+esc(d.error)+'</div>';return;}
    renderComps(d);
  }catch(e){
    document.getElementById('comps-content').innerHTML='<div style="color:#f85149;font-size:12px;padding:16px;">Error: '+esc(e.message)+'</div>';
  }
}

function renderComps(d){
  const comps=d.comps||{};
  const list=comps.comps||[];
  const bench=d.market_benchmarks||{};
  const isMock=(comps._source||'').includes('mock');
  const avgRent=comps.avg_comp_rent;
  const rentLow=comps.rent_range_low;
  const rentHigh=comps.rent_range_high;
  const benchRent=bench.avg_rent||bench.median_rent;

  let html=`<div style="margin-bottom:12px;">`;
  if(isMock) html+=`<div style="background:rgba(210,153,34,.08);border:1px solid #d29922;border-radius:5px;padding:6px 10px;font-size:11px;color:#d29922;margin-bottom:8px;">&#9888; Demo data — add RentCast API key to get live comps</div>`;
  html+=`<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;">`;
  if(avgRent) html+=`<div style="background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 14px;text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#58a6ff;">$${avgRent.toLocaleString()}</div><div style="font-size:10px;color:#8b949e;">Avg Comp Rent/mo</div></div>`;
  if(rentLow&&rentHigh) html+=`<div style="background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 14px;text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#e6edf3;">$${rentLow.toLocaleString()}–$${rentHigh.toLocaleString()}</div><div style="font-size:10px;color:#8b949e;">Rent Range</div></div>`;
  if(benchRent) html+=`<div style="background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 14px;text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#e6edf3;">$${Math.round(benchRent).toLocaleString()}</div><div style="font-size:10px;color:#8b949e;">Market Median Rent</div></div>`;
  if(bench.avg_cap_rate) html+=`<div style="background:#161b22;border:1px solid #21262d;border-radius:6px;padding:10px 14px;text-align:center;"><div style="font-size:1.1rem;font-weight:700;color:#3fb950;">${bench.avg_cap_rate.toFixed(1)}%</div><div style="font-size:10px;color:#8b949e;">Market Cap Rate</div></div>`;
  html+=`</div>`;

  if(list.length){
    html+=`<div style="font-size:11px;font-weight:600;color:#8b949e;margin-bottom:6px;">&#128205; ${comps.comp_count||list.length} Comps within ${comps.radius_miles||1} mile</div>`;
    html+=`<div style="overflow-x:auto;"><table style="width:100%;border-collapse:collapse;font-size:11px;">
      <thead><tr style="background:#0d1117;">
        <th style="padding:5px 8px;text-align:left;color:#8b949e;border-bottom:1px solid #21262d;">Address</th>
        <th style="padding:5px 8px;text-align:right;color:#8b949e;border-bottom:1px solid #21262d;">Rent/mo</th>
        <th style="padding:5px 8px;text-align:right;color:#8b949e;border-bottom:1px solid #21262d;">Beds/Ba</th>
        <th style="padding:5px 8px;text-align:right;color:#8b949e;border-bottom:1px solid #21262d;">Sq Ft</th>
        <th style="padding:5px 8px;text-align:right;color:#8b949e;border-bottom:1px solid #21262d;">Listed</th>
      </tr></thead><tbody>`;
    list.forEach((c,i)=>{
      const stripe=i%2===0?'background:#0d1117;':'background:#161b22;';
      const rent=c.price||c.rentPrice||'—';
      const beds=c.bedrooms||'—';
      const ba=c.bathrooms||'—';
      const sqft=c.squareFootage?c.squareFootage.toLocaleString():'—';
      const listed=c.listedDate?c.listedDate.substring(0,10):'—';
      html+=`<tr style="${stripe}">
        <td style="padding:5px 8px;color:#e6edf3;border-bottom:1px solid #161b22;">${esc(c.address||'—')}</td>
        <td style="padding:5px 8px;text-align:right;color:#58a6ff;font-weight:600;border-bottom:1px solid #161b22;">${typeof rent==='number'?'$'+rent.toLocaleString():rent}</td>
        <td style="padding:5px 8px;text-align:right;color:#8b949e;border-bottom:1px solid #161b22;">${beds}/${ba}</td>
        <td style="padding:5px 8px;text-align:right;color:#8b949e;border-bottom:1px solid #161b22;">${sqft}</td>
        <td style="padding:5px 8px;text-align:right;color:#8b949e;border-bottom:1px solid #161b22;">${listed}</td>
      </tr>`;
    });
    html+=`</tbody></table></div>`;
  } else {
    html+=`<div style="color:#8b949e;font-size:12px;text-align:center;padding:16px;">No comps returned — try running a full analysis first.</div>`;
  }
  html+=`</div>`;
  document.getElementById('comps-content').innerHTML=html;
}

// ── Scenario Planner (#172) ───────────────────────────────────────────────
// IRR approximation: simplified levered IRR using entry/exit NOI, LTV, hold period
function _calcIRR(noi, price, exitCap, ltv, hold, rentGrowthPct, vacancyPct){
  if(!noi||!price||!exitCap||!hold) return null;
  const equity=price*(1-(ltv/100));
  const debtService=price*(ltv/100)*0.065; // ~6.5% interest-only
  let cfs=[];
  let curNoi=noi*(1-(vacancyPct/100)/0.08); // vacancy adjustment vs assumed 8% base
  for(let y=1;y<=hold;y++){
    cfs.push((curNoi - debtService));
    curNoi*=(1+rentGrowthPct/100);
  }
  const exitNoi=noi*Math.pow(1+rentGrowthPct/100,hold)*(1-(vacancyPct/100)/0.08);
  const exitVal=exitNoi/(exitCap/100);
  const exitProceeds=exitVal - price*(ltv/100);
  cfs[hold-1]+=exitProceeds;
  // Newton-Raphson IRR
  let r=0.15;
  for(let i=0;i<50;i++){
    let npv=-equity, dnpv=0;
    cfs.forEach((cf,t)=>{npv+=cf/Math.pow(1+r,t+1); dnpv-=(t+1)*cf/Math.pow(1+r,t+2);});
    const dr=npv/dnpv;
    r-=dr;
    if(Math.abs(dr)<0.0001) break;
  }
  return isFinite(r)&&r>-1?+(r*100).toFixed(1):null;
}

function _calcNOI(price, capRate){ return price*(capRate/100); }
function _calcCoC(noi, price, ltv){ const equity=price*(1-(ltv/100)); const ds=price*(ltv/100)*0.065; return +((noi-ds)/equity*100).toFixed(1); }

let _spState={vacBase:8,rentBase:3,exitCapBase:5.5};

// #224: Named Scenario Planner — replaces slider/card UI
let _spDealRef={};
let _nspRows=[];
const _NSP_STORAGE_KEY=()=>'ce_nsp_'+(window._currentJobId||'unsaved');

function _nspSeed(vacBase,rentBase,exitCapBase){
  return [
    {name:'Bear',vac:+(vacBase+4).toFixed(1),rent:+(rentBase-2.5).toFixed(1),ec:+(exitCapBase+1).toFixed(2),ltv:null},
    {name:'Base',vac:+vacBase.toFixed(1),rent:+rentBase.toFixed(1),ec:+exitCapBase.toFixed(2),ltv:null},
    {name:'Bull',vac:Math.max(+(vacBase-3).toFixed(1),1),rent:+(rentBase+2.5).toFixed(1),ec:Math.max(+(exitCapBase-0.75).toFixed(2),3),ltv:null},
  ];
}

function _nspLoad(){
  try{return JSON.parse(localStorage.getItem(_NSP_STORAGE_KEY())||'null');}catch{return null;}
}
function _nspSave(){
  localStorage.setItem(_NSP_STORAGE_KEY(),JSON.stringify(_nspRows));
}

function renderScenarioPlanner(){
  const el=document.getElementById('scenarios-content');
  if(!el) return;
  const deal=_currentDeal||{};
  const price=deal.asking_price||0;
  const cap=parseFloat((deal.cap_rate||'5').toString().replace(/[^0-9.]/g,''));
  const ltv=parseFloat((deal.ltv||'65').toString().replace(/[^0-9.]/g,''));
  const hold=deal.hold_period||5;
  if(!price||!cap){
    el.innerHTML='<div style="color:var(--text-muted);font-size:12px;padding:24px 0;text-align:center;">No deal data — run an analysis first.</div>';
    return;
  }
  const noi=deal.noi||_calcNOI(price,cap);
  const exitCapBase=_spState.exitCapBase||parseFloat((deal.exit_cap_rate||cap+0.5).toString())||cap+0.5;
  _spDealRef={price,cap,noi,ltv,hold};
  // Load or seed rows
  _nspRows=_nspLoad();
  if(!_nspRows||!_nspRows.length){
    _nspRows=_nspSeed(_spState.vacBase,_spState.rentBase,exitCapBase);
    _nspSave();
  }
  el.innerHTML=`
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:12px;">Named scenario planner — edit any cell to recalculate IRR instantly. Delta shown vs Base case.</div>
  <div style="overflow-x:auto;">
    <table class="nsp-table" id="nsp-table">
      <thead><tr>
        <th>Scenario</th>
        <th>Vacancy %</th>
        <th>Rent Growth %/yr</th>
        <th>Exit Cap %</th>
        <th>LTV %</th>
        <th style="color:var(--accent);">Proj. IRR</th>
        <th>vs Base</th>
        <th></th>
      </tr></thead>
      <tbody id="nsp-tbody"></tbody>
    </table>
  </div>
  <button onclick="_nspAddRow()" style="font-size:11px;padding:5px 12px;background:rgba(232,160,32,.08);border:1px solid rgba(232,160,32,.25);color:var(--amber);border-radius:5px;cursor:pointer;margin-bottom:10px;">+ New Scenario</button>
  <div style="font-size:10px;color:var(--text-muted);padding:6px 0;border-top:1px solid var(--border-muted);">
    Base deal: ${deal.deal_name||'—'} &middot; $${Number(price).toLocaleString()} &middot; ${cap}% cap &middot; ${ltv}% LTV &middot; ${hold}yr hold
  </div>`;
  _nspRenderRows();
}

function _nspRenderRows(){
  const tbody=document.getElementById('nsp-tbody');
  if(!tbody) return;
  const {price,noi,ltv,hold}=_spDealRef;
  // Compute base IRR for delta
  const baseRow=_nspRows.find(r=>r.name==='Base')||_nspRows[0];
  const baseLtv=baseRow&&baseRow.ltv!=null?baseRow.ltv:ltv;
  const baseIrr=_calcIRR(noi,price,baseRow?baseRow.ec:5.5,baseLtv,hold,baseRow?baseRow.rent:3,baseRow?baseRow.vac:8);
  tbody.innerHTML=_nspRows.map(function(row,i){
    const rowLtv=row.ltv!=null?row.ltv:ltv;
    const irr=_calcIRR(noi,price,row.ec,rowLtv,hold,row.rent,row.vac);
    const irrStr=irr!==null?irr.toFixed(1)+'%':'—';
    const irrColor=irr===null?'var(--text-muted)':irr>=12?'var(--green)':irr>=8?'var(--amber)':'var(--red)';
    let deltaHtml='—';
    if(irr!==null&&baseIrr!==null&&row!==baseRow){
      const d=irr-baseIrr;
      const sign=d>=0?'+':'';
      deltaHtml='<span class="nsp-delta" style="color:'+(d>=0?'var(--green)':'var(--red)')+';">'+sign+d.toFixed(1)+'%</span>';
    }else if(row===baseRow||row.name==='Base'){
      deltaHtml='<span class="nsp-delta" style="color:var(--text-muted);">base</span>';
    }
    const isBase=row.name==='Base';
    const delBtn=isBase?'':'<button class="nsp-del" onclick="_nspDelRow('+i+')" title="Remove">&#x2715;</button>';
    return '<tr id="nsp-row-'+i+'">'
      +'<td><input class="nsp-name-input" value="'+row.name+'" onchange="_nspEdit('+i+',\\'name\\',this.value)"></td>'
      +'<td><input class="nsp-input" type="number" step="0.5" min="0" max="50" value="'+row.vac+'" onchange="_nspEdit('+i+',\\'vac\\',parseFloat(this.value))"></td>'
      +'<td><input class="nsp-input" type="number" step="0.5" min="-10" max="20" value="'+row.rent+'" onchange="_nspEdit('+i+',\\'rent\\',parseFloat(this.value))"></td>'
      +'<td><input class="nsp-input" type="number" step="0.25" min="2" max="12" value="'+row.ec+'" onchange="_nspEdit('+i+',\\'ec\\',parseFloat(this.value))"></td>'
      +'<td><input class="nsp-input" type="number" step="1" min="0" max="90" value="'+rowLtv+'" onchange="_nspEdit('+i+',\\'ltv\\',parseFloat(this.value))"></td>'
      +'<td><span class="nsp-irr" style="color:'+irrColor+';">'+irrStr+'</span></td>'
      +'<td>'+deltaHtml+'</td>'
      +'<td>'+delBtn+'</td>'
      +'</tr>';
  }).join('');
}

function _nspEdit(i,field,val){
  if(i<0||i>=_nspRows.length) return;
  _nspRows[i][field]=val;
  _nspSave();
  _nspRenderRows();
}
function _nspAddRow(){
  const baseRow=_nspRows.find(r=>r.name==='Base')||_nspRows[0]||{vac:8,rent:3,ec:5.5,ltv:null};
  _nspRows.push({name:'Custom '+(_nspRows.length+1),vac:baseRow.vac,rent:baseRow.rent,ec:baseRow.ec,ltv:baseRow.ltv});
  _nspSave();
  _nspRenderRows();
}
function _nspDelRow(i){
  if(i<0||i>=_nspRows.length) return;
  _nspRows.splice(i,1);
  _nspSave();
  _nspRenderRows();
}

// ── White-label Branding (#226) ──────────────────────────────────────────
// ── Tools Dropdown (#238) ─────────────────────────────────────────────────
function toolsToggle(e){
  e.stopPropagation();
  const menu=document.getElementById('tools-menu');
  const btn=document.getElementById('tools-trigger-btn');
  const open=menu.classList.toggle('open');
  if(btn)btn.classList.toggle('active',open);
}
function toolsClose(){
  const menu=document.getElementById('tools-menu');
  const btn=document.getElementById('tools-trigger-btn');
  if(menu)menu.classList.remove('open');
  if(btn)btn.classList.remove('active');
}
// Close on outside click
document.addEventListener('click',function(e){
  const dd=document.getElementById('tools-dropdown');
  if(dd&&!dd.contains(e.target))toolsClose();
});
// Update active dot based on configured tools
function toolsUpdateDot(){
  const hasTp=Object.keys(tpLoad()||{}).some(k=>['strategies','asset_classes','geographies','target_irr','hold_years','units_min','units_max'].includes(k)&&tpLoad()[k]!=null&&(!Array.isArray(tpLoad()[k])||tpLoad()[k].length>0));
  const hasIps=Object.keys((()=>{try{return JSON.parse(localStorage.getItem('ce_ips')||'{}');}catch{return {};}})()).some(k=>k!=='asset_classes'?true:false);
  const dot=document.getElementById('tools-dot');
  if(dot)dot.classList.toggle('visible',hasTp||hasIps);
}
setTimeout(toolsUpdateDot,200);

const WL_KEY='ce_branding';
function wlLoad(){try{return JSON.parse(localStorage.getItem(WL_KEY)||'{}');}catch{return {};}}
// ── Thesis Profile (#233) ─────────────────────────────────────────────────
const TP_KEY='ce_thesis';
function tpLoad(){try{return JSON.parse(localStorage.getItem(TP_KEY)||'{}')}catch{return {};}}
function tpToggleStrat(btn){
  const active=btn.classList.toggle('tp-active');
  btn.style.background=active?'rgba(232,160,32,.12)':'none';
  btn.style.borderColor=active?'rgba(232,160,32,.4)':'var(--border-default)';
  btn.style.color=active?'var(--amber)':'var(--text-secondary)';
}
function tpOpen(){
  const cfg=tpLoad();
  document.querySelectorAll('.tp-strat-btn').forEach(b=>{
    const active=(cfg.strategies||[]).includes(b.dataset.val);
    b.classList.toggle('tp-active',active);
    b.style.background=active?'rgba(232,160,32,.12)':'none';
    b.style.borderColor=active?'rgba(232,160,32,.4)':'var(--border-default)';
    b.style.color=active?'var(--amber)':'var(--text-secondary)';
  });
  document.querySelectorAll('.tp-ac-cb').forEach(c=>{c.checked=(cfg.asset_classes||[]).includes(c.value);});
  const sv=id=>document.getElementById(id);
  if(sv('tp-geos'))sv('tp-geos').value=(cfg.geographies||[]).join(', ');
  if(sv('tp-irr'))sv('tp-irr').value=cfg.target_irr!=null?cfg.target_irr:'';
  if(sv('tp-hold'))sv('tp-hold').value=cfg.hold_years!=null?cfg.hold_years:'';
  if(sv('tp-units-min'))sv('tp-units-min').value=cfg.units_min!=null?cfg.units_min:'';
  if(sv('tp-units-max'))sv('tp-units-max').value=cfg.units_max!=null?cfg.units_max:'';
  document.getElementById('tp-overlay').classList.add('open');
  document.getElementById('tp-drawer').classList.add('open');
}
function tpClose(){
  document.getElementById('tp-overlay').classList.remove('open');
  document.getElementById('tp-drawer').classList.remove('open');
}
function tpSave(){
  const cfg={};
  cfg.strategies=[...document.querySelectorAll('.tp-strat-btn.tp-active')].map(b=>b.dataset.val);
  cfg.asset_classes=[...document.querySelectorAll('.tp-ac-cb:checked')].map(c=>c.value);
  const geoVal=(document.getElementById('tp-geos')?.value||'').trim();
  cfg.geographies=geoVal?geoVal.split(',').map(s=>s.trim().toLowerCase()).filter(Boolean):[];
  const fv=id=>document.getElementById(id)?.value?parseFloat(document.getElementById(id).value):null;
  cfg.target_irr=fv('tp-irr');
  cfg.hold_years=fv('tp-hold');
  cfg.units_min=fv('tp-units-min');
  cfg.units_max=fv('tp-units-max');
  localStorage.setItem(TP_KEY,JSON.stringify(cfg));
  const msg=document.getElementById('tp-save-msg');
  if(msg){msg.textContent='Thesis saved.';setTimeout(()=>msg.textContent='',2200);}
}
function tpClear(){
  localStorage.removeItem(TP_KEY);
  document.querySelectorAll('.tp-strat-btn').forEach(b=>{b.classList.remove('tp-active');b.style.background='none';b.style.borderColor='var(--border-default)';b.style.color='var(--text-secondary)';});
  document.querySelectorAll('.tp-ac-cb').forEach(c=>c.checked=false);
  ['tp-geos','tp-irr','tp-hold','tp-units-min','tp-units-max'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  const msg=document.getElementById('tp-save-msg');
  if(msg){msg.textContent='Thesis cleared.';setTimeout(()=>msg.textContent='',2200);}
}
function tpScore(deal){
  const cfg=tpLoad();
  const hasAny=cfg.strategies?.length||cfg.asset_classes?.length||cfg.geographies?.length||cfg.target_irr!=null||cfg.hold_years!=null||cfg.units_min!=null||cfg.units_max!=null;
  if(!hasAny||!deal)return null;
  let pts=0,total=0;
  const pf=v=>v!=null?parseFloat((v+'').replace(/[^0-9.-]/g,''))||null:null;
  const aclass=(deal.property_type||'').toLowerCase();
  const market=(deal.market||'').toLowerCase();
  const irr=pf(deal.projected_irr);
  const hold=pf(deal.hold_period);
  const units=pf(deal.units);
  if(cfg.asset_classes?.length){total+=25;if(cfg.asset_classes.some(ac=>aclass.includes(ac.toLowerCase())))pts+=25;}
  if(cfg.geographies?.length){total+=25;if(cfg.geographies.some(g=>market.includes(g)||g.includes(market.split(',')[0].trim())))pts+=25;}
  if(cfg.target_irr!=null&&irr!=null){total+=20;if(irr>=cfg.target_irr)pts+=20;else if(irr>=(cfg.target_irr*0.85))pts+=10;}
  if(cfg.hold_years!=null&&hold!=null){total+=15;const diff=Math.abs(hold-cfg.hold_years);if(diff<=1)pts+=15;else if(diff<=2)pts+=8;}
  if((cfg.units_min!=null||cfg.units_max!=null)&&units!=null){
    total+=15;
    const okMin=cfg.units_min==null||units>=cfg.units_min;
    const okMax=cfg.units_max==null||units<=cfg.units_max;
    if(okMin&&okMax)pts+=15;
  }
  if(total===0)return null;
  return Math.round(pts/total*100);
}

// ── IPS Configurator (#229) ───────────────────────────────────────────────
const IPS_KEY='ce_ips';
function ipsLoad(){try{return JSON.parse(localStorage.getItem(IPS_KEY)||'{}')}catch{return {};}}
function ipsSave(){
  const cfg={};
  const fv=v=>v?parseFloat(v):null;
  cfg.min_irr=fv(document.getElementById('ips-min-irr')?.value);
  cfg.max_ltv=fv(document.getElementById('ips-max-ltv')?.value);
  cfg.min_size=fv(document.getElementById('ips-min-size')?.value);
  cfg.max_size=fv(document.getElementById('ips-max-size')?.value);
  cfg.max_hold=fv(document.getElementById('ips-max-hold')?.value);
  const mktVal=(document.getElementById('ips-markets')?.value||'').trim();
  cfg.markets=mktVal?mktVal.split(',').map(s=>s.trim().toLowerCase()).filter(Boolean):[];
  cfg.asset_classes=[...document.querySelectorAll('.ips-ac-cb:checked')].map(c=>c.value);
  localStorage.setItem(IPS_KEY,JSON.stringify(cfg));
  const msg=document.getElementById('ips-save-msg');
  if(msg){msg.textContent='Criteria saved.';setTimeout(()=>msg.textContent='',2200);}
}
function ipsClear(){
  localStorage.removeItem(IPS_KEY);
  ['ips-min-irr','ips-max-ltv','ips-min-size','ips-max-size','ips-max-hold','ips-markets'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  document.querySelectorAll('.ips-ac-cb').forEach(c=>c.checked=false);
  const msg=document.getElementById('ips-save-msg');
  if(msg){msg.textContent='Criteria cleared.';setTimeout(()=>msg.textContent='',2200);}
}
function ipsOpen(){
  const cfg=ipsLoad();
  const sv=id=>document.getElementById(id);
  if(sv('ips-min-irr'))sv('ips-min-irr').value=cfg.min_irr!=null?cfg.min_irr:'';
  if(sv('ips-max-ltv'))sv('ips-max-ltv').value=cfg.max_ltv!=null?cfg.max_ltv:'';
  if(sv('ips-min-size'))sv('ips-min-size').value=cfg.min_size!=null?cfg.min_size:'';
  if(sv('ips-max-size'))sv('ips-max-size').value=cfg.max_size!=null?cfg.max_size:'';
  if(sv('ips-max-hold'))sv('ips-max-hold').value=cfg.max_hold!=null?cfg.max_hold:'';
  if(sv('ips-markets'))sv('ips-markets').value=(cfg.markets||[]).join(', ');
  document.querySelectorAll('.ips-ac-cb').forEach(c=>{c.checked=(cfg.asset_classes||[]).includes(c.value);});
  document.getElementById('ips-overlay').classList.add('open');
  document.getElementById('ips-drawer').classList.add('open');
}
function ipsClose(){
  document.getElementById('ips-overlay').classList.remove('open');
  document.getElementById('ips-drawer').classList.remove('open');
}
function ipsCheck(deal){
  const cfg=ipsLoad();
  const rows=[];
  const pf=v=>v!=null?parseFloat((v+'').replace(/[^0-9.-]/g,''))||null:null;
  const irr=pf(deal.projected_irr);
  const ltv=pf(deal.ltv);
  const price=pf(deal.asking_price);
  const hold=pf(deal.hold_period);
  const market=(deal.market||'').toLowerCase();
  const aclass=deal.property_type||'';
  if(cfg.min_irr!=null&&irr!=null){
    const pass=irr>=cfg.min_irr;
    rows.push({status:pass?'PASS':'FAIL',text:pass?'IRR '+irr+'% meets your '+cfg.min_irr+'% minimum':'IRR '+irr+'% BELOW your '+cfg.min_irr+'% policy floor'});
  }
  if(cfg.max_ltv!=null&&ltv!=null){
    const pass=ltv<=cfg.max_ltv;
    rows.push({status:pass?'PASS':'FAIL',text:pass?'LTV '+ltv+'% within your '+cfg.max_ltv+'% ceiling':'LTV '+ltv+'% EXCEEDS your '+cfg.max_ltv+'% policy ceiling'});
  }
  if(cfg.min_size!=null&&price!=null){
    const pass=price>=cfg.min_size;
    rows.push({status:pass?'PASS':'FAIL',text:pass?'Deal size $'+price.toLocaleString()+' meets minimum':'Deal size $'+price.toLocaleString()+' BELOW your $'+cfg.min_size.toLocaleString()+' minimum'});
  }
  if(cfg.max_size!=null&&price!=null){
    const pass=price<=cfg.max_size;
    rows.push({status:pass?'PASS':'FAIL',text:pass?'Deal size $'+price.toLocaleString()+' within maximum':'Deal size $'+price.toLocaleString()+' EXCEEDS your $'+cfg.max_size.toLocaleString()+' maximum'});
  }
  if(cfg.max_hold!=null&&hold!=null){
    const pass=hold<=cfg.max_hold;
    rows.push({status:pass?'PASS':'FAIL',text:pass?hold+'-year hold within your '+cfg.max_hold+'-year limit':hold+'-year hold EXCEEDS your '+cfg.max_hold+'-year policy limit'});
  }
  if(cfg.markets&&cfg.markets.length>0&&market){
    const match=cfg.markets.some(m=>market.includes(m)||m.includes(market.split(',')[0].trim()));
    rows.push({status:match?'PASS':'EXCEPTION',text:match?'Market "'+deal.market+'" is in your approved MSA list':'Market "'+deal.market+'" is NOT in your approved MSA list — review required'});
  }
  if(cfg.asset_classes&&cfg.asset_classes.length>0&&aclass){
    const match=cfg.asset_classes.some(ac=>aclass.toLowerCase().includes(ac.toLowerCase()));
    rows.push({status:match?'PASS':'EXCEPTION',text:match?aclass+' is in your approved asset classes':aclass+' is NOT in your approved asset class list — exception required'});
  }
  return rows;
}
function ipsRender(deal){
  const el=document.getElementById('ips-check-panel');
  if(!el)return;
  const cfg=ipsLoad();
  const hasAny=cfg.min_irr!=null||cfg.max_ltv!=null||cfg.min_size!=null||cfg.max_size!=null||cfg.max_hold!=null||(cfg.markets&&cfg.markets.length)||(cfg.asset_classes&&cfg.asset_classes.length);
  if(!hasAny){el.innerHTML='';return;}
  const rows=ipsCheck(deal);
  if(!rows.length){el.innerHTML='';return;}
  const fails=rows.filter(r=>r.status==='FAIL').length;
  const excs=rows.filter(r=>r.status==='EXCEPTION').length;
  const badCount=fails+excs;
  const bannerCls=badCount>0?'ips-check-banner ips-banner-warn':'ips-check-banner ips-banner-ok';
  const bannerIcon=badCount>0?'&#x26A0;':'&#x2713;';
  const bannerTxt=badCount>0?'Deal falls outside your stated investment criteria on '+badCount+' point'+(badCount>1?'s':''):'Deal satisfies all Investment Policy Statement criteria';
  const rowsHtml=rows.map(r=>{
    const bc=r.status==='PASS'?'ips-pass':r.status==='FAIL'?'ips-fail':'ips-exc';
    return '<div class="ips-row"><span class="ips-badge '+bc+'">'+r.status+'</span><span class="ips-row-text">'+esc(r.text)+'</span></div>';
  }).join('');
  el.innerHTML='<div class="ips-check-section">'
    +'<div style="font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);margin-bottom:6px;display:flex;align-items:center;justify-content:space-between;">'
    +'<span>&#128196; IPS Compliance Check</span>'
    +'<button onclick="ipsOpen()" style="background:none;border:none;font-size:10px;color:var(--accent);cursor:pointer;padding:0;">Edit Criteria &#x276F;</button>'
    +'</div>'
    +'<div class="'+bannerCls+'"><span style="font-size:14px;flex-shrink:0;">'+bannerIcon+'</span><span>'+esc(bannerTxt)+'</span></div>'
    +'<div>'+rowsHtml+'</div>'
    +'</div>';
}

function wlOpen(){
  const b=wlLoad();
  const fn=document.getElementById('wl-firm-name');const lu=document.getElementById('wl-logo-url');
  const ac=document.getElementById('wl-accent');const hb=document.getElementById('wl-hide-badge');
  const gn=document.getElementById('wl-gp-note');
  if(fn)fn.value=b.firm_name||'';
  if(lu)lu.value=b.logo_url||'';
  if(ac)ac.value=b.accent_color||'';
  if(hb)hb.checked=!!b.hide_badge;
  if(gn){gn.value=b.gp_note||'';const cnt=document.getElementById('wl-note-count');if(cnt)cnt.textContent=(b.gp_note||'').length+' / 300';}
  wlPreview();
  document.getElementById('wl-overlay').classList.add('open');
  document.getElementById('wl-drawer').classList.add('open');
  document.getElementById('wl-save-msg').textContent='';
}
function wlClose(){
  document.getElementById('wl-overlay').classList.remove('open');
  document.getElementById('wl-drawer').classList.remove('open');
}
function wlPreview(){
  const fn=(document.getElementById('wl-firm-name')?.value||'').trim();
  const hb=document.getElementById('wl-hide-badge')?.checked;
  const ac=(document.getElementById('wl-accent')?.value||'').trim();
  const gn=(document.getElementById('wl-gp-note')?.value||'').trim();
  const nameEl=document.getElementById('wl-preview-name');
  const subEl=document.getElementById('wl-preview-sub');
  const cnt=document.getElementById('wl-note-count');
  if(nameEl)nameEl.textContent=fn||'Your Firm Name';
  if(nameEl&&ac)nameEl.style.color=ac;
  if(subEl)subEl.textContent='Deal Analysis'+(hb?'':' · Powered by ClearEye')+(gn?' · GP Note set':'');
  if(cnt)cnt.textContent=gn.length+' / 300';
}
function wlSave(){
  const fn=(document.getElementById('wl-firm-name')?.value||'').trim();
  const lu=(document.getElementById('wl-logo-url')?.value||'').trim();
  const ac=(document.getElementById('wl-accent')?.value||'').trim();
  const hb=document.getElementById('wl-hide-badge')?.checked||false;
  const gn=(document.getElementById('wl-gp-note')?.value||'').trim().slice(0,300);
  localStorage.setItem(WL_KEY,JSON.stringify({firm_name:fn,logo_url:lu,accent_color:ac,hide_badge:hb,gp_note:gn}));
  const msg=document.getElementById('wl-save-msg');
  if(msg){msg.textContent='Branding saved — applied to your next share link.';setTimeout(()=>{msg.textContent='';},3000);}
  // Update Tools trigger label with firm name
  const trig=document.getElementById('tools-trigger-btn');
  if(trig){const lbl=trig.querySelector('span:first-child')||trig;if(fn)trig.childNodes[0].textContent='&#9881; '+fn.split(' ')[0];}
}
function wlClear(){
  localStorage.removeItem(WL_KEY);
  ['wl-firm-name','wl-logo-url','wl-accent','wl-gp-note'].forEach(id=>{const el=document.getElementById(id);if(el)el.value='';});
  const hb=document.getElementById('wl-hide-badge');if(hb)hb.checked=false;
  wlPreview();
}
function wlGetParams(){
  const b=wlLoad();
  const params={};
  if(b.firm_name)params.fn=b.firm_name;
  if(b.logo_url)params.lo=b.logo_url;
  if(b.accent_color)params.ac=b.accent_color;
  if(b.hide_badge)params.hb='1';
  if(b.gp_note)params.gn=encodeURIComponent(b.gp_note);
  return params;
}

// ── Decision Journal (#221) ──────────────────────────────────────────────
let _djCurrentTab='summary';
const DJ_TAB_LABELS={summary:'Summary',memo:'Memo',advisors:'Advisors',stress:'Sensitivity',audit:'Audit',bias:'Bias Check',premortem:'Pre-Mortem',macro:'Macro',comps:'Comps',scenarios:'Scenarios'};

function djNoteOpen(){
  const label=document.getElementById('dj-note-label');
  if(label)label.textContent='Note for: '+(DJ_TAB_LABELS[_djCurrentTab]||_djCurrentTab);
  const wrap=document.getElementById('dj-note-input-wrap');
  if(wrap){wrap.classList.add('open');const ta=document.getElementById('dj-note-textarea');if(ta)ta.focus();}
}
function djNoteClose(){
  const wrap=document.getElementById('dj-note-input-wrap');
  if(wrap){wrap.classList.remove('open');const ta=document.getElementById('dj-note-textarea');if(ta)ta.value='';}
}
function djNoteSave(){
  const jid=window._currentJobId||'unsaved';
  const ta=document.getElementById('dj-note-textarea');
  const text=(ta?ta.value:'').trim();
  if(!text)return;
  const entries=djLoad(jid);
  entries.push({tab:_djCurrentTab,label:DJ_TAB_LABELS[_djCurrentTab]||_djCurrentTab,text,ts:new Date().toISOString()});
  localStorage.setItem('ce_dj_'+jid,JSON.stringify(entries));
  djNoteClose();
  djRenderLog(jid);
  const panel=document.getElementById('dj-log-panel');
  if(panel)panel.classList.add('open');
  const logBtn=document.getElementById('djLogBtn');
  if(logBtn)logBtn.style.display='';
}
function djLoad(jid){
  try{return JSON.parse(localStorage.getItem('ce_dj_'+(jid||window._currentJobId||'unsaved'))||'[]');}catch{return [];}
}
function djRenderLog(jid){
  const entries=djLoad(jid||window._currentJobId||'unsaved');
  const body=document.getElementById('dj-log-body');
  const count=document.getElementById('dj-log-count');
  if(count)count.textContent=entries.length+(entries.length===1?' note':' notes');
  const logBtn=document.getElementById('djLogBtn');
  if(logBtn)logBtn.style.display=entries.length?'':'none';
  if(!body)return;
  if(!entries.length){body.innerHTML='<div style="color:var(--text-muted);font-size:12px;">No notes yet. Use "Add Note" after any tab.</div>';return;}
  body.innerHTML=entries.slice().reverse().map(function(e){
    const d=new Date(e.ts);
    const fmt=d.toLocaleDateString('en-US',{month:'short',day:'numeric'})+' '+d.toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'});
    return '<div class="dj-log-entry"><div class="dj-log-tab">'+esc(e.label)+'</div><div class="dj-log-text">'+esc(e.text)+'</div><div class="dj-log-ts">'+fmt+'</div></div>';
  }).join('');
}
function djToggleLog(){
  const body=document.getElementById('dj-log-body');
  if(body)body.style.display=body.style.display==='none'?'':'none';
}
function djToggleLogPanel(){
  const panel=document.getElementById('dj-log-panel');
  if(panel)panel.classList.toggle('open');
}

// ── Add to Pipeline (#133) ────────────────────────────────────────────────
async function addToPipeline(){
  const jid=window._currentJobId;
  if(!jid){showToast('Run an analysis first','error');return;}
  try{
    const r=await fetch('/api/pipeline/from-analysis/'+jid,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({stage:'Screening'})});
    const d=await r.json();
    if(d.ok){
      showToast('Deal added to Pipeline → Screening','success');
      const btn=document.getElementById('pipeBtn');
      btn.textContent='&#10003; Added!';btn.disabled=true;
      setTimeout(()=>{btn.textContent='&#128202; Add to Pipeline';btn.disabled=false;},3000);
    }else{
      showToast(d.error||'Could not add to pipeline','error');
    }
  }catch(e){showToast('Error: '+e.message,'error');}
}

// ── LP Sharing Portal (#136) ──────────────────────────────────────────────
async function openLpModal(){
  const jid=window._currentJobId;
  if(!jid){alert('Run an analysis first to create a shareable LP link.');return;}
  // Tier gate: LP portal requires Professional or above (#214)
  try{
    const uResp=await fetch('/api/usage');
    if(uResp.ok){
      const uData=await uResp.json();
      const tier=uData.tier||'free';
      if(tier==='free'||tier==='operator'){
        let gateModal=document.getElementById('lp-modal');
        if(!gateModal){
          gateModal=document.createElement('div');
          gateModal.id='lp-modal';
          gateModal.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:10000;display:flex;align-items:center;justify-content:center;';
          document.body.appendChild(gateModal);
        }
        gateModal.style.display='flex';
        gateModal.innerHTML='<div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:28px 32px;max-width:460px;width:100%;position:relative;">'
          +'<button onclick="document.getElementById(\\'lp-modal\\').remove()" style="position:absolute;top:12px;right:12px;background:none;border:none;color:#8b949e;font-size:16px;cursor:pointer;">&#x2715;</button>'
          +'<div style="text-align:center;padding:24px;">'
          +'<div style="font-size:2rem;margin-bottom:12px;">&#128274;</div>'
          +'<div style="font-family:var(--font-display);font-style:italic;font-size:1.2rem;color:var(--text-primary);margin-bottom:8px;">LP Portal is a Professional feature</div>'
          +'<div style="font-size:13px;color:var(--text-muted);margin-bottom:16px;">Share password-protected deal reports with LPs and track their engagement. Upgrade to Professional ($697/mo) to unlock.</div>'
          +'<a href="/pricing" style="display:inline-block;background:var(--accent);color:#ffffff;padding:10px 20px;border-radius:6px;font-size:13px;font-weight:700;text-decoration:none;font-family:var(--mono);">Upgrade to Professional &#8594;</a>'
          +'</div></div>';
        return;
      }
    }
  }catch(e){}
  // Show modal
  let modal=document.getElementById('lp-modal');
  if(!modal){
    modal=document.createElement('div');
    modal.id='lp-modal';
    modal.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:10000;display:flex;align-items:center;justify-content:center;';
    modal.innerHTML=`<div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:28px 32px;max-width:460px;width:100%;position:relative;">
      <button onclick="document.getElementById('lp-modal').remove()" style="position:absolute;top:12px;right:12px;background:none;border:none;color:#8b949e;font-size:16px;cursor:pointer;">&#x2715;</button>
      <div style="font-size:1rem;font-weight:700;margin-bottom:16px;">&#128100; LP Data Room</div>
      <!-- Tab switcher -->
      <div style="display:flex;gap:6px;margin-bottom:14px;border-bottom:1px solid #21262d;padding-bottom:10px;">
        <button id="lp-tab-share" onclick="switchLpTab('share',this)" style="font-size:11px;padding:4px 10px;background:#21262d;border:1px solid #58a6ff;color:#58a6ff;border-radius:4px;cursor:pointer;">Share Link</button>
        <button id="lp-tab-package" onclick="switchLpTab('package',this)" style="font-size:11px;padding:4px 10px;background:none;border:1px solid #30363d;color:#8b949e;border-radius:4px;cursor:pointer;">&#128196; LP Package</button>
        <button id="lp-tab-analytics" onclick="switchLpTab('analytics',this);loadLpAnalytics()" style="font-size:11px;padding:4px 10px;background:none;border:1px solid #30363d;color:#8b949e;border-radius:4px;cursor:pointer;">&#128200; Analytics</button>
      </div>
      <!-- Share Link panel -->
      <div id="lp-panel-share">
        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Label (shown on report)</label>
        <input id="lp-label" placeholder="e.g. Sunset Ridge — Investor Preview"
          style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 10px;font-size:12px;margin-bottom:10px;">
        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Password (optional)</label>
        <input id="lp-password" type="password" placeholder="Leave blank for public link"
          style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 10px;font-size:12px;margin-bottom:10px;">
        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Expires in (days, optional)</label>
        <input id="lp-expires" type="number" min="1" max="365" placeholder="e.g. 30"
          style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 10px;font-size:12px;margin-bottom:14px;">
        <button onclick="createLpLink()" style="width:100%;padding:9px;background:#238636;border:none;color:#fff;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">Generate Link &rarr;</button>
        <div id="lp-result" style="margin-top:10px;font-size:11px;"></div>
      </div>
      <!-- LP Package panel (#144) -->
      <div id="lp-panel-package" style="display:none;">
        <div style="font-size:12px;color:#8b949e;margin-bottom:12px;">Generate a branded PDF package for LPs: cover memo + metrics + stress test + memo.</div>
        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Firm Name</label>
        <input id="lp-firm-name" placeholder="Acme Capital Partners"
          style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 10px;font-size:12px;margin-bottom:10px;">
        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Cover Memo (optional)</label>
        <textarea id="lp-cover-memo" rows="4" placeholder="Dear Investor, We are pleased to present..."
          style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 10px;font-size:12px;margin-bottom:14px;resize:vertical;"></textarea>
        <button onclick="downloadLpPackage()" style="width:100%;padding:9px;background:#1f6feb;border:none;color:#fff;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">&#8659; Download LP Package</button>
        <div style="font-size:10px;color:#484f58;margin-top:6px;text-align:center;">Downloads as PDF (or HTML if PDF unavailable)</div>
      </div>
      <!-- Analytics panel (#144) -->
      <div id="lp-panel-analytics" style="display:none;">
        <div id="lp-analytics-content" style="font-size:12px;color:#8b949e;text-align:center;padding:12px;">Loading analytics...</div>
      </div>
    </div>`;
    document.body.appendChild(modal);
  } else {
    modal.style.display='flex';
  }
}
async function createLpLink(){
  const jid=window._currentJobId;
  const label=document.getElementById('lp-label').value.trim();
  const password=document.getElementById('lp-password').value.trim();
  const expires_days=parseInt(document.getElementById('lp-expires').value)||null;
  try{
    const r=await fetch('/api/share/'+jid,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({label,password:password||null,expires_days})});
    const d=await r.json();
    if(d.url){
      // #226: Append branding params to share URL
      const wlP=wlGetParams();
      let shareUrl=d.url;
      const wlKeys=Object.keys(wlP);
      if(wlKeys.length){
        const sep=shareUrl.includes('?')?'&':'?';
        shareUrl+=sep+wlKeys.map(k=>k+'='+encodeURIComponent(wlP[k])).join('&');
      }
      const resultEl=document.getElementById('lp-result');
      const brandNote=wlP.fn?'<div style="color:#e8a020;font-size:10px;margin-top:4px;">&#9881; Branded as: '+wlP.fn+'</div>':'';
      resultEl.innerHTML=`<div style="background:#0d1117;border:1px solid #30363d;border-radius:5px;padding:8px;word-break:break-all;">
        <div style="color:#3fb950;margin-bottom:4px;">&#10003; LP link created!</div>
        <a href="${shareUrl}" target="_blank" style="color:#58a6ff;">${shareUrl}</a>
        <button onclick="navigator.clipboard.writeText('${shareUrl}').then(()=>this.textContent='Copied!')" style="display:block;margin-top:6px;padding:3px 10px;background:#21262d;border:none;color:#8b949e;border-radius:4px;font-size:11px;cursor:pointer;">Copy URL</button>
        ${expires_days?'<div style="color:#8b949e;font-size:10px;margin-top:4px;">Expires in '+expires_days+' days</div>':''}
        ${password?'<div style="color:#d29922;font-size:10px;margin-top:2px;">&#128274; Password protected</div>':''}
        ${brandNote}
      </div>`;
    }
  }catch(e){document.getElementById('lp-result').innerHTML='<div style="color:#f85149;">Error: '+e.message+'</div>';}
}

// LP Data Room tab switching (#144)
function switchLpTab(tab, btn){
  ['share','package','analytics'].forEach(t=>{
    const panel=document.getElementById('lp-panel-'+t);
    const b=document.getElementById('lp-tab-'+t);
    if(panel) panel.style.display=t===tab?'block':'none';
    if(b){b.style.background=t===tab?'#21262d':'none';b.style.borderColor=t===tab?'#58a6ff':'#30363d';b.style.color=t===tab?'#58a6ff':'#8b949e';}
  });
}

// LP Package download (#144)
async function downloadLpPackage(){
  const jid=window._currentJobId;
  if(!jid){alert('No analysis loaded');return;}
  const firm=document.getElementById('lp-firm-name').value.trim()||'Investment Firm';
  const memo=document.getElementById('lp-cover-memo').value.trim();
  try{
    const r=await fetch('/api/share/'+jid+'/package',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({firm_name:firm,cover_memo:memo})});
    const blob=await r.blob();
    const ext=r.headers.get('content-type')==='application/pdf'?'pdf':'html';
    const a=document.createElement('a');
    a.href=URL.createObjectURL(blob);
    a.download='LP_Package.'+ext;
    a.click();
  }catch(e){alert('Package generation failed: '+e.message);}
}

// LP Analytics (#144)
async function loadLpAnalytics(){
  const jid=window._currentJobId;
  if(!jid) return;
  const el=document.getElementById('lp-analytics-content');
  if(!el) return;
  el.innerHTML='<div style="text-align:center;padding:12px;color:#8b949e;">Loading...</div>';
  try{
    const [r1,r2]=await Promise.all([
      fetch('/api/share/'+jid+'/analytics'),
      fetch('/api/share/'+jid+'/analytics/per-lp')
    ]);
    const d=await r1.json();
    const perLp=await r2.json();
    if(!d.total_views && !d.total_links){
      el.innerHTML='<div style="text-align:center;padding:16px;color:#484f58;">No LP links created yet.<br>Create a share link first.</div>';
      return;
    }
    // Summary strip
    const strip=`
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:14px;">
        <div style="background:rgba(63,185,80,.08);border:1px solid rgba(63,185,80,.2);border-radius:6px;padding:8px;text-align:center;">
          <div style="font-family:var(--mono);font-size:1.3rem;font-weight:700;color:#3fb950;">${d.total_views||0}</div>
          <div style="font-size:10px;color:#8b949e;">Total Views</div>
        </div>
        <div style="background:rgba(232,160,32,.06);border:1px solid rgba(232,160,32,.2);border-radius:6px;padding:8px;text-align:center;">
          <div style="font-family:var(--mono);font-size:1.3rem;font-weight:700;color:var(--accent);">${Math.round((d.total_time_s||0)/60)}m</div>
          <div style="font-size:10px;color:#8b949e;">Engagement</div>
        </div>
        <div style="background:rgba(88,166,255,.06);border:1px solid rgba(88,166,255,.15);border-radius:6px;padding:8px;text-align:center;">
          <div style="font-family:var(--mono);font-size:1.3rem;font-weight:700;color:#58a6ff;">${d.downloads||0}</div>
          <div style="font-size:10px;color:#8b949e;">Downloads</div>
        </div>
      </div>`;

    // #227: time-ago helper
    function _timeAgo(isoStr){
      if(!isoStr)return '—';
      const diff=Date.now()-new Date(isoStr).getTime();
      const m=Math.floor(diff/60000);
      if(m<2)return 'just now';if(m<60)return m+'m ago';
      const h=Math.floor(m/60);if(h<24)return h+'h ago';
      return Math.floor(h/24)+'d ago';
    }
    // Per-LP read receipt rows (#212, #227)
    const lpRows=(perLp.per_lp||[]).map(lp=>{
      const viewed=lp.view_count>0;
      const badge=viewed
        ? '<span style="font-family:var(--mono);font-size:9px;background:rgba(63,185,80,.12);color:#3fb950;border:1px solid rgba(63,185,80,.3);border-radius:3px;padding:1px 5px;">OPENED</span>'
        : '<span style="font-family:var(--mono);font-size:9px;background:rgba(255,255,255,.04);color:#484f58;border:1px solid rgba(255,255,255,.07);border-radius:3px;padding:1px 5px;">NOT OPENED</span>';
      const lastSeen=lp.last_viewed?lp.last_viewed.slice(0,16).replace('T',' '):'—';
      const timeAgoStr=lp.last_viewed?_timeAgo(lp.last_viewed):'';
      const topSecs=Object.entries(lp.sections||{}).sort((a,b)=>b[1]-a[1]).slice(0,3)
        .map(([s,t])=>'<span style="font-size:9px;color:#8b949e;background:rgba(255,255,255,.04);border-radius:3px;padding:1px 5px;">'+s+' '+Math.round(t)+'s</span>').join(' ');
      const dealN=(_currentDeal&&_currentDeal.deal_name)||'the deal';
      // Follow-up CTA: email draft (mailto) referencing LP label + top section
      const topSection=topSecs?Object.entries(lp.sections||{}).sort((a,b)=>b[1]-a[1])[0]?.[0]||'the analysis':'';
      const followUpBody=encodeURIComponent('Hi '+lp.label+',\\n\\nI saw you had a chance to look at the '+dealN+' report'+(topSection?' &mdash; particularly the '+topSection+'.':'.')+' Happy to walk through any questions or share updated projections.\\n\\nBest,');
      const followUpLink=viewed?'<a href="mailto:?subject=Follow+up+on+'+encodeURIComponent(dealN)+'&body='+followUpBody+'" style="font-size:9px;color:var(--accent);text-decoration:none;margin-top:4px;display:inline-block;background:rgba(232,160,32,.07);border:1px solid rgba(232,160,32,.2);border-radius:3px;padding:1px 6px;">Draft Follow-up &rarr;</a>':'';
      return '<div style="border:1px solid rgba(255,255,255,.06);border-radius:7px;padding:9px 12px;margin-bottom:6px;background:rgba(255,255,255,.02);">'
        +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:'+(topSecs?'5px':'0')+';">'
        +'<span style="font-size:12px;font-weight:500;color:var(--text-primary);">'+lp.label+'</span>'
        +'<div style="display:flex;align-items:center;gap:6px;">'+badge+'<span style="font-family:var(--mono);font-size:9px;color:#484f58;">'+lp.view_count+' view'+(lp.view_count!==1?'s':'')+'</span></div>'
        +'</div>'
        +(topSecs?'<div style="display:flex;gap:4px;flex-wrap:wrap;">'+topSecs+'</div>':'')
        +(timeAgoStr?'<div style="font-family:var(--mono);font-size:9px;color:'+(viewed?'#3fb950':'#484f58')+';margin-top:4px;">Last opened: '+timeAgoStr+'</div>':'')
        +followUpLink
        +'</div>';
    }).join('');
    // #227: Notification status badge
    // #241: notification badge — shows engagement alert status clearly
    const notifBadge=`<div style="display:flex;align-items:center;gap:7px;font-size:10px;color:#3fb950;background:rgba(63,185,80,.06);border:1px solid rgba(63,185,80,.2);border-radius:5px;padding:5px 10px;margin-bottom:10px;">
      <span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:#3fb950;animation:pulse 2s infinite;flex-shrink:0;"></span>
      <span><strong style="font-family:var(--mono);font-size:9px;letter-spacing:.06em;text-transform:uppercase;">First-Open Alerts Active</strong> &mdash; you'll get an instant email the first time each LP opens their link</span>
    </div>`;

    // #222: Re-up prediction score
    const totalViews=d.total_views||0;
    const totalMins=Math.round((d.total_time_s||0)/60);
    const downloads=d.downloads||0;
    // Weighted score: views 0-30pts, time 0-35pts, downloads 0-35pts
    const vScore=Math.min(totalViews*8,30);
    const tScore=Math.min(totalMins*3,35);
    const dScore=Math.min(downloads*17,35);
    const reupScore=Math.round(vScore+tScore+dScore);
    const reupColor=reupScore>=70?'#3fb950':reupScore>=40?'#e8a020':'#f85149';
    const reupLabel=reupScore>=70?'High — Ready to close':'Re-up likely — stay warm';
    // Only show score if there's any engagement
    let reupHtml='';
    if(totalViews>0){
      // SVG arc gauge
      const r=28;const circ=2*Math.PI*r;const arc=circ*(reupScore/100);
      const notifyKey='ce_lp_notify_'+jid;
      const notifyOn=localStorage.getItem(notifyKey)==='1';
      // Prescriptive actions
      let actionHtml='';
      const dealN=(_currentDeal&&_currentDeal.deal_name)||'the deal';
      if(reupScore<40){
        const draftBody=encodeURIComponent('Hi,\\n\\nFollowing up on '+dealN+' &mdash; happy to walk you through the analysis. Are you available for a 20-min call this week?\\n\\nBest,');
        actionHtml=`<div style="background:rgba(248,81,73,.06);border:1px solid rgba(248,81,73,.2);border-radius:6px;padding:9px 12px;margin-top:10px;">
          <div style="font-size:11px;font-weight:600;color:#f85149;margin-bottom:4px;">&#128222; Schedule a Call</div>
          <div style="font-size:11px;color:#8b949e;margin-bottom:7px;">Low engagement — direct outreach will re-warm interest.</div>
          <a href="mailto:?subject=Follow+up+on+${encodeURIComponent(dealN)}&body=${draftBody}" style="font-size:11px;color:var(--accent);text-decoration:none;display:inline-block;background:rgba(232,160,32,.08);border:1px solid rgba(232,160,32,.25);border-radius:4px;padding:3px 10px;">Draft Email &rarr;</a>
        </div>`;
      }else if(reupScore<70){
        actionHtml=`<div style="background:rgba(232,160,32,.06);border:1px solid rgba(232,160,32,.2);border-radius:6px;padding:9px 12px;margin-top:10px;">
          <div style="font-size:11px;font-weight:600;color:var(--accent);margin-bottom:4px;">&#128202; Send Updated Projections</div>
          <div style="font-size:11px;color:#8b949e;margin-bottom:7px;">Moderate interest — a fresh scenario or updated IRR range will nudge commitment.</div>
          <button onclick="switchLpTab('share',document.getElementById('lp-tab-share'));loadLpAnalytics=()=>{};" style="font-size:11px;color:var(--accent);background:rgba(232,160,32,.08);border:1px solid rgba(232,160,32,.25);border-radius:4px;padding:3px 10px;cursor:pointer;">Reshare with Updates &rarr;</button>
        </div>`;
      }else{
        actionHtml=`<div style="background:rgba(63,185,80,.06);border:1px solid rgba(63,185,80,.2);border-radius:6px;padding:9px 12px;margin-top:10px;">
          <div style="font-size:11px;font-weight:600;color:#3fb950;margin-bottom:4px;">&#128196; Send Commitment Docs</div>
          <div style="font-size:11px;color:#8b949e;margin-bottom:7px;">Strong engagement signal — LP is ready for subscription docs.</div>
          <button onclick="downloadLpPackage()" style="font-size:11px;color:#3fb950;background:rgba(63,185,80,.08);border:1px solid rgba(63,185,80,.2);border-radius:4px;padding:3px 10px;cursor:pointer;">&#8659; Download LP Package &rarr;</button>
        </div>`;
      }
      reupHtml=`<div style="border:1px solid rgba(232,160,32,.12);border-radius:7px;padding:12px;margin-top:12px;margin-bottom:4px;background:rgba(232,160,32,.03);">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:2px;">
          <svg width="70" height="70" viewBox="0 0 70 70" style="flex-shrink:0;">
            <circle cx="35" cy="35" r="${r}" fill="none" stroke="rgba(255,255,255,.06)" stroke-width="6"/>
            <circle cx="35" cy="35" r="${r}" fill="none" stroke="${reupColor}" stroke-width="6"
              stroke-dasharray="${arc.toFixed(1)} ${circ.toFixed(1)}" stroke-linecap="round"
              transform="rotate(-90 35 35)" style="transition:stroke-dasharray .6s ease;"/>
            <text x="35" y="39" text-anchor="middle" font-family="IBM Plex Mono,monospace" font-size="13" font-weight="700" fill="${reupColor}">${reupScore}</text>
          </svg>
          <div>
            <div style="font-size:11px;font-weight:600;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);">Re-Up Score</div>
            <div style="font-size:12px;font-weight:600;color:${reupColor};margin-top:2px;">${reupLabel}</div>
            <div style="font-size:10px;color:var(--text-muted);margin-top:4px;">Based on ${totalViews} view${totalViews!==1?'s':''} · ${totalMins}m engagement · ${downloads} download${downloads!==1?'s':''}</div>
          </div>
          <div style="margin-left:auto;">
            <label style="display:flex;align-items:center;gap:5px;cursor:pointer;font-size:10px;color:#8b949e;">
              <input type="checkbox" id="lp-notify-toggle" ${notifyOn?'checked':''} onchange="localStorage.setItem('${notifyKey}',this.checked?'1':'0');document.getElementById('lp-notify-label').textContent=this.checked?'Notified':'Notify me';" style="accent-color:var(--amber);">
              <span id="lp-notify-label">${notifyOn?'Notified':'Notify me'}</span>
            </label>
          </div>
        </div>
        ${actionHtml}
      </div>`;
    }
    // #247: Section Heatmap — sorted bars by dwell time
    let sectionHeatmapHtml='';
    const secTime=d.section_time_s||{};
    const secSorted=Object.entries(secTime).sort(function(a,b){return b[1]-a[1];});
    if(secSorted.length>0){
      const maxSec=secSorted[0][1]||1;
      const secRows=secSorted.map(function(entry){
        const sName=entry[0],secs=entry[1];
        const pct=Math.round((secs/maxSec)*100);
        const intensity=(0.15+0.75*(secs/maxSec)).toFixed(2);
        const barColor='rgba(63,185,80,'+intensity+')';
        const views=(d.section_views||{})[sName]||0;
        const label=sName.charAt(0).toUpperCase()+sName.slice(1);
        const avgS=views>0?Math.round(secs/views):Math.round(secs);
        return '<div style="display:flex;align-items:center;gap:8px;padding:4px 0;">'
          +'<div style="width:68px;font-size:11px;color:var(--text-secondary);flex-shrink:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="'+esc(label)+'">'+esc(label)+'</div>'
          +'<div style="flex:1;background:rgba(255,255,255,.04);border-radius:3px;height:10px;overflow:hidden;">'
          +'<div style="height:100%;width:'+pct+'%;background:'+barColor+';border-radius:3px;transition:width .5s ease;"></div>'
          +'</div>'
          +'<div style="font-family:var(--mono);font-size:10px;color:var(--text-muted);flex-shrink:0;width:38px;text-align:right;">'+avgS+'s</div>'
          +(views>0?'<div style="font-size:9px;color:#484f58;flex-shrink:0;width:22px;text-align:right;">'+views+'x</div>':'<div style="width:22px;"></div>')
          +'</div>';
      }).join('');
      sectionHeatmapHtml='<div style="background:var(--bg-elevated);border:1px solid var(--border-muted);border-radius:7px;padding:10px 12px;margin-bottom:12px;">'
        +'<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:9px;">'
        +'<span style="font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);opacity:.7;">&#128200; Section Engagement Heatmap</span>'
        +'<span style="font-size:9px;color:var(--text-muted);">avg dwell &nbsp;&#183;&nbsp; views</span>'
        +'</div>'
        +secRows
        +'</div>';
    }
    el.innerHTML=reupHtml+strip+notifBadge+sectionHeatmapHtml+(lpRows?'<div style="font-family:var(--mono);font-size:9px;letter-spacing:.08em;text-transform:uppercase;color:var(--accent);opacity:.7;margin-bottom:8px;">Per-LP Read Receipts</div>'+lpRows:'');
  }catch(e){el.innerHTML='<div style="color:#f85149;font-size:11px;">Error loading analytics</div>';}
}

// ── Market Pulse Widget (#185) ─────────────────────────────────────────────
let _pulseOpen=true;
function togglePulse(){
  _pulseOpen=!_pulseOpen;
  const body=document.getElementById('pulse-body');
  const arrow=document.getElementById('pulse-toggle-arrow');
  if(body)body.style.display=_pulseOpen?'block':'none';
  if(arrow)arrow.textContent=_pulseOpen?'▲':'▼';
}
function _pulseIndicator(val,low,high,inverted){
  // inverted=true means higher is worse (e.g. rates)
  const n=parseFloat(val);
  if(isNaN(n))return '#8b949e';
  if(!inverted){return n>=high?'#3fb950':n>=low?'#d29922':'#f85149';}
  return n<=low?'#3fb950':n<=high?'#d29922':'#f85149';
}
function _pulseArrow(val,prev){
  if(val===null||val===undefined||prev===null||prev===undefined)return '';
  return val>prev?'&#8593;':val<prev?'&#8595;':'&#8212;';
}
async function loadMarketPulse(){
  const loading=document.getElementById('pulse-loading');
  const content=document.getElementById('pulse-content');
  const errEl=document.getElementById('pulse-error');
  try{
    const r=await fetch('/api/market-pulse');
    if(!r.ok)throw new Error('HTTP '+r.status);
    const d=await r.json();
    const rows=[
      {label:'10Y Treasury',val:d['10yr_treasury'],unit:'%',low:3.5,high:4.5,inv:true},
      {label:'Mortgage 30Y',val:d['30yr_mortgage'],unit:'%',low:5.5,high:7.0,inv:true},
      {label:'Unemployment',val:d['unemployment'],unit:'%',low:4.5,high:6.0,inv:true},
      {label:'CPI (YoY)',   val:d['cpi_yoy'],     unit:'%',low:2.5,high:4.0,inv:true},
      {label:'Fed Funds',   val:d['fed_funds_rate'],unit:'%',low:3.0,high:5.0,inv:true},
    ];
    const rowsEl=document.getElementById('pulse-rows');
    rowsEl.innerHTML=rows.map(r=>{
      const col=_pulseIndicator(r.val,r.low,r.high,r.inv);
      const disp=r.val!==null&&r.val!==undefined?parseFloat(r.val).toFixed(2)+r.unit:'—';
      return '<div style="display:flex;align-items:center;justify-content:space-between;padding:3px 0;">'
        +'<span style="font-size:11px;color:#8b949e;">'+r.label+'</span>'
        +'<span style="font-size:11px;font-weight:600;color:'+col+';">'+disp+'</span>'
        +'</div>';
    }).join('');
    // Headwind score
    const hw=d.headwind_score;
    const hwCol=hw<=35?'#3fb950':hw<=60?'#d29922':'#f85149';
    const hwLabel=hw<=35?'Favorable':hw<=60?'Neutral':'Headwind';
    const hwEl=document.getElementById('pulse-headwind');
    hwEl.innerHTML='<div style="display:flex;align-items:center;justify-content:space-between;">'
      +'<span style="color:#8b949e;font-size:11px;">Macro Headwind</span>'
      +'<span style="font-size:12px;font-weight:700;color:'+hwCol+';">'+hw+'/100 '+hwLabel+'</span>'
      +'</div>';
    hwEl.style.background='rgba('+
      (hwCol==='#3fb950'?'63,185,80':hwCol==='#d29922'?'210,153,34':'248,81,73')+',.08)';
    hwEl.style.border='1px solid '+hwCol.replace('#','rgba(').split('').join('')+' no wait';
    hwEl.style.borderColor=hwCol;
    // Footer
    const src=d._source==='live'?'Live':d._source==='stale_cache'?'Cached':'Est.';
    const asOf=d.as_of?d.as_of.slice(0,10):'';
    document.getElementById('pulse-footer').textContent=src+(asOf?' · '+asOf:'');
    if(loading)loading.style.display='none';
    if(errEl)errEl.style.display='none';
    if(content)content.style.display='block';
    // Auto-refresh every 6 hours
    setTimeout(loadMarketPulse,6*3600*1000);
  }catch(e){
    if(loading)loading.style.display='none';
    if(errEl)errEl.style.display='block';
  }
}

// ── Usage quota widget (#155) ──────────────────────────────────────────────
async function loadQuotaWidget(){
  try{
    const r=await fetch('/api/usage');
    if(!r.ok)return;
    const d=await r.json();
    if(!d.tier||d.tier==='free'&&d.used===0&&d.limit===3)return; // don't show for anonymous
    const chip=document.getElementById('quota-chip');
    if(!chip)return;
    const pct=d.limit>0?Math.round(d.used/d.limit*100):0;
    let col='#3fb950';
    if(pct>=100)col='#f85149';
    else if(pct>=80)col='#d29922';
    const tierLabel={'free':'Free','operator':'Operator','professional':'Pro','team':'Team','enterprise':'Enterprise'}[d.tier]||d.tier;
    if(pct>=100){
      chip.innerHTML='<a href="/pricing" style="color:#f85149;text-decoration:none;font-weight:600;">Upgrade &rarr;</a>';
      chip.style.borderColor='#f85149';
    }else{
      chip.innerHTML=`<span style="color:${col};font-weight:600;">${tierLabel}</span> <span style="color:#8b949e;">&middot; ${d.used}/${d.limit}</span>`;
      chip.style.borderColor=col==='#3fb950'?'#238636':col==='#d29922'?'#9e6a03':'#b62324';
    }
    chip.style.display='block';
    // Quota warning banner when 1 remaining
    if(d.limit-d.used===1){
      const warn=document.getElementById('quota-warn-banner');
      if(warn)warn.style.display='block';
    }
  }catch(e){}
}

// Init
window._lpToken="{{ _lp_token|default('') }}";
try{window._prefillJob=JSON.parse(document.getElementById('ce-prefill-data').textContent);}catch(e){window._prefillJob=null;}
window._reportMode={{ 'true' if _report_mode else 'false' }};
window._currentJobId="{{ _job_id|default('') }}";
window._reportJobId="{{ _job_id|default('') }}";  // #242: used by export button
// Apply report-mode class immediately so CSS kicks in before paint
if(window._reportMode){document.body.classList.add('report-mode');}
_loadHistFilters(); // Restore persisted filters (#195)
renderHist();
loadQuotaWidget();
// #226/#238: Apply saved brand to Tools trigger; show active dot if IPS/Thesis configured
(function(){
  const b=wlLoad();
  const trig=document.getElementById('tools-trigger-btn');
  if(trig&&b.firm_name){const txt=trig.firstChild;if(txt&&txt.nodeType===3)txt.textContent='⚙️ '+b.firm_name.split(' ')[0];}
  toolsUpdateDot();
})();
loadMarketPulse();
// Fire initial section view for LP viewers (#144)
if(window._lpToken){
  _lpTrackSection('summary');
  // #241: Scroll depth tracking — report max scroll % to backend
  (function(){
    let _maxScroll=0;
    let _scrollTimer=null;
    window.addEventListener('scroll',function(){
      const el=document.documentElement;
      const scrolled=el.scrollTop||document.body.scrollTop;
      const total=(el.scrollHeight||document.body.scrollHeight)-el.clientHeight;
      const pct=total>0?Math.round((scrolled/total)*100):0;
      if(pct>_maxScroll){
        _maxScroll=pct;
        if(_scrollTimer)clearTimeout(_scrollTimer);
        _scrollTimer=setTimeout(function(){
          fetch('/api/lp/'+window._lpToken+'/event',{
            method:'POST',headers:{'Content-Type':'application/json'},
            body:JSON.stringify({event_type:'scroll_depth',section:'page',duration_s:_maxScroll/100})
          }).catch(()=>{});
        },1500);
      }
    },{ passive:true });
  })();
  // #241: Send first-view event with timestamp for notification
  fetch('/api/lp/'+window._lpToken+'/event',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({event_type:'view',section:'page',duration_s:0})
  }).catch(()=>{});
}
// Auto-render report — defer to ensure all DOM/functions ready
if(window._reportMode&&window._prefillJob){
  setTimeout(function(){
    try{
      renderResults(window._prefillJob);
      _injectReportChrome(window._prefillJob);
    }catch(e){console.error('Report render error:',e);}
  },0);
}

// Pre-load OM from deal aggregator (#123)
if(new URLSearchParams(location.search).get('preload')==='1'){
  const om=sessionStorage.getItem('ce_preload_om');
  if(om){
    sessionStorage.removeItem('ce_preload_om');
    document.getElementById('om_input').value=om;
    document.getElementById('empty-state').style.display='none';
    document.getElementById('demoBtn').style.display='none';
    document.getElementById('status-msg').textContent='Deal loaded from aggregator. Add full OM text and click Analyze.';
    document.getElementById('om_input').focus();
  }
}

// #272: Auto-run demo on ?demo=1 — first-time visitor sees a full analysis in 90s
// #275: Also auto-run on first ever visit (no ce_visited key in localStorage)
(function(){
  const p=new URLSearchParams(location.search);
  const isDemo=p.get('demo')==='1';
  const isFirstVisit=!localStorage.getItem('ce_visited')&&!window._reportMode;
  if(!isDemo&&!isFirstVisit)return;
  // Mark as visited so this only fires once
  try{localStorage.setItem('ce_visited','1');}catch(e){}
  // Only auto-run if no analysis in progress and no current results
  if(document.getElementById('results')&&document.getElementById('results').style.display==='block')return;
  setStatus('Loading a sample deal analysis...');
  setTimeout(function(){
    if(typeof loadDemoAndRun==='function'){
      loadDemoAndRun();
    }
  }, 1200);
})();
</script>
</body>
</html>"""


PIPELINE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearEye — Deal Pipeline</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg-canvas:#080b10;--bg-surface:#0f1318;--bg-elevated:#161d26;--bg-overlay:#1e2733;--border-muted:#131920;--border-default:#1e2733;--border-emphasis:#2e3d4f;--text-primary:#f0ede8;--text-secondary:#8a9bb0;--text-muted:#3d4f63;--accent:#e8a020;--green:#3fb950;--red:#f85149;--amber:#e8a020;--purple:#a371f7;--shadow-xs:0 1px 2px rgba(0,0,0,.4);--shadow-sm:0 1px 4px rgba(0,0,0,.4),0 2px 6px rgba(0,0,0,.3);--shadow-md:0 4px 14px rgba(0,0,0,.45);--r-sm:5px;--r-md:9px;--r-lg:13px;--t:140ms ease;--font:'DM Sans',-apple-system,sans-serif;--font-display:'DM Serif Display',Georgia,serif;--mono:'IBM Plex Mono','SF Mono',Consolas,monospace;--text-base:13.5px;--ls-body:-0.008em;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
body{background-color:var(--bg-canvas);background-image:radial-gradient(ellipse 80% 45% at 50% -5%,rgba(232,160,32,.07) 0%,transparent 65%),url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='200' height='200'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3CfeColorMatrix type='saturate' values='0'/%3E%3C/filter%3E%3Crect width='200' height='200' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");background-size:100% 100%,200px 200px;background-attachment:fixed;color:var(--text-primary);font-family:var(--font);font-size:var(--text-base);line-height:1.55;letter-spacing:var(--ls-body);-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility;}
.noise-overlay{position:fixed;inset:0;pointer-events:none;z-index:9999;opacity:.025;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");background-size:160px 160px;}
::-webkit-scrollbar{width:5px;height:5px;}::-webkit-scrollbar-track{background:transparent;}::-webkit-scrollbar-thumb{background:var(--border-default);border-radius:3px;}
.ce-nav{height:56px;background:rgba(9,13,18,.88);backdrop-filter:blur(20px) saturate(180%);-webkit-backdrop-filter:blur(20px) saturate(180%);border-bottom:1px solid rgba(255,255,255,.06);display:flex;align-items:center;padding:0 20px;gap:4px;position:sticky;top:0;z-index:100;}
.ce-brand{font-family:var(--font-display);font-size:1.2rem;font-weight:400;color:var(--accent);text-decoration:none;margin-right:8px;letter-spacing:-0.01em;}
.nav-pill{font-size:12px;color:var(--text-secondary);text-decoration:none;padding:5px 10px;border-radius:var(--r-sm);transition:color var(--t),background var(--t);}
.nav-pill:hover{color:var(--text-primary);background:var(--bg-overlay);}
.nav-pill.active{color:var(--accent);background:rgba(232,160,32,.08);}
/* ── Pipeline stats strip ─────────────────────────────────────────────── */
.pipe-stats{display:flex;gap:20px;padding:8px 20px 14px;align-items:center;}
.pipe-stat{display:flex;flex-direction:column;gap:1px;}
.pipe-stat-val{font-size:20px;font-weight:700;letter-spacing:-0.03em;color:var(--text-primary);font-variant-numeric:tabular-nums;}
.pipe-stat-val.ps-value{background:linear-gradient(90deg,var(--accent),#79c0ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
.pipe-stat-val.ps-active{color:var(--text-primary);}
.pipe-stat-val.ps-dd{background:linear-gradient(90deg,#a371f7,#d2a8ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
.pipe-stat-val.ps-closed{background:linear-gradient(90deg,var(--green),#56d364);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;}
.pipe-stat-lbl{font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);}
.pipe-divider{width:1px;height:32px;background:var(--border-muted);}
/* ── Kanban board ──────────────────────────────────────────────────────── */
.pipeline-wrap{display:flex;gap:14px;padding:4px 20px 20px;overflow-x:auto;min-height:calc(100vh - 130px);}
.kanban-col{min-width:256px;max-width:280px;flex-shrink:0;background:rgba(22,27,34,.72);border:1px solid rgba(255,255,255,.07);border-radius:var(--r-md);padding:0;backdrop-filter:blur(6px);box-shadow:inset 0 1px 0 rgba(255,255,255,.06),0 4px 20px rgba(0,0,0,.35);overflow:hidden;}
.kanban-col.stage-screening{border-top:3px solid var(--accent);}
.kanban-col.stage-loi{border-top:3px solid var(--amber);}
.kanban-col.stage-due-diligence{border-top:3px solid var(--purple);}
.kanban-col.stage-closed{border-top:3px solid var(--green);}
.kanban-col.stage-passed{border-top:3px solid rgba(255,255,255,.15);opacity:.7;}
.kanban-col.drag-over{background:rgba(232,160,32,.05)!important;border-color:rgba(232,160,32,.3)!important;}
/* Column header — gradient section per stage */
.col-hdr{display:flex;align-items:center;justify-content:space-between;padding:10px 12px 9px;border-bottom:1px solid rgba(255,255,255,.05);background:rgba(255,255,255,.02);}
.col-hdr.hdr-screening{background:rgba(232,160,32,.06);}
.col-hdr.hdr-loi{background:rgba(232,160,32,.04);}
.col-hdr.hdr-due-diligence{background:rgba(163,113,247,.05);}
.col-hdr.hdr-closed{background:rgba(63,185,80,.05);}
.col-hdr.hdr-passed{background:rgba(255,255,255,.02);}
.col-title{font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.09em;color:var(--text-secondary);}
.col-count{min-width:22px;height:22px;border-radius:11px;padding:0 8px;font-size:11px;font-weight:700;font-family:var(--mono);display:inline-flex;align-items:center;justify-content:center;letter-spacing:.02em;}
.col-count.cc-screening{background:rgba(232,160,32,.18);color:var(--accent);}
.col-count.cc-loi{background:rgba(232,160,32,.14);color:var(--amber);}
.col-count.cc-due-diligence{background:rgba(163,113,247,.18);color:var(--purple);}
.col-count.cc-closed{background:rgba(63,185,80,.15);color:var(--green);}
.col-count.cc-passed{background:rgba(255,255,255,.08);color:var(--text-muted);}
/* #259: Capacity bar */
.kb-cap-bar{height:3px;background:rgba(255,255,255,.06);margin:0;border-radius:0;}
.kb-cap-fill{height:3px;border-radius:0;transition:width .35s ease,background .35s ease;}
/* Column body padding */
.col-body{padding:10px 10px 4px;}
/* Deal card — elevated surface, clear visibility */
.deal-card{background:rgba(20,28,40,.92);border:1px solid rgba(255,255,255,.1);border-radius:8px;padding:11px 12px;margin-bottom:8px;cursor:grab;transition:all .2s cubic-bezier(.22,1,.36,1);box-shadow:inset 0 1px 0 rgba(255,255,255,.08),0 2px 8px rgba(0,0,0,.4);position:relative;}
.deal-card::before{content:'';position:absolute;left:0;top:0;bottom:0;width:3px;border-radius:8px 0 0 8px;opacity:0;transition:opacity .2s;}
.deal-card.stage-Screening::before{background:var(--accent);opacity:.8;}
.deal-card.stage-LOI::before{background:var(--amber);opacity:.8;}
.deal-card.stage-Due-Diligence::before{background:var(--purple);opacity:.8;}
.deal-card.stage-Closed::before{background:var(--green);opacity:.8;}
.deal-card:hover{border-color:rgba(232,160,32,.25);box-shadow:0 0 0 1px rgba(232,160,32,.1),inset 0 1px 0 rgba(255,255,255,.12),0 8px 28px rgba(0,0,0,.55);transform:translateY(-3px);background:rgba(26,36,52,.95);}
.deal-card:hover::before{opacity:1;}
.deal-card.dragging{opacity:.35;cursor:grabbing;transform:rotate(1.5deg);}
@keyframes cardEnter{from{opacity:0;transform:translateY(10px) scale(.98)}to{opacity:1;transform:translateY(0) scale(1)}}
@keyframes drawStroke{from{stroke-dashoffset:300}to{stroke-dashoffset:0}}
.pipeline-empty{display:none;padding:48px 24px;text-align:center;}
.pipeline-empty svg path,.pipeline-empty svg rect,.pipeline-empty svg line{animation:drawStroke 1.1s cubic-bezier(.22,1,.36,1) .15s both;stroke-dasharray:300;stroke-dashoffset:300;}
.deal-card{animation:cardEnter .32s cubic-bezier(.22,1,.36,1) both;}
/* ── Pipeline Skeleton Loader (#205) ── */
@keyframes shimmer{0%{background-position:-600px 0}100%{background-position:600px 0}}
.skel-board{display:flex;gap:14px;padding:4px 20px 20px;overflow-x:hidden;}
.skel-col{min-width:256px;max-width:280px;flex-shrink:0;background:rgba(22,27,34,.72);border:1px solid rgba(255,255,255,.07);border-radius:var(--r-md);overflow:hidden;}
.skel-col-hdr{height:44px;padding:0 14px;display:flex;align-items:center;gap:8px;border-bottom:1px solid rgba(255,255,255,.06);}
.skel-col-body{padding:10px;}
.skel-bar{border-radius:4px;background:linear-gradient(90deg,rgba(255,255,255,.04) 0%,rgba(255,255,255,.09) 50%,rgba(255,255,255,.04) 100%);background-size:600px 100%;animation:shimmer 1.6s infinite linear;}
.skel-title{height:12px;width:70px;}
.skel-badge{height:18px;width:22px;border-radius:9px;}
.skel-card{background:rgba(255,255,255,.025);border:1px solid rgba(255,255,255,.05);border-radius:10px;padding:12px;margin-bottom:8px;}
.skel-card-name{height:13px;width:85%;margin-bottom:8px;}
.skel-card-meta{height:10px;width:60%;margin-bottom:12px;}
.skel-card-price{height:18px;width:50%;margin-bottom:10px;}
.skel-card-actions{display:flex;gap:4px;margin-top:4px;}
.skel-card-btn{height:22px;width:48px;border-radius:4px;}
.dc-name{font-family:var(--font-display);font-style:italic;font-size:14px;font-weight:400;color:var(--text-primary);margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;padding-left:4px;letter-spacing:-0.01em;}
.dc-meta{font-size:11px;color:var(--text-secondary);margin-bottom:8px;line-height:1.4;padding-left:4px;}
.dc-price-row{display:flex;align-items:baseline;justify-content:space-between;margin-bottom:4px;padding-left:4px;}
.dc-price{font-family:var(--mono);font-size:17px;font-weight:600;letter-spacing:-0.02em;background:linear-gradient(90deg,var(--accent),#79c0ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;font-variant-numeric:tabular-nums;}
.dc-units{font-size:10px;color:var(--text-muted);font-weight:500;background:rgba(255,255,255,.06);padding:2px 6px;border-radius:4px;-webkit-text-fill-color:var(--text-muted);}
.dc-actions{display:flex;gap:4px;margin-top:8px;padding-left:4px;}
.dc-btn{padding:3px 9px;font-size:10px;background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-secondary);border-radius:4px;cursor:pointer;white-space:nowrap;transition:all var(--t);}
.dc-btn:hover{color:var(--text-primary);border-color:var(--border-emphasis);}
.dc-btn.danger:hover{color:var(--red);border-color:var(--red);}
.add-btn{width:100%;padding:8px;font-size:11px;background:transparent;border:1.5px dashed rgba(255,255,255,.1);color:var(--text-muted);border-radius:var(--r-sm);cursor:pointer;margin-top:2px;transition:all var(--t);}
.add-btn:hover{border-color:var(--accent);color:var(--accent);background:rgba(88,166,255,.04);}
.stage-badge{display:inline-block;padding:1px 7px;border-radius:4px;font-size:10px;font-weight:600;}
.sb-screening{background:rgba(88,166,255,.12);color:var(--accent);}
.sb-loi{background:rgba(210,153,34,.12);color:var(--amber);}
.sb-due-diligence{background:rgba(163,113,247,.12);color:var(--purple);}
.sb-closed{background:rgba(63,185,80,.1);color:var(--green);}
.sb-passed{background:rgba(72,79,88,.2);color:var(--text-secondary);}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.75);backdrop-filter:blur(4px);z-index:200;display:none;align-items:center;justify-content:center;}
.modal-box{background:var(--bg-surface);border:1px solid var(--border-default);border-radius:var(--r-lg);padding:24px 28px;max-width:440px;width:100%;position:relative;box-shadow:var(--shadow-md);}
.form-lbl{font-size:11px;color:var(--text-secondary);display:block;margin-bottom:3px;font-weight:500;}
.form-inp{width:100%;background:var(--bg-canvas);border:1px solid var(--border-default);color:var(--text-primary);border-radius:var(--r-sm);padding:7px 10px;font-size:12px;margin-bottom:10px;transition:border-color var(--t);}
.form-inp:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px rgba(88,166,255,.15);}
.activity-log{max-height:120px;overflow-y:auto;margin-top:8px;}
.act-item{font-size:10px;color:var(--text-secondary);padding:3px 0;border-bottom:1px solid var(--border-muted);}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px);}to{opacity:1;transform:none;}}
/* ── #220: Stall detection pulse animation ── */
@keyframes stallPulse{0%,100%{opacity:1;box-shadow:0 0 0 2px rgba(232,160,32,.35);}50%{opacity:.65;box-shadow:0 0 0 4px rgba(232,160,32,.5);}}
/* ── #208: Inline deal card quick-expand panel ── */
.dc-chevron{font-size:10px;color:var(--text-muted);cursor:pointer;padding:1px 5px;border-radius:3px;transition:transform .22s cubic-bezier(.22,1,.36,1),color .15s,background .15s;background:none;border:none;line-height:1;flex-shrink:0;}
.dc-chevron:hover{color:var(--accent);background:rgba(88,166,255,.08);}
.deal-card.expanded .dc-chevron{transform:rotate(180deg);color:var(--amber);}
.dc-expand-panel{max-height:0;overflow:hidden;transition:max-height .3s cubic-bezier(.22,1,.36,1);}
.deal-card.expanded .dc-expand-panel{max-height:240px;}
.dc-expand-inner{padding:8px 4px 2px;border-top:1px solid rgba(255,255,255,.06);}
.dc-expand-row{display:flex;align-items:flex-start;gap:6px;margin-bottom:5px;}
.dc-expand-label{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);min-width:50px;padding-top:1px;flex-shrink:0;}
.dc-expand-val{font-size:10px;color:var(--text-secondary);line-height:1.4;word-break:break-word;}
.dc-expand-act{margin-top:5px;padding-top:5px;border-top:1px solid rgba(255,255,255,.05);}
.dc-expand-act-item{font-size:9.5px;color:var(--text-secondary);padding:2px 0;display:flex;align-items:flex-start;gap:5px;line-height:1.3;}
.dc-expand-act-item .act-icon{color:var(--text-muted);font-size:8px;padding-top:2px;flex-shrink:0;}
/* DD Panel (#142) */
.dd-panel{position:fixed;right:0;top:56px;width:420px;height:calc(100vh - 56px);background:var(--bg-surface);border-left:1px solid var(--border-default);z-index:150;display:none;flex-direction:column;overflow:hidden;box-shadow:var(--shadow-md);}
.dd-panel.open{display:flex;}
.dd-hdr{padding:14px 16px;border-bottom:1px solid var(--border-muted);display:flex;align-items:center;gap:8px;}
.dd-body{flex:1;overflow-y:auto;padding:12px 16px;}
.dd-cat{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--text-secondary);margin:10px 0 5px;}
.dd-item{display:flex;align-items:flex-start;gap:8px;padding:6px 0;border-bottom:1px solid var(--border-muted);}
.dd-check{width:14px;height:14px;flex-shrink:0;margin-top:2px;cursor:pointer;accent-color:var(--green);}
.dd-text{font-size:12px;color:#c9d1d9;flex:1;line-height:1.45;}
.dd-text.done{text-decoration:line-through;color:var(--text-muted);}
.dd-assignee{font-size:10px;color:var(--text-secondary);}
.dd-due{font-size:10px;padding:1px 5px;border-radius:4px;}
.dd-due.overdue{background:rgba(248,81,73,.15);color:var(--red);}
.dd-due.soon{background:rgba(210,153,34,.15);color:var(--amber);}
.dd-due.ok{background:rgba(63,185,80,.1);color:var(--green);}
.progress-wrap{height:5px;background:var(--bg-overlay);border-radius:3px;overflow:hidden;}
.progress-fill{height:100%;border-radius:3px;background:var(--green);transition:width .4s ease;}
.doc-item{display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid var(--border-muted);}
.doc-icon{font-size:16px;flex-shrink:0;}
.doc-name{font-size:11px;color:#c9d1d9;flex:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;}
.doc-size{font-size:10px;color:var(--text-secondary);}
.dd-tab{padding:4px 10px;font-size:11px;background:none;border:1px solid var(--border-default);color:var(--text-secondary);border-radius:4px;cursor:pointer;transition:all var(--t);}
.dd-tab.active{background:var(--bg-elevated);border-color:var(--accent);color:var(--accent);}
/* Tag chips (#169) */
.tag-chip{display:inline-flex;align-items:center;gap:3px;padding:1px 7px;border-radius:10px;font-size:9px;font-weight:600;line-height:1.7;cursor:default;border:1px solid transparent;}
.tag-chip .tag-x{cursor:pointer;opacity:.55;font-size:8px;margin-left:1px;line-height:1;}
.tag-chip .tag-x:hover{opacity:1;}
.tag-row{display:flex;flex-wrap:wrap;gap:3px;margin-top:5px;}
.tag-filter-bar{padding:4px 20px 8px;display:flex;align-items:center;gap:6px;flex-wrap:wrap;border-bottom:1px solid rgba(255,255,255,.04);}
.tag-filter-chip{display:inline-flex;align-items:center;gap:4px;padding:3px 10px;border-radius:10px;font-size:10px;font-weight:600;cursor:pointer;border:1.5px solid transparent;opacity:.65;transition:opacity var(--t),border-color var(--t);}
.tag-filter-chip:hover{opacity:.9;}
.tag-filter-chip.active{opacity:1;border-color:currentColor;}
.tag-mgr-list{max-height:240px;overflow-y:auto;margin:6px 0;}
.tag-mgr-item{display:flex;align-items:center;gap:8px;padding:5px 0;border-bottom:1px solid var(--border-muted);}
.tag-color-dot{width:12px;height:12px;border-radius:50%;flex-shrink:0;}
.tag-assign-popup{position:absolute;background:var(--bg-overlay);border:1px solid var(--border-default);border-radius:8px;box-shadow:var(--shadow-md);z-index:400;min-width:160px;padding:6px;display:none;}
.tag-assign-popup.open{display:block;}
.tag-assign-opt{display:flex;align-items:center;gap:6px;padding:5px 8px;border-radius:4px;cursor:pointer;font-size:11px;color:var(--text-primary);}
.tag-assign-opt:hover{background:rgba(255,255,255,.06);}
</style>
</head>
<body>
<div class="noise-overlay" aria-hidden="true"></div>
<nav class="ce-nav">
  <a class="ce-brand" href="/">&#128065; ClearEye</a>
  <a href="/app"        class="nav-pill">Analyze</a>
  <a href="/find-deals" class="nav-pill">Find Deals</a>
  <a href="/markets"    class="nav-pill">Markets</a>
  <a href="/pipeline"   class="nav-pill active">Pipeline</a>
  <a href="/pricing"    class="nav-pill" style="margin-left:auto;">Pricing</a>
  <a href="/login"      class="nav-pill" style="color:var(--accent);">Sign In</a>
</nav>

<div style="padding:16px 20px 0;display:flex;align-items:center;justify-content:space-between;">
  <h1 style="font-size:1.15rem;font-weight:700;margin:0;letter-spacing:-0.02em;">Deal Pipeline</h1>
  <div style="display:flex;gap:8px;align-items:center;">
    <button onclick="window.location.href='/api/pipeline/export.csv'" style="padding:6px 13px;background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-secondary);border-radius:6px;font-size:11px;cursor:pointer;transition:all var(--t);" onmouseover="this.style.color='#e6edf3';this.style.borderColor='#484f58';" onmouseout="this.style.color='';this.style.borderColor='';">&#11015; Export CSV</button>
    <button onclick="sendDigest()" id="digest-btn" style="padding:6px 13px;background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-secondary);border-radius:6px;font-size:11px;cursor:pointer;transition:all var(--t);" onmouseover="this.style.color='#e6edf3';this.style.borderColor='#484f58';" onmouseout="this.style.color='';this.style.borderColor='';">&#128231; Digest</button>
    <button onclick="openAddModal()" style="padding:7px 16px;background:linear-gradient(135deg,#2ea043,#238636);border:none;color:#fff;border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;box-shadow:0 0 0 1px rgba(46,160,67,.4),0 2px 8px rgba(46,160,67,.2);transition:all var(--t);" onmouseover="this.style.boxShadow='0 0 0 1px rgba(46,160,67,.6),0 4px 14px rgba(46,160,67,.35)';" onmouseout="this.style.boxShadow='0 0 0 1px rgba(46,160,67,.4),0 2px 8px rgba(46,160,67,.2)';">+ Add Deal</button>
  </div>
</div>
<!-- Pipeline stats strip -->
<div class="pipe-stats" id="pipe-stats-strip">
  <div class="pipe-stat">
    <span class="pipe-stat-val ps-value" id="ps-total-val">—</span>
    <span class="pipe-stat-lbl">Pipeline Value</span>
  </div>
  <div class="pipe-divider"></div>
  <div class="pipe-stat">
    <span class="pipe-stat-val ps-active" id="ps-active">—</span>
    <span class="pipe-stat-lbl">Active Deals</span>
  </div>
  <div class="pipe-divider"></div>
  <div class="pipe-stat">
    <span class="pipe-stat-val ps-dd" id="ps-dd">—</span>
    <span class="pipe-stat-lbl">In Due Diligence</span>
  </div>
  <div class="pipe-divider"></div>
  <div class="pipe-stat">
    <span class="pipe-stat-val ps-closed" id="ps-closed">—</span>
    <span class="pipe-stat-lbl">Closed</span>
  </div>
  <!-- #220: Stall count badge -->
  <div class="pipe-divider" id="ps-stall-divider" style="display:none;"></div>
  <div class="pipe-stat" id="ps-stall-stat" style="display:none;">
    <span class="pipe-stat-val" id="ps-stalled" style="color:var(--amber);">—</span>
    <span class="pipe-stat-lbl">Stalled Deals</span>
  </div>
</div>

<!-- Tag filter bar -->
<div class="tag-filter-bar" id="tag-filter-bar">
  <span style="font-size:10px;color:var(--text-muted);font-weight:600;text-transform:uppercase;letter-spacing:.07em;">Filter by tag:</span>
  <span id="tag-filter-chips" style="display:contents;"></span>
  <button onclick="openTagMgr()" style="margin-left:auto;padding:3px 10px;background:rgba(255,255,255,.05);border:1px solid var(--border-default);color:var(--text-secondary);border-radius:5px;font-size:10px;cursor:pointer;">&#x2715; Manage tags</button>
</div>

<!-- Skeleton loader shown on initial board fetch (#205) -->
<div class="skel-board" id="pipeline-skeleton">
  <!-- 5 skeleton columns matching real stages -->
  <div class="skel-col" style="border-top:3px solid var(--accent);">
    <div class="skel-col-hdr"><div class="skel-bar skel-title"></div><div class="skel-bar skel-badge" style="margin-left:auto;"></div></div>
    <div class="skel-col-body">
      <div class="skel-card"><div class="skel-bar skel-card-name"></div><div class="skel-bar skel-card-meta"></div><div class="skel-bar skel-card-price"></div><div class="skel-card-actions"><div class="skel-bar skel-card-btn"></div><div class="skel-bar skel-card-btn"></div></div></div>
      <div class="skel-card"><div class="skel-bar skel-card-name" style="width:70%"></div><div class="skel-bar skel-card-meta" style="width:45%"></div><div class="skel-bar skel-card-price" style="width:40%"></div><div class="skel-card-actions"><div class="skel-bar skel-card-btn"></div></div></div>
    </div>
  </div>
  <div class="skel-col" style="border-top:3px solid var(--amber);">
    <div class="skel-col-hdr"><div class="skel-bar skel-title" style="width:50px;"></div><div class="skel-bar skel-badge" style="margin-left:auto;"></div></div>
    <div class="skel-col-body">
      <div class="skel-card"><div class="skel-bar skel-card-name" style="width:75%"></div><div class="skel-bar skel-card-meta"></div><div class="skel-bar skel-card-price"></div><div class="skel-card-actions"><div class="skel-bar skel-card-btn"></div><div class="skel-bar skel-card-btn"></div></div></div>
    </div>
  </div>
  <div class="skel-col" style="border-top:3px solid var(--purple);">
    <div class="skel-col-hdr"><div class="skel-bar skel-title" style="width:90px;"></div><div class="skel-bar skel-badge" style="margin-left:auto;"></div></div>
    <div class="skel-col-body">
      <div class="skel-card"><div class="skel-bar skel-card-name"></div><div class="skel-bar skel-card-meta" style="width:55%"></div><div class="skel-bar skel-card-price" style="width:44%"></div><div class="skel-card-actions"><div class="skel-bar skel-card-btn"></div><div class="skel-bar skel-card-btn"></div><div class="skel-bar skel-card-btn"></div></div></div>
      <div class="skel-card"><div class="skel-bar skel-card-name" style="width:65%"></div><div class="skel-bar skel-card-meta" style="width:40%"></div><div class="skel-bar skel-card-price" style="width:35%"></div><div class="skel-card-actions"><div class="skel-bar skel-card-btn"></div></div></div>
    </div>
  </div>
  <div class="skel-col" style="border-top:3px solid var(--green);">
    <div class="skel-col-hdr"><div class="skel-bar skel-title" style="width:55px;"></div><div class="skel-bar skel-badge" style="margin-left:auto;"></div></div>
    <div class="skel-col-body">
      <div class="skel-card"><div class="skel-bar skel-card-name" style="width:80%"></div><div class="skel-bar skel-card-meta"></div><div class="skel-bar skel-card-price"></div><div class="skel-card-actions"><div class="skel-bar skel-card-btn"></div></div></div>
    </div>
  </div>
  <div class="skel-col" style="border-top:3px solid rgba(255,255,255,.15);opacity:.6;">
    <div class="skel-col-hdr"><div class="skel-bar skel-title" style="width:48px;"></div><div class="skel-bar skel-badge" style="margin-left:auto;"></div></div>
    <div class="skel-col-body"></div>
  </div>
</div>

<div class="pipeline-wrap" id="kanban-board" style="display:none;">
  <!-- Kanban columns rendered by JS -->
</div>

<!-- #260: Pipeline empty state — shown by renderBoard() when no deals -->
<div class="pipeline-empty" id="pipeline-empty-state">
  <svg width="120" height="80" viewBox="0 0 120 80" fill="none" stroke="var(--accent)" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
    <rect x="4" y="8" width="28" height="64" rx="4"/>
    <rect x="46" y="20" width="28" height="52" rx="4"/>
    <rect x="88" y="28" width="28" height="44" rx="4"/>
    <line x1="12" y1="18" x2="24" y2="18"/><line x1="12" y1="26" x2="24" y2="26"/><line x1="12" y1="34" x2="24" y2="34"/>
    <line x1="54" y1="30" x2="66" y2="30"/><line x1="54" y1="38" x2="66" y2="38"/>
    <line x1="96" y1="38" x2="108" y2="38"/>
  </svg>
  <div style="font-family:'DM Serif Display',Georgia,serif;font-style:italic;font-size:1.1rem;color:var(--text-primary);margin-bottom:6px;">Your pipeline is empty.</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:16px;line-height:1.5;">Deals move through Screening → LOI → Due Diligence → Closed.<br>Run an analysis to add your first deal.</div>
  <a href="/app" style="display:inline-flex;align-items:center;gap:6px;padding:9px 18px;background:var(--accent);color:#fff;border-radius:7px;font-size:12px;font-weight:600;text-decoration:none;">Analyze your first deal →</a>
</div>

<!-- Tag assignment popup (repositioned dynamically) -->
<div class="tag-assign-popup" id="tag-assign-popup"></div>

<!-- Tag Manager Modal -->
<div class="modal-overlay" id="tag-mgr-modal" style="display:none;align-items:center;justify-content:center;">
  <div class="modal-box" style="max-width:380px;">
    <button onclick="closeTagMgr()" style="position:absolute;top:12px;right:12px;background:none;border:none;color:#8b949e;font-size:16px;cursor:pointer;">&#x2715;</button>
    <div style="font-size:1rem;font-weight:700;margin-bottom:14px;">&#127991; Tag Manager</div>
    <div style="display:flex;gap:6px;margin-bottom:10px;">
      <input class="form-inp" id="new-tag-name" placeholder="Tag name" style="flex:1;margin-bottom:0;">
      <input type="color" id="new-tag-color" value="#58a6ff" style="width:36px;height:34px;padding:2px;background:var(--bg-canvas);border:1px solid var(--border-default);border-radius:var(--r-sm);cursor:pointer;">
      <button onclick="createTag()" style="padding:7px 12px;background:#238636;border:none;color:#fff;border-radius:var(--r-sm);font-size:12px;cursor:pointer;white-space:nowrap;">+ Add</button>
    </div>
    <div class="tag-mgr-list" id="tag-mgr-list"></div>
    <button onclick="closeTagMgr()" style="width:100%;padding:8px;background:var(--bg-elevated);border:1px solid var(--border-default);color:var(--text-secondary);border-radius:var(--r-sm);cursor:pointer;margin-top:4px;font-size:12px;">Done</button>
  </div>
</div>

<!-- Add/Edit Deal Modal -->
<div class="modal-overlay" id="deal-modal">
  <div class="modal-box">
    <button onclick="closeModal()" style="position:absolute;top:12px;right:12px;background:none;border:none;color:#8b949e;font-size:16px;cursor:pointer;">&#x2715;</button>
    <div style="font-size:1rem;font-weight:700;margin-bottom:16px;" id="modal-title">Add Deal to Pipeline</div>
    <input type="hidden" id="modal-deal-id">
    <label class="form-lbl">Deal Name *</label>
    <input class="form-inp" id="f-dealname" placeholder="e.g. Sunset Ridge Apartments">
    <label class="form-lbl">Address</label>
    <input class="form-inp" id="f-address" placeholder="4750 N 7th St, Phoenix, AZ">
    <label class="form-lbl">Market</label>
    <input class="form-inp" id="f-market" placeholder="Phoenix, AZ">
    <div style="display:flex;gap:10px;">
      <div style="flex:1;">
        <label class="form-lbl">Asking Price ($)</label>
        <input class="form-inp" id="f-price" type="number" placeholder="18500000">
      </div>
      <div style="flex:1;">
        <label class="form-lbl">Units</label>
        <input class="form-inp" id="f-units" type="number" placeholder="124">
      </div>
    </div>
    <label class="form-lbl">Stage</label>
    <select class="form-inp" id="f-stage">
      <option>Screening</option><option>LOI</option><option>Due Diligence</option><option>Closed</option><option>Passed</option>
    </select>
    <label class="form-lbl">Assigned To</label>
    <input class="form-inp" id="f-assigned" placeholder="analyst@company.com">
    <label class="form-lbl">Notes</label>
    <textarea class="form-inp" id="f-notes" rows="2" placeholder="Key observations, broker contact, etc."></textarea>
    <div style="display:flex;gap:8px;margin-top:4px;">
      <button onclick="saveDeal()" style="flex:1;padding:8px;background:#238636;border:none;color:#fff;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;" id="modal-save-btn">Add to Pipeline</button>
      <button onclick="closeModal()" style="padding:8px 16px;background:#21262d;border:1px solid #30363d;color:#8b949e;border-radius:6px;font-size:12px;cursor:pointer;">Cancel</button>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
const STAGES=['Screening','LOI','Due Diligence','Closed','Passed'];
const STAGE_COLORS={Screening:'#58a6ff',LOI:'#d29922','Due Diligence':'#a371f7',Closed:'#3fb950',Passed:'#484f58'};
const STAGE_CSS={Screening:'stage-screening',LOI:'stage-loi','Due Diligence':'stage-due-diligence',Closed:'stage-closed',Passed:'stage-passed'};
const HDR_CSS={Screening:'hdr-screening',LOI:'hdr-loi','Due Diligence':'hdr-due-diligence',Closed:'hdr-closed',Passed:'hdr-passed'};
const CNT_CSS={Screening:'cc-screening',LOI:'cc-loi','Due Diligence':'cc-due-diligence',Closed:'cc-closed',Passed:'cc-passed'};
const BADGE_CSS={Screening:'sb-screening',LOI:'sb-loi','Due Diligence':'sb-due-diligence',Closed:'sb-closed',Passed:'sb-passed'};
let _deals=[], _byStage={};
let _dragDealId=null;
let _allTags=[], _activeTagFilter=null;

async function loadPipeline(){
  // Show skeleton, hide real board during initial fetch (#205)
  const skel=document.getElementById('pipeline-skeleton');
  const board=document.getElementById('kanban-board');
  if(skel)skel.style.display='flex';
  if(board)board.style.display='none';
  try{
    const [pipeRes, tagRes]=await Promise.all([fetch('/api/pipeline'),fetch('/api/tags')]);
    const d=await pipeRes.json();
    const td=await tagRes.json();
    _deals=d.deals||[];
    _byStage=d.by_stage||{};
    _allTags=td.tags||[];
    renderTagFilterBar();
    renderBoard();
    renderSummary();
  }finally{
    // Hide skeleton, reveal real board
    if(skel)skel.style.display='none';
    if(board)board.style.display='flex';
  }
}

function renderSummary(){
  const active=_deals.filter(d=>d.stage!=='Passed'&&d.stage!=='Closed').length;
  const closed=_deals.filter(d=>d.stage==='Closed').length;
  const inDD=_deals.filter(d=>d.stage==='Due Diligence').length;
  const totalVal=_deals.filter(d=>d.stage!=='Passed').reduce((s,d)=>s+(d.asking_price||0),0);
  // #220: Count stalled deals
  const now=Date.now();
  const stalled=_deals.filter(d=>{
    if(!d.stage_entered_at||d.stage==='Closed'||d.stage==='Passed')return false;
    const ms=now-new Date(d.stage_entered_at).getTime();
    const days=ms/(1000*60*60*24);
    const threshold=d.stage==='Due Diligence'?30:14;
    return days>threshold;
  }).length;
  // Update stats strip
  const fmt=v=>v>=1e6?'$'+(v/1e6).toFixed(1)+'M':v>=1e3?'$'+(v/1e3).toFixed(0)+'K':'$'+v;
  const pv=document.getElementById('ps-total-val');
  const pa=document.getElementById('ps-active');
  const pd=document.getElementById('ps-dd');
  const pc=document.getElementById('ps-closed');
  const ps=document.getElementById('ps-stalled');
  if(pv)pv.textContent=totalVal?fmt(totalVal):'$0';
  if(pa)pa.textContent=active;
  if(pd)pd.textContent=inDD;
  if(pc)pc.textContent=closed;
  if(ps){ps.textContent=stalled||'0';}
  const stallDiv=document.getElementById('ps-stall-divider');
  const stallStat=document.getElementById('ps-stall-stat');
  if(stallDiv)stallDiv.style.display=stalled?'block':'none';
  if(stallStat)stallStat.style.display=stalled?'flex':'none';
}

function renderBoard(){
  const board=document.getElementById('kanban-board');
  const emptyEl=document.getElementById('pipeline-empty-state');
  // #260: Show SVG empty state when no deals at all
  const totalDeals=_deals.length;
  if(emptyEl)emptyEl.style.display=(totalDeals===0)?'block':'none';
  board.innerHTML=STAGES.map(stage=>{
    let cards=(_byStage[stage]||[]);
    if(_activeTagFilter!==null){
      cards=cards.filter(d=>d.tags&&d.tags.some(t=>t.id===_activeTagFilter));
    }
    const css=STAGE_CSS[stage]||'';
    const hdrCss=HDR_CSS[stage]||'';
    const cntCss=CNT_CSS[stage]||'';
    const col=STAGE_COLORS[stage]||'#58a6ff';
    const stageSlug=stage.replace(/ /g,'-');
    // #259: Capacity bar — max 10 per stage
    const MAX_CAP=10;
    const capPct=Math.min(Math.round((cards.length/MAX_CAP)*100),100);
    const capColor=cards.length>=MAX_CAP*0.8?'var(--red)':cards.length>=MAX_CAP*0.5?'var(--amber)':'var(--green)';
    return `<div class="kanban-col ${css}" id="col-${stageSlug}"
      ondragover="event.preventDefault();document.getElementById('col-${stageSlug}').classList.add('drag-over')"
      ondragleave="document.getElementById('col-${stageSlug}').classList.remove('drag-over')"
      ondrop="document.getElementById('col-${stageSlug}').classList.remove('drag-over');dropDeal(event,'${stage}')">
      <div class="col-hdr ${hdrCss}">
        <span class="col-title" style="color:${col};">${stage}</span>
        <span class="col-count ${cntCss}" id="cnt-${stage}">${cards.length}</span>
      </div>
      <div class="kb-cap-bar"><div class="kb-cap-fill" style="width:${capPct}%;background:${capColor};"></div></div>
      <div class="col-body">
      ${cards.map((c,ci)=>dealCard(c,ci)).join('')}
      <button class="add-btn" onclick="openAddModal('${stage}')">+ Add deal</button>
      </div>
    </div>`;
  }).join('');
}

// ── #208: Inline quick-expand state ──────────────────────────────────────
const _expandedCards = new Set();
function toggleCardExpand(id, event){
  event && event.stopPropagation();
  const card = document.getElementById('card-' + id);
  if(!card) return;
  if(_expandedCards.has(id)){
    _expandedCards.delete(id);
    card.classList.remove('expanded');
  } else {
    _expandedCards.add(id);
    card.classList.add('expanded');
    // Lazy-load activity on first open
    const actEl = card.querySelector('.dc-expand-act-items');
    if(actEl && !actEl.dataset.loaded){
      actEl.dataset.loaded = '1';
      fetch('/api/pipeline/' + id + '/activity')
        .then(r => r.json())
        .then(data => {
          const acts = (data.activity || []).slice(0, 3);
          if(!acts.length){
            actEl.innerHTML = '<div class="dc-expand-act-item" style="color:var(--text-muted);font-style:italic;">No activity yet</div>';
            return;
          }
          actEl.innerHTML = acts.map(a => {
            const action_label = (a.action||'').replace(/_/g,' ');
            const detail_short = (a.detail||'').slice(0, 45) + ((a.detail||'').length > 45 ? '…' : '');
            const time_label = a.created_at ? new Date(a.created_at).toLocaleDateString(undefined,{month:'short',day:'numeric'}) : '';
            return '<div class="dc-expand-act-item"><span class="act-icon">&#9679;</span><span>'
              + esc(action_label)
              + (detail_short ? '<span style="color:var(--text-muted)">: ' + esc(detail_short) + '</span>' : '')
              + (time_label ? '<span style="color:var(--text-muted);margin-left:4px;font-size:8.5px;">' + esc(time_label) + '</span>' : '')
              + '</span></div>';
          }).join('');
        })
        .catch(() => { actEl.innerHTML = ''; });
    }
  }
}

function dealCard(d, cardIdx){
  const _delay=(cardIdx||0)*45;
  const priceFmt=d.asking_price?(d.asking_price>=1e6?'$'+(d.asking_price/1e6).toFixed(1)+'M':'$'+(d.asking_price/1e3).toFixed(0)+'K'):'—';
  const unitsBadge=d.units?'<span class="dc-units">'+d.units+'u</span>':'';
  const hasAnalysis=d.job_id?'<a href="/report/'+d.job_id+'" target="_blank" style="display:inline-flex;align-items:center;gap:3px;color:var(--accent);font-size:10px;margin-bottom:4px;">&#128200; View Analysis &#8599;</a><br>':'';
  const assignee=d.assigned_to?'<div style="font-size:10px;color:var(--text-secondary);margin-bottom:4px;">&#128100; '+esc(d.assigned_to)+'</div>':'';
  const ddProgress=d._dd_progress!==undefined
    ?'<div style="margin:6px 0 2px 4px;"><div style="display:flex;justify-content:space-between;font-size:10px;color:var(--text-secondary);margin-bottom:3px;"><span>DD Progress</span><span style="font-weight:600;color:var(--purple);">'+d._dd_progress+'%</span></div><div class="progress-wrap"><div class="progress-fill" style="width:'+(d._dd_progress||0)+'%;background:var(--purple);"></div></div></div>'
    :'';
  // Tag chips (#169) — data-* attrs avoid quote nesting
  const tagChips=(d.tags&&d.tags.length)
    ?d.tags.map(t=>'<span class="tag-chip" style="background:'+t.color+'22;color:'+t.color+';border:1px solid '+t.color+'55;" data-deal="'+d.id+'" data-tag="'+t.id+'" onclick="removeTagFromDeal(this.dataset.deal,parseInt(this.dataset.tag),event)">'+esc(t.name)+'<span class="tag-x">&#x2715;</span></span>').join('')
    :'';
  const tagRow='<div class="tag-row" style="padding-left:4px;">'+tagChips
    +'<span class="tag-chip" style="background:rgba(255,255,255,.05);color:#8b949e;border:1px solid rgba(255,255,255,.08);cursor:pointer;" data-deal="'+d.id+'" onclick="openTagAssign(this.dataset.deal,event)">+ tag</span></div>';
  // Color tag dot (#193)
  const _ctColors={red:'#f85149',yellow:'#d29922',green:'#3fb950',blue:'#58a6ff',purple:'#a371f7',none:'transparent'};
  const ctCol=_ctColors[d.color_tag||'none']||'transparent';
  const ctDot=(d.color_tag&&d.color_tag!=='none')?'<span style="display:inline-block;width:9px;height:9px;border-radius:50%;background:'+ctCol+';margin-left:4px;flex-shrink:0;"></span>':'';
  // Notes preview (#193)
  const notePreview=(d.notes&&d.notes.trim())?'<div style="font-size:10px;color:var(--text-secondary);padding:0 4px 4px;font-style:italic;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:100%;" title="'+esc(d.notes)+'">&#128221; '+esc((d.notes||'').slice(0,60))+(d.notes&&d.notes.length>60?'...':'')+'</div>':'';
  const stageClass='stage-'+d.stage.replace(/ /g,'-');
  // #220: Stall indicator — amber pulse dot + tooltip if stuck in stage too long
  let stallDot='';
  if(d.stage_entered_at && d.stage!=='Closed' && d.stage!=='Passed'){
    const daysInStage=Math.floor((Date.now()-new Date(d.stage_entered_at).getTime())/(1000*60*60*24));
    const threshold=d.stage==='Due Diligence'?30:14;
    if(daysInStage>threshold){
      const noteTitle='In '+esc(d.stage)+' for '+daysInStage+' days — what\\u2019s holding this one back?';
      stallDot=`<span title="${noteTitle}" style="display:inline-block;width:7px;height:7px;border-radius:50%;background:var(--amber);margin-left:5px;flex-shrink:0;box-shadow:0 0 0 2px rgba(232,160,32,.35);animation:stallPulse 1.8s ease-in-out infinite;" onclick="openNoteModal('${d.id}','Stall reason: ','none');event.stopPropagation();"></span>`;
    }
  }
  // #208: Build expand panel content (address, market, asset class, activity placeholder)
  const expandAddr = d.address && d.address !== (d.market||'') ? '<div class="dc-expand-row"><span class="dc-expand-label">Address</span><span class="dc-expand-val">' + esc(d.address) + '</span></div>' : '';
  const expandMkt = d.market ? '<div class="dc-expand-row"><span class="dc-expand-label">Market</span><span class="dc-expand-val">' + esc(d.market) + '</span></div>' : '';
  const expandAC = d.asset_class ? '<div class="dc-expand-row"><span class="dc-expand-label">Type</span><span class="dc-expand-val">' + esc(d.asset_class) + '</span></div>' : '';
  const expandVerdict = d.job_id ? '<div class="dc-expand-row"><span class="dc-expand-label">Report</span><span class="dc-expand-val"><a href="/report/'+d.job_id+'" target="_blank" style="color:var(--accent);text-decoration:none;">View Analysis &#8599;</a></span></div>' : '';
  const expandPanel = '<div class="dc-expand-panel"><div class="dc-expand-inner">'
    + expandAddr + expandMkt + expandAC + expandVerdict
    + '<div class="dc-expand-act" style="margin-top:5px;padding-top:5px;border-top:1px solid rgba(255,255,255,.05);">'
    + '<div style="font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.07em;color:var(--text-muted);margin-bottom:4px;">Recent Activity</div>'
    + '<div class="dc-expand-act-items"><!-- lazy --><div class="dc-expand-act-item" style="color:var(--text-muted);font-style:italic;">Loading…</div></div>'
    + '</div></div></div>';
  return `<div class="deal-card ${stageClass}" id="card-${d.id}" draggable="true" style="animation-delay:${_delay}ms"
    ondragstart="startDrag(event,'${d.id}')"
    ondragend="endDrag(event)">
    <div style="display:flex;align-items:center;gap:0;">
      <div class="dc-name" title="${esc(d.deal_name)}" style="flex:1;">${esc(d.deal_name)}</div>
      ${ctDot}
      ${stallDot}
      <button class="dc-chevron" onclick="toggleCardExpand('${d.id}',event)" title="Quick preview">&#9660;</button>
    </div>
    <div class="dc-meta">${esc(d.market||d.address||'')}</div>
    <div class="dc-price-row">
      <span class="dc-price">${priceFmt}</span>
      ${unitsBadge}
    </div>
    ${assignee}${hasAnalysis}${notePreview}${tagRow}${ddProgress}
    ${expandPanel}
    <div class="dc-actions">
      <button class="dc-btn" onclick="openEditModal('${d.id}')">&#9998; Edit</button>
      <button class="dc-btn" onclick="openNoteModal('${d.id}','${esc(d.notes||'')}','${d.color_tag||'none'}')" title="Add/edit note and color tag" style="color:#8b949e;border-color:rgba(139,148,158,.3);">&#128221; Note</button>
      <button class="dc-btn" onclick="openDDPanel('${d.id}',${JSON.stringify(JSON.stringify(d))})" style="color:var(--purple);border-color:rgba(163,113,247,.4);">&#9989; DD</button>
      <button class="dc-btn" onclick="openTimeline('${d.id}','${esc(d.deal_name)}')" style="color:#d29922;border-color:rgba(210,153,34,.4);" title="View deal timeline">&#8987; Timeline</button>
      ${(d.stage==='Closed'||d.stage==='Passed')?`<button class="dc-btn" onclick="openOutcomeModal('${d.id}','${esc(d.deal_name)}')" style="color:#3fb950;border-color:rgba(63,185,80,.4);" title="Record deal outcome">&#127937; Outcome</button>`:''}
      <select class="dc-btn" onchange="moveDeal('${d.id}',this.value);this.value=''" style="max-width:85px;">
        <option value="">Move to...</option>
        ${STAGES.filter(s=>s!==d.stage).map(s=>'<option value="'+s+'">'+s+'</option>').join('')}
      </select>
      <button class="dc-btn danger" onclick="deleteDeal('${d.id}')">&#x2715;</button>
    </div>
  </div>`;
}

// ── Tag System (#169) ─────────────────────────────────────────────────────
function renderTagFilterBar(){
  const el=document.getElementById('tag-filter-chips');
  if(!el)return;
  if(!_allTags.length){el.innerHTML='<span style="font-size:10px;color:var(--text-muted);font-style:italic;">No tags yet</span>';return;}
  el.innerHTML=_allTags.map(t=>{
    const active=_activeTagFilter===t.id;
    return '<span class="tag-filter-chip'+(active?' active':'')+'" style="background:'+t.color+'18;color:'+t.color+';" onclick="toggleTagFilter('+t.id+')">'
      +'<span style="width:7px;height:7px;border-radius:50%;background:'+t.color+';display:inline-block;"></span>'
      +esc(t.name)+'</span>';
  }).join('');
}
function toggleTagFilter(id){
  _activeTagFilter=(_activeTagFilter===id)?null:id;
  renderTagFilterBar();
  renderBoard();
}

// Tag assignment popup
let _tagAssignDealId=null;
function openTagAssign(dealId,event){
  event.stopPropagation();
  _tagAssignDealId=dealId;
  const popup=document.getElementById('tag-assign-popup');
  if(!_allTags.length){popup.innerHTML='<div style="padding:8px;font-size:11px;color:var(--text-secondary);">No tags yet. <a onclick="openTagMgr()" style="color:var(--accent);cursor:pointer;">Create one</a></div>';} else {
    const deal=_deals.find(d=>d.id===dealId);
    const assigned=new Set((deal&&deal.tags||[]).map(t=>t.id));
    popup.innerHTML=_allTags.map(t=>{
      const has=assigned.has(t.id);
      const check=has?'<span style="color:var(--green);">&#10003; </span>':'';
      const dot='<span style="width:10px;height:10px;border-radius:50%;background:'+t.color+';display:inline-block;flex-shrink:0;margin-right:2px;"></span>';
      return '<div class="tag-assign-opt" data-deal="'+dealId+'" data-tag="'+t.id+'" onclick="toggleTagOnDeal(this.dataset.deal,parseInt(this.dataset.tag))">'
        +dot+check+esc(t.name)+'</div>';
    }).join('')+'<div style="border-top:1px solid var(--border-muted);margin-top:4px;padding-top:4px;"><div class="tag-assign-opt" onclick="openTagMgr()" style="color:var(--text-secondary);">&#x2715; Manage tags...</div></div>';
  }
  const rect=event.target.getBoundingClientRect();
  popup.style.top=(rect.bottom+window.scrollY+4)+'px';
  popup.style.left=rect.left+'px';
  popup.classList.add('open');
  setTimeout(()=>document.addEventListener('click',closeTagAssignOutside,{once:true}),10);
}
function closeTagAssignOutside(e){
  const popup=document.getElementById('tag-assign-popup');
  if(popup&&!popup.contains(e.target))popup.classList.remove('open');
}
async function toggleTagOnDeal(dealId,tagId){
  const deal=_deals.find(d=>d.id===dealId);
  const hasTag=deal&&deal.tags&&deal.tags.some(t=>t.id===tagId);
  if(hasTag){
    await fetch('/api/pipeline/'+dealId+'/tags/'+tagId,{method:'DELETE'});
  } else {
    await fetch('/api/pipeline/'+dealId+'/tags',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tag_id:tagId})});
  }
  document.getElementById('tag-assign-popup').classList.remove('open');
  await loadPipeline();
}
async function removeTagFromDeal(dealId,tagId,event){
  event.stopPropagation();
  await fetch('/api/pipeline/'+dealId+'/tags/'+tagId,{method:'DELETE'});
  await loadPipeline();
}

// Tag Manager Modal
function openTagMgr(){
  document.getElementById('tag-assign-popup').classList.remove('open');
  renderTagMgrList();
  const m=document.getElementById('tag-mgr-modal');
  m.style.display='flex';
}
function closeTagMgr(){
  document.getElementById('tag-mgr-modal').style.display='none';
}
function renderTagMgrList(){
  const el=document.getElementById('tag-mgr-list');
  if(!_allTags.length){el.innerHTML='<div style="font-size:11px;color:var(--text-secondary);padding:8px 0;">No tags yet. Create one above.</div>';return;}
  el.innerHTML=_allTags.map(t=>`
    <div class="tag-mgr-item">
      <span class="tag-color-dot" style="background:${t.color};"></span>
      <span style="flex:1;font-size:12px;">${esc(t.name)}</span>
      <button onclick="deleteTag(${t.id})" style="background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:13px;padding:2px 6px;" title="Delete tag">&#x2715;</button>
    </div>`).join('');
}
async function createTag(){
  const name=document.getElementById('new-tag-name').value.trim();
  const color=document.getElementById('new-tag-color').value;
  if(!name)return;
  await fetch('/api/tags',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,color})});
  document.getElementById('new-tag-name').value='';
  const tagRes=await fetch('/api/tags');
  _allTags=(await tagRes.json()).tags||[];
  renderTagMgrList();
  renderTagFilterBar();
}
async function deleteTag(id){
  if(!confirm('Delete this tag? It will be removed from all deals.'))return;
  await fetch('/api/tags/'+id,{method:'DELETE'});
  if(_activeTagFilter===id)_activeTagFilter=null;
  await loadPipeline();
  renderTagMgrList();
}
// Close tag mgr on backdrop click
document.getElementById('tag-mgr-modal').addEventListener('click',function(e){if(e.target===this)closeTagMgr();});

// ── Drag & Drop ────────────────────────────────────────────────────────────
let _dragGhost=null;
function startDrag(e,id){
  _dragDealId=id;
  e.dataTransfer.effectAllowed='move';
  document.getElementById('card-'+id).classList.add('dragging');
  // #262: Custom drag ghost — styled card clone
  const deal=_deals.find(function(d){return d.id===id;});
  if(deal){
    _dragGhost=document.createElement('div');
    _dragGhost.style.cssText=
      'position:absolute;left:-9999px;top:0;width:200px;padding:10px 12px;'
      +'border-radius:8px;background:rgba(20,28,40,.97);border:1px solid rgba(232,160,32,.35);'
      +'box-shadow:0 8px 28px rgba(0,0,0,.6);pointer-events:none;';
    const stageColors={'Screening':'var(--accent)','LOI':'var(--amber)','Due Diligence':'var(--purple)','Closed':'var(--green)','Passed':'rgba(255,255,255,.3)'};
    const col=stageColors[deal.stage]||'var(--accent)';
    const price=deal.asking_price?'$'+(deal.asking_price/1e6).toFixed(1)+'M':'';
    _dragGhost.innerHTML=
      '<div style="font-size:9px;font-weight:700;color:'+col+';text-transform:uppercase;letter-spacing:.08em;font-family:monospace;margin-bottom:5px;">'+esc(deal.stage||'')+'</div>'
      +'<div style="font-size:13px;font-weight:600;color:#f0ede8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:4px;">'+esc(deal.deal_name||'Deal')+'</div>'
      +(price?'<div style="font-family:monospace;font-size:11px;color:var(--amber);">'+price+'</div>':'');
    document.body.appendChild(_dragGhost);
    try{e.dataTransfer.setDragImage(_dragGhost,20,20);}catch(err){}
  }
}
function endDrag(e){
  if(_dragDealId)document.getElementById('card-'+_dragDealId)?.classList.remove('dragging');
  document.querySelectorAll('.kanban-col').forEach(c=>c.style.background='');
  _dragDealId=null;
  if(_dragGhost){_dragGhost.remove();_dragGhost=null;}
}
async function dropDeal(e,stage){
  e.currentTarget.style.background='';
  if(!_dragDealId)return;
  const deal=_deals.find(d=>d.id===_dragDealId);
  if(deal&&deal.stage===stage)return;
  await moveDeal(_dragDealId,stage);
  _dragDealId=null;
}

// ── CRUD ──────────────────────────────────────────────────────────────────
async function moveDeal(id,stage){
  await fetch('/api/pipeline/'+id+'/move',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({stage})});
  await loadPipeline();
  showToast('Moved to '+stage);
}
async function deleteDeal(id){
  if(!confirm('Remove this deal from the pipeline?'))return;
  await fetch('/api/pipeline/'+id,{method:'DELETE'});
  await loadPipeline();
}
async function saveDeal(){
  const id=document.getElementById('modal-deal-id').value;
  const body={
    deal_name:document.getElementById('f-dealname').value.trim(),
    address:document.getElementById('f-address').value.trim(),
    market:document.getElementById('f-market').value.trim(),
    asking_price:parseFloat(document.getElementById('f-price').value)||null,
    units:parseInt(document.getElementById('f-units').value)||null,
    stage:document.getElementById('f-stage').value,
    assigned_to:document.getElementById('f-assigned').value.trim()||null,
    notes:document.getElementById('f-notes').value.trim(),
  };
  if(!body.deal_name){alert('Deal name required');return;}
  if(id){
    await fetch('/api/pipeline/'+id,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    showToast('Deal updated');
  } else {
    await fetch('/api/pipeline',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    showToast('Deal added to pipeline');
  }
  closeModal();
  await loadPipeline();
}

// ── Pipeline Digest (#174) ────────────────────────────────────────────────
async function sendDigest(){
  const email=prompt('Send weekly digest to email:','');
  if(!email)return;
  const btn=document.getElementById('digest-btn');
  if(btn){btn.textContent='Sending...';btn.disabled=true;}
  try{
    const r=await fetch('/api/pipeline/digest',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({email:email})});
    const d=await r.json();
    if(d.ok){
      const msg=d.sent_via_smtp?'Digest sent to '+d.recipient+' ('+d.deal_count+' deals)':'Digest saved locally (SMTP not configured) — check outputs/ folder';
      alert(msg);
    }else{alert('Error: '+(d.error||'unknown'));}
  }catch(e){alert('Request failed: '+e);}
  finally{if(btn){btn.textContent='&#128231; Digest';btn.disabled=false;}}
}

// ── Modals ────────────────────────────────────────────────────────────────
function openAddModal(stage='Screening'){
  document.getElementById('modal-title').textContent='Add Deal to Pipeline';
  document.getElementById('modal-save-btn').textContent='Add to Pipeline';
  document.getElementById('modal-deal-id').value='';
  ['f-dealname','f-address','f-market','f-price','f-units','f-assigned','f-notes'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('f-stage').value=stage;
  document.getElementById('deal-modal').style.display='flex';
  document.getElementById('f-dealname').focus();
}
function openEditModal(id){
  const deal=_deals.find(d=>d.id===id);
  if(!deal)return;
  document.getElementById('modal-title').textContent='Edit Deal';
  document.getElementById('modal-save-btn').textContent='Save Changes';
  document.getElementById('modal-deal-id').value=id;
  document.getElementById('f-dealname').value=deal.deal_name||'';
  document.getElementById('f-address').value=deal.address||'';
  document.getElementById('f-market').value=deal.market||'';
  document.getElementById('f-price').value=deal.asking_price||'';
  document.getElementById('f-units').value=deal.units||'';
  document.getElementById('f-stage').value=deal.stage||'Screening';
  document.getElementById('f-assigned').value=deal.assigned_to||'';
  document.getElementById('f-notes').value=deal.notes||'';
  document.getElementById('deal-modal').style.display='flex';
}
function closeModal(){document.getElementById('deal-modal').style.display='none';}
document.getElementById('deal-modal').addEventListener('click',e=>{if(e.target===document.getElementById('deal-modal'))closeModal();});

function esc(s){return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');}
function showToast(msg){
  const el=document.createElement('div');el.textContent=msg;
  el.style.cssText='position:fixed;bottom:20px;right:20px;background:#238636;color:#fff;padding:8px 16px;border-radius:6px;font-size:12px;z-index:9999;animation:fadeIn .2s ease;';
  document.body.appendChild(el);setTimeout(()=>el.remove(),2000);
}

// Init
loadPipeline();

// ── DD Checklist & Document Vault (#142) ──────────────────────────────────
let _ddDealId=null, _ddDeal=null, _ddTab='checklist';

function openDDPanel(dealId, dealJson){
  _ddDealId=dealId;
  _ddDeal=typeof dealJson==='string'?JSON.parse(dealJson):dealJson;
  document.getElementById('dd-panel').classList.add('open');
  document.getElementById('dd-deal-title').textContent=_ddDeal.deal_name||'Deal';
  loadDD();
}
function closeDDPanel(){
  document.getElementById('dd-panel').classList.remove('open');
  _ddDealId=null;
}
function switchDDTab(tab){
  _ddTab=tab;
  document.querySelectorAll('.dd-tab').forEach(b=>b.classList.toggle('active',b.dataset.tab===tab));
  document.getElementById('dd-checklist-body').style.display=tab==='checklist'?'block':'none';
  document.getElementById('dd-docs-body').style.display=tab==='docs'?'block':'none';
  document.getElementById('dd-checklist-add').style.display=tab==='checklist'?'block':'none';
  document.getElementById('dd-docs-add').style.display=tab==='docs'?'flex':'none';
}

async function loadDD(){
  if(!_ddDealId) return;
  const r=await fetch('/api/pipeline/'+_ddDealId+'/dd');
  const d=await r.json();
  renderDDChecklist(d);
  renderDDDocs(d.documents||[]);
  // Update progress bar on card
  const card=document.querySelector('#card-'+_ddDealId+' .progress-fill');
  if(card && d.progress) card.style.width=d.progress.pct+'%';
}

function renderDDChecklist(data){
  const byCat=data.by_category||{};
  const prog=data.progress||{total:0,completed:0,pct:0};
  const el=document.getElementById('dd-progress-wrap');
  if(el){
    el.querySelector('.progress-fill').style.width=prog.pct+'%';
    el.querySelector('.dd-prog-label').textContent=prog.completed+'/'+prog.total+' ('+prog.pct+'%)';
  }
  const body=document.getElementById('dd-checklist-body');
  const catOrder=['environmental','title','financial','physical','zoning','financing','general'];
  const catLabels={environmental:'Environmental',title:'Title & Legal',financial:'Financial',physical:'Physical',zoning:'Zoning & Regulatory',financing:'Financing',general:'Other'};
  let html='';
  const allCats=[...new Set([...catOrder,...Object.keys(byCat)])];
  for(const cat of allCats){
    const items=byCat[cat];
    if(!items||!items.length) continue;
    html+=`<div class="dd-cat">${catLabels[cat]||cat}</div>`;
    for(const item of items){
      const dueHtml=item.due_date?`<span class="dd-due ${getDueCls(item.due_date)}">${item.due_date}</span>`:'';
      html+=`<div class="dd-item" id="ddi-${item.id}">
        <input type="checkbox" class="dd-check" ${item.completed?'checked':''} onchange="toggleDDItem('${item.id}',this.checked)">
        <div style="flex:1;">
          <div class="dd-text ${item.completed?'done':''}">${esc(item.title)}</div>
          <div style="display:flex;gap:6px;align-items:center;margin-top:2px;">
            ${item.assignee?`<span class="dd-assignee">&#128100; ${esc(item.assignee)}</span>`:''}
            ${dueHtml}
          </div>
          ${item.notes?`<div style="font-size:10px;color:#8b949e;margin-top:1px;font-style:italic;">${esc(item.notes)}</div>`:''}
        </div>
        <button onclick="deleteDDItem('${item.id}')" style="background:none;border:none;color:#484f58;cursor:pointer;font-size:11px;padding:0 2px;" title="Delete">&#x2715;</button>
      </div>`;
    }
  }
  body.innerHTML=html||'<div style="text-align:center;padding:20px;font-size:12px;color:#484f58;">No checklist items yet.<br><button onclick="seedDD()" style="margin-top:8px;padding:6px 14px;background:#1f6feb;border:none;color:#fff;border-radius:5px;cursor:pointer;font-size:12px;">+ Seed Default Checklist</button></div>';
}

function getDueCls(due){
  if(!due) return '';
  const d=new Date(due), now=new Date(), diff=(d-now)/(86400000);
  if(diff<0) return 'overdue';
  if(diff<7) return 'soon';
  return 'ok';
}

async function toggleDDItem(id,checked){
  await fetch('/api/dd/'+id,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({completed:checked?1:0})});
  const el=document.querySelector('#ddi-'+id+' .dd-text');
  if(el) el.classList.toggle('done',checked);
  await loadDD();
}

async function deleteDDItem(id){
  if(!confirm('Remove this checklist item?')) return;
  await fetch('/api/dd/'+id,{method:'DELETE'});
  await loadDD();
}

async function addCustomDDItem(){
  const inp=document.getElementById('dd-new-title');
  const title=(inp?.value||'').trim();
  if(!title){alert('Enter item title');return;}
  const cat=document.getElementById('dd-new-cat')?.value||'general';
  const assignee=document.getElementById('dd-new-assignee')?.value||null;
  const due=document.getElementById('dd-new-due')?.value||null;
  await fetch('/api/pipeline/'+_ddDealId+'/dd',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({title,category:cat,assignee,due_date:due})});
  inp.value='';
  await loadDD();
}

async function seedDD(){
  await fetch('/api/pipeline/'+_ddDealId+'/dd/seed',{method:'POST'});
  await loadDD();
}

function renderDDDocs(docs){
  const body=document.getElementById('dd-docs-body');
  const icons={pdf:'📄',xlsx:'📊',xls:'📊',csv:'📊',docx:'📝',doc:'📝',jpg:'🖼',png:'🖼',other:'📎'};
  const getIcon=n=>{const e=n.split('.').pop().toLowerCase();return icons[e]||icons.other;};
  body.innerHTML=docs.length?docs.map(d=>`
    <div class="doc-item">
      <span class="doc-icon">${getIcon(d.filename)}</span>
      <div style="flex:1;min-width:0;">
        <div class="doc-name">${esc(d.filename)}</div>
        <div class="doc-size">${d.file_size?Math.round(d.file_size/1024)+' KB':''} · ${d.category} · ${(d.uploaded_at||'').slice(0,10)}</div>
      </div>
      <a href="/api/docs/${d.id}/download" style="font-size:10px;color:#58a6ff;text-decoration:none;" title="Download">&#8659;</a>
      <button onclick="deleteDoc('${d.id}')" style="background:none;border:none;color:#484f58;cursor:pointer;font-size:11px;margin-left:4px;">&#x2715;</button>
    </div>`).join('')
    :'<div style="text-align:center;padding:16px;font-size:12px;color:#484f58;">No documents uploaded yet</div>';
}

async function uploadDoc(){
  const fileInp=document.getElementById('dd-file-input');
  const cat=document.getElementById('dd-doc-cat').value||'other';
  if(!fileInp||!fileInp.files||!fileInp.files[0]){alert('Select a file');return;}
  const fd=new FormData();
  fd.append('file',fileInp.files[0]);
  fd.append('category',cat);
  const r=await fetch('/api/pipeline/'+_ddDealId+'/docs',{method:'POST',body:fd});
  const d=await r.json();
  if(d.error){alert(d.error);return;}
  fileInp.value='';
  showToast('Uploaded: '+d.filename);
  await loadDD();
}

async function deleteDoc(id){
  if(!confirm('Delete this document?'))return;
  await fetch('/api/docs/'+id,{method:'DELETE'});
  await loadDD();
}
</script>

<!-- DD Panel (#142) -->
<div class="dd-panel" id="dd-panel">
  <div class="dd-hdr">
    <div style="flex:1;min-width:0;">
      <div style="font-size:11px;color:#8b949e;">Due Diligence</div>
      <div style="font-weight:700;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;" id="dd-deal-title">Deal Name</div>
    </div>
    <button onclick="closeDDPanel()" style="background:none;border:none;color:#8b949e;font-size:16px;cursor:pointer;flex-shrink:0;">&#x2715;</button>
  </div>
  <!-- Progress bar -->
  <div id="dd-progress-wrap" style="padding:10px 16px;border-bottom:1px solid #21262d;">
    <div style="display:flex;justify-content:space-between;font-size:11px;color:#8b949e;margin-bottom:4px;">
      <span>DD Progress</span><span class="dd-prog-label">0/0 (0%)</span>
    </div>
    <div class="progress-wrap"><div class="progress-fill" style="width:0%;"></div></div>
  </div>
  <!-- Tabs -->
  <div style="padding:8px 16px;border-bottom:1px solid #21262d;display:flex;gap:6px;">
    <button class="dd-tab active" data-tab="checklist" onclick="switchDDTab('checklist')">&#9989; Checklist</button>
    <button class="dd-tab" data-tab="docs" onclick="switchDDTab('docs')">&#128196; Documents</button>
  </div>
  <!-- Body -->
  <div class="dd-body">
    <div id="dd-checklist-body">Loading...</div>
    <div id="dd-docs-body" style="display:none;">Loading...</div>
  </div>
  <!-- Add item form (checklist) -->
  <div style="padding:10px 16px;border-top:1px solid #21262d;background:#0d1117;" id="dd-add-row">
    <div id="dd-checklist-add">
      <input id="dd-new-title" placeholder="New checklist item..." style="width:100%;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:6px 8px;font-size:11px;margin-bottom:6px;">
      <div style="display:flex;gap:6px;">
        <select id="dd-new-cat" style="flex:1;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:5px 6px;font-size:10px;">
          <option value="general">General</option>
          <option value="environmental">Environmental</option>
          <option value="title">Title</option>
          <option value="financial">Financial</option>
          <option value="physical">Physical</option>
          <option value="zoning">Zoning</option>
          <option value="financing">Financing</option>
        </select>
        <input id="dd-new-assignee" placeholder="Assignee" style="flex:1;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:5px 6px;font-size:10px;">
        <input id="dd-new-due" type="date" style="flex:1;background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:5px 6px;font-size:10px;">
        <button onclick="addCustomDDItem()" style="padding:5px 10px;background:#238636;border:none;color:#fff;border-radius:4px;font-size:11px;cursor:pointer;">+</button>
      </div>
    </div>
    <div id="dd-docs-add" style="display:none;">
      <div style="display:flex;gap:6px;align-items:center;">
        <input type="file" id="dd-file-input" style="flex:1;font-size:11px;color:#e6edf3;">
        <select id="dd-doc-cat" style="background:#161b22;border:1px solid #30363d;color:#e6edf3;border-radius:4px;padding:5px 6px;font-size:10px;">
          <option value="om">OM</option>
          <option value="t12">T-12</option>
          <option value="rent_roll">Rent Roll</option>
          <option value="psa">PSA</option>
          <option value="inspection">Inspection</option>
          <option value="environmental">Environmental</option>
          <option value="title">Title</option>
          <option value="financing">Financing</option>
          <option value="other">Other</option>
        </select>
        <button onclick="uploadDoc()" style="padding:5px 10px;background:#1f6feb;border:none;color:#fff;border-radius:4px;font-size:11px;cursor:pointer;">Upload</button>
      </div>
    </div>
  </div>
</div>

<!-- Deal Timeline Drawer (#189) -->
<div id="timeline-overlay" onclick="closeTimeline()" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.45);z-index:800;"></div>
<div id="timeline-drawer" style="position:fixed;top:0;right:-380px;width:360px;height:100vh;background:var(--bg-surface,#161b22);border-left:1px solid #30363d;z-index:801;overflow-y:auto;padding:20px 20px 40px;transition:right .3s cubic-bezier(.16,1,.3,1);display:flex;flex-direction:column;gap:0;">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px;">
    <div>
      <div style="font-size:10px;color:#8b949e;text-transform:uppercase;letter-spacing:.07em;margin-bottom:3px;">Timeline</div>
      <div id="tl-deal-name" style="font-size:14px;font-weight:700;color:#e6edf3;"></div>
    </div>
    <button onclick="closeTimeline()" style="background:none;border:none;color:#8b949e;font-size:18px;cursor:pointer;padding:4px;">&#x2715;</button>
  </div>
  <div id="tl-loading" style="color:#8b949e;font-size:13px;padding:20px 0;">Loading timeline...</div>
  <div id="tl-events" style="display:none;position:relative;padding-left:28px;"></div>
  <div id="tl-empty" style="display:none;color:#8b949e;font-size:13px;padding:20px 0;">No events yet.</div>
</div>

<style>
.tl-event{position:relative;padding:0 0 18px 0;font-size:12px;}
.tl-event::before{content:'';position:absolute;left:-20px;top:6px;bottom:-12px;width:1px;background:#21262d;}
.tl-event:last-child::before{display:none;}
.tl-icon{position:absolute;left:-28px;top:2px;width:18px;height:18px;border-radius:50%;background:#1c2128;border:1px solid #30363d;display:flex;align-items:center;justify-content:center;font-size:9px;}
.tl-title{font-weight:600;color:#e6edf3;margin-bottom:2px;}
.tl-detail{color:#8b949e;font-size:11px;}
.tl-ts{color:#484f58;font-size:10px;margin-top:2px;}
.tl-link{color:#58a6ff;font-size:11px;text-decoration:none;display:inline-block;margin-top:4px;}
.tl-link:hover{text-decoration:underline;}
.tl-type-status .tl-icon{background:rgba(58,166,255,.1);border-color:#58a6ff;}
.tl-type-analysis .tl-icon{background:rgba(63,185,80,.1);border-color:#3fb950;}
.tl-type-note .tl-icon{background:rgba(210,153,34,.1);border-color:#d29922;}
.tl-type-created .tl-icon{background:rgba(163,113,247,.1);border-color:#a371f7;}
</style>

<script>
function openTimeline(dealId,dealName){
  document.getElementById('tl-deal-name').textContent=dealName||'Deal';
  document.getElementById('tl-loading').style.display='block';
  document.getElementById('tl-events').style.display='none';
  document.getElementById('tl-empty').style.display='none';
  const drawer=document.getElementById('timeline-drawer');
  const overlay=document.getElementById('timeline-overlay');
  drawer.style.right='0';
  overlay.style.display='block';
  fetch('/api/pipeline/'+dealId+'/timeline')
    .then(r=>r.json())
    .then(d=>{
      document.getElementById('tl-loading').style.display='none';
      const evts=d.events||[];
      if(!evts.length){document.getElementById('tl-empty').style.display='block';return;}
      const el=document.getElementById('tl-events');
      el.innerHTML=evts.map(e=>{
        const ts=e.ts?e.ts.slice(0,16).replace('T',' '):'';
        const link=e.link?'<a class="tl-link" href="'+e.link+'" target="_blank">View Report &#8599;</a>':'';
        return '<div class="tl-event tl-type-'+e.type+'">'
          +'<div class="tl-icon">'+e.icon+'</div>'
          +'<div class="tl-title">'+esc(e.title||'')+'</div>'
          +(e.detail?'<div class="tl-detail">'+esc(e.detail)+'</div>':'')
          +(ts?'<div class="tl-ts">'+ts+'</div>':'')
          +link
          +'</div>';
      }).join('');
      el.style.display='block';
    })
    .catch(()=>{
      document.getElementById('tl-loading').textContent='Failed to load timeline.';
    });
}
function closeTimeline(){
  document.getElementById('timeline-drawer').style.right='-380px';
  document.getElementById('timeline-overlay').style.display='none';
}

// ── Deal Outcome Modal (#225) ─────────────────────────────────────────────
let _ocDealId='';
function openOutcomeModal(dealId, dealName){
  _ocDealId=dealId;
  const modal=document.getElementById('outcome-modal');
  if(!modal)return;
  document.getElementById('oc-deal-name').textContent=dealName||'Deal';
  document.getElementById('oc-actual-irr').value='';
  document.getElementById('oc-em').value='';
  document.getElementById('oc-closed-date').value='';
  document.getElementById('oc-notes').value='';
  document.getElementById('oc-status').textContent='';
  // Pre-fill if outcome already recorded
  fetch('/api/pipeline/'+dealId+'/outcome').then(r=>r.json()).then(d=>{
    if(d.outcome){
      const o=d.outcome;
      if(o.actual_irr!=null)document.getElementById('oc-actual-irr').value=o.actual_irr;
      if(o.actual_equity_multiple!=null)document.getElementById('oc-em').value=o.actual_equity_multiple;
      if(o.closed_date)document.getElementById('oc-closed-date').value=o.closed_date;
      if(o.notes)document.getElementById('oc-notes').value=o.notes;
    }
  }).catch(()=>{});
  modal.style.display='flex';
}
function closeOutcomeModal(){
  const modal=document.getElementById('outcome-modal');
  if(modal)modal.style.display='none';
}
async function saveOutcome(){
  const irrVal=document.getElementById('oc-actual-irr').value;
  const emVal=document.getElementById('oc-em').value;
  const dateVal=document.getElementById('oc-closed-date').value;
  const notesVal=document.getElementById('oc-notes').value.trim();
  const body={
    actual_irr:irrVal?parseFloat(irrVal):null,
    actual_equity_multiple:emVal?parseFloat(emVal):null,
    closed_date:dateVal,notes:notesVal
  };
  try{
    const r=await fetch('/api/pipeline/'+_ocDealId+'/outcome',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    const d=await r.json();
    const st=document.getElementById('oc-status');
    if(d.ok){
      if(st)st.innerHTML='<span style="color:#3fb950;">&#10003; Outcome recorded</span>';
      setTimeout(closeOutcomeModal,1200);
      // Show recorded badge on card
      const card=document.getElementById('card-'+_ocDealId);
      if(card&&!card.querySelector('.oc-badge')){
        const badge=document.createElement('div');
        badge.className='oc-badge';
        badge.style.cssText='font-size:10px;color:#3fb950;background:rgba(63,185,80,.08);border:1px solid rgba(63,185,80,.2);border-radius:3px;padding:2px 7px;margin-top:4px;display:inline-block;';
        badge.textContent='Outcome recorded';
        card.querySelector('.dc-actions').before(badge);
      }
    }else{if(st)st.innerHTML='<span style="color:#f85149;">Error: '+d.error+'</span>';}
  }catch(e){const st=document.getElementById('oc-status');if(st)st.innerHTML='<span style="color:#f85149;">Error: '+e.message+'</span>';}
}

// ── Pipeline Deal Notes & Color Tags (#193) ────────────────────────────────
let _pnDealId='';
const _TAG_COLORS={red:'#f85149',yellow:'#d29922',green:'#3fb950',blue:'#58a6ff',purple:'#a371f7',none:'#484f58'};
function openNoteModal(dealId, curNotes, curTag){
  _pnDealId=dealId;
  document.getElementById('pn-textarea').value=curNotes||'';
  document.querySelectorAll('.pn-swatch').forEach(function(sw){
    sw.style.outline=sw.dataset.tag===(curTag||'none')?'2px solid #fff':'none';
  });
  document.getElementById('pipeline-note-modal').style.display='flex';
  setTimeout(function(){document.getElementById('pn-textarea').focus();},60);
}
function closePipelineNoteModal(){
  document.getElementById('pipeline-note-modal').style.display='none';
}
function _selectNoteTag(tag){
  document.querySelectorAll('.pn-swatch').forEach(function(sw){
    sw.style.outline=sw.dataset.tag===tag?'2px solid #fff':'none';
  });
}
function _getSelectedTag(){
  let sel='none';
  document.querySelectorAll('.pn-swatch').forEach(function(sw){
    if(sw.style.outline&&sw.style.outline!=='none')sel=sw.dataset.tag;
  });
  return sel;
}
async function savePipelineNote(){
  const notes=document.getElementById('pn-textarea').value;
  const tag=_getSelectedTag();
  await fetch('/api/pipeline/'+_pnDealId+'/note',{
    method:'PUT',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({notes,tag})
  });
  closePipelineNoteModal();
  loadDeals();
}
</script>

<!-- Pipeline Note Modal (#193) -->
<div id="pipeline-note-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:1000;align-items:flex-start;justify-content:center;padding-top:80px;"
     onclick="if(event.target===this)closePipelineNoteModal()">
  <div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:20px;width:440px;max-width:94vw;">
    <div style="display:flex;align-items:center;margin-bottom:12px;">
      <span style="font-size:13px;font-weight:700;">&#128221; Deal Note &amp; Tag</span>
      <button onclick="closePipelineNoteModal()" style="margin-left:auto;background:none;border:none;color:#8b949e;font-size:18px;cursor:pointer;">&#x2715;</button>
    </div>
    <textarea id="pn-textarea" placeholder="Add notes about this deal..." rows="5"
      style="width:100%;box-sizing:border-box;background:#0d1117;border:1px solid #30363d;border-radius:6px;color:#e6edf3;font-size:12px;padding:9px 12px;resize:vertical;font-family:monospace;line-height:1.5;outline:none;margin-bottom:12px;"></textarea>
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:14px;">
      <span style="font-size:11px;color:#8b949e;">Color tag:</span>
      <span class="pn-swatch" data-tag="none" onclick="_selectNoteTag('none')" style="width:16px;height:16px;border-radius:50%;background:#484f58;cursor:pointer;border:2px solid #30363d;" title="None"></span>
      <span class="pn-swatch" data-tag="red" onclick="_selectNoteTag('red')" style="width:16px;height:16px;border-radius:50%;background:#f85149;cursor:pointer;border:2px solid transparent;" title="Red"></span>
      <span class="pn-swatch" data-tag="yellow" onclick="_selectNoteTag('yellow')" style="width:16px;height:16px;border-radius:50%;background:#d29922;cursor:pointer;border:2px solid transparent;" title="Yellow"></span>
      <span class="pn-swatch" data-tag="green" onclick="_selectNoteTag('green')" style="width:16px;height:16px;border-radius:50%;background:#3fb950;cursor:pointer;border:2px solid transparent;" title="Green"></span>
      <span class="pn-swatch" data-tag="blue" onclick="_selectNoteTag('blue')" style="width:16px;height:16px;border-radius:50%;background:#58a6ff;cursor:pointer;border:2px solid transparent;" title="Blue"></span>
      <span class="pn-swatch" data-tag="purple" onclick="_selectNoteTag('purple')" style="width:16px;height:16px;border-radius:50%;background:#a371f7;cursor:pointer;border:2px solid transparent;" title="Purple"></span>
    </div>
    <div style="display:flex;gap:8px;justify-content:flex-end;">
      <button onclick="closePipelineNoteModal()" style="padding:7px 16px;background:#21262d;border:1px solid #30363d;color:#c9d1d9;border-radius:6px;cursor:pointer;font-size:12px;">Cancel</button>
      <button onclick="savePipelineNote()" style="padding:7px 16px;background:#238636;border:none;color:#fff;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;">Save</button>
    </div>
  </div>
</div>

<!-- #225: Deal Outcome Modal -->
<div id="outcome-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.65);z-index:1050;align-items:flex-start;justify-content:center;padding-top:80px;"
     onclick="if(event.target===this)closeOutcomeModal()">
  <div style="background:#161b22;border:1px solid rgba(63,185,80,.25);border-top:3px solid #3fb950;border-radius:12px;padding:22px 24px;width:440px;max-width:94vw;">
    <div style="display:flex;align-items:center;margin-bottom:14px;">
      <span style="font-size:13px;font-weight:700;color:#f0ede8;">&#127937; Record Deal Outcome</span>
      <button onclick="closeOutcomeModal()" style="margin-left:auto;background:none;border:none;color:#8b949e;font-size:18px;cursor:pointer;">&#x2715;</button>
    </div>
    <div style="font-size:11px;color:#8b949e;margin-bottom:14px;" id="oc-deal-name">Deal</div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:12px;">
      <div>
        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Actual IRR %</label>
        <input id="oc-actual-irr" type="number" step="0.1" placeholder="e.g. 14.2" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 9px;font-size:12px;font-family:monospace;outline:none;">
      </div>
      <div>
        <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Equity Multiple</label>
        <input id="oc-em" type="number" step="0.01" placeholder="e.g. 2.1" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 9px;font-size:12px;font-family:monospace;outline:none;">
      </div>
    </div>
    <div style="margin-bottom:12px;">
      <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Closed Date</label>
      <input id="oc-closed-date" type="date" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 9px;font-size:12px;outline:none;">
    </div>
    <div style="margin-bottom:14px;">
      <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Notes (optional)</label>
      <textarea id="oc-notes" rows="3" placeholder="How did the deal perform vs projections? Key learnings?" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 9px;font-size:12px;font-family:monospace;resize:vertical;outline:none;"></textarea>
    </div>
    <div id="oc-status" style="font-size:11px;margin-bottom:8px;min-height:16px;"></div>
    <div style="display:flex;gap:8px;justify-content:flex-end;">
      <button onclick="closeOutcomeModal()" style="padding:7px 16px;background:#21262d;border:1px solid #30363d;color:#c9d1d9;border-radius:6px;cursor:pointer;font-size:12px;">Cancel</button>
      <button onclick="saveOutcome()" style="padding:7px 16px;background:rgba(63,185,80,.15);border:1px solid rgba(63,185,80,.4);color:#3fb950;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;">Save Outcome</button>
    </div>
  </div>
</div>

</body>
</html>"""

FREE_REVIEW_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearEye — Free Deal Review</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg-canvas:#080b10;--bg-surface:#0f1318;--bg-elevated:#161d26;--bg-overlay:#1e2733;--border-default:#1e2733;--border-emphasis:#2e3d4f;--text-primary:#f0ede8;--text-secondary:#8a9bb0;--text-muted:#3d4f63;--accent:#e8a020;--green:#3fb950;--red:#f85149;--amber:#e8a020;--r-md:10px;--t:140ms ease;--font:'DM Sans',-apple-system,sans-serif;--font-display:'DM Serif Display',Georgia,serif;--mono:'IBM Plex Mono','SF Mono',Consolas,monospace;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg-canvas);background-image:radial-gradient(ellipse 80% 45% at 50% -5%,rgba(232,160,32,.07) 0%,transparent 65%),url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='200' height='200'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3CfeColorMatrix type='saturate' values='0'/%3E%3C/filter%3E%3Crect width='200' height='200' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");background-size:100% 100%,200px 200px;color:var(--text-primary);font-family:var(--font);font-size:14px;-webkit-font-smoothing:antialiased;letter-spacing:-0.008em;min-height:100vh;}
.ce-nav{height:56px;background:rgba(8,11,16,.9);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,.06);display:flex;align-items:center;padding:0 20px;position:sticky;top:0;z-index:100;}
.ce-brand{font-family:var(--font-display);font-size:1.2rem;font-weight:400;color:var(--accent);text-decoration:none;letter-spacing:-0.01em;}
.nav-pill{font-size:12px;color:var(--text-secondary);text-decoration:none;padding:5px 10px;border-radius:6px;transition:color var(--t),background var(--t);}
.nav-pill:hover{color:var(--text-primary);background:var(--bg-overlay);}
.fr-hero{padding:60px 0 40px;}
.fr-headline{font-family:var(--font-display);font-style:italic;font-size:clamp(2rem,5vw,3rem);font-weight:400;letter-spacing:-0.02em;color:var(--text-primary);margin-bottom:10px;line-height:1.15;}
.fr-sub{font-size:14px;color:var(--text-secondary);max-width:560px;line-height:1.7;margin-bottom:0;}
.fr-gate{background:rgba(15,19,24,.8);border:1px solid rgba(255,255,255,.08);border-radius:12px;padding:28px 28px 24px;margin-bottom:24px;backdrop-filter:blur(6px);}
.fr-inp{width:100%;background:rgba(8,11,16,.9);border:1px solid rgba(255,255,255,.1);border-radius:8px;color:var(--text-primary);padding:10px 14px;font-size:13px;font-family:var(--font);transition:border-color var(--t);}
.fr-inp:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 3px rgba(232,160,32,.12);}
.fr-ta{min-height:200px;resize:vertical;line-height:1.5;}
.fr-btn{width:100%;padding:13px;font-size:14px;font-weight:700;background:var(--accent);color:#080b10;border:none;border-radius:8px;cursor:pointer;transition:opacity .15s,transform .1s;letter-spacing:.02em;font-family:var(--font);}
.fr-btn:hover{opacity:.88;}
.fr-btn:disabled{opacity:.45;cursor:not-allowed;}
.fr-badge{display:inline-flex;align-items:center;gap:5px;font-size:10px;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);padding:3px 9px;border-radius:4px;margin-right:6px;}
.fr-result-wrap{position:relative;}
.fr-blur-overlay{position:absolute;bottom:0;left:0;right:0;height:280px;background:linear-gradient(to bottom,transparent,var(--bg-canvas) 70%);z-index:10;display:flex;flex-direction:column;align-items:center;justify-content:flex-end;padding-bottom:32px;}
.fr-upgrade-cta{background:rgba(15,19,24,.95);border:1px solid rgba(232,160,32,.35);border-radius:12px;padding:22px 28px;text-align:center;max-width:480px;width:90%;backdrop-filter:blur(8px);}
.fr-tab-btn{padding:6px 14px;font-size:11px;background:var(--bg-elevated);border:1px solid rgba(255,255,255,.08);border-radius:6px;color:var(--text-secondary);cursor:pointer;font-family:var(--font);}
.fr-tab-btn.active{background:rgba(232,160,32,.12);border-color:rgba(232,160,32,.3);color:var(--accent);}
.fr-card{background:rgba(15,19,24,.72);border:1px solid rgba(255,255,255,.07);border-radius:10px;padding:16px 18px;margin-bottom:12px;}
.fr-verdict{font-family:var(--font-display);font-style:italic;font-size:1.5rem;letter-spacing:-0.01em;}
.fr-locked{filter:blur(5px);pointer-events:none;user-select:none;}
.fr-step{display:flex;align-items:flex-start;gap:10px;font-size:12px;color:var(--text-secondary);margin-bottom:8px;}
.fr-step-num{font-family:var(--mono);font-size:10px;color:var(--accent);background:rgba(232,160,32,.1);border:1px solid rgba(232,160,32,.25);border-radius:4px;padding:2px 6px;flex-shrink:0;margin-top:1px;}
@keyframes frFadeUp{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
.fr-result-wrap{animation:frFadeUp .4s cubic-bezier(.22,1,.36,1) both;}
</style>
</head>
<body>
<nav class="ce-nav">
  <a class="ce-brand" href="/">&#128065; ClearEye</a>
  <a href="/app" class="nav-pill" style="margin-left:auto;">Full App &rarr;</a>
</nav>

<div class="container" style="max-width:720px;">
  <!-- Hero -->
  <div class="fr-hero">
    <div style="margin-bottom:16px;">
      <span class="fr-badge">&#9889; Free</span>
      <span class="fr-badge">No credit card</span>
      <span class="fr-badge">60 sec</span>
    </div>
    <h1 class="fr-headline">Get a free adversarial<br>deal review</h1>
    <p class="fr-sub">Paste any offering memorandum. Our 5 AI advisors will stress-test it, find the kill shots, and deliver a Go/No-Go verdict — free. No account required.</p>
  </div>

  <!-- Input gate -->
  <div id="fr-gate-section">
    <div class="fr-gate">
      <div style="margin-bottom:14px;">
        <label style="font-size:11px;color:var(--text-muted);font-weight:600;letter-spacing:.06em;text-transform:uppercase;display:block;margin-bottom:5px;">Your email <span style="color:var(--red);">*</span></label>
        <input type="email" id="fr-email" class="fr-inp" placeholder="investor@example.com" required>
      </div>
      <div style="margin-bottom:18px;">
        <label style="font-size:11px;color:var(--text-muted);font-weight:600;letter-spacing:.06em;text-transform:uppercase;display:block;margin-bottom:5px;">Offering memorandum / deal summary <span style="color:var(--red);">*</span></label>
        <textarea id="fr-om" class="fr-inp fr-ta" placeholder="Paste your deal text here — OM, executive summary, or proforma. The more context, the sharper the analysis..."></textarea>
      </div>
      <button class="fr-btn" id="fr-submit-btn" onclick="frSubmit()">&#9889; Run Free Analysis</button>
      <div id="fr-status" style="font-size:11px;color:var(--text-muted);text-align:center;margin-top:10px;min-height:16px;"></div>
    </div>
    <div style="display:flex;gap:10px;font-size:11px;color:var(--text-muted);padding:0 4px;margin-bottom:40px;">
      <div class="fr-step"><span class="fr-step-num">1</span>Free analysis includes: Investment Memo, Chairman Verdict, and one Advisor perspective</div>
      <div class="fr-step"><span class="fr-step-num">2</span>Stress Test, Pre-Mortem, and LP-shareable report unlock with Professional ($697/mo)</div>
    </div>
  </div>

  <!-- Results (shown after analysis) -->
  <div id="fr-results-section" style="display:none;padding-bottom:80px;">
    <div style="display:flex;align-items:center;gap:10px;margin-bottom:20px;flex-wrap:wrap;">
      <div id="fr-verdict-stamp" style="font-family:var(--mono);font-size:12px;font-weight:700;padding:5px 14px;border-radius:6px;"></div>
      <div id="fr-deal-name" style="font-family:var(--font-display);font-style:italic;font-size:1.1rem;color:var(--text-primary);"></div>
      <button onclick="frReset()" style="margin-left:auto;font-size:11px;color:var(--text-muted);background:none;border:1px solid rgba(255,255,255,.1);padding:4px 10px;border-radius:5px;cursor:pointer;">&#8592; New analysis</button>
    </div>

    <!-- Tab bar -->
    <div style="display:flex;gap:6px;margin-bottom:16px;flex-wrap:wrap;">
      <button class="fr-tab-btn active" onclick="frTab('memo',this)">&#128196; Memo</button>
      <button class="fr-tab-btn active" onclick="frTab('advisor',this)">&#128100; Advisor</button>
      <button class="fr-tab-btn" onclick="frShowUpgrade()" title="Upgrade to unlock">&#128274; Stress Test</button>
      <button class="fr-tab-btn" onclick="frShowUpgrade()" title="Upgrade to unlock">&#128274; Pre-Mortem</button>
      <button class="fr-tab-btn" onclick="frShowUpgrade()" title="Upgrade to unlock">&#128274; LP Report</button>
    </div>

    <!-- Free tabs -->
    <div id="fr-tab-memo" class="fr-result-wrap">
      <div class="fr-card" id="fr-memo-content"></div>
    </div>
    <div id="fr-tab-advisor" class="fr-result-wrap" style="display:none;">
      <div class="fr-card" id="fr-advisor-content"></div>
    </div>

    <!-- Locked preview + upgrade CTA -->
    <div class="fr-result-wrap" style="margin-top:8px;">
      <div class="fr-card fr-locked" id="fr-locked-preview">
        <div style="font-size:11px;color:var(--text-muted);margin-bottom:8px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;">Stress Test Results</div>
        <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;">
          <div style="background:rgba(63,185,80,.06);border:1px solid rgba(63,185,80,.15);border-radius:6px;padding:10px;text-align:center;"><div style="font-size:18px;font-weight:700;color:#3fb950;">22.4%</div><div style="font-size:10px;color:var(--text-muted);">Bull IRR</div></div>
          <div style="background:rgba(232,160,32,.06);border:1px solid rgba(232,160,32,.15);border-radius:6px;padding:10px;text-align:center;"><div style="font-size:18px;font-weight:700;color:var(--accent);">17.8%</div><div style="font-size:10px;color:var(--text-muted);">Base IRR</div></div>
          <div style="background:rgba(248,81,73,.06);border:1px solid rgba(248,81,73,.15);border-radius:6px;padding:10px;text-align:center;"><div style="font-size:18px;font-weight:700;color:#f85149;">9.2%</div><div style="font-size:10px;color:var(--text-muted);">Bear IRR</div></div>
        </div>
      </div>
      <div class="fr-blur-overlay" id="fr-upgrade-overlay">
        <div class="fr-upgrade-cta" id="fr-upgrade-cta">
          <div style="font-size:20px;margin-bottom:6px;">&#128274;</div>
          <div style="font-family:var(--font-display);font-style:italic;font-size:1.2rem;margin-bottom:8px;color:var(--text-primary);">Your LPs expect this rigor</div>
          <div style="font-size:12px;color:var(--text-secondary);margin-bottom:16px;line-height:1.6;">Unlock Stress Test, Pre-Mortem, Bias Kill Shot, and shareable LP portal. The full analysis that closes capital.</div>
          <a href="/pricing" style="display:block;padding:12px;background:var(--accent);color:#080b10;border-radius:7px;text-decoration:none;font-weight:700;font-size:13px;letter-spacing:.02em;margin-bottom:8px;">Get Full Analysis — $697/mo</a>
          <a href="/app" style="display:block;font-size:11px;color:var(--text-muted);text-decoration:none;">Try full app free &rarr;</a>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
let _frJobId = null;

async function frSubmit(){
  const email = (document.getElementById('fr-email').value||'').trim();
  const om = (document.getElementById('fr-om').value||'').trim();
  if(!email || !om){ alert('Please enter your email and deal text.'); return; }
  const btn = document.getElementById('fr-submit-btn');
  const status = document.getElementById('fr-status');
  btn.disabled = true;
  btn.textContent = '&#9889; Analyzing...';
  status.textContent = 'Running adversarial analysis — usually under 90 seconds...';
  // Save email to waitlist
  try {
    await fetch('/api/waitlist', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({email, source:'free_review'})});
  } catch(e){}
  // Start analysis
  try {
    const r = await fetch('/analyze', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({om_text:om, email})});
    const d = await r.json();
    if(!d.job_id){throw new Error(d.error||'Analysis failed');}
    _frJobId = d.job_id;
    status.textContent = 'Analysis running... checking results...';
    await frPoll(_frJobId);
  } catch(err){
    status.textContent = 'Error: ' + (err.message||'Analysis failed. Please try again.');
    btn.disabled = false;
    btn.innerHTML = '&#9889; Run Free Analysis';
  }
}

async function frPoll(jid){
  const status = document.getElementById('fr-status');
  for(let i=0;i<60;i++){
    await new Promise(r=>setTimeout(r,3000));
    try{
      const r=await fetch('/stream/'+jid);
      const text=await r.text();
      const lines=text.split('\\n').filter(l=>l.startsWith('data:'));
      if(!lines.length)continue;
      const last=JSON.parse(lines[lines.length-1].slice(5));
      if(last.status==='done'){
        frRenderResults(last);
        return;
      }
      if(last.status==='error'){throw new Error(last.error||'Analysis error');}
      if(last.progress)status.textContent='Step '+last.progress+'...';
    }catch(e){if(e.message&&e.message.includes('Analysis'))throw e;}
  }
  throw new Error('Analysis timed out. Please try again.');
}

function frRenderResults(data){
  document.getElementById('fr-gate-section').style.display='none';
  document.getElementById('fr-results-section').style.display='block';
  // Verdict
  const memo = data.memo||'';
  const mu = memo.toUpperCase();
  let vt='CONDITIONAL', vc='#d29922', vbg='rgba(210,153,34,.08)';
  if(mu.includes('NO-GO')){vt='NO-GO';vc='#f85149';vbg='rgba(248,81,73,.08)';}
  else if(/\\bGO\\b/.test(mu)&&!mu.includes('CONDITIONAL')){vt='GO';vc='#3fb950';vbg='rgba(63,185,80,.08)';}
  const vEl=document.getElementById('fr-verdict-stamp');
  vEl.textContent=vt;
  vEl.style.color=vc;vEl.style.background=vbg;vEl.style.border='1px solid '+vc+'44';
  document.getElementById('fr-deal-name').textContent=(data.deal&&data.deal.deal_name)||'Deal Analysis';
  // Memo tab
  const memoEl=document.getElementById('fr-memo-content');
  memoEl.innerHTML='<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin-bottom:10px;">Investment Memo</div>'
    +'<div style="font-size:12px;color:var(--text-secondary);line-height:1.7;white-space:pre-wrap;">'+((memo||'').slice(0,1600))+(memo.length>1600?'\\n\\n[Full memo available in Professional]':'')+'</div>';
  // Advisor tab — show first advisor only
  const adv=(data.advisors&&data.advisors[0])||null;
  const advEl=document.getElementById('fr-advisor-content');
  if(adv){
    advEl.innerHTML='<div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin-bottom:8px;">'+((adv.name||'Advisor 1').slice(0,40))+'</div>'
      +'<div style="font-size:12px;color:var(--text-secondary);line-height:1.7;white-space:pre-wrap;">'+((adv.analysis||adv.text||'').slice(0,800))+((adv.analysis||adv.text||'').length>800?'\\n\\n<span style="color:var(--accent);">[4 more advisors available in Professional]</span>':'')+'</div>';
  } else {
    advEl.innerHTML='<div style="color:var(--text-muted);font-style:italic;font-size:12px;">Advisor analysis loading...</div>';
  }
}

function frTab(name, btn){
  document.getElementById('fr-tab-memo').style.display = name==='memo'?'block':'none';
  document.getElementById('fr-tab-advisor').style.display = name==='advisor'?'block':'none';
  document.querySelectorAll('.fr-tab-btn').forEach(b=>{
    if(['memo','advisor'].includes(name)) b.classList.remove('active');
  });
  if(btn) btn.classList.add('active');
}

function frShowUpgrade(){
  const el=document.getElementById('fr-upgrade-cta');
  if(el){el.style.transform='scale(1.03)';setTimeout(()=>el.style.transform='',300);}
  document.getElementById('fr-locked-preview').scrollIntoView({behavior:'smooth',block:'center'});
}

function frReset(){
  document.getElementById('fr-gate-section').style.display='block';
  document.getElementById('fr-results-section').style.display='none';
  document.getElementById('fr-status').textContent='';
  const btn=document.getElementById('fr-submit-btn');
  btn.disabled=false;btn.innerHTML='&#9889; Run Free Analysis';
  document.getElementById('fr-om').value='';
}
</script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>"""


MARKETS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearEye — Market Heat Map</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg-canvas:#080b10;--bg-surface:#0f1318;--bg-elevated:#161d26;--bg-overlay:#1e2733;--border-default:#1e2733;--border-emphasis:#2e3d4f;--text-primary:#f0ede8;--text-secondary:#8a9bb0;--text-muted:#3d4f63;--accent:#e8a020;--accent-dim:rgba(232,160,32,.09);--green:#3fb950;--red:#f85149;--amber:#e8a020;--r-md:10px;--t:140ms ease;--font:'DM Sans',-apple-system,sans-serif;--font-display:'DM Serif Display',Georgia,serif;--mono:'IBM Plex Mono','SF Mono',Consolas,monospace;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
body{background-color:var(--bg-canvas);background-image:radial-gradient(ellipse 80% 45% at 50% -5%,rgba(232,160,32,.07) 0%,transparent 65%),url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='200' height='200'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3CfeColorMatrix type='saturate' values='0'/%3E%3C/filter%3E%3Crect width='200' height='200' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");background-size:100% 100%,200px 200px;background-attachment:fixed;color:var(--text-primary);font-family:var(--font);font-size:14px;-webkit-font-smoothing:antialiased;letter-spacing:-0.008em;}
::-webkit-scrollbar{width:4px;}::-webkit-scrollbar-track{background:transparent;}::-webkit-scrollbar-thumb{background:rgba(232,160,32,.25);border-radius:4px;}::-webkit-scrollbar-thumb:hover{background:rgba(232,160,32,.5);}
.ce-nav{height:56px;background:rgba(8,11,16,.9);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,.06);display:flex;align-items:center;padding:0 20px;position:sticky;top:0;z-index:100;}
.ce-brand{font-family:var(--font-display);font-size:1.2rem;font-weight:400;color:var(--accent);text-decoration:none;letter-spacing:-0.01em;}
.nav-pill{font-size:12px;color:var(--text-secondary);text-decoration:none;padding:5px 10px;border-radius:6px;transition:color var(--t),background var(--t);}
.nav-pill:hover{color:var(--text-primary);background:var(--bg-overlay);}
@keyframes mktCardEnter{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
.mkt-card{background:rgba(15,19,24,.72);border:1px solid var(--border-default);border-left:3px solid transparent;border-radius:var(--r-md);padding:14px 16px;margin-bottom:8px;transition:border-color var(--t),transform var(--t);animation:mktCardEnter .35s cubic-bezier(.22,1,.36,1) both;backdrop-filter:blur(4px);}
.mkt-card:hover{border-color:rgba(232,160,32,.35);transform:translateY(-1px);}
.mkt-card.active-card{border-left-color:var(--accent);background:rgba(232,160,32,.04);}
.score-bar{height:4px;border-radius:3px;background:rgba(255,255,255,.07);margin-top:8px;}
.score-fill{height:100%;border-radius:3px;transition:width 1s cubic-bezier(.22,1,.36,1);}
.trend-up{color:var(--green);}
.trend-down{color:var(--red);}
.trend-flat{color:var(--amber);}
.mkt-rank{font-family:var(--mono);font-size:10px;color:var(--text-muted);letter-spacing:.05em;margin-bottom:2px;}
.mkt-name{font-family:var(--font-display);font-style:italic;font-size:1.05rem;font-weight:400;letter-spacing:-0.01em;color:var(--text-primary);}
.mkt-score{font-family:var(--mono);font-size:1.6rem;font-weight:600;min-width:52px;letter-spacing:-0.03em;}
.mkt-metrics{font-size:11px;color:var(--text-secondary);margin-top:4px;font-family:var(--mono);letter-spacing:.01em;}
.mkt-card{cursor:pointer;}
/* ── #209: MSA detail side drawer ── */
.msa-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.5);backdrop-filter:blur(3px);z-index:200;opacity:0;pointer-events:none;transition:opacity .25s ease;}
.msa-backdrop.open{opacity:1;pointer-events:all;}
.msa-drawer{position:fixed;right:0;top:0;bottom:0;width:360px;background:var(--bg-surface);border-left:1px solid var(--border-default);border-top:3px solid var(--accent);z-index:201;display:flex;flex-direction:column;transform:translateX(100%);transition:transform .3s cubic-bezier(.22,1,.36,1);box-shadow:-6px 0 30px rgba(0,0,0,.5);}
.msa-drawer.open{transform:translateX(0);}
.msa-drawer-hdr{padding:16px 18px;border-bottom:1px solid var(--border-default);display:flex;align-items:flex-start;gap:12px;}
.msa-drawer-score{font-family:var(--mono);font-size:2.4rem;font-weight:600;letter-spacing:-0.04em;line-height:1;}
.msa-drawer-title{font-family:var(--font-display);font-style:italic;font-size:1.3rem;font-weight:400;letter-spacing:-0.01em;color:var(--text-primary);margin-bottom:2px;}
.msa-drawer-sub{font-size:10px;color:var(--text-muted);font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;}
.msa-close{margin-left:auto;background:none;border:none;color:var(--text-muted);font-size:18px;cursor:pointer;padding:2px 6px;border-radius:4px;transition:color .15s,background .15s;flex-shrink:0;}
.msa-close:hover{color:var(--text-primary);background:var(--bg-elevated);}
.msa-drawer-body{flex:1;overflow-y:auto;padding:18px;}
.msa-metric-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:20px;}
.msa-metric-box{background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:8px;padding:12px 14px;}
.msa-metric-label{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin-bottom:4px;}
.msa-metric-val{font-family:var(--mono);font-size:1.25rem;font-weight:600;letter-spacing:-0.03em;color:var(--text-primary);}
.msa-metric-sub{font-size:10px;color:var(--text-secondary);margin-top:2px;}
.msa-sparkline-wrap{background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:8px;padding:14px 16px;margin-bottom:20px;}
.msa-spark-label{font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--text-muted);margin-bottom:10px;}
.msa-verdict-row{display:flex;align-items:center;gap:10px;padding:12px 14px;border-radius:8px;border:1px solid rgba(232,160,32,.25);background:rgba(232,160,32,.05);margin-bottom:14px;}
.msa-verdict-icon{font-size:20px;}
.msa-verdict-text{font-size:11px;color:var(--text-secondary);line-height:1.4;}
.analyze-here-link{display:inline-flex;align-items:center;gap:6px;color:var(--accent);font-size:12px;font-family:var(--mono);text-decoration:none;padding:8px 14px;border:1px solid rgba(232,160,32,.25);border-radius:6px;transition:background var(--t),border-color var(--t);}
.analyze-here-link:hover{background:rgba(232,160,32,.06);border-color:rgba(232,160,32,.45);}
</style>
</head>
<body>
<nav class="ce-nav">
  <a class="ce-brand" href="/app">&#128065; ClearEye</a>
  <span style="font-size:11px;color:var(--text-muted);margin-left:16px;font-family:var(--mono);text-transform:uppercase;letter-spacing:.06em;">Market Heat Map</span>
  <a href="/app" class="nav-pill" style="margin-left:auto;color:var(--accent);">&#8592; Back to App</a>
</nav>
<div class="container py-4" style="max-width:860px;">
  <div style="margin-bottom:24px;">
    <h1 style="font-family:var(--font-display);font-style:italic;font-size:clamp(1.6rem,3vw,2.2rem);font-weight:400;letter-spacing:-0.02em;margin-bottom:6px;">Best Markets Right Now</h1>
    <p style="font-size:12px;color:var(--text-muted);font-family:var(--mono);">ClearEye Market Score (0–100) based on cap rate spread, rent momentum, and vacancy trend. Click any market for full detail.</p>
  </div>
  <div id="loading" style="text-align:center;padding:40px;color:var(--text-muted);">Loading market data...</div>
  <div id="markets-list" style="display:none;"></div>
</div>

<!-- #209: MSA detail side drawer -->
<div class="msa-backdrop" id="msa-backdrop" onclick="closeMsaDrawer()"></div>
<div class="msa-drawer" id="msa-drawer">
  <div class="msa-drawer-hdr">
    <div>
      <div class="msa-drawer-title" id="msa-d-name">—</div>
      <div class="msa-drawer-sub" id="msa-d-sub">MSA Detail</div>
    </div>
    <div class="msa-drawer-score" id="msa-d-score" style="color:var(--accent);">—</div>
    <button class="msa-close" onclick="closeMsaDrawer()">&#x2715;</button>
  </div>
  <div class="msa-drawer-body" id="msa-drawer-body">
    <!-- filled by openMsaDrawer() -->
  </div>
</div>

<script>
let _msaData = [];

function closeMsaDrawer(){
  document.getElementById('msa-drawer').classList.remove('open');
  document.getElementById('msa-backdrop').classList.remove('open');
  document.querySelectorAll('.mkt-card').forEach(c=>c.classList.remove('active-card'));
}
document.addEventListener('keydown', function(e){ if(e.key==='Escape') closeMsaDrawer(); });

function buildSparkline(points, color){
  if(!points||!points.length) return '<svg width="100%" height="40"><text x="50%" y="50%" text-anchor="middle" fill="#3d4f63" font-size="10">No trend data</text></svg>';
  const w=290, h=50;
  const mn=Math.min(...points), mx=Math.max(...points);
  const range=mx-mn||1;
  const pts=points.map((v,i)=>{
    const x=(i/(points.length-1||1))*w;
    const y=h-((v-mn)/range)*(h*0.8)-h*0.1;
    return x+','+y;
  });
  const area='M '+pts[0]+' '+pts.slice(1).map(p=>'L '+p).join(' ')+' L '+w+','+h+' L 0,'+h+' Z';
  const line='M '+pts[0]+' '+pts.slice(1).map(p=>'L '+p).join(' ');
  return '<svg viewBox="0 0 '+w+' '+h+'" width="100%" height="50" preserveAspectRatio="none">'
    +'<defs><linearGradient id="sg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="'+color+'" stop-opacity="0.28"/><stop offset="100%" stop-color="'+color+'" stop-opacity="0.02"/></linearGradient></defs>'
    +'<path d="'+area+'" fill="url(#sg)"/>'
    +'<path d="'+line+'" fill="none" stroke="'+color+'" stroke-width="1.8" stroke-linejoin="round" stroke-linecap="round"/>'
    +'</svg>';
}

function openMsaDrawer(m, rank){
  const sc = m.score||0;
  const fillColor = sc>=70?'#3fb950':sc>=50?'#e8a020':sc>=30?'#d29922':'#f85149';
  const trendLbl = m.trend==='up'?'&#9650; Trending Up':m.trend==='down'?'&#9660; Trending Down':'&#8212; Flat';
  const trendCls = m.trend==='up'?'trend-up':m.trend==='down'?'trend-down':'trend-flat';
  const srcLbl = m._source==='rentcast_live'?'&#9679; Live RentCast':'&#9679; Static Estimate';
  const srcColor = m._source==='rentcast_live'?'#3fb950':'#3d4f63';

  document.getElementById('msa-d-name').textContent = m.market||'—';
  document.getElementById('msa-d-sub').textContent = 'Rank #'+(rank+1)+' · MSA Detail';
  const scoreEl=document.getElementById('msa-d-score');
  scoreEl.textContent = sc;
  scoreEl.style.color = fillColor;

  // 6-metric grid
  const metrics=[
    {label:'Cap Rate', val:(m.cap_rate||'?')+'%', sub:'Current avg'},
    {label:'Rent Growth', val:(m.rent_growth||'?')+'%', sub:'YoY momentum'},
    {label:'Vacancy', val:(m.vacancy||'?')+'%', sub:'Market avg'},
    {label:'Score', val:sc+'/100', sub:'ClearEye composite'},
    {label:'Trend', val:trendLbl, sub:'Direction', color:m.trend==='up'?'#3fb950':m.trend==='down'?'#f85149':'#e8a020'},
    {label:'Source', val:srcLbl, sub:'Data freshness', color:srcColor},
  ];
  const metricGrid=metrics.map(metric=>{
    const valColor=metric.color||'var(--text-primary)';
    return '<div class="msa-metric-box">'
      +'<div class="msa-metric-label">'+metric.label+'</div>'
      +'<div class="msa-metric-val" style="color:'+valColor+';font-size:'+(metric.label==='Trend'||metric.label==='Source'?'11px':'1.25rem')+';">'+metric.val+'</div>'
      +'<div class="msa-metric-sub">'+metric.sub+'</div>'
      +'</div>';
  }).join('');

  // Sparkline: generate synthetic trend from rent_growth & score
  const rg=parseFloat(m.rent_growth||0);
  const sparkPoints=Array.from({length:8},(_,i)=>{
    const noise=(Math.sin(i*2.3+sc*0.1)*0.4+Math.cos(i*1.7)*0.3)*1.2;
    return Math.max(0, sc - (7-i)*(rg>0?-Math.abs(rg)*0.5:Math.abs(rg)*0.4) + noise);
  });
  const sparkColor = sc>=70?'#3fb950':sc>=50?'#e8a020':'#f85149';
  const sparkHtml = buildSparkline(sparkPoints, sparkColor);

  // Verdict recommendation
  let verdictIcon='&#128200;', verdictText='';
  if(sc>=70) verdictText='Strong fundamentals — this market scores in the top tier for cap rate spread and rent momentum. Consider accelerating deal sourcing here.';
  else if(sc>=50) verdictText='Solid market with positive indicators. Rent growth is trending favorably; monitor vacancy for continued improvement.';
  else if(sc>=30) verdictText='Mixed signals — cap rate compression or vacancy risk may offset rent momentum. Proceed with selective underwriting.';
  else verdictText='Below-average fundamentals currently. This market shows elevated vacancy or weak rent growth. Apply caution and stress-test assumptions.';

  document.getElementById('msa-drawer-body').innerHTML =
    '<div class="msa-verdict-row"><span class="msa-verdict-icon">'+verdictIcon+'</span><div class="msa-verdict-text">'+verdictText+'</div></div>'
    +'<div class="msa-metric-grid">'+metricGrid+'</div>'
    +'<div class="msa-sparkline-wrap"><div class="msa-spark-label">Score Trend (estimated 8-period)</div>'+sparkHtml+'</div>'
    +'<div style="margin-top:8px;"><a href="/app" class="analyze-here-link">&#128269; Analyze a deal in this market &#8599;</a></div>';

  document.getElementById('msa-drawer').classList.add('open');
  document.getElementById('msa-backdrop').classList.add('open');
}

async function loadMarkets(){
  const r=await fetch('/api/market-scores');
  _msaData=await r.json();
  document.getElementById('loading').style.display='none';
  const el=document.getElementById('markets-list');
  el.style.display='block';
  el.innerHTML=_msaData.map((m,i)=>{
    const sc=m.score||0;
    const fillColor=sc>=70?'#3fb950':sc>=50?'#e8a020':sc>=30?'#d29922':'#f85149';
    const trend=m.trend==='up'?'&#9650; ':'&#9660; ';
    const trendCls=m.trend==='up'?'trend-up':m.trend==='down'?'trend-down':'trend-flat';
    const src=m._source==='rentcast_live'?'<span style="color:#3fb950;">&#9679; Live</span>':'<span style="color:var(--text-muted);">&#9679; Static</span>';
    const delay=i*55;
    return `<div class="mkt-card" style="animation-delay:${delay}ms" data-idx="${i}" onclick="document.querySelectorAll('.mkt-card').forEach(c=>c.classList.remove('active-card'));this.classList.add('active-card');openMsaDrawer(_msaData[${i}],${i});">
      <div style="display:flex;align-items:center;gap:14px;">
        <span class="mkt-score" style="color:${fillColor};">${sc}</span>
        <div style="flex:1;">
          <div class="mkt-rank">#${i+1}</div>
          <div class="mkt-name">${m.market}</div>
          <div class="mkt-metrics">Cap: ${m.cap_rate||'?'}% &nbsp;·&nbsp; Rent Growth: <span class="${trendCls}">${trend}${m.rent_growth||'?'}%</span> &nbsp;·&nbsp; Vac: ${m.vacancy||'?'}% &nbsp;·&nbsp; ${src}</div>
          <div class="score-bar"><div class="score-fill" style="width:${sc}%;background:${fillColor};"></div></div>
        </div>
        <span style="color:var(--text-muted);font-size:12px;flex-shrink:0;">&#8250;</span>
      </div>
    </div>`;
  }).join('');
}
loadMarkets();
</script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>"""


PORTFOLIO_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Portfolio Analytics — ClearEye</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg-canvas:#080b10;--bg-surface:#0f1318;--bg-elevated:#161d26;--border-default:#1e2733;--border-emphasis:#2e3d4f;--text-primary:#f0ede8;--text-secondary:#8a9bb0;--text-muted:#3d4f63;--accent:#e8a020;--green:#3fb950;--red:#f85149;--amber:#e8a020;--mono:'IBM Plex Mono','SF Mono',Consolas,monospace;--font:'DM Sans',-apple-system,sans-serif;--font-display:'DM Serif Display',Georgia,serif;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg-canvas);color:var(--text-primary);font-family:var(--font);font-size:13.5px;line-height:1.55;-webkit-font-smoothing:antialiased;}
.ce-nav{height:56px;background:rgba(9,13,18,.88);backdrop-filter:blur(20px);border-bottom:1px solid rgba(255,255,255,.06);display:flex;align-items:center;padding:0 20px;gap:4px;position:sticky;top:0;z-index:100;}
.ce-brand{font-family:var(--font-display);font-size:1.2rem;color:var(--accent);text-decoration:none;margin-right:8px;}
.nav-pill{font-size:12px;color:var(--text-secondary);text-decoration:none;padding:5px 10px;border-radius:5px;}
.nav-pill:hover{color:var(--text-primary);background:rgba(255,255,255,.06);}
.nav-pill.active{color:var(--accent);background:rgba(232,160,32,.08);}
.pf-main{max-width:1100px;margin:0 auto;padding:28px 20px;}
.pf-title{font-family:var(--font-display);font-style:italic;font-size:1.8rem;color:var(--text-primary);margin-bottom:4px;}
.pf-sub{font-size:12px;color:var(--text-muted);font-family:var(--mono);margin-bottom:28px;}
.stat-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:28px;}
.stat-card{background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:9px;padding:16px 18px;}
.stat-val{font-family:var(--mono);font-size:1.6rem;font-weight:700;color:var(--text-primary);margin-bottom:2px;}
.stat-lbl{font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;}
.section-hdr{font-size:11px;font-weight:700;color:var(--text-muted);text-transform:uppercase;letter-spacing:.07em;font-family:var(--mono);margin-bottom:10px;padding-bottom:6px;border-bottom:1px solid var(--border-default);}
.grid-2{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:24px;}
.panel{background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:9px;padding:18px;}
.verdict-row{display:flex;align-items:center;justify-content:space-between;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.04);}
.verdict-row:last-child{border-bottom:none;}
.pattern-row{display:flex;align-items:center;justify-content:space-between;padding:5px 0;border-bottom:1px solid rgba(255,255,255,.04);font-size:12px;}
.pattern-row:last-child{border-bottom:none;}
.bar-bg{background:rgba(255,255,255,.06);border-radius:2px;height:5px;flex:1;margin:0 10px;overflow:hidden;}
.bar-fill{height:100%;border-radius:2px;background:var(--accent);transition:width .6s ease;}
.timeline-wrap{background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:9px;padding:18px;margin-bottom:24px;}
.tl-dot{position:absolute;width:8px;height:8px;border-radius:50%;cursor:pointer;transform:translate(-50%,-50%);}
.tl-dot:hover::after{content:attr(data-tip);position:absolute;left:10px;top:-20px;background:#1e2733;border:1px solid var(--border-emphasis);border-radius:4px;padding:3px 7px;font-size:10px;font-family:var(--mono);white-space:nowrap;color:var(--text-primary);z-index:10;}
.loading{color:var(--text-muted);font-size:12px;padding:40px;text-align:center;}
.deal-table{width:100%;border-collapse:collapse;font-size:12px;}
.deal-table th{font-size:10px;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);font-weight:500;padding:7px 10px;text-align:left;border-bottom:1px solid var(--border-default);}
.deal-table td{padding:8px 10px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:top;}
.deal-table tr:hover td{background:rgba(255,255,255,.02);}
.v-go{color:var(--green);}.v-nogo{color:var(--red);}.v-cond{color:var(--amber);}
.accuracy-panel{background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:9px;padding:18px;margin-bottom:24px;}
/* #255: IRR Accuracy Widget */
.irr-acc-panel{background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:9px;padding:18px;margin-bottom:24px;}
.irr-acc-hdr{font-family:var(--mono);font-size:9px;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:var(--text-muted);margin-bottom:12px;}
.irr-acc-canvas-wrap{position:relative;height:240px;width:100%;}
.irr-acc-mae{margin-top:10px;font-family:var(--mono);font-size:11px;color:var(--text-muted);}
.irr-acc-mae strong{color:var(--amber);}
.irr-acc-empty{color:var(--text-muted);font-size:12px;padding:24px 0;text-align:center;}
</style>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
</head>
<body>
<nav class="ce-nav">
  <a href="/app" class="ce-brand">ClearEye</a>
  <a href="/markets" class="nav-pill">Markets</a>
  <a href="/pipeline" class="nav-pill">Pipeline</a>
  <a href="/portfolio" class="nav-pill active">Portfolio</a>
  <a href="/deals" class="nav-pill">Deal History</a>
</nav>

<div class="pf-main">
  <div class="pf-title">Portfolio Intelligence</div>
  <div class="pf-sub" id="pf-sub">Loading deals...</div>

  <!-- Summary stats strip -->
  <div class="stat-strip" id="pf-stats">
    <div class="stat-card"><div class="stat-val loading" style="font-size:1rem;padding:0;">—</div><div class="stat-lbl">Loading</div></div>
  </div>

  <!-- Verdict breakdown + Top Markets -->
  <div class="grid-2" id="pf-grid">
    <div class="panel" id="pf-verdicts"><div class="section-hdr">Verdict Breakdown</div><div style="color:var(--text-muted);font-size:12px;">Loading...</div></div>
    <div class="panel" id="pf-markets"><div class="section-hdr">Top Markets</div><div style="color:var(--text-muted);font-size:12px;">Loading...</div></div>
  </div>

  <!-- Timeline scatter -->
  <div class="timeline-wrap" id="pf-timeline-wrap">
    <div class="section-hdr">IRR Timeline <span style="font-size:9px;font-weight:400;opacity:.5;">— dot = deal, color = verdict</span></div>
    <div id="pf-timeline" style="position:relative;height:140px;overflow:hidden;"></div>
    <div style="display:flex;gap:14px;margin-top:8px;">
      <span style="font-size:10px;color:var(--green);">&#9679; GO</span>
      <span style="font-size:10px;color:var(--red);">&#9679; NO-GO</span>
      <span style="font-size:10px;color:var(--amber);">&#9679; CONDITIONAL</span>
    </div>
  </div>

  <!-- Asset class + bias flags -->
  <div class="grid-2" style="margin-bottom:24px;">
    <div class="panel" id="pf-assets"><div class="section-hdr">Asset Classes</div><div style="color:var(--text-muted);font-size:12px;">Loading...</div></div>
    <div class="panel" id="pf-bias"><div class="section-hdr">Avg Bias Flags / Deal</div><div style="color:var(--text-muted);font-size:12px;">Loading...</div></div>
  </div>

  <!-- #255: IRR Accuracy tracker widget -->
  <div class="irr-acc-panel" id="pf-irr-accuracy" style="display:none;">
    <div class="irr-acc-hdr">IRR Accuracy — Projected vs Actual</div>
    <div class="irr-acc-canvas-wrap"><canvas id="irr-accuracy-chart"></canvas></div>
    <div class="irr-acc-mae" id="irr-acc-mae"></div>
  </div>

  <!-- Deal table -->
  <div class="panel" style="margin-bottom:32px;">
    <div class="section-hdr">All Analyzed Deals</div>
    <div style="overflow-x:auto;"><table class="deal-table" id="pf-deal-table"><thead><tr><th>Deal</th><th>Verdict</th><th>IRR</th><th>Cap Rate</th><th>Market</th><th>Asset</th><th>Date</th></tr></thead><tbody id="pf-deal-tbody"><tr><td colspan="7" style="color:var(--text-muted);text-align:center;">Loading...</td></tr></tbody></table></div>
  </div>
</div>

<script>
async function loadPortfolio(){
  try{
    const r=await fetch('/api/portfolio/stats');
    const data=await r.json();
    const deals=data.deals||[];
    if(!deals.length){
      document.getElementById('pf-sub').textContent='No completed analyses yet. Run your first deal analysis to populate portfolio.';
      document.getElementById('pf-stats').innerHTML='<div style="color:var(--text-muted);font-size:12px;padding:20px 0;">No data</div>';
      return;
    }
    document.getElementById('pf-sub').textContent=deals.length+' deal'+(deals.length===1?'':'s')+' analyzed · Last updated '+new Date().toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'});
    renderStats(deals);
    renderVerdicts(deals);
    renderMarkets(deals);
    renderTimeline(deals);
    renderAssets(deals);
    renderBias(deals);
    renderTable(deals);
    renderIrrAccuracy(deals);  // #255
  }catch(e){
    document.getElementById('pf-sub').textContent='Error loading portfolio: '+e.message;
  }
}

function fmtNum(n){if(!n&&n!==0)return '—';return n>=1e6?'$'+(n/1e6).toFixed(1)+'M':n>=1e3?'$'+(n/1e3).toFixed(0)+'K':n.toString();}
function fmtIrr(v){if(v===null||v===undefined)return '—';return v.toFixed(1)+'%';}
function esc(s){const d=document.createElement('div');d.textContent=s||'';return d.innerHTML;}

function renderStats(deals){
  const goD=deals.filter(d=>d.verdict==='GO');
  const nogoD=deals.filter(d=>d.verdict==='NO-GO');
  const condD=deals.filter(d=>d.verdict==='CONDITIONAL');
  const irrs=deals.filter(d=>d.irr!==null&&d.irr!==undefined).map(d=>d.irr);
  const avgIrr=irrs.length?irrs.reduce((a,b)=>a+b,0)/irrs.length:null;
  const caps=deals.filter(d=>d.cap_rate&&parseFloat(d.cap_rate)>0).map(d=>parseFloat(d.cap_rate));
  const avgCap=caps.length?caps.reduce((a,b)=>a+b,0)/caps.length:null;
  const prices=deals.filter(d=>d.asking_price&&d.asking_price>0).map(d=>d.asking_price);
  const totalEquity=prices.length?prices.reduce((a,b)=>a+b,0)*0.35:0;
  const avgConf=deals.filter(d=>d.confidence>0).map(d=>d.confidence);
  const avgConfVal=avgConf.length?Math.round(avgConf.reduce((a,b)=>a+b,0)/avgConf.length):null;
  document.getElementById('pf-stats').innerHTML=[
    {val:deals.length,lbl:'Total Deals',color:'var(--text-primary)'},
    {val:avgIrr!==null?avgIrr.toFixed(1)+'%':'—',lbl:'Avg Proj. IRR',color:avgIrr>=12?'var(--green)':avgIrr>=8?'var(--amber)':'var(--red)'},
    {val:avgCap!==null?avgCap.toFixed(2)+'%':'—',lbl:'Avg Cap Rate',color:'var(--text-primary)'},
    {val:totalEquity>0?fmtNum(totalEquity):'—',lbl:'Total Deal Vol.',color:'var(--accent)'},
    {val:goD.length+' / '+condD.length+' / '+nogoD.length,lbl:'GO / COND / NO-GO',color:'var(--text-primary)'},
    {val:avgConfVal!==null?avgConfVal+'%':'—',lbl:'Avg Confidence',color:'var(--text-primary)'},
  ].map(s=>'<div class="stat-card"><div class="stat-val" style="color:'+s.color+';font-size:1.3rem;">'+s.val+'</div><div class="stat-lbl">'+s.lbl+'</div></div>').join('');
}

function renderVerdicts(deals){
  const counts={GO:0,'NO-GO':0,CONDITIONAL:0};
  deals.forEach(d=>{const v=d.verdict||'CONDITIONAL';if(counts[v]!==undefined)counts[v]++;else counts.CONDITIONAL++;});
  const total=deals.length;
  const el=document.getElementById('pf-verdicts');
  el.innerHTML='<div class="section-hdr">Verdict Breakdown</div>'+
    ['GO','CONDITIONAL','NO-GO'].map(v=>{
      const n=counts[v];const pct=total?Math.round(n/total*100):0;
      const c=v==='GO'?'var(--green)':v==='NO-GO'?'var(--red)':'var(--amber)';
      return '<div class="verdict-row"><span style="font-size:12px;font-weight:600;color:'+c+';width:90px;">'+v+'</span><div class="bar-bg"><div class="bar-fill" style="width:'+pct+'%;background:'+c+';"></div></div><span style="font-family:var(--mono);font-size:12px;font-weight:600;color:'+c+';">'+n+'<span style="font-size:10px;color:var(--text-muted);"> ('+pct+'%)</span></span></div>';
    }).join('');
}

function renderMarkets(deals){
  const mkt={};
  deals.forEach(d=>{const m=(d.market||'Unknown').split(',')[0].trim()||'Unknown';mkt[m]=(mkt[m]||0)+1;});
  const sorted=Object.entries(mkt).sort((a,b)=>b[1]-a[1]).slice(0,6);
  const max=sorted[0]?sorted[0][1]:1;
  const el=document.getElementById('pf-markets');
  el.innerHTML='<div class="section-hdr">Top Markets</div>'+sorted.map(([m,n])=>{
    const pct=Math.round(n/max*100);
    return '<div class="pattern-row"><span style="flex:0 0 130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">'+esc(m)+'</span><div class="bar-bg"><div class="bar-fill" style="width:'+pct+'%;"></div></div><span style="font-family:var(--mono);font-size:11px;min-width:24px;text-align:right;">'+n+'</span></div>';
  }).join('')+'<div style="font-size:10px;color:var(--text-muted);margin-top:8px;">'+Object.keys(mkt).length+' unique markets</div>';
}

function renderAssets(deals){
  const cls={};
  deals.forEach(d=>{const a=(d.asset_class||'Unknown').trim()||'Unknown';cls[a]=(cls[a]||0)+1;});
  const sorted=Object.entries(cls).sort((a,b)=>b[1]-a[1]).slice(0,6);
  const max=sorted[0]?sorted[0][1]:1;
  const el=document.getElementById('pf-assets');
  el.innerHTML='<div class="section-hdr">Asset Classes</div>'+sorted.map(([a,n])=>{
    const pct=Math.round(n/max*100);
    return '<div class="pattern-row"><span style="flex:0 0 130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">'+esc(a)+'</span><div class="bar-bg"><div class="bar-fill" style="width:'+pct+'%;"></div></div><span style="font-family:var(--mono);font-size:11px;min-width:24px;text-align:right;">'+n+'</span></div>';
  }).join('');
}

function renderBias(deals){
  const withFlags=deals.filter(d=>d.bias_flags>0);
  const avgFlags=withFlags.length?withFlags.reduce((a,b)=>a+b.bias_flags,0)/deals.length:0;
  const high=deals.filter(d=>d.bias_flags>=3).length;
  const clean=deals.filter(d=>d.bias_flags===0).length;
  const el=document.getElementById('pf-bias');
  el.innerHTML='<div class="section-hdr">Avg Bias Flags / Deal</div>'
    +'<div style="font-family:var(--mono);font-size:2rem;font-weight:700;color:'+(avgFlags>=2?'var(--red)':avgFlags>=1?'var(--amber)':'var(--green)')+';">'+avgFlags.toFixed(1)+'</div>'
    +'<div style="font-size:11px;color:var(--text-muted);margin-top:8px;">'
    +'<span style="color:var(--red);">'+high+' deal'+(high!==1?'s':'')+' with 3+ flags</span><br>'
    +'<span style="color:var(--green);">'+clean+' deal'+(clean!==1?'s':'')+' bias-clean</span>'
    +'</div>';
}

function renderTimeline(deals){
  const el=document.getElementById('pf-timeline');
  const withIrr=deals.filter(d=>d.irr!==null&&d.irr!==undefined&&d.created_at).sort((a,b)=>a.created_at.localeCompare(b.created_at));
  if(!withIrr.length){el.innerHTML='<div style="color:var(--text-muted);font-size:12px;padding:20px;">No IRR data available.</div>';return;}
  const minIrr=Math.min(...withIrr.map(d=>d.irr));
  const maxIrr=Math.max(...withIrr.map(d=>d.irr));
  const irrRange=Math.max(maxIrr-minIrr,2);
  const W=el.parentElement.offsetWidth-36||700;
  const H=120;
  const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');
  svg.setAttribute('width',W);svg.setAttribute('height',H);svg.style.overflow='visible';
  // axis labels
  [0,25,50,75,100].forEach(pct=>{
    const irr=minIrr+irrRange*(1-pct/100);
    const y=H*pct/100;
    const line=document.createElementNS('http://www.w3.org/2000/svg','line');
    line.setAttribute('x1',0);line.setAttribute('x2',W);line.setAttribute('y1',y);line.setAttribute('y2',y);
    line.setAttribute('stroke','rgba(255,255,255,.04)');line.setAttribute('stroke-width','1');
    svg.appendChild(line);
    const lbl=document.createElementNS('http://www.w3.org/2000/svg','text');
    lbl.setAttribute('x',2);lbl.setAttribute('y',y-2);lbl.setAttribute('font-size','9');lbl.setAttribute('fill','rgba(255,255,255,.25)');lbl.setAttribute('font-family','IBM Plex Mono,monospace');
    lbl.textContent=irr.toFixed(0)+'%';
    svg.appendChild(lbl);
  });
  withIrr.forEach((d,i)=>{
    const xPct=withIrr.length>1?i/(withIrr.length-1):0.5;
    const x=20+xPct*(W-40);
    const yPct=(d.irr-minIrr)/irrRange;
    const y=H-(yPct*(H-20))-10;
    const v=d.verdict||'CONDITIONAL';
    const c=v==='GO'?'#3fb950':v==='NO-GO'?'#f85149':'#e8a020';
    const dot=document.createElementNS('http://www.w3.org/2000/svg','circle');
    dot.setAttribute('cx',x);dot.setAttribute('cy',y);dot.setAttribute('r',5);
    dot.setAttribute('fill',c);dot.setAttribute('opacity','0.85');
    dot.setAttribute('stroke',c);dot.setAttribute('stroke-width','1');
    dot.setAttribute('stroke-opacity','0.4');
    dot.style.cursor='pointer';
    dot.setAttribute('title',(d.deal_name||'Deal')+' · IRR '+d.irr.toFixed(1)+'%');
    svg.appendChild(dot);
  });
  el.innerHTML='';el.appendChild(svg);
}

// #255: IRR Accuracy tracker — scatter plot projected vs actual
let _irrChart=null;
function renderIrrAccuracy(deals){
  const panel=document.getElementById('pf-irr-accuracy');
  const maeEl=document.getElementById('irr-acc-mae');
  const canvas=document.getElementById('irr-accuracy-chart');
  if(!panel||!canvas)return;
  // Filter deals with both projected and actual IRR
  const pairs=deals.filter(function(d){return d.irr!=null&&d.actual_irr!=null;});
  if(pairs.length<1){
    // No outcomes yet — show empty state
    panel.style.display='block';
    canvas.parentElement.innerHTML='<div class="irr-acc-empty">No outcome data yet.<br>Record actual IRR on Closed pipeline deals to populate this chart.</div>';
    if(maeEl)maeEl.innerHTML='Add outcomes via Pipeline &rarr; Outcome on closed deals.';
    return;
  }
  panel.style.display='block';
  // Compute MAE in basis points
  const errors=pairs.map(function(d){return Math.abs(d.irr-d.actual_irr);});
  const mae=errors.reduce(function(a,b){return a+b;},0)/errors.length;
  const maeBp=Math.round(mae*100);
  if(maeEl){
    maeEl.innerHTML='Mean Absolute Error: <strong>'+maeBp+' bps ('+mae.toFixed(1)+'%)</strong> across '+pairs.length+' deal'+(pairs.length===1?'':'s')+' with recorded outcomes.';
  }
  // Range for perfect-prediction diagonal
  const allVals=pairs.flatMap(function(d){return[d.irr,d.actual_irr];});
  const minV=Math.min.apply(null,allVals)-2;
  const maxV=Math.max.apply(null,allVals)+2;
  // Build scatter data
  const scatterData=pairs.map(function(d){return{x:d.irr,y:d.actual_irr,label:d.deal_name};});
  const diagData=[{x:minV,y:minV},{x:maxV,y:maxV}];
  if(_irrChart){_irrChart.destroy();_irrChart=null;}
  _irrChart=new Chart(canvas,{
    type:'scatter',
    data:{
      datasets:[
        {label:'Deals',data:scatterData,backgroundColor:'rgba(232,160,32,.75)',pointRadius:7,pointHoverRadius:9},
        {label:'Perfect prediction',data:diagData,type:'line',borderColor:'rgba(255,255,255,.12)',borderWidth:1,borderDash:[4,4],pointRadius:0,fill:false}
      ]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{
        legend:{display:false},
        tooltip:{callbacks:{label:function(ctx){
          if(ctx.datasetIndex===0){const d=pairs[ctx.dataIndex];return(d.deal_name||'Deal')+': proj '+d.irr.toFixed(1)+'% → actual '+d.actual_irr.toFixed(1)+'%';}
          return null;
        }}}
      },
      scales:{
        x:{title:{display:true,text:'Projected IRR (%)',color:'rgba(255,255,255,.35)',font:{size:10}},
           ticks:{color:'rgba(255,255,255,.35)',font:{size:9}},
           grid:{color:'rgba(255,255,255,.05)'},
           min:minV,max:maxV},
        y:{title:{display:true,text:'Actual IRR (%)',color:'rgba(255,255,255,.35)',font:{size:10}},
           ticks:{color:'rgba(255,255,255,.35)',font:{size:9}},
           grid:{color:'rgba(255,255,255,.05)'},
           min:minV,max:maxV}
      }
    }
  });
}

function renderTable(deals){
  const tbody=document.getElementById('pf-deal-tbody');
  if(!deals.length){tbody.innerHTML='<tr><td colspan="7" style="color:var(--text-muted);text-align:center;">No deals</td></tr>';return;}
  tbody.innerHTML=deals.slice(0,50).map(d=>{
    const v=d.verdict||'CONDITIONAL';
    const vcls=v==='GO'?'v-go':v==='NO-GO'?'v-nogo':'v-cond';
    return '<tr>'
      +'<td><a href="/report/'+esc(d.id)+'" style="color:var(--accent);text-decoration:none;">'+esc(d.deal_name||'Unknown')+'</a></td>'
      +'<td class="'+vcls+'" style="font-family:var(--mono);font-size:11px;font-weight:700;">'+esc(v)+'</td>'
      +'<td style="font-family:var(--mono);">'+fmtIrr(d.irr)+'</td>'
      +'<td style="font-family:var(--mono);">'+(d.cap_rate?parseFloat(d.cap_rate).toFixed(2)+'%':'—')+'</td>'
      +'<td>'+esc(d.market||'—')+'</td>'
      +'<td>'+esc(d.asset_class||'—')+'</td>'
      +'<td style="font-family:var(--mono);font-size:11px;color:var(--text-muted);">'+esc(d.created_at||'—')+'</td>'
      +'</tr>';
  }).join('');
}

loadPortfolio();
window.addEventListener('resize',()=>{
  const el=document.getElementById('pf-timeline');
  if(el&&el._deals)renderTimeline(el._deals);
});

// #225: Accuracy panel — load outcomes for pipeline deals
async function loadAccuracyPanel(){
  try{
    const r=await fetch('/api/pipeline?limit=200');
    const data=await r.json();
    const pDeals=data.deals||[];
    if(!pDeals.length)return;
    const closedDeals=pDeals.filter(d=>d.stage==='Closed'||d.stage==='Passed');
    if(!closedDeals.length)return;
    // Fetch outcomes for each closed deal
    const results=await Promise.all(closedDeals.map(d=>
      fetch('/api/pipeline/'+d.id+'/outcome').then(r=>r.json()).catch(()=>({outcome:null}))
    ));
    const pairs=closedDeals.map((d,i)=>({deal:d,outcome:results[i].outcome})).filter(p=>p.outcome&&p.outcome.actual_irr!=null);
    if(!pairs.length)return;
    // Render accuracy panel
    const container=document.querySelector('.pf-main');
    if(!container)return;
    const panel=document.createElement('div');
    panel.className='accuracy-panel';
    const rows=pairs.map(p=>{
      const proj=p.deal.job_id?null:null; // projected IRR not easily available here
      const actual=p.outcome.actual_irr;
      const actEm=p.outcome.actual_equity_multiple;
      return '<tr>'
        +'<td><a href="/pipeline" style="color:var(--accent);text-decoration:none;">'+esc(p.deal.deal_name||'Deal')+'</a></td>'
        +'<td style="font-family:var(--mono);color:#3fb950;">'+actual.toFixed(1)+'%</td>'
        +'<td style="font-family:var(--mono);">'+(actEm!=null?actEm.toFixed(2)+'x':'—')+'</td>'
        +'<td style="font-family:var(--mono);font-size:11px;color:var(--text-muted);">'+(p.outcome.closed_date||'—')+'</td>'
        +'</tr>';
    }).join('');
    panel.innerHTML='<div class="section-hdr" style="margin-bottom:12px;">Deal Outcomes &amp; Accuracy</div>'
      +'<table style="width:100%;border-collapse:collapse;font-size:12px;">'
      +'<thead><tr><th style="text-align:left;font-size:10px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);padding:6px 8px;border-bottom:1px solid var(--border-default);">Deal</th>'
      +'<th style="text-align:left;font-size:10px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);padding:6px 8px;border-bottom:1px solid var(--border-default);">Actual IRR</th>'
      +'<th style="text-align:left;font-size:10px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);padding:6px 8px;border-bottom:1px solid var(--border-default);">Equity Multiple</th>'
      +'<th style="text-align:left;font-size:10px;font-weight:600;color:var(--text-muted);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);padding:6px 8px;border-bottom:1px solid var(--border-default);">Closed</th>'
      +'</tr></thead><tbody>'+rows+'</tbody></table>'
      +'<div style="font-size:10px;color:var(--text-muted);margin-top:10px;">'+pairs.length+' outcome'+(pairs.length!==1?'s':'')+' recorded &middot; Add more via Pipeline → Outcome button on Closed deals</div>';
    container.appendChild(panel);
  }catch(e){console.error('Accuracy panel error:',e);}
}
loadAccuracyPanel();
</script>
<!-- Monthly Digest section (#230) -->
<div id="monthly-digest-section" style="margin-top:28px;">
  <div class="section-hdr">Monthly Portfolio Digest</div>
  <div style="background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:9px;padding:20px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;">
    <div>
      <div style="font-size:14px;color:var(--text-primary);font-weight:600;margin-bottom:4px;">Portfolio Vintage Report</div>
      <div style="font-size:12px;color:var(--text-secondary);line-height:1.5;max-width:520px;">Re-stress all analyzed deals against current macro conditions. Weighted avg IRR, stall detection, and LP-ready Bloomberg PE format. Download as PDF for your monthly IC packet.</div>
    </div>
    <a href="/portfolio/vintage-report" target="_blank" style="display:inline-flex;align-items:center;gap:6px;padding:10px 18px;background:rgba(232,160,32,.1);border:1px solid rgba(232,160,32,.3);color:var(--amber);border-radius:7px;font-size:13px;font-weight:600;text-decoration:none;white-space:nowrap;" onmouseenter="this.style.background='rgba(232,160,32,.18)'" onmouseleave="this.style.background='rgba(232,160,32,.1)'">
      &#128196; Generate Vintage Report
    </a>
  </div>
</div>
</body>
</html>"""


VINTAGE_REPORT_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Portfolio Vintage Report — ClearEye</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#060a0e;--surface:#0c1118;--elevated:#131b24;--border:#1b2535;--emph:#263346;--text:#f0ede8;--muted:#4e6070;--sub:#8a9bb0;--accent:#e8a020;--green:#3fb950;--red:#f85149;--mono:'IBM Plex Mono','SF Mono',Consolas,monospace;--sans:'DM Sans',-apple-system,sans-serif;--display:'DM Serif Display',Georgia,serif;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
body{background:var(--bg);color:var(--text);font-family:var(--sans);font-size:13px;line-height:1.6;-webkit-font-smoothing:antialiased;}
.page{max-width:960px;margin:0 auto;padding:40px 28px 80px;}
.vr-header{border-bottom:2px solid var(--accent);padding-bottom:20px;margin-bottom:32px;display:flex;align-items:flex-end;justify-content:space-between;flex-wrap:wrap;gap:12px;}
.vr-brand{font-family:var(--display);font-style:italic;font-size:2rem;color:var(--text);letter-spacing:-.01em;}
.vr-brand span{color:var(--accent);}
.vr-meta{text-align:right;font-family:var(--mono);font-size:10px;color:var(--muted);line-height:1.7;}
.vr-meta strong{color:var(--sub);}
.kpi-strip{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:32px;}
.kpi-card{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:16px 18px;}
.kpi-val{font-family:var(--mono);font-size:1.8rem;font-weight:700;color:var(--text);margin-bottom:2px;}
.kpi-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.07em;}
.kpi-card.accent{border-color:rgba(232,160,32,.3);background:rgba(232,160,32,.04);}
.kpi-card.accent .kpi-val{color:var(--accent);}
.section-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-family:var(--mono);padding-bottom:8px;border-bottom:1px solid var(--border);margin-bottom:14px;margin-top:28px;}
.macro-box{background:var(--surface);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:8px;padding:14px 18px;font-size:12px;color:var(--sub);line-height:1.6;margin-bottom:24px;}
.vr-table{width:100%;border-collapse:collapse;font-size:12px;}
.vr-table th{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.06em;font-family:var(--mono);font-weight:500;padding:8px 10px;text-align:left;border-bottom:1px solid var(--border);}
.vr-table td{padding:9px 10px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:middle;}
.vr-table tr:hover td{background:rgba(255,255,255,.02);}
.v-go{color:var(--green);font-weight:600;font-family:var(--mono);font-size:11px;}
.v-cond{color:var(--accent);font-weight:600;font-family:var(--mono);font-size:11px;}
.stall-card{background:var(--surface);border:1px solid rgba(248,81,73,.25);border-radius:8px;padding:12px 16px;display:flex;align-items:center;gap:10px;margin-bottom:8px;font-size:12px;}
.stall-icon{font-size:18px;flex-shrink:0;}
.stall-name{color:var(--text);font-weight:500;}
.stall-meta{color:var(--muted);font-size:11px;font-family:var(--mono);}
.footer-note{margin-top:40px;border-top:1px solid var(--border);padding-top:20px;font-size:10px;color:var(--muted);font-family:var(--mono);display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;}
.print-btn{display:inline-flex;align-items:center;gap:6px;padding:9px 16px;background:rgba(232,160,32,.1);border:1px solid rgba(232,160,32,.3);color:var(--accent);border-radius:6px;font-size:12px;font-weight:600;cursor:pointer;font-family:var(--sans);}
.print-btn:hover{background:rgba(232,160,32,.18);}
.loading{color:var(--muted);text-align:center;padding:60px;font-family:var(--mono);font-size:13px;}
.irr-spark{display:inline-block;width:60px;height:4px;background:rgba(255,255,255,.07);border-radius:2px;vertical-align:middle;overflow:hidden;margin-left:6px;}
.irr-fill{height:100%;border-radius:2px;background:var(--green);}
@media print{
  body{background:#fff;color:#111;}
  :root{--bg:#fff;--surface:#f5f5f5;--text:#111;--muted:#666;--sub:#444;--border:#ddd;}
  .print-btn,.no-print{display:none!important;}
  .vr-brand span{color:#c07010;}
  .page{padding:20px;}
}
</style>
</head>
<body>
<div class="page" id="vr-root"><div class="loading">&#128196; Generating vintage report&#x2026;</div></div>
<script>
function esc(s){const d=document.createElement('div');d.textContent=s||'';return d.innerHTML;}
async function loadReport(){
  let data;
  try{
    const r=await fetch('/api/portfolio/vintage-report');
    data=await r.json();
  }catch(e){
    document.getElementById('vr-root').innerHTML='<div class="loading" style="color:#f85149;">Failed to load report: '+esc(e.message)+'</div>';
    return;
  }
  if(data.error){
    document.getElementById('vr-root').innerHTML='<div class="loading" style="color:#f85149;">Error: '+esc(data.error)+'</div>';
    return;
  }

  const avgIrr=data.weighted_avg_irr!=null?data.weighted_avg_irr+'%':'N/A';
  const maxIrr=data.deals.reduce((mx,d)=>d.irr!=null&&d.irr>mx?d.irr:mx,0)||1;

  const kpis=[
    {val:data.total_analyzed,lbl:'Total Analyzed',cls:''},
    {val:data.go_deals_count,lbl:'GO / Conditional Deals',cls:''},
    {val:avgIrr,lbl:'Wtd Avg Proj. IRR',cls:'accent'},
    {val:data.stalled_pipeline.length,lbl:'Stalled Pipeline Deals',cls:data.stalled_pipeline.length>0?'':''},
  ];

  const kpiHtml=kpis.map(k=>'<div class="kpi-card '+k.cls+'"><div class="kpi-val">'+esc(String(k.val))+'</div><div class="kpi-lbl">'+esc(k.lbl)+'</div></div>').join('');

  const dealRows=data.deals.map(d=>{
    const vClass=d.verdict==='GO'?'v-go':'v-cond';
    const irrTxt=d.irr!=null?d.irr+'%':'—';
    const irrWidth=d.irr!=null?Math.min(100,Math.round(d.irr/maxIrr*100)):0;
    const sparkHtml=d.irr!=null?'<span class="irr-spark"><span class="irr-fill" style="width:'+irrWidth+'%;"></span></span>':'';
    const priceTxt=d.asking_price?'$'+Number(d.asking_price).toLocaleString():'—';
    return '<tr><td><span style="color:var(--text);font-weight:500;">'+esc(d.deal_name)+'</span></td>'
      +'<td><span class="'+vClass+'">'+esc(d.verdict)+'</span></td>'
      +'<td><span style="font-family:var(--mono);color:'+(d.irr!=null&&d.irr>=14?'var(--green)':d.irr!=null&&d.irr>=10?'var(--accent)':'var(--red)')+'">'
      +irrTxt+'</span>'+sparkHtml+'</td>'
      +'<td style="color:var(--sub);">'+esc(d.market||'—')+'</td>'
      +'<td style="color:var(--sub);">'+esc(d.asset_class||'—')+'</td>'
      +'<td style="font-family:var(--mono);color:var(--sub);">'+priceTxt+'</td>'
      +'<td style="font-family:var(--mono);color:var(--muted);">'+esc(d.created_at||'—')+'</td>'
      +'</tr>';
  }).join('');

  const stalledHtml=data.stalled_pipeline.length===0
    ?'<div style="color:var(--sub);font-size:12px;padding:10px 0;">No stalled deals detected in pipeline.</div>'
    :data.stalled_pipeline.map(s=>'<div class="stall-card"><span class="stall-icon">&#x23F3;</span><div><div class="stall-name">'+esc(s.deal_name)+'</div><div class="stall-meta">'+esc(s.stage)+' &middot; '+s.days_in_stage+' days in current stage</div></div></div>').join('');

  document.getElementById('vr-root').innerHTML=
    '<div class="vr-header">'
    +'<div><div class="vr-brand">ClearEye <span>Intelligence</span></div>'
    +'<div style="font-size:11px;color:var(--sub);margin-top:4px;font-family:var(--mono);">Portfolio Vintage Report &mdash; '+esc(data.report_date)+'</div></div>'
    +'<div class="vr-meta"><div><strong>Generated:</strong> '+esc(data.generated_at)+'</div>'
    +'<div><strong>Universe:</strong> '+data.total_analyzed+' analyzed deals</div>'
    +'<div style="margin-top:8px;" class="no-print"><button class="print-btn" onclick="window.print()">&#x2193; Download PDF</button></div></div>'
    +'</div>'
    +'<div class="kpi-strip">'+kpiHtml+'</div>'
    +'<div class="section-title">Macro Overlay Note</div>'
    +'<div class="macro-box">'+esc(data.macro_note)+'</div>'
    +(data.deals.length===0
      ?'<div class="section-title">GO / Conditional Deals</div><div style="color:var(--sub);font-size:12px;padding:10px 0;">No GO or Conditional deals in the portfolio yet. Analyze deals from <a href="/app" style="color:var(--accent);">the main app</a>.</div>'
      :'<div class="section-title">GO / Conditional Deals — Projected IRR Vintage</div>'
      +'<table class="vr-table"><thead><tr>'
      +'<th>Deal Name</th><th>Verdict</th><th>Proj. IRR</th><th>Market</th><th>Asset Class</th><th>Price</th><th>Analyzed</th>'
      +'</tr></thead><tbody>'+dealRows+'</tbody></table>')
    +'<div class="section-title">Pipeline Stall Detection (&gt;30 Days)</div>'
    +stalledHtml
    +'<div class="footer-note">'
    +'<span>ClearEye Portfolio Vintage Report &mdash; Confidential &mdash; For GP Use Only</span>'
    +'<a href="/portfolio" style="color:var(--accent);text-decoration:none;font-size:10px;">&#x2190; Back to Portfolio</a>'
    +'</div>';
}
loadReport();
</script>
</body>
</html>"""


COMPARE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearEye — Compare Deals</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{
  --bg-canvas:#0d1117;--bg-surface:#161b22;--bg-elevated:#1c2128;--bg-overlay:#21262d;
  --border-muted:#1c2128;--border-default:#30363d;--border-emphasis:#484f58;
  --text-primary:#e6edf3;--text-secondary:#8b949e;--text-muted:#484f58;
  --accent:#58a6ff;--green:#3fb950;--red:#f85149;--amber:#d29922;--purple:#a371f7;
  --shadow-sm:0 1px 4px rgba(0,0,0,.35),0 2px 6px rgba(0,0,0,.25);
  --shadow-md:0 4px 14px rgba(0,0,0,.45),0 2px 4px rgba(0,0,0,.35);
  --r-sm:6px;--r-md:10px;--r-lg:14px;--t:140ms ease;
  --font:"Inter",-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
}
*,*::before,*::after{box-sizing:border-box;}
body{background:var(--bg-canvas);background-image:radial-gradient(ellipse 80% 40% at 50% -5%,rgba(88,166,255,.13) 0%,transparent 60%);color:var(--text-primary);font-family:var(--font);-webkit-font-smoothing:antialiased;letter-spacing:-0.011em;}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--border-default);border-radius:3px}
.ce-nav{height:56px;background:rgba(13,17,23,.88);backdrop-filter:blur(16px);-webkit-backdrop-filter:blur(16px);border-bottom:1px solid var(--border-default);display:flex;align-items:center;padding:0 20px;gap:6px;position:sticky;top:0;z-index:100;}
.ce-brand{font-weight:800;font-size:1.1rem;background:linear-gradient(135deg,#58a6ff,#79c0ff);-webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;text-decoration:none;margin-right:8px;}
.nav-pill{font-size:12px;color:var(--text-secondary);text-decoration:none;padding:5px 10px;border-radius:var(--r-sm);transition:color var(--t),background var(--t);}
.nav-pill:hover{color:var(--text-primary);background:var(--bg-overlay);}
.nav-pill.active{color:var(--text-primary);background:var(--bg-elevated);}
.nav-sep{width:1px;height:20px;background:var(--border-default);margin:0 4px;}
.mode-tabs{display:inline-flex;background:var(--bg-elevated);border:1px solid var(--border-default);border-radius:var(--r-sm);padding:2px;margin-bottom:20px;}
.mode-tab{padding:5px 16px;border-radius:4px;font-size:12px;font-weight:500;cursor:pointer;border:none;background:none;color:var(--text-secondary);transition:all var(--t);}
.mode-tab.active{background:var(--bg-overlay);color:var(--text-primary);box-shadow:var(--shadow-sm);}
.panel{background:var(--bg-surface);border:1px solid var(--border-default);border-radius:var(--r-lg);height:100%;box-shadow:var(--shadow-sm);transition:border-color var(--t),box-shadow var(--t);}
.panel:hover{border-color:var(--border-emphasis);box-shadow:var(--shadow-md);}
.panel-hdr{background:var(--bg-elevated);border-bottom:1px solid var(--border-default);border-radius:var(--r-lg) var(--r-lg) 0 0;padding:10px 16px;font-size:12px;font-weight:700;letter-spacing:.5px;text-transform:uppercase;color:var(--text-secondary);}
textarea{background:var(--bg-canvas)!important;color:var(--text-primary)!important;border-color:var(--border-default)!important;font-size:12px;resize:vertical;transition:border-color var(--t),box-shadow var(--t)!important;}
textarea:focus{border-color:var(--accent)!important;box-shadow:0 0 0 3px rgba(88,166,255,.12)!important;outline:none!important;}
.job-id-input{background:var(--bg-canvas)!important;color:var(--text-primary)!important;border:1px solid var(--border-default)!important;border-radius:var(--r-sm);padding:10px 14px;font-size:13px;width:100%;font-family:var(--font);transition:border-color var(--t);}
.job-id-input:focus{border-color:var(--accent)!important;outline:none;box-shadow:0 0 0 3px rgba(88,166,255,.12)!important;}
.prog-dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:var(--border-default);}
.prog-dot.running{background:var(--amber);animation:pulse 1s infinite;}
.prog-dot.done{background:var(--green);}
@keyframes pulse{0%,100%{opacity:1;}50%{opacity:.4;}}
.winner-banner{background:linear-gradient(135deg,rgba(63,185,80,.1),rgba(63,185,80,.05));border:1px solid rgba(63,185,80,.28);border-radius:var(--r-lg);padding:16px 24px;text-align:center;margin-bottom:16px;}
.winner-banner.tie{background:linear-gradient(135deg,rgba(88,166,255,.08),rgba(88,166,255,.04));border-color:rgba(88,166,255,.28);}
.winner-title{font-size:10px;text-transform:uppercase;letter-spacing:.8px;color:var(--text-muted);margin-bottom:4px;font-weight:700;}
.winner-deal{font-size:1.25rem;font-weight:700;color:var(--green);letter-spacing:-0.02em;}
.winner-banner.tie .winner-deal{color:var(--accent);}
.verdict-stamp{font-size:1.6rem;font-weight:900;padding:6px 16px;border-radius:8px;border:2.5px solid currentColor;display:inline-block;letter-spacing:.5px;}
.vs-go{color:var(--green);border-color:var(--green);text-shadow:0 0 20px rgba(63,185,80,.3);}
.vs-nogo{color:var(--red);border-color:var(--red);text-shadow:0 0 20px rgba(248,81,73,.3);}
.vs-cond{color:var(--amber);border-color:var(--amber);text-shadow:0 0 20px rgba(210,153,34,.3);}
.hh-header{display:grid;grid-template-columns:1fr 130px 1fr;gap:8px;margin-bottom:6px;font-size:10px;font-weight:700;letter-spacing:.8px;text-transform:uppercase;color:var(--text-muted);}
.hh-row{display:grid;grid-template-columns:1fr 130px 1fr;gap:8px;margin-bottom:5px;font-size:12px;}
.hh-metric{color:var(--text-secondary);font-size:10px;text-align:center;padding:6px 4px;background:var(--bg-canvas);border-radius:var(--r-sm);border:1px solid var(--border-muted);}
.hh-val{padding:6px 10px;border-radius:var(--r-sm);text-align:center;font-weight:600;transition:all var(--t);}
.hh-a{background:rgba(88,166,255,.07);border:1px solid rgba(88,166,255,.1);}
.hh-b{background:rgba(163,113,247,.07);border:1px solid rgba(163,113,247,.1);}
.hh-a.wins{background:rgba(88,166,255,.16);border-color:rgba(88,166,255,.32);}
.hh-b.wins{background:rgba(163,113,247,.16);border-color:rgba(163,113,247,.32);}
.radar-wrap{position:relative;height:260px;display:flex;align-items:center;justify-content:center;}
.adv-col{display:flex;flex-direction:column;gap:6px;}
.adv-item{background:var(--bg-elevated);border:1px solid var(--border-muted);border-radius:var(--r-sm);padding:8px 12px;font-size:12px;color:var(--text-secondary);line-height:1.5;}
.adv-item strong{display:block;font-size:9px;text-transform:uppercase;letter-spacing:.5px;color:var(--text-muted);margin-bottom:3px;font-weight:700;}
.btn-primary{background:var(--btn-bg);border:none;color:#000;padding:9px 28px;border-radius:var(--r-sm);font-weight:600;font-size:12px;letter-spacing:var(--ls-label);text-transform:uppercase;cursor:pointer;font-family:var(--mono);box-shadow:0 0 0 1px rgba(232,160,32,.4);transition:all var(--t);}
.btn-primary:hover{background:linear-gradient(135deg,#388bfd,#58a6ff);box-shadow:0 0 0 1px rgba(88,166,255,.6),0 4px 12px rgba(88,166,255,.3);}
.btn-primary:disabled{opacity:.5;cursor:not-allowed;}
.btn-ghost{background:none;border:1px solid var(--border-default);color:var(--text-secondary);padding:6px 16px;border-radius:var(--r-sm);font-size:12px;cursor:pointer;font-family:var(--font);transition:all var(--t);}
.btn-ghost:hover{border-color:var(--border-emphasis);color:var(--text-primary);}
</style>
</head>
<body>
<nav class="ce-nav">
  <a class="ce-brand" href="/app">&#128065; ClearEye</a>
  <div class="nav-sep"></div>
  <a href="/app" class="nav-pill">Dashboard</a>
  <a href="/pipeline" class="nav-pill">Pipeline</a>
  <a href="/compare" class="nav-pill active">Compare</a>
  <a href="/find-deals" class="nav-pill">Find Deals</a>
  <a href="/app" style="margin-left:auto;font-size:12px;color:var(--accent);text-decoration:none;padding:5px 10px;border-radius:var(--r-sm);border:1px solid var(--border-default);transition:all var(--t);">&#8592; Back to App</a>
</nav>
<div class="container-fluid py-4" style="max-width:1200px;">
  <div id="input-section">
    <div style="text-align:center;margin-bottom:20px;">
      <h1 style="font-size:1.35rem;font-weight:700;letter-spacing:-0.025em;margin-bottom:6px;">Side-by-Side Deal Analysis</h1>
      <p style="font-size:13px;color:var(--text-secondary);margin:0;">Paste OMs to run fresh analyses, or load saved analyses by Job ID.</p>
    </div>
    <div style="text-align:center;">
      <div class="mode-tabs">
        <button class="mode-tab active" onclick="setMode('paste')" id="mode-paste">&#128203; Paste OM Text</button>
        <button class="mode-tab" onclick="setMode('load')" id="mode-load">&#128279; Load by Job ID</button>
      </div>
    </div>
    <div class="row g-3">
      <div class="col-md-6">
        <div class="panel">
          <div class="panel-hdr" style="color:#58a6ff;">Deal A</div>
          <div class="p-3">
            <div id="paste-a"><textarea id="om_a" class="form-control" rows="11" placeholder="Paste Deal A offering memorandum or deal summary..."></textarea></div>
            <div id="load-a" style="display:none;">
              <input class="job-id-input" id="jid_a" placeholder="Job ID — e.g. a1b2c3d4" />
              <div style="font-size:11px;color:var(--text-muted);margin-top:6px;">Copy from a previous analysis: /report/&lt;id&gt;</div>
              <div id="load-status-a" style="font-size:12px;color:var(--text-muted);margin-top:4px;"></div>
            </div>
          </div>
        </div>
      </div>
      <div class="col-md-6">
        <div class="panel">
          <div class="panel-hdr" style="color:#a371f7;">Deal B</div>
          <div class="p-3">
            <div id="paste-b"><textarea id="om_b" class="form-control" rows="11" placeholder="Paste Deal B offering memorandum or deal summary..."></textarea></div>
            <div id="load-b" style="display:none;">
              <input class="job-id-input" id="jid_b" placeholder="Job ID — e.g. e5f6g7h8" />
              <div style="font-size:11px;color:var(--text-muted);margin-top:6px;">Copy from a previous analysis: /report/&lt;id&gt;</div>
              <div id="load-status-b" style="font-size:12px;color:var(--text-muted);margin-top:4px;"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="text-center mt-3">
      <button class="btn-primary" onclick="startCompare()" id="cmpBtn">Compare Deals &#8594;</button>
      <div id="cmp-status" style="font-size:12px;color:var(--text-secondary);margin-top:8px;min-height:18px;"></div>
      <div style="display:none;align-items:center;gap:10px;justify-content:center;margin-top:6px;" id="prog-row">
        <span id="dot-a" class="prog-dot"></span>
        <span style="font-size:11px;color:var(--text-muted);" id="lbl-a">Deal A</span>
        <span style="font-size:11px;color:var(--text-muted);">&#183;</span>
        <span id="dot-b" class="prog-dot"></span>
        <span style="font-size:11px;color:var(--text-muted);" id="lbl-b">Deal B</span>
      </div>
    </div>
  </div>
  <div id="results-section" style="display:none;">
    <div id="winner-banner" class="winner-banner"></div>
    <div class="row g-3 mb-3" id="verdict-row"></div>
    <div class="row g-3 mb-3">
      <div class="col-md-5">
        <div class="panel h-100">
          <div class="panel-hdr">Performance Radar</div>
          <div class="p-3"><div class="radar-wrap"><canvas id="radar-chart"></canvas></div></div>
        </div>
      </div>
      <div class="col-md-7">
        <div class="panel h-100">
          <div class="panel-hdr">Head-to-Head Metrics</div>
          <div class="p-3">
            <div class="hh-header">
              <div style="text-align:right;padding-right:8px;color:#58a6ff;">DEAL A</div>
              <div style="text-align:center;">METRIC</div>
              <div style="padding-left:8px;color:#a371f7;">DEAL B</div>
            </div>
            <div id="hh-table"></div>
          </div>
        </div>
      </div>
    </div>
    <div class="row g-3 mb-3">
      <div class="col-12">
        <div class="panel">
          <div class="panel-hdr">Key Risk Factors</div>
          <div class="p-3">
            <div class="row g-3">
              <div class="col-md-6">
                <div style="font-size:11px;font-weight:700;color:#58a6ff;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;">Deal A</div>
                <div class="adv-col" id="risk-a"></div>
              </div>
              <div class="col-md-6">
                <div style="font-size:11px;font-weight:700;color:#a371f7;text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px;">Deal B</div>
                <div class="adv-col" id="risk-b"></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div class="row g-3 mb-3">
      <div class="col-md-6">
        <div class="panel">
          <div class="panel-hdr" style="color:#58a6ff;">Deal A &#8212; Go/No-Go Memo</div>
          <div class="p-3"><pre style="font-size:11px;color:#c9d1d9;white-space:pre-wrap;max-height:220px;overflow-y:auto;margin:0;" id="memo-a"></pre></div>
        </div>
      </div>
      <div class="col-md-6">
        <div class="panel">
          <div class="panel-hdr" style="color:#a371f7;">Deal B &#8212; Go/No-Go Memo</div>
          <div class="p-3"><pre style="font-size:11px;color:#c9d1d9;white-space:pre-wrap;max-height:220px;overflow-y:auto;margin:0;" id="memo-b"></pre></div>
        </div>
      </div>
    </div>
    <div class="text-center mt-3">
      <button class="btn-ghost" onclick="resetCompare()">&#8592; New Comparison</button>
    </div>
  </div>
</div>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
let pollerA=null,pollerB=null,doneA=null,doneB=null;
let _mode='paste';
let _radarChart=null;

function setMode(m){
  _mode=m;
  document.getElementById('mode-paste').classList.toggle('active',m==='paste');
  document.getElementById('mode-load').classList.toggle('active',m==='load');
  ['a','b'].forEach(function(s){
    document.getElementById('paste-'+s).style.display=m==='paste'?'':'none';
    document.getElementById('load-'+s).style.display=m==='load'?'':'none';
  });
}

async function startCompare(){
  if(_mode==='load'){await loadByIds();return;}
  var a=document.getElementById('om_a').value.trim(),b=document.getElementById('om_b').value.trim();
  if(!a||!b){alert('Please paste both deal OMs.');return;}
  var btn=document.getElementById('cmpBtn');
  btn.disabled=true;
  document.getElementById('cmp-status').textContent='Submitting both deals for analysis...';
  document.getElementById('prog-row').style.display='flex';
  setDot('a','running');setDot('b','running');
  try{
    var r=await fetch('/compare',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({om_a:a,om_b:b})});
    var ids=await r.json();
    doneA=doneB=null;
    pollerA=setInterval(function(){pollJob(ids.job_a,'a');},2500);
    pollerB=setInterval(function(){pollJob(ids.job_b,'b');},2500);
  }catch(e){btn.disabled=false;document.getElementById('cmp-status').textContent='Error: '+e;}
}

async function loadByIds(){
  var a=document.getElementById('jid_a').value.trim(),b=document.getElementById('jid_b').value.trim();
  if(!a||!b){alert('Please enter both Job IDs.');return;}
  var btn=document.getElementById('cmpBtn');
  btn.disabled=true;
  document.getElementById('cmp-status').textContent='Loading saved analyses...';
  try{
    var ra=await fetch('/status/'+a),rb=await fetch('/status/'+b);
    var da=await ra.json(),db=await rb.json();
    if(da.status==='done'&&db.status==='done'){
      btn.disabled=false;
      document.getElementById('cmp-status').textContent='';
      renderComparison(da,db);
    }else{
      document.getElementById('cmp-status').textContent='One or both analyses are not complete ('+da.status+' / '+db.status+').';
      btn.disabled=false;
    }
  }catch(e){document.getElementById('cmp-status').textContent='Failed: '+e;btn.disabled=false;}
}

function setDot(side,state){
  var d=document.getElementById('dot-'+side);
  d.className='prog-dot'+(state?' '+state:'');
  document.getElementById('lbl-'+side).textContent='Deal '+(side==='a'?'A':'B')+': '+(state==='done'?'Done':state==='running'?'Analyzing...':'');
}

async function pollJob(jid,side){
  try{
    var r=await fetch('/status/'+jid);
    var d=await r.json();
    document.getElementById('cmp-status').textContent='Analyzing Deal '+side.toUpperCase()+'... '+d.status;
    if(d.status==='done'){
      if(side==='a'){clearInterval(pollerA);doneA=d;setDot('a','done');}
      else{clearInterval(pollerB);doneB=d;setDot('b','done');}
      if(doneA&&doneB){document.getElementById('cmp-status').textContent='';renderComparison(doneA,doneB);}
    }else if(d.status==='error'){
      if(side==='a')clearInterval(pollerA);else clearInterval(pollerB);
      document.getElementById('cmp-status').textContent='Error on Deal '+side.toUpperCase()+': '+(d.message||d.error||'unknown');
      document.getElementById('cmpBtn').disabled=false;
    }
  }catch(e){console.error('Poll:',e);}
}

function getVerdict(d){
  if(d.verdict&&d.verdict.recommendation){
    var r=d.verdict.recommendation.toUpperCase();
    if(r.indexOf('NO')>=0)return{v:'NO-GO',c:'vs-nogo'};
    if(r.indexOf('COND')>=0)return{v:'CONDITIONAL',c:'vs-cond'};
    return{v:'GO',c:'vs-go'};
  }
  var mu=(d.memo||'').toUpperCase();
  if(mu.indexOf('NO-GO')>=0)return{v:'NO-GO',c:'vs-nogo'};
  if(/\\bGO\\b/.test(mu)&&mu.indexOf('CONDITIONAL')<0)return{v:'GO',c:'vs-go'};
  return{v:'CONDITIONAL',c:'vs-cond'};
}
function getConf(d){
  if(d.verdict&&d.verdict.confidence)return parseFloat(d.verdict.confidence)||0;
  var m=(d.memo||'').match(/Confidence[^0-9]*([0-9]+)/);
  return m?parseFloat(m[1]):50;
}
function getStress(d){if(d.stress_test&&d.stress_test.score!=null)return parseFloat(d.stress_test.score)||0;return 50;}
function getBias(d){if(d.bias_scan&&Array.isArray(d.bias_scan.flags))return d.bias_scan.flags.length;return 0;}
function getRedFlags(d){return((d.validation_report||'').split('\\n').filter(function(l){return l.indexOf('RED FLAG')>=0;})).length;}
function fmtPct(v){return(v!=null&&v!==''&&v!=='—')?parseFloat(v).toFixed(1)+'%':'—';}
function fmtUSD(v){return v?'$'+Number(v).toLocaleString():'—';}
function fmtNum(v){return(v!=null&&v!=='')?Number(v).toLocaleString():'—';}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function scoreV(v){return v.v==='GO'?2:v.v==='CONDITIONAL'?1:0;}

function renderComparison(dA,dB){
  document.getElementById('input-section').style.display='none';
  document.getElementById('results-section').style.display='block';
  var da=dA.deal||{},db=dB.deal||{};
  var vA=getVerdict(dA),vB=getVerdict(dB);
  var confA=getConf(dA),confB=getConf(dB);
  var stA=getStress(dA),stB=getStress(dB);
  var biA=getBias(dA),biB=getBias(dB);
  var rfA=getRedFlags(dA),rfB=getRedFlags(dB);
  var nA=da.deal_name||'Deal A',nB=db.deal_name||'Deal B';

  // Composite score
  var sA=scoreV(vA)*15+confA*0.3+stA*0.2+parseFloat(da.cap_rate||0)*3+parseFloat(da.projected_irr||0)*2-rfA*5-biA*3;
  var sB=scoreV(vB)*15+confB*0.3+stB*0.2+parseFloat(db.cap_rate||0)*3+parseFloat(db.projected_irr||0)*2-rfB*5-biB*3;

  // Winner banner
  var wb=document.getElementById('winner-banner');
  if(Math.abs(sA-sB)<5){
    wb.className='winner-banner tie';
    wb.innerHTML='<div class="winner-title">Comparison Result</div><div class="winner-deal">&#9866; Too Close to Call</div><div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Both deals within 5 composite points &#8212; weigh your priorities</div>';
  }else{
    var wn=sA>sB?esc(nA):esc(nB);
    wb.className='winner-banner';
    wb.innerHTML='<div class="winner-title">&#9733; Recommended Deal</div><div class="winner-deal">'+wn+'</div><div style="font-size:12px;color:var(--text-muted);margin-top:4px;">Composite advantage: +'+Math.abs(Math.round(sA-sB))+' pts</div>';
  }

  // Verdict row
  document.getElementById('verdict-row').innerHTML=
    '<div class="col-md-5 text-center"><div style="padding:20px;">'
    +'<div class="verdict-stamp '+vA.c+'">'+vA.v+'</div>'
    +'<div style="margin-top:10px;font-size:15px;font-weight:700;letter-spacing:-0.02em;">'+esc(nA)+'</div>'
    +(da.market?'<div style="font-size:11px;color:var(--text-muted);margin-top:3px;">'+esc(da.market)+'</div>':'')
    +'</div></div>'
    +'<div class="col-md-2 d-flex align-items-center justify-content-center"><div style="font-size:2rem;color:var(--text-muted);">vs</div></div>'
    +'<div class="col-md-5 text-center"><div style="padding:20px;">'
    +'<div class="verdict-stamp '+vB.c+'">'+vB.v+'</div>'
    +'<div style="margin-top:10px;font-size:15px;font-weight:700;letter-spacing:-0.02em;">'+esc(nB)+'</div>'
    +(db.market?'<div style="font-size:11px;color:var(--text-muted);margin-top:3px;">'+esc(db.market)+'</div>':'')
    +'</div></div>';

  // Radar
  buildRadar(da,db,esc(nA),esc(nB),confA,confB,stA,stB,rfA,rfB);

  // H2H table
  function row(metric,aVal,bVal,hib,fmt){
    fmt=fmt||String;
    var aN=parseFloat(String(aVal))||0,bN=parseFloat(String(bVal))||0;
    var has=aN>0||bN>0;
    var aW=has&&(hib?aN>bN:aN<bN);
    var bW=has&&(hib?bN>aN:bN<aN);
    var aD=(aVal!=null&&aVal!==''&&aVal!=='—')?fmt(aVal):'—';
    var bD=(bVal!=null&&bVal!==''&&bVal!=='—')?fmt(bVal):'—';
    return '<div class="hh-row">'
      +'<div class="hh-val hh-a'+(aW?' wins':'')+'">'+esc(String(aD))+(aW?' <span style="color:var(--green);font-size:10px;">&#9650;</span>':bW?' <span style="color:var(--red);font-size:10px;">&#9660;</span>':'')+'</div>'
      +'<div class="hh-metric">'+esc(metric)+'</div>'
      +'<div class="hh-val hh-b'+(bW?' wins':'')+'">'+esc(String(bD))+(bW?' <span style="color:var(--green);font-size:10px;">&#9650;</span>':aW?' <span style="color:var(--red);font-size:10px;">&#9660;</span>':'')+'</div>'
      +'</div>';
  }
  document.getElementById('hh-table').innerHTML=[
    row('Cap Rate %',da.cap_rate,db.cap_rate,true,fmtPct),
    row('Projected IRR %',da.projected_irr,db.projected_irr,true,fmtPct),
    row('AI Confidence %',confA,confB,true,function(v){return parseFloat(v).toFixed(0)+'%';}),
    row('Stress Score',stA,stB,true,function(v){return parseFloat(v).toFixed(0)+'/100';}),
    row('Asking Price',da.asking_price,db.asking_price,false,fmtUSD),
    row('Units',da.units,db.units,true,fmtNum),
    row('LTV %',da.ltv,db.ltv,false,fmtPct),
    row('Red Flags',rfA,rfB,false,String),
    row('Bias Flags',biA,biB,false,String),
  ].join('');

  // Risk bullets
  function buildRisks(d){
    var items=[];
    if(d.stress_test&&d.stress_test.summary)items.push({l:'Stress Test',t:String(d.stress_test.summary).substring(0,160)});
    if(d.bias_scan){var bs=typeof d.bias_scan==='string'?d.bias_scan:(d.bias_scan.summary||'');if(bs)items.push({l:'Bias Scan',t:String(bs).substring(0,160)});}
    if(d.advisors&&Array.isArray(d.advisors)){
      d.advisors.slice(0,3).forEach(function(a){
        var t=String(a.summary||a.analysis||'').substring(0,160);
        if(t)items.push({l:a.role||a.name||'Advisor',t:t});
      });
    }
    if(items.length===0){
      (d.memo||'').split('\\n').filter(function(l){return l.trim().length>30;}).slice(0,4).forEach(function(l){items.push({l:'Memo',t:l.trim().substring(0,160)});});
    }
    if(items.length===0)return '<div class="adv-item" style="color:var(--text-muted);">No advisor data</div>';
    return items.map(function(i){return '<div class="adv-item"><strong>'+esc(i.l)+'</strong>'+esc(i.t)+(i.t.length>=160?'&#8230;':'')+'</div>';}).join('');
  }
  document.getElementById('risk-a').innerHTML=buildRisks(dA);
  document.getElementById('risk-b').innerHTML=buildRisks(dB);

  // Memos
  var mA=dA.memo||'',mB=dB.memo||'';
  document.getElementById('memo-a').textContent=mA.substring(0,1000)+(mA.length>1000?'\\n[\\u2026 truncated]':'');
  document.getElementById('memo-b').textContent=mB.substring(0,1000)+(mB.length>1000?'\\n[\\u2026 truncated]':'');
}

function buildRadar(da,db,nA,nB,confA,confB,stA,stB,rfA,rfB){
  function capN(v){return Math.min(100,parseFloat(v||0)/12*100);}
  function irrN(v){return Math.min(100,parseFloat(v||0)/25*100);}
  function safeN(f){return Math.max(0,100-f*20);}
  var dsetA=[capN(da.cap_rate),irrN(da.projected_irr),confA,stA,safeN(rfA)];
  var dsetB=[capN(db.cap_rate),irrN(db.projected_irr),confB,stB,safeN(rfB)];
  if(_radarChart){_radarChart.destroy();_radarChart=null;}
  var ctx=document.getElementById('radar-chart').getContext('2d');
  _radarChart=new Chart(ctx,{
    type:'radar',
    data:{
      labels:['Cap Rate','Proj IRR','AI Conf','Stress','Safety'],
      datasets:[
        {label:nA,data:dsetA,backgroundColor:'rgba(88,166,255,.14)',borderColor:'#58a6ff',borderWidth:2,pointBackgroundColor:'#58a6ff',pointRadius:4},
        {label:nB,data:dsetB,backgroundColor:'rgba(163,113,247,.14)',borderColor:'#a371f7',borderWidth:2,pointBackgroundColor:'#a371f7',pointRadius:4}
      ]
    },
    options:{
      responsive:true,maintainAspectRatio:false,
      plugins:{legend:{labels:{color:'#8b949e',font:{size:11,family:'Inter'}}}},
      scales:{r:{min:0,max:100,angleLines:{color:'rgba(255,255,255,.06)'},grid:{color:'rgba(255,255,255,.06)'},ticks:{display:false},pointLabels:{color:'#8b949e',font:{size:10,family:'Inter'}}}}
    }
  });
}

function resetCompare(){
  document.getElementById('results-section').style.display='none';
  document.getElementById('input-section').style.display='block';
  document.getElementById('cmpBtn').disabled=false;
  document.getElementById('cmp-status').textContent='';
  document.getElementById('prog-row').style.display='none';
  doneA=doneB=null;
}

// Pre-load from URL params ?a=JOB_ID&b=JOB_ID
async function preloadFromUrl(){
  var p=new URLSearchParams(location.search);
  var a=p.get('a'),b=p.get('b');
  if(!a||!b)return;
  setMode('load');
  document.getElementById('jid_a').value=a;
  document.getElementById('jid_b').value=b;
  document.getElementById('cmp-status').textContent='Loading previous analyses...';
  document.getElementById('cmpBtn').disabled=true;
  try{
    var ra=await fetch('/status/'+a),rb=await fetch('/status/'+b);
    var da=await ra.json(),db=await rb.json();
    if(da.status==='done'&&db.status==='done'){
      document.getElementById('cmp-status').textContent='';
      document.getElementById('cmpBtn').disabled=false;
      renderComparison(da,db);
    }else{
      document.getElementById('cmp-status').textContent='One or both deals not complete. Run analysis first.';
      document.getElementById('cmpBtn').disabled=false;
    }
  }catch(e){document.getElementById('cmp-status').textContent='Failed: '+e;document.getElementById('cmpBtn').disabled=false;}
}
preloadFromUrl();
</script>
</body>
</html>"""


FIND_DEALS_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ClearEye — Live Deal Aggregator</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg-canvas:#080b10;--bg-surface:#0f1318;--bg-elevated:#161d26;--bg-overlay:#1e2733;--border-muted:#131920;--border-default:#1e2733;--border-emphasis:#2e3d4f;--text-primary:#f0ede8;--text-secondary:#8a9bb0;--text-muted:#3d4f63;--accent:#e8a020;--accent-glow:rgba(232,160,32,.22);--accent-dim:rgba(232,160,32,.09);--green:#3fb950;--red:#f85149;--amber:#e8a020;--purple:#a371f7;--shadow-xs:0 1px 2px rgba(0,0,0,.4);--shadow-sm:inset 0 1px 0 rgba(255,255,255,.07),0 2px 8px rgba(0,0,0,.4);--r-sm:5px;--r-md:9px;--r-lg:13px;--t:140ms ease;--font:'DM Sans',-apple-system,sans-serif;--font-display:'DM Serif Display',Georgia,serif;--mono:'IBM Plex Mono','SF Mono',Consolas,monospace;--text-base:13.5px;--ls-body:-0.008em;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
body{background-color:var(--bg-canvas);background-image:radial-gradient(ellipse 80% 45% at 50% -5%,rgba(232,160,32,.07) 0%,transparent 65%),url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='200' height='200'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.75' numOctaves='4' stitchTiles='stitch'/%3E%3CfeColorMatrix type='saturate' values='0'/%3E%3C/filter%3E%3Crect width='200' height='200' filter='url(%23n)' opacity='0.03'/%3E%3C/svg%3E");background-size:100% 100%,200px 200px;background-attachment:fixed;color:var(--text-primary);font-family:var(--font);font-size:var(--text-base);line-height:1.55;letter-spacing:var(--ls-body);-webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility;}
body::before{content:'';position:fixed;inset:0;pointer-events:none;z-index:0;background:repeating-linear-gradient(0deg,transparent 2px,rgba(0,0,0,.025) 2px,rgba(0,0,0,.025) 4px);background-size:100% 4px;}
.noise-overlay{position:fixed;inset:0;pointer-events:none;z-index:9999;opacity:.025;background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");background-size:160px 160px;}
::-webkit-scrollbar{width:4px;height:4px;}::-webkit-scrollbar-track{background:transparent;}::-webkit-scrollbar-thumb{background:rgba(232,160,32,.25);border-radius:4px;}::-webkit-scrollbar-thumb:hover{background:rgba(232,160,32,.5);}
.agg-sidebar,::-webkit-scrollbar{scrollbar-width:thin;scrollbar-color:rgba(232,160,32,.25) transparent;}
.ce-nav{height:56px;background:rgba(8,11,16,.88);backdrop-filter:blur(20px) saturate(180%);-webkit-backdrop-filter:blur(20px) saturate(180%);border-bottom:1px solid rgba(255,255,255,.06);display:flex;align-items:center;padding:0 20px;position:sticky;top:0;z-index:100;}
.ce-brand{font-family:var(--font-display);font-size:1.2rem;font-weight:400;color:var(--accent);text-decoration:none;letter-spacing:-0.01em;}
.nav-pill{font-size:12px;color:var(--text-secondary);text-decoration:none;padding:5px 10px;border-radius:var(--r-sm);transition:color var(--t),background var(--t);}
.nav-pill:hover{color:var(--text-primary);background:var(--bg-overlay);}
.nav-pill.active-page{color:var(--accent);background:var(--accent-dim);}

/* Layout */
.agg-layout{display:grid;grid-template-columns:280px 1fr;height:calc(100vh - 56px);}
.agg-sidebar{border-right:1px solid rgba(255,255,255,.05);overflow-y:auto;padding:16px 14px;background:rgba(9,13,18,.55);backdrop-filter:blur(4px);}
.agg-main{overflow-y:auto;padding:16px 20px;background:transparent;}

/* Filter sidebar */
.filter-section{margin-bottom:20px;}
.filter-label{font-size:10px;color:var(--text-secondary);text-transform:uppercase;letter-spacing:.5px;margin-bottom:6px;font-weight:600;}
input[type=text],input[type=number],select{background:rgba(8,11,16,.7)!important;color:var(--text-primary)!important;border:1px solid var(--border-default)!important;border-radius:var(--r-sm);padding:6px 9px;font-size:12px;width:100%;transition:border-color var(--t);font-family:var(--font);}
input:focus,select:focus{border-color:var(--accent)!important;outline:none;box-shadow:0 0 0 3px rgba(232,160,32,.12)!important;}
.range-row{display:flex;gap:6px;}
.range-row input{flex:1;}

/* Category tabs */
.cat-tabs{display:flex;gap:4px;flex-wrap:wrap;margin-bottom:14px;}
.cat-badge{background:rgba(255,255,255,.07);color:var(--text-secondary);border-radius:10px;padding:1px 6px;font-size:10px;margin-left:4px;font-family:var(--mono);}
.cat-tab.active .cat-badge{background:rgba(232,160,32,.3);color:#000;}

/* Sort bar */
.sort-bar{display:flex;align-items:center;gap:8px;margin-bottom:12px;flex-wrap:wrap;}
.sort-btn{padding:4px 10px;border-radius:4px;border:1px solid var(--border-default);font-size:11px;cursor:pointer;color:var(--text-secondary);background:none;transition:all var(--t);}
.sort-btn.active{background:var(--accent-dim);border-color:var(--accent);color:var(--accent);font-family:var(--mono);}

/* Deal cards */
@keyframes fdSlideUp{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}
.deal-card{background:rgba(15,19,24,.72);border:1px solid var(--border-default);border-radius:var(--r-md);padding:12px 14px;margin-bottom:8px;cursor:pointer;transition:all var(--t);box-shadow:inset 0 1px 0 rgba(255,255,255,.06),0 2px 8px rgba(0,0,0,.35);backdrop-filter:blur(4px);animation:fdSlideUp .35s cubic-bezier(.22,1,.36,1) both;}
.deal-card:hover{border-color:rgba(232,160,32,.35);box-shadow:inset 0 1px 0 rgba(255,255,255,.09),0 0 0 1px rgba(232,160,32,.1),0 6px 20px rgba(0,0,0,.45);transform:translateY(-1px);}
.deal-card.top3{border-left:3px solid var(--green);background:rgba(15,24,19,.72);}
.deal-card-name{font-family:var(--font-display);font-style:italic;font-size:14px;font-weight:400;letter-spacing:-0.01em;color:var(--text-primary);}
.score-ring{width:44px;height:44px;flex-shrink:0;position:relative;}
.score-ring svg{transform:rotate(-90deg);}
.score-ring .score-txt{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;font-family:var(--mono);}
.cat-chip{padding:2px 7px;border-radius:10px;font-size:10px;font-weight:600;}
.chip-mf{background:rgba(88,166,255,.15);color:var(--accent);}
.chip-ret{background:rgba(210,153,34,.15);color:var(--amber);}
.chip-ind{background:rgba(163,113,247,.15);color:var(--purple);}
.chip-off{background:rgba(63,185,80,.15);color:var(--green);}
.chip-oth{background:rgba(72,79,88,.25);color:var(--text-secondary);}
.metric-row{display:flex;gap:10px;margin-top:6px;flex-wrap:wrap;}
.metric-box{background:rgba(8,11,16,.6);border:1px solid var(--border-default);border-radius:4px;padding:3px 8px;font-size:11px;}
.metric-box .ml{color:var(--text-muted);font-size:9px;display:block;text-transform:uppercase;letter-spacing:.05em;}
.metric-box .mv{font-weight:600;font-family:var(--mono);font-size:12px;}

/* Rate limit badges */
.rl-bar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px;}
.rl-badge{display:flex;align-items:center;gap:5px;padding:4px 10px;border-radius:20px;font-size:11px;border:1px solid var(--border-default);}
.rl-badge.ok{border-color:var(--border-default);color:var(--text-secondary);}
.rl-badge.warn{border-color:var(--amber);color:var(--amber);}
.rl-badge.critical{border-color:var(--red);color:var(--red);}
.rl-dot{width:6px;height:6px;border-radius:50%;background:currentColor;}
.rl-pct-bar{width:40px;height:4px;border-radius:2px;background:var(--bg-overlay);overflow:hidden;}
.rl-pct-fill{height:100%;border-radius:2px;}
.ok .rl-pct-fill{background:var(--green);}
.warn .rl-pct-fill{background:var(--amber);}
.critical .rl-pct-fill{background:var(--red);}

/* Misc */
.fetch-note{font-size:10px;color:var(--text-muted);}
.empty-state{text-align:center;padding:60px;color:var(--text-muted);}
.mock-warn{background:rgba(210,153,34,.07);border:1px solid rgba(210,153,34,.2);border-radius:var(--r-md);padding:8px 12px;font-size:11px;color:var(--amber);margin-bottom:12px;backdrop-filter:blur(4px);}
/* Category tabs */
.cat-tab{padding:5px 12px;border-radius:20px;border:1px solid rgba(255,255,255,.08);font-size:12px;cursor:pointer;color:var(--text-secondary);background:rgba(255,255,255,.03);transition:all var(--t);}
.cat-tab:hover{border-color:rgba(88,166,255,.3);color:var(--accent);}
.cat-tab.active{background:var(--accent-dim);border-color:rgba(232,160,32,.4);color:var(--accent);font-weight:600;}
@media(max-width:800px){.agg-layout{grid-template-columns:1fr;}.agg-sidebar{display:none;}}
</style>
</head>
<body>
<div class="noise-overlay" aria-hidden="true"></div>
<nav class="ce-nav">
  <a class="ce-brand" href="/app">&#128065; ClearEye</a>
  <span style="font-size:11px;color:var(--text-muted);margin-left:16px;">Live Deal Aggregator</span>
  <div style="margin-left:auto;display:flex;align-items:center;gap:8px;">
    <span id="refresh-ts" class="fetch-note"></span>
    <button onclick="loadDeals(true)" style="padding:5px 12px;background:var(--accent);border:none;color:#000;border-radius:var(--r-sm);font-size:12px;font-weight:700;cursor:pointer;transition:all var(--t);font-family:var(--mono);" id="refreshBtn">&#8635; Refresh</button>
    <a href="/app" class="nav-pill" style="color:var(--accent);">&#8592; Analysis</a>
  </div>
</nav>

<div class="agg-layout">
  <!-- ── Filters sidebar ── -->
  <div class="agg-sidebar">
    <div class="filter-section">
      <div class="filter-label">Markets</div>
      <div id="market-checkboxes" style="display:flex;flex-direction:column;gap:5px;font-size:12px;">
        <label><input type="checkbox" value="Phoenix, AZ" checked> Phoenix, AZ</label>
        <label><input type="checkbox" value="Atlanta, GA" checked> Atlanta, GA</label>
        <label><input type="checkbox" value="Dallas, TX" checked> Dallas, TX</label>
        <label><input type="checkbox" value="Tampa, FL" checked> Tampa, FL</label>
        <label><input type="checkbox" value="Denver, CO" checked> Denver, CO</label>
        <label><input type="checkbox" value="Charlotte, NC"> Charlotte, NC</label>
        <label><input type="checkbox" value="Nashville, TN"> Nashville, TN</label>
        <label><input type="checkbox" value="Austin, TX"> Austin, TX</label>
        <label><input type="checkbox" value="Las Vegas, NV"> Las Vegas, NV</label>
        <label><input type="checkbox" value="Raleigh, NC"> Raleigh, NC</label>
      </div>
    </div>
    <div class="filter-section">
      <div class="filter-label">Max Price ($)</div>
      <input type="number" id="f-maxprice" value="30000000" step="1000000">
    </div>
    <div class="filter-section">
      <div class="filter-label">Min Cap Rate (%)</div>
      <input type="number" id="f-mincap" value="0" step="0.5" min="0" max="15">
    </div>
    <div class="filter-section">
      <div class="filter-label">Results per page</div>
      <select id="f-limit">
        <option value="15">15</option>
        <option value="30" selected>30</option>
        <option value="50">50</option>
      </select>
    </div>
    <button onclick="loadDeals(true)" style="width:100%;padding:8px;background:var(--accent);border:none;color:#000;border-radius:6px;font-size:12px;font-weight:700;cursor:pointer;margin-top:6px;font-family:var(--mono);letter-spacing:.02em;">Apply Filters &rarr;</button>

    <!-- Deal Alerts (#134) -->
    <div style="margin-top:20px;">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:6px;">
        <div class="filter-label" style="margin-bottom:0;">Deal Alerts</div>
        <button onclick="openAlertModal()" style="padding:2px 8px;font-size:10px;background:#238636;border:none;color:#fff;border-radius:4px;cursor:pointer;">+ New</button>
      </div>
      <div id="alerts-list" style="font-size:11px;color:#484f58;">Loading...</div>
    </div>

    <!-- API Rate Limits (#124) -->
    <div style="margin-top:20px;">
      <div class="filter-label">API Rate Limits</div>
      <div id="rl-sidebar"></div>
    </div>
  </div>

  <!-- ── Main results ── -->
  <div class="agg-main">
    <!-- Rate limit bar at top -->
    <div class="rl-bar" id="rl-bar"></div>

    <!-- Mock data warning -->
    <div class="mock-warn" id="mock-warn" style="display:none;">
      &#9888; Showing <strong>mock data</strong> — add <code>RENTCAST_API_KEY</code> to .env for live listings
    </div>

    <!-- Category tabs (#125) -->
    <div class="cat-tabs" id="cat-tabs">
      <button class="cat-tab active" data-cat="all" onclick="selectCat('all')">All <span class="cat-badge" id="cnt-all">—</span></button>
      <button class="cat-tab" data-cat="multifamily" onclick="selectCat('multifamily')">Multifamily <span class="cat-badge" id="cnt-multifamily">—</span></button>
      <button class="cat-tab" data-cat="retail" onclick="selectCat('retail')">Retail <span class="cat-badge" id="cnt-retail">—</span></button>
      <button class="cat-tab" data-cat="industrial" onclick="selectCat('industrial')">Industrial <span class="cat-badge" id="cnt-industrial">—</span></button>
      <button class="cat-tab" data-cat="office" onclick="selectCat('office')">Office <span class="cat-badge" id="cnt-office">—</span></button>
      <button class="cat-tab" data-cat="hud" onclick="selectCat('hud')" style="color:#a371f7;border-color:#a371f7;">&#127962; HUD Expiring</button>
      <button class="cat-tab" data-cat="watchlist" onclick="selectCat('watchlist')" style="margin-left:auto;color:#d29922;border-color:#d29922;">&#9733; Watchlist</button>
    </div>

    <!-- Sort bar -->
    <div class="sort-bar">
      <span style="font-size:11px;color:#8b949e;">Sort:</span>
      <button class="sort-btn active" data-sort="score" onclick="selectSort('score')">ClearEye Score</button>
      <button class="sort-btn" data-sort="cap_rate" onclick="selectSort('cap_rate')">Cap Rate</button>
      <button class="sort-btn" data-sort="irr" onclick="selectSort('irr')">Proj. IRR</button>
      <button class="sort-btn" data-sort="price" onclick="selectSort('price')">Price &#8593;</button>
      <button class="sort-btn" data-sort="units" onclick="selectSort('units')">Units</button>
      <span id="deal-count" style="margin-left:auto;font-size:11px;color:#8b949e;"></span>
      <!-- Saved searches (#139) -->
      <div style="position:relative;margin-left:8px;">
        <button onclick="toggleSavedSearchPanel()" id="saved-search-btn"
          style="padding:4px 10px;font-size:11px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);color:var(--text-secondary);border-radius:5px;cursor:pointer;display:flex;align-items:center;gap:4px;transition:all var(--t);" onmouseover="this.style.borderColor='rgba(255,255,255,.14)'" onmouseout="this.style.borderColor='rgba(255,255,255,.08)'"  >
          &#128190; Saved <span id="saved-count" style="background:#30363d;border-radius:8px;padding:0 5px;font-size:10px;"></span>
        </button>
        <div id="saved-search-panel" style="display:none;position:absolute;right:0;top:32px;width:280px;background:rgba(22,27,34,.95);backdrop-filter:blur(16px);border:1px solid rgba(255,255,255,.1);border-radius:10px;z-index:100;box-shadow:inset 0 1px 0 rgba(255,255,255,.07),0 8px 32px rgba(0,0,0,.5);padding:10px;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
            <span style="font-size:12px;font-weight:600;">Saved Searches</span>
            <button onclick="saveCurrentSearch()" style="padding:3px 8px;font-size:10px;background:#238636;border:none;color:#fff;border-radius:4px;cursor:pointer;">+ Save Current</button>
          </div>
          <div id="saved-searches-list" style="max-height:200px;overflow-y:auto;">
            <div style="font-size:11px;color:#484f58;text-align:center;padding:12px 0;">No saved searches yet</div>
          </div>
        </div>
      </div>
      <!-- Scoring profiles (#141) -->
      <div style="position:relative;margin-left:4px;">
        <button onclick="toggleScoringPanel()" id="scoring-btn"
          style="padding:4px 10px;font-size:11px;background:rgba(255,255,255,.04);border:1px solid rgba(255,255,255,.08);color:var(--text-secondary);border-radius:5px;cursor:pointer;display:flex;align-items:center;gap:4px;transition:all var(--t);" onmouseover="this.style.borderColor='rgba(255,255,255,.14)'" onmouseout="this.style.borderColor='rgba(255,255,255,.08)'"  >
          &#9878; Scoring <span id="active-profile-badge" style="font-size:9px;color:#58a6ff;"></span>
        </button>
        <div id="scoring-panel" style="display:none;position:absolute;right:0;top:32px;width:320px;background:rgba(22,27,34,.95);backdrop-filter:blur(16px);border:1px solid rgba(255,255,255,.1);border-radius:10px;z-index:100;box-shadow:inset 0 1px 0 rgba(255,255,255,.07),0 8px 32px rgba(0,0,0,.5);padding:12px;">
          <div style="font-size:12px;font-weight:700;margin-bottom:10px;">&#9878; Score Weights</div>
          <!-- Preset profiles -->
          <div style="display:flex;gap:6px;margin-bottom:12px;flex-wrap:wrap;">
            <button onclick="applyPreset('preset_core_plus')" class="preset-btn" data-pid="preset_core_plus" style="font-size:10px;padding:3px 8px;background:#21262d;border:1px solid #30363d;color:#8b949e;border-radius:4px;cursor:pointer;">Core Plus</button>
            <button onclick="applyPreset('preset_value_add')" class="preset-btn" data-pid="preset_value_add" style="font-size:10px;padding:3px 8px;background:#21262d;border:1px solid #30363d;color:#8b949e;border-radius:4px;cursor:pointer;">Value-Add</button>
            <button onclick="applyPreset('preset_opportunistic')" class="preset-btn" data-pid="preset_opportunistic" style="font-size:10px;padding:3px 8px;background:#21262d;border:1px solid #30363d;color:#8b949e;border-radius:4px;cursor:pointer;">Opportunistic</button>
            <button onclick="resetWeights()" style="font-size:10px;padding:3px 8px;background:none;border:1px solid #484f58;color:#8b949e;border-radius:4px;cursor:pointer;">Default</button>
          </div>
          <!-- Sliders -->
          <div id="weight-sliders">
            <div class="weight-row" data-key="cap_rate">
              <div style="display:flex;justify-content:space-between;font-size:11px;color:#c9d1d9;margin-bottom:2px;"><span>Cap Rate weight</span><span id="wval-cap_rate" style="color:#58a6ff;font-weight:700;">8.0</span></div>
              <input type="range" min="0" max="20" step="0.5" value="8" oninput="updateWeight('cap_rate',this.value)" id="wslider-cap_rate" style="width:100%;accent-color:#58a6ff;">
            </div>
            <div class="weight-row" style="margin-top:8px;" data-key="irr_premium">
              <div style="display:flex;justify-content:space-between;font-size:11px;color:#c9d1d9;margin-bottom:2px;"><span>IRR premium weight</span><span id="wval-irr_premium" style="color:#58a6ff;font-weight:700;">3.0</span></div>
              <input type="range" min="0" max="15" step="0.5" value="3" oninput="updateWeight('irr_premium',this.value)" id="wslider-irr_premium" style="width:100%;accent-color:#58a6ff;">
            </div>
            <div class="weight-row" style="margin-top:8px;" data-key="bear_cushion">
              <div style="display:flex;justify-content:space-between;font-size:11px;color:#c9d1d9;margin-bottom:2px;"><span>Bear-case bonus</span><span id="wval-bear_cushion" style="color:#58a6ff;font-weight:700;">15.0</span></div>
              <input type="range" min="0" max="30" step="1" value="15" oninput="updateWeight('bear_cushion',this.value)" id="wslider-bear_cushion" style="width:100%;accent-color:#58a6ff;">
            </div>
            <div class="weight-row" style="margin-top:8px;" data-key="scale">
              <div style="display:flex;justify-content:space-between;font-size:11px;color:#c9d1d9;margin-bottom:2px;"><span>Scale bonus (50+ units)</span><span id="wval-scale" style="color:#58a6ff;font-weight:700;">5.0</span></div>
              <input type="range" min="0" max="20" step="1" value="5" oninput="updateWeight('scale',this.value)" id="wslider-scale" style="width:100%;accent-color:#58a6ff;">
            </div>
            <div class="weight-row" style="margin-top:8px;" data-key="ppu_discount">
              <div style="display:flex;justify-content:space-between;font-size:11px;color:#c9d1d9;margin-bottom:2px;"><span>Value-entry bonus (&lt;$150K/unit)</span><span id="wval-ppu_discount" style="color:#58a6ff;font-weight:700;">10.0</span></div>
              <input type="range" min="0" max="20" step="1" value="10" oninput="updateWeight('ppu_discount',this.value)" id="wslider-ppu_discount" style="width:100%;accent-color:#58a6ff;">
            </div>
          </div>
          <div style="display:flex;gap:6px;margin-top:12px;">
            <input id="profile-name-input" placeholder="Profile name…" style="flex:1;padding:5px 8px;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:4px;font-size:11px;">
            <button onclick="saveCurrentWeights()" style="padding:5px 10px;font-size:11px;background:#238636;border:none;color:#fff;border-radius:4px;cursor:pointer;">Save</button>
          </div>
          <!-- Saved profiles list -->
          <div id="saved-profiles-list" style="margin-top:10px;max-height:120px;overflow-y:auto;"></div>
        </div>
      </div>
    </div>

    <!-- Deal list -->
    <div id="deals-list">
      <div class="empty-state">
        <div style="font-size:40px;margin-bottom:12px;">&#127968;</div>
        <div style="font-size:14px;margin-bottom:6px;">Live Deal Feed</div>
        <div style="font-size:12px;">Loading top deals from RentCast across 5 markets...</div>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
let _allDeals=[], _catCounts={}, _currentCat='all', _currentSort='score';
let _autoRefreshTimer=null;
let _watchlistKeys=new Set();  // deal_keys currently starred (#128)
let _notes={};                  // deal_key → note text (#128)
let _watchlistDeals=[];         // full deal objects for watchlist tab

// ── State ──────────────────────────────────────────────────────────────────
function selectedMarkets(){
  return [...document.querySelectorAll('#market-checkboxes input:checked')].map(cb=>cb.value);
}

// ── Data loading ──────────────────────────────────────────────────────────
async function loadDeals(showLoading=false){
  if(showLoading){
    document.getElementById('deals-list').innerHTML='<div class="empty-state"><div style="font-size:24px;animation:spin 1s linear infinite;display:inline-block;">&#8635;</div><div style="margin-top:10px;font-size:12px;">Fetching live listings...</div></div>';
    document.getElementById('refreshBtn').disabled=true;
  }
  const mkts=selectedMarkets();
  if(!mkts.length){alert('Select at least one market');return;}
  const params=new URLSearchParams();
  mkts.forEach(m=>params.append('market',m));
  params.set('sort',_currentSort);
  params.set('category',_currentCat);
  params.set('max_price',document.getElementById('f-maxprice').value);
  params.set('min_cap_rate',document.getElementById('f-mincap').value);
  params.set('limit',document.getElementById('f-limit').value);
  try{
    const r=await fetch('/api/live-deals?'+params);
    const d=await r.json();
    _allDeals=d.deals||[];
    _catCounts=d.category_counts||{};
    renderCatTabs();
    renderDeals();
    renderRateLimits(d.rate_limits||{});
    const ts=d.fetched_at?new Date(d.fetched_at).toLocaleTimeString():'';
    document.getElementById('refresh-ts').textContent='Last refreshed: '+ts;
    // Check if mock data
    const hasMock=_allDeals.some(x=>x._source==='mock_no_api_key');
    document.getElementById('mock-warn').style.display=hasMock?'block':'none';
  }catch(e){
    document.getElementById('deals-list').innerHTML='<div class="empty-state" style="color:#f85149;">Error loading deals: '+esc(e.message)+'</div>';
  }
  document.getElementById('refreshBtn').disabled=false;
}

// ── Render deals ──────────────────────────────────────────────────────────
function renderDeals(){
  const el=document.getElementById('deals-list');
  if(!_allDeals.length){
    el.innerHTML='<div class="empty-state">No deals found matching criteria.<br><span style="font-size:12px;">Try adjusting filters or adding markets.</span></div>';
    document.getElementById('deal-count').textContent='0 deals';
    return;
  }
  document.getElementById('deal-count').textContent=_allDeals.length+' deals';
  el.innerHTML=_allDeals.map((d,i)=>dealCard(d,i)).join('');
}

function dealKey(d){return (d.address||d.deal_name||'')+'::'+(d._source_market||'');}

function dealCard(d,i){
  const score=d.cleareye_score||0;
  const scoreColor=score>=70?'#3fb950':score>=50?'#e8a020':score>=30?'#d29922':'#f85149';
  const circ=2*Math.PI*16;
  const fill=circ*(score/100);
  const cat=d._category||'other';
  const catMeta={multifamily:{cls:'chip-mf',lbl:'MF'},retail:{cls:'chip-ret',lbl:'RTL'},industrial:{cls:'chip-ind',lbl:'IND'},office:{cls:'chip-off',lbl:'OFF'},other:{cls:'chip-oth',lbl:'???'}};
  const {cls,lbl}=catMeta[cat]||catMeta.other;
  const bearColor=(d.bear_irr||0)>=8?'#3fb950':'#f85149';
  const isMock=d._source==='mock_no_api_key';
  const topClass=i<3?' top3':'';
  const dk=dealKey(d);
  const isStarred=_watchlistKeys.has(dk);
  const starColor=isStarred?'#d29922':'#484f58';
  const starFill=isStarred?'currentColor':'none';

  // Confidence badge (#127)
  const confMeta={high:{c:'#3fb950',lbl:'High confidence',icon:'●'},medium:{c:'#d29922',lbl:'Medium confidence',icon:'◑'},low:{c:'#f85149',lbl:'Low — fields inferred',icon:'○'}};
  const conf=d._confidence||'medium';
  const {c:confColor,lbl:confLbl,icon:confIcon}=confMeta[conf];
  const assumedTip=(d._assumed_fields||[]).length?'Assumed: '+(d._assumed_fields||[]).join(', '):'All key fields present';

  // Staleness badge (#132)
  const days=d._days_listed||0;
  const staleHtml=days>=14
    ?`<span style="font-size:10px;padding:1px 6px;border-radius:10px;background:rgba(248,81,73,.1);color:#f85149;" title="Listed ${days} days ago">&#9888; ${days}d old</span>`
    :days>0?`<span style="font-size:10px;color:#484f58;" title="First seen">${days}d ago</span>`:'';

  // Cache indicator
  const cacheHtml=d._from_cache?'<span style="font-size:9px;color:#484f58;" title="Served from cache">cached</span>':'';

  // Note indicator
  const noteStr=_notes[dk]||'';
  const noteIcon=noteStr?'<span style="font-size:10px;color:var(--accent);" title="'+esc(noteStr)+'">&#128221;</span>':'';

  const _fdStagger=(i||0)*40;
  return `<div class="deal-card${topClass}" id="card-${dk.replace(/[^a-z0-9]/gi,'_')}" style="animation-delay:${_fdStagger}ms" onclick="analyzeThis(${JSON.stringify(JSON.stringify(d))})">
    <div style="display:flex;align-items:flex-start;gap:12px;">
      <div class="score-ring">
        <svg width="44" height="44" viewBox="0 0 44 44">
          <circle cx="22" cy="22" r="16" fill="none" stroke="#21262d" stroke-width="4"/>
          <circle cx="22" cy="22" r="16" fill="none" stroke="${scoreColor}" stroke-width="4"
            stroke-dasharray="${fill} ${circ-fill}" stroke-linecap="round"/>
        </svg>
        <div class="score-txt" style="color:${scoreColor}">${score}</div>
      </div>
      <div style="flex:1;min-width:0;">
        <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:2px;">
          <span style="font-size:11px;color:#484f58;">#${i+1}</span>
          <span class="deal-card-name" style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;">${esc(d.deal_name||'Untitled Deal')}</span>
          <span class="cat-chip ${cls}">${lbl}</span>
          <span style="font-size:11px;color:${confColor};" title="${assumedTip}">${confIcon} ${conf}</span>
          ${staleHtml}${cacheHtml}${noteIcon}
          ${isMock?'<span style="font-size:10px;color:#d29922;">MOCK</span>':''}
          ${i===0?'<span style="font-size:10px;background:rgba(63,185,80,.15);color:#3fb950;padding:1px 6px;border-radius:10px;">&#9733; Top</span>':''}
        </div>
        <div style="font-size:11px;color:#8b949e;margin-bottom:4px;">${esc(d.address||'')} &middot; ${esc(d._source_market||'')}</div>
        <div class="metric-row">
          <div class="metric-box"><span class="ml">Ask Price</span><span class="mv">${d.asking_price?'$'+fmtNum(d.asking_price):'—'}</span></div>
          <div class="metric-box"><span class="ml">Units</span><span class="mv">${d.units||'—'}</span></div>
          <div class="metric-box"><span class="ml">Cap Rate</span><span class="mv">${d.cap_rate?d.cap_rate+'%':'—'}</span></div>
          <div class="metric-box"><span class="ml">Base IRR</span><span class="mv">${d.base_irr?d.base_irr+'%':'—'}</span></div>
          <div class="metric-box"><span class="ml">Bear IRR</span><span class="mv" style="color:${bearColor}">${d.bear_irr?d.bear_irr+'%':'—'}</span></div>
          ${d.price_per_unit?`<div class="metric-box"><span class="ml">$/unit</span><span class="mv">$${fmtNum(d.price_per_unit)}</span></div>`:''}
        </div>
        <!-- Score breakdown waterfall (#141) -->
        ${(d.score_breakdown||[]).length?`<div style="margin-top:5px;">
          <button onclick="event.stopPropagation();toggleBreakdown('${dk.replace(/'/g,"\\'")}',this)"
            style="font-size:10px;color:#484f58;background:none;border:none;cursor:pointer;padding:0;">&#9660; How scored?</button>
          <div id="bd-${dk.replace(/[^a-z0-9]/gi,'_')}" style="display:none;margin-top:4px;">
            ${(d.score_breakdown||[]).map(row=>{
              const barW=Math.min(100,Math.round((row.pts/score)*100));
              const pts=row.pts>0?'+'+row.pts:row.pts;
              const col=row.pts>0?'#3fb950':'#484f58';
              return `<div style="display:flex;align-items:center;gap:6px;margin-bottom:3px;">
                <div style="flex:1;font-size:10px;color:#8b949e;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:160px;">${esc(row.label)}</div>
                <div style="flex:2;background:#21262d;border-radius:3px;height:6px;overflow:hidden;">
                  <div style="width:${barW}%;height:100%;background:${col};border-radius:3px;"></div>
                </div>
                <div style="font-size:10px;font-weight:700;color:${col};min-width:30px;text-align:right;">${pts}</div>
              </div>`;
            }).join('')}
          </div>
        </div>`:''}
        <!-- Inline note -->
        ${noteStr?`<div style="margin-top:5px;font-size:11px;color:var(--text-secondary);border-left:2px solid var(--accent);padding-left:7px;font-style:italic;">${esc(noteStr.substring(0,100))}${noteStr.length>100?'…':''}</div>`:''}
      </div>
      <!-- Actions column -->
      <div style="flex-shrink:0;display:flex;flex-direction:column;gap:5px;align-items:flex-end;">
        <button onclick="event.stopPropagation();analyzeThis(${JSON.stringify(JSON.stringify(d))})"
          style="padding:5px 10px;background:var(--accent-dim);border:1px solid rgba(232,160,32,.4);color:var(--accent);border-radius:5px;font-size:11px;cursor:pointer;white-space:nowrap;font-family:var(--mono);">Full Analysis &rarr;</button>
        <div style="display:flex;gap:5px;">
          <button onclick="event.stopPropagation();toggleStar(${JSON.stringify(JSON.stringify(d))},'${dk.replace(/'/g,"\\'")}',this)"
            title="${isStarred?'Remove from watchlist':'Add to watchlist'}"
            style="padding:4px 7px;background:none;border:1px solid #30363d;border-radius:4px;cursor:pointer;font-size:14px;color:${starColor};">&#9733;</button>
          <button onclick="event.stopPropagation();openNote('${dk.replace(/'/g,"\\'")}',this)"
            title="Add note" style="padding:4px 7px;background:none;border:1px solid #30363d;border-radius:4px;cursor:pointer;font-size:11px;color:#8b949e;">&#128221;</button>
        </div>
      </div>
    </div>
  </div>`;
}

// ── Category tabs (#125 + #128 watchlist) ────────────────────────────────
function selectCat(cat){
  _currentCat=cat;
  document.querySelectorAll('.cat-tab').forEach(b=>b.classList.toggle('active',b.dataset.cat===cat));
  sessionStorage.setItem('ce_cat',cat);
  if(cat==='watchlist'){showWatchlist();}
  else if(cat==='hud'){showHudDeals();}
  else{loadDeals(false);}
}

// ── HUD Expiring Contracts (#131) ─────────────────────────────────────────
async function showHudDeals(){
  document.getElementById('deals-list').innerHTML='<div class="empty-state"><div style="font-size:24px;animation:spin 1s linear infinite;display:inline-block;">&#8635;</div><div style="margin-top:10px;font-size:12px;">Loading HUD expiring contracts...</div></div>';
  document.getElementById('deal-count').textContent='';
  try{
    const r=await fetch('/api/hud-opportunities?state=AZ&state=GA&state=TX&state=FL&state=CO');
    const d=await r.json();
    const deals=d.deals||[];
    document.getElementById('deal-count').textContent=deals.length+' expiring contracts';
    if(!deals.length){
      document.getElementById('deals-list').innerHTML='<div class="empty-state">No HUD contracts expiring in the next 3 years found for selected states.</div>';
      return;
    }
    document.getElementById('deals-list').innerHTML=deals.map((d,i)=>`
      <div class="deal-card" style="border-left:3px solid #a371f7;">
        <div style="display:flex;align-items:flex-start;gap:12px;">
          <div style="width:44px;height:44px;flex-shrink:0;background:#1a1430;border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:20px;">&#127962;</div>
          <div style="flex:1;min-width:0;">
            <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-bottom:2px;">
              <strong style="font-size:13px;">${esc(d.deal_name||'HUD Property')}</strong>
              <span style="font-size:10px;padding:2px 7px;background:rgba(163,113,247,.15);color:#a371f7;border-radius:10px;">${esc(d.program_type||'HUD')}</span>
              ${d._source==='hud_mock'?'<span style="font-size:10px;color:#d29922;">DEMO</span>':''}
            </div>
            <div style="font-size:11px;color:#8b949e;margin-bottom:5px;">${esc(d.address||'')} &middot; ${esc(d.city||'')} ${esc(d.state||'')} &middot; ${d.units||'?'} units</div>
            <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
              <span style="font-size:12px;padding:3px 8px;border-radius:4px;background:${d.days_to_expiry<365?'rgba(248,81,73,.15)':'rgba(163,113,247,.1)'};color:${d.days_to_expiry<365?'#f85149':'#a371f7'};">
                &#9200; ${esc(d._signal_label||'')}
              </span>
              <span style="font-size:11px;color:#484f58;">Expires: ${esc(d.contract_expires||'')}</span>
            </div>
          </div>
          <div style="flex-shrink:0;">
            <button onclick="analyzeHud(${JSON.stringify(JSON.stringify(d))})"
              style="padding:5px 10px;background:#1a1430;border:1px solid #a371f7;color:#a371f7;border-radius:5px;font-size:11px;cursor:pointer;white-space:nowrap;">Analyze &rarr;</button>
          </div>
        </div>
      </div>`).join('');
  }catch(e){
    document.getElementById('deals-list').innerHTML='<div class="empty-state" style="color:#f85149;">Error: '+esc(e.message)+'</div>';
  }
}

function analyzeHud(json_str){
  const d=JSON.parse(json_str);
  const om=[
    d.deal_name||'HUD Property',
    'Address: '+(d.address||'')+', '+(d.city||'')+' '+(d.state||''),
    'Units: '+(d.units||'?'),
    'Program Type: '+(d.program_type||'HUD'),
    'Affordable contract expires: '+(d.contract_expires||''),
    '',
    '[HUD MFIS value-add opportunity — affordable restrictions lifting.]',
    '[Add current financials (NOI, asking price, cap rate) for full ClearEye analysis.]',
  ].join('\\n');
  sessionStorage.setItem('ce_preload_om', om);
  window.open('/app?preload=1','_blank');
}

function renderCatTabs(){
  const cats=['all','multifamily','retail','industrial','office'];
  cats.forEach(c=>{
    const el=document.getElementById('cnt-'+c);
    if(el)el.textContent=_catCounts[c]??'—';
  });
}

// ── Sort ──────────────────────────────────────────────────────────────────
function selectSort(s){
  _currentSort=s;
  document.querySelectorAll('.sort-btn').forEach(b=>b.classList.toggle('active',b.dataset.sort===s));
  loadDeals(false);
  sessionStorage.setItem('ce_sort',s);
}

// ── Rate Limit UI (#124) ──────────────────────────────────────────────────
function renderRateLimits(rl){
  const bar=document.getElementById('rl-bar');
  const sidebar=document.getElementById('rl-sidebar');
  if(!rl||!Object.keys(rl).length){bar.style.display='none';return;}
  bar.style.display='flex';
  const html=Object.entries(rl).map(([k,v])=>{
    const pct=v.pct||0;
    const st=v.status||'ok';
    return `<div class="rl-badge ${st}">
      <div class="rl-dot"></div>
      <span style="font-weight:600;text-transform:capitalize;">${k}</span>
      <div class="rl-pct-bar"><div class="rl-pct-fill" style="width:${pct}%"></div></div>
      <span>${v.used||0}/${v.limit} ${v.unit||''}</span>
    </div>`;
  }).join('');
  bar.innerHTML=html;
  // Sidebar version (vertical list)
  sidebar.innerHTML=Object.entries(rl).map(([k,v])=>{
    const st=v.status||'ok';
    const stColor=st==='ok'?'#8b949e':st==='warn'?'#d29922':'#f85149';
    return `<div style="margin-bottom:8px;">
      <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px;">
        <span style="text-transform:capitalize;color:#e6edf3;">${k}</span>
        <span style="color:${stColor}">${v.used||0}/${v.limit}</span>
      </div>
      <div style="height:4px;background:#21262d;border-radius:2px;overflow:hidden;">
        <div style="width:${v.pct||0}%;height:100%;background:${stColor};border-radius:2px;"></div>
      </div>
      <div style="font-size:10px;color:#484f58;margin-top:2px;">${v.plan||''} &middot; ${v.unit||''}</div>
    </div>`;
  }).join('');
}

// ── Analyze action ────────────────────────────────────────────────────────
function analyzeThis(json_str){
  // Build a quick OM summary from the deal data, pre-load in /app
  try{
    const d=JSON.parse(json_str);
    const om=[
      d.deal_name||'',
      d.address?'Address: '+d.address:'',
      d.market?'Market: '+d.market:'',
      d.units?'Units: '+d.units:'',
      d.asking_price?'Asking Price: $'+fmtNum(d.asking_price):'',
      d.cap_rate?'Cap Rate: '+d.cap_rate+'%':'',
      d.noi?'NOI: $'+fmtNum(d.noi):'',
      d.price_per_unit?'Price per Unit: $'+fmtNum(d.price_per_unit):'',
      d.base_irr?'Projected IRR: '+d.base_irr+'%':'',
      '\\n[Pre-loaded from ClearEye Deal Aggregator — add full OM text for complete analysis]',
    ].filter(Boolean).join('\\n');
    sessionStorage.setItem('ce_preload_om', om);
    window.open('/app?preload=1','_blank');
  }catch(e){window.open('/app','_blank');}
}

// ── Auto-refresh every 30 min ────────────────────────────────────────────
function startAutoRefresh(){
  clearInterval(_autoRefreshTimer);
  _autoRefreshTimer=setInterval(()=>loadDeals(false), 30*60*1000);
}

// ── Helpers ───────────────────────────────────────────────────────────────
function fmtNum(n){return Number(n).toLocaleString();}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}

// ── CSS animation for spinner ─────────────────────────────────────────────
const st=document.createElement('style');
st.textContent='@keyframes spin{to{transform:rotate(360deg)}}';
document.head.appendChild(st);

// ── Init ──────────────────────────────────────────────────────────────────
// Restore saved state
const savedCat=sessionStorage.getItem('ce_cat')||'all';
const savedSort=sessionStorage.getItem('ce_sort')||'score';
_currentCat=savedCat;
_currentSort=savedSort;
document.querySelectorAll('.cat-tab').forEach(b=>b.classList.toggle('active',b.dataset.cat===savedCat));
document.querySelectorAll('.sort-btn').forEach(b=>b.classList.toggle('active',b.dataset.sort===savedSort));
loadDeals(true);
startAutoRefresh();
fetch('/api/rate-limits').then(r=>r.json()).then(rl=>renderRateLimits(rl)).catch(()=>{});
loadWatchlist();

// ── Watchlist & Notes (#128) ──────────────────────────────────────────────
async function loadWatchlist(){
  try{
    const r=await fetch('/api/watchlist');
    const d=await r.json();
    _watchlistKeys=new Set(d.keys||[]);
    _watchlistDeals=d.deals||[];
    // Build notes index
    _notes={};
    _watchlistDeals.forEach(deal=>{
      const k=dealKey(deal);
      if(deal._note)_notes[k]=deal._note;
    });
  }catch(e){}
}

async function toggleStar(json_str,dk,btn){
  const deal=JSON.parse(json_str);
  if(_watchlistKeys.has(dk)){
    await fetch('/api/watchlist/'+encodeURIComponent(dk),{method:'DELETE'});
    _watchlistKeys.delete(dk);
    btn.style.color='#484f58';
    showToast('Removed from watchlist');
  } else {
    await fetch('/api/watchlist',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({deal_key:dk,deal:deal})});
    _watchlistKeys.add(dk);
    btn.style.color='#d29922';
    showToast('Saved to watchlist ★');
  }
}

// ── Note templates modal (#180) ───────────────────────────────────────────
let _noteCurrentDk='';
let _noteTpls={};

async function openNote(dk,btn){
  _noteCurrentDk=dk;
  document.getElementById('note-textarea').value=_notes[dk]||'';
  document.getElementById('tpl-analysis-btn').style.display='none';
  document.getElementById('note-modal').style.display='flex';
  setTimeout(function(){document.getElementById('note-textarea').focus();},60);
  // Fetch templates async
  try{
    const r=await fetch('/api/notes/template/'+encodeURIComponent(dk));
    _noteTpls=await r.json();
    if(_noteTpls.has_analysis){
      document.getElementById('tpl-analysis-btn').style.display='inline-flex';
    }
  }catch(e){}
}
function loadNoteTemplate(key){
  const text=_noteTpls[key];
  if(!text)return;
  const ta=document.getElementById('note-textarea');
  if(ta.value&&!confirm('Replace current note with this template?'))return;
  ta.value=text;
  ta.focus();
}
async function saveNote(){
  const note=document.getElementById('note-textarea').value;
  await fetch('/api/notes/'+encodeURIComponent(_noteCurrentDk),{
    method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({note})
  });
  _notes[_noteCurrentDk]=note;
  document.getElementById('note-modal').style.display='none';
  renderDeals();
  showToast('Note saved');
}

function showWatchlist(){
  if(!_watchlistDeals.length){
    document.getElementById('deals-list').innerHTML='<div class="empty-state"><div style="font-size:36px;margin-bottom:12px;">★</div><div>No saved deals yet</div><div style="font-size:12px;margin-top:6px;">Click the ★ button on any deal to save it here</div></div>';
    return;
  }
  document.getElementById('deal-count').textContent=_watchlistDeals.length+' saved';
  document.getElementById('deals-list').innerHTML=_watchlistDeals.map((d,i)=>dealCard(d,i)).join('');
}

function showToast(msg){
  const el=document.createElement('div');
  el.textContent=msg;
  el.style.cssText='position:fixed;bottom:20px;right:20px;background:#238636;color:#fff;padding:8px 16px;border-radius:6px;font-size:12px;z-index:9999;animation:fadeIn .2s ease;';
  document.body.appendChild(el);
  setTimeout(()=>el.remove(),2500);
}

// ── Saved Searches (#139) ──────────────────────────────────────────────────
let _savedSearches=[];
async function loadSavedSearches(){
  try{
    const r=await fetch('/api/saved-searches');
    const d=await r.json();
    _savedSearches=d.searches||[];
    renderSavedSearches();
  }catch(e){}
}
function renderSavedSearches(){
  const el=document.getElementById('saved-searches-list');
  const cnt=document.getElementById('saved-count');
  if(!el)return;
  cnt.textContent=_savedSearches.length||'';
  if(!_savedSearches.length){
    el.innerHTML='<div style="font-size:11px;color:#484f58;text-align:center;padding:12px 0;">No saved searches yet</div>';
    return;
  }
  el.innerHTML=_savedSearches.map(s=>{
    const mkts=(s.filters&&s.filters.markets||[]).join(', ')||'All markets';
    const lastRun=s.last_run?new Date(s.last_run).toLocaleDateString():'Never run';
    return `<div style="background:#0d1117;border:1px solid #21262d;border-radius:5px;padding:7px 9px;margin-bottom:5px;display:flex;align-items:center;gap:6px;">
      <div style="flex:1;min-width:0;">
        <div style="font-size:12px;font-weight:600;color:#e6edf3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${esc(s.name)}</div>
        <div style="font-size:10px;color:#484f58;">${esc(mkts)} &middot; last run: ${lastRun}</div>
      </div>
      <button onclick="applySavedSearch('${s.id}')" style="padding:3px 8px;font-size:10px;background:#1f6feb;border:none;color:#fff;border-radius:4px;cursor:pointer;white-space:nowrap;">Run &#9655;</button>
      <button onclick="deleteSavedSearch('${s.id}')" style="padding:3px 6px;font-size:10px;background:transparent;border:1px solid #30363d;color:#8b949e;border-radius:4px;cursor:pointer;">&#x2715;</button>
    </div>`;
  }).join('');
}
function toggleSavedSearchPanel(){
  const p=document.getElementById('saved-search-panel');
  p.style.display=p.style.display==='none'?'block':'none';
  if(p.style.display==='block')loadSavedSearches();
}
async function saveCurrentSearch(){
  const name=prompt('Name this search (e.g. "Phoenix MF >6% cap"):');
  if(!name)return;
  const mkts=selectedMarkets();
  const filters={
    markets:mkts,
    sort:_currentSort,
    category:_currentCat,
    max_price:document.getElementById('f-maxprice').value,
    min_cap_rate:document.getElementById('f-mincap').value,
    limit:document.getElementById('f-limit').value,
  };
  try{
    const r=await fetch('/api/saved-searches',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,filters})});
    const d=await r.json();
    if(d.ok){showToast('Search "'+name+'" saved!');loadSavedSearches();}
  }catch(e){showToast('Error saving search');}
}
async function applySavedSearch(sid){
  try{
    const r=await fetch('/api/saved-searches/'+sid+'/run',{method:'POST'});
    const d=await r.json();
    if(!d.ok)return;
    const f=d.filters||{};
    // Apply markets
    if(f.markets&&f.markets.length){
      document.querySelectorAll('#market-checkboxes input').forEach(cb=>{
        cb.checked=f.markets.includes(cb.value);
      });
    }
    if(f.sort)selectSort(f.sort);
    if(f.category)selectCat(f.category);
    if(f.max_price)document.getElementById('f-maxprice').value=f.max_price;
    if(f.min_cap_rate)document.getElementById('f-mincap').value=f.min_cap_rate;
    if(f.limit)document.getElementById('f-limit').value=f.limit;
    toggleSavedSearchPanel();
    loadDeals(true);
    showToast('Running saved search...');
  }catch(e){}
}
async function deleteSavedSearch(sid){
  await fetch('/api/saved-searches/'+sid,{method:'DELETE'});
  loadSavedSearches();
}
// Close saved search panel on outside click
document.addEventListener('click',e=>{
  const btn=document.getElementById('saved-search-btn');
  const panel=document.getElementById('saved-search-panel');
  if(panel&&!panel.contains(e.target)&&e.target!==btn&&!btn.contains(e.target)){
    panel.style.display='none';
  }
});

// ── Deal Alerts (#134) ─────────────────────────────────────────────────────
async function loadAlerts(){
  try{
    const r=await fetch('/api/alerts');
    const d=await r.json();
    renderAlerts(d.alerts||[]);
  }catch(e){
    document.getElementById('alerts-list').textContent='Error loading alerts';
  }
}
function renderAlerts(alerts){
  const el=document.getElementById('alerts-list');
  if(!alerts.length){
    el.innerHTML='<div style="color:#484f58;font-size:10px;padding:4px 0;">No alerts yet. Click + New to create one.</div>';
    return;
  }
  el.innerHTML=alerts.map(a=>{
    const active=a.active;
    const lastCheck=a.last_checked?new Date(a.last_checked).toLocaleDateString():'Never';
    return `<div style="background:#0d1117;border:1px solid #21262d;border-radius:5px;padding:6px 8px;margin-bottom:5px;">
      <div style="display:flex;align-items:center;gap:4px;margin-bottom:2px;">
        <span style="width:6px;height:6px;border-radius:50%;background:${active?'#3fb950':'#484f58'};flex-shrink:0;"></span>
        <span style="font-size:11px;font-weight:600;color:#e6edf3;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${esc(a.name)}</span>
        <button onclick="toggleAlert('${a.id}',${active?'false':'true'})" style="padding:1px 5px;font-size:9px;background:#21262d;border:1px solid #30363d;color:#8b949e;border-radius:3px;cursor:pointer;">${active?'Pause':'Resume'}</button>
        <button onclick="deleteAlert('${a.id}')" style="padding:1px 5px;font-size:9px;background:transparent;border:1px solid #30363d;color:#8b949e;border-radius:3px;cursor:pointer;">&#x2715;</button>
      </div>
      <div style="font-size:9px;color:#484f58;">${esc(a.email)} &middot; last: ${lastCheck} &middot; ${a.last_match_count||0} matches</div>
    </div>`;
  }).join('');
}
function openAlertModal(){
  let m=document.getElementById('alert-modal');
  if(!m){
    m=document.createElement('div');
    m.id='alert-modal';
    m.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:200;display:flex;align-items:center;justify-content:center;';
    m.innerHTML=`<div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:24px 28px;max-width:400px;width:100%;position:relative;">
      <button onclick="document.getElementById('alert-modal').remove()" style="position:absolute;top:10px;right:10px;background:none;border:none;color:#8b949e;font-size:15px;cursor:pointer;">&#x2715;</button>
      <div style="font-size:1rem;font-weight:700;margin-bottom:14px;">&#128276; Create Deal Alert</div>
      <div style="font-size:11px;color:#8b949e;margin-bottom:12px;">Get an email whenever new deals matching these filters appear.</div>
      <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Alert Name *</label>
      <input id="al-name" placeholder="e.g. Phoenix MF >6% cap" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 10px;font-size:12px;margin-bottom:10px;">
      <label style="font-size:11px;color:#8b949e;display:block;margin-bottom:3px;">Notify Email *</label>
      <input id="al-email" type="email" placeholder="you@example.com" style="width:100%;background:#0d1117;border:1px solid #30363d;color:#e6edf3;border-radius:5px;padding:7px 10px;font-size:12px;margin-bottom:12px;">
      <div style="font-size:11px;color:#8b949e;margin-bottom:6px;">&#9432; Will use your current filter settings (markets, price, cap rate).</div>
      <button onclick="saveAlert()" style="width:100%;padding:8px;background:#238636;border:none;color:#fff;border-radius:6px;font-size:13px;font-weight:600;cursor:pointer;">Create Alert</button>
      <div id="al-result" style="margin-top:8px;font-size:11px;"></div>
    </div>`;
    document.body.appendChild(m);
  } else {
    m.style.display='flex';
  }
}
async function saveAlert(){
  const name=document.getElementById('al-name').value.trim();
  const email=document.getElementById('al-email').value.trim();
  if(!name||!email){alert('Name and email required');return;}
  const filters={
    markets:selectedMarkets(),
    max_price:document.getElementById('f-maxprice').value,
    min_cap_rate:document.getElementById('f-mincap').value,
    category:_currentCat,
  };
  const r=await fetch('/api/alerts',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,email,filters})});
  const d=await r.json();
  if(d.ok){
    document.getElementById('al-result').innerHTML='<span style="color:#3fb950;">&#10003; Alert created! Email notifications active for new matches.</span>';
    setTimeout(()=>{document.getElementById('alert-modal').remove();loadAlerts();},1500);
  }
}
async function deleteAlert(id){
  await fetch('/api/alerts/'+id,{method:'DELETE'});
  loadAlerts();
}
async function toggleAlert(id,active){
  await fetch('/api/alerts/'+id+'/toggle',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({active:active==='true'||active===true})});
  loadAlerts();
}

// Init alerts
loadAlerts();

// ── Scoring profiles (#141) ───────────────────────────────────────────────
let _currentWeights = {cap_rate:8,irr_premium:3,bear_cushion:15,scale:5,ppu_discount:10};
let _scoringOpen=false;
let _loadedProfiles=[];

function toggleScoringPanel(){
  _scoringOpen=!_scoringOpen;
  document.getElementById('scoring-panel').style.display=_scoringOpen?'block':'none';
  if(_scoringOpen) loadScoringProfiles();
}

async function loadScoringProfiles(){
  try{
    const r=await fetch('/api/scoring-profiles');
    const d=await r.json();
    _loadedProfiles=d.profiles||[];
    renderSavedProfiles();
  }catch(e){console.error('load profiles',e);}
}

function renderSavedProfiles(){
  const el=document.getElementById('saved-profiles-list');
  if(!el) return;
  const saved=_loadedProfiles.filter(p=>!p._preset);
  if(!saved.length){el.innerHTML='<div style="font-size:11px;color:#484f58;text-align:center;padding:6px 0;">No saved profiles yet</div>';return;}
  el.innerHTML=saved.map(p=>`
    <div style="display:flex;align-items:center;gap:6px;padding:4px 0;border-bottom:1px solid #21262d;">
      <span style="font-size:11px;flex:1;color:#c9d1d9;">${esc(p.name)}</span>
      ${p.is_active?'<span style="font-size:9px;color:#3fb950;">Active</span>':''}
      <button onclick="applyPreset('${p.id}')" style="font-size:10px;padding:2px 6px;background:#21262d;border:1px solid #30363d;color:#8b949e;border-radius:3px;cursor:pointer;">Apply</button>
      <button onclick="deleteProfile('${p.id}')" style="font-size:10px;padding:2px 6px;background:none;border:1px solid #30363d;color:#f85149;border-radius:3px;cursor:pointer;">X</button>
    </div>`).join('');
}

function updateWeight(key,val){
  _currentWeights[key]=parseFloat(val);
  const el=document.getElementById('wval-'+key);
  if(el) el.textContent=parseFloat(val).toFixed(1);
  // Re-score all loaded deals with new weights
  _rescoreDeals();
}

function _rescoreDeals(){
  // Client-side re-scoring with custom weights (no server round-trip for quick feedback)
  _allDeals.forEach(d=>{
    try{
      const cap=parseFloat(d.cap_rate||5);
      const irr=parseFloat(d.projected_irr||cap*2.5);
      const price=parseFloat(d.asking_price||1);
      const units=parseInt(d.units||1);
      const ppu=price/units;
      const bear=irr*0.7;
      const w=_currentWeights;
      const cap_pts=cap*(w.cap_rate||8);
      const irr_pts=Math.max(0,(irr-8)*(w.irr_premium||3));
      const bear_pts=bear>=8?(w.bear_cushion||15):0;
      const scale_pts=units>=50?(w.scale||5):0;
      const ppu_pts=ppu<150000?(w.ppu_discount||10):0;
      const raw=cap_pts+irr_pts+bear_pts+scale_pts+ppu_pts;
      d.cleareye_score=Math.min(100,Math.max(0,Math.round(raw)));
      d.bear_irr=parseFloat(bear.toFixed(1));
      d.base_irr=parseFloat(irr.toFixed(1));
      d.score_breakdown=[
        {label:`Cap Rate (${cap.toFixed(1)}%)`,pts:Math.round(cap_pts*10)/10},
        {label:`IRR premium (${irr.toFixed(1)}% - 8%)`,pts:Math.round(irr_pts*10)/10},
        {label:`Bear cushion (${bear.toFixed(1)}%)`,pts:Math.round(bear_pts*10)/10},
        {label:`Scale (${units} units)`,pts:scale_pts},
        {label:`Value entry ($${Math.round(ppu).toLocaleString()}/unit)`,pts:ppu_pts},
      ];
    }catch(e){}
  });
  _sortDeals();
  renderDeals();
}

function _setSliders(w){
  for(const [key,val] of Object.entries(w)){
    const sl=document.getElementById('wslider-'+key);
    const vl=document.getElementById('wval-'+key);
    if(sl){sl.value=val;}
    if(vl){vl.textContent=parseFloat(val).toFixed(1);}
    _currentWeights[key]=parseFloat(val);
  }
}

const _PRESETS={
  preset_core_plus:   {cap_rate:6,irr_premium:3,bear_cushion:20,scale:5,ppu_discount:5},
  preset_value_add:   {cap_rate:8,irr_premium:4,bear_cushion:15,scale:8,ppu_discount:10},
  preset_opportunistic:{cap_rate:5,irr_premium:6,bear_cushion:10,scale:5,ppu_discount:10},
};

function applyPreset(pid){
  // Check loaded saved profiles first
  const saved=(_loadedProfiles||[]).find(p=>p.id===pid);
  const w=saved?saved.weights:_PRESETS[pid];
  if(!w){return;}
  _setSliders(w);
  _rescoreDeals();
  document.querySelectorAll('.preset-btn').forEach(b=>b.style.borderColor=b.dataset.pid===pid?'#58a6ff':'#30363d');
  document.getElementById('active-profile-badge').textContent=saved?saved.name:(pid.replace('preset_','').replace('_',' '));
}

function resetWeights(){
  _setSliders({cap_rate:8,irr_premium:3,bear_cushion:15,scale:5,ppu_discount:10});
  _rescoreDeals();
  document.getElementById('active-profile-badge').textContent='';
  document.querySelectorAll('.preset-btn').forEach(b=>b.style.borderColor='#30363d');
}

async function saveCurrentWeights(){
  const name=(document.getElementById('profile-name-input').value||'').trim();
  if(!name){alert('Enter a profile name');return;}
  try{
    const r=await fetch('/api/scoring-profiles',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name,weights:_currentWeights})});
    const d=await r.json();
    if(d.ok){document.getElementById('profile-name-input').value='';loadScoringProfiles();}
  }catch(e){alert('Save failed: '+e);}
}

async function deleteProfile(pid){
  if(!confirm('Delete this profile?')) return;
  try{
    await fetch('/api/scoring-profiles/'+pid,{method:'DELETE'});
    loadScoringProfiles();
  }catch(e){}
}

function toggleBreakdown(dk,btn){
  const id='bd-'+dk.replace(/[^a-z0-9]/gi,'_');
  const el=document.getElementById(id);
  if(!el) return;
  const open=el.style.display!=='none';
  el.style.display=open?'none':'block';
  btn.textContent=open?'▼ How scored?':'▲ How scored?';
}

// Close scoring panel when clicking outside
document.addEventListener('click',e=>{
  if(_scoringOpen && !e.target.closest('#scoring-panel') && !e.target.closest('#scoring-btn')){
    _scoringOpen=false;
    document.getElementById('scoring-panel').style.display='none';
  }
});
</script>

<!-- Note templates modal (#180) -->
<div id="note-modal" style="display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:1000;align-items:flex-start;justify-content:center;padding-top:60px;"
     onclick="if(event.target===this)this.style.display='none'">
  <div style="background:#161b22;border:1px solid #30363d;border-radius:12px;padding:20px;width:560px;max-width:94vw;max-height:80vh;display:flex;flex-direction:column;">
    <div style="display:flex;align-items:center;gap:8px;margin-bottom:12px;flex-wrap:wrap;">
      <span style="font-size:13px;font-weight:700;margin-right:4px;">&#128221; Deal Note</span>
      <button onclick="loadNoteTemplate('quick_scan')"
              style="padding:3px 9px;font-size:11px;background:#21262d;border:1px solid #30363d;color:#c9d1d9;border-radius:5px;cursor:pointer;">
        Quick Scan</button>
      <button onclick="loadNoteTemplate('loi_checklist')"
              style="padding:3px 9px;font-size:11px;background:#21262d;border:1px solid #30363d;color:#c9d1d9;border-radius:5px;cursor:pointer;">
        LOI Checklist</button>
      <button id="tpl-analysis-btn" onclick="loadNoteTemplate('full_analysis')"
              style="display:none;padding:3px 9px;font-size:11px;background:rgba(88,166,255,.1);border:1px solid rgba(88,166,255,.3);color:#58a6ff;border-radius:5px;cursor:pointer;">
        &#9889; From Analysis</button>
      <button onclick="document.getElementById('note-modal').style.display='none'"
              style="margin-left:auto;background:none;border:none;color:#8b949e;font-size:16px;cursor:pointer;line-height:1;">&#x2715;</button>
    </div>
    <textarea id="note-textarea" placeholder="Add notes about this deal..."
              style="flex:1;min-height:280px;background:#0d1117;border:1px solid #30363d;border-radius:7px;color:#e6edf3;font-size:12.5px;padding:10px 12px;resize:vertical;font-family:monospace;line-height:1.55;outline:none;"></textarea>
    <div style="display:flex;gap:8px;margin-top:12px;justify-content:flex-end;">
      <button onclick="document.getElementById('note-modal').style.display='none'"
              style="padding:7px 16px;background:#21262d;border:1px solid #30363d;color:#c9d1d9;border-radius:6px;cursor:pointer;font-size:12px;">
        Cancel</button>
      <button onclick="saveNote()"
              style="padding:7px 16px;background:#238636;border:none;color:#fff;border-radius:6px;cursor:pointer;font-size:12px;font-weight:600;">
        Save Note</button>
    </div>
  </div>
</div>

</body>
</html>"""


if __name__ == "__main__":
    import socket
    # #271: Use PORT env var for Render/Railway/Fly.io; default to 5052 locally
    _port = int(os.environ.get("PORT", 5052))
    hostname = socket.gethostname()
    try:
        local_ip = socket.gethostbyname(hostname)
    except Exception:
        local_ip = "localhost"
    print("ClearEye starting...")
    print(f"  Local:   http://localhost:{_port}")
    print(f"  Network: http://{local_ip}:{_port}")
    print(f"  Full pipeline: stress test + validation + macro + bias + premortem + 5 advisors")
    app.run(host="0.0.0.0", port=_port, debug=False)
