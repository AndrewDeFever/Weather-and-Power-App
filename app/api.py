from __future__ import annotations

import difflib
import json
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from app.power_router import get_power_status, probe_power_status

log = logging.getLogger("wnp")

app = FastAPI(title="Weather & Power App")

# -----------------------------
# Static frontend (served locally for dev / unified repo)
# -----------------------------
# NOTE: In prod your frontend is still hosted via S3 static website,
# but we keep these in-repo and can serve locally.
BASE_DIR = Path(__file__).resolve().parent.parent
FRONTEND_DIR = BASE_DIR / "frontend"
STATIC_DIR = FRONTEND_DIR / "static"


# -----------------------------
# Security headers middleware
# -----------------------------
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        resp = await call_next(request)

        # Basic hardened headers
        resp.headers["X-Content-Type-Options"] = "nosniff"
        resp.headers["X-Frame-Options"] = "DENY"
        resp.headers["Referrer-Policy"] = "no-referrer"

        # HSTS (only meaningful over HTTPS)
        resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"

        # Permissions Policy (lock down common sensor / browser APIs)
        resp.headers["Permissions-Policy"] = (
            "accelerometer=(), autoplay=(), camera=(), encrypted-media=(), fullscreen=(), "
            "geolocation=(), gyroscope=(), magnetometer=(), microphone=(), midi=(), payment=(), "
            "picture-in-picture=(), publickey-credentials-get=(), usb=()"
        )

        # CSP - keep it strict but compatible with your static frontend.
        # Adjust as needed if you add external assets.
        resp.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "img-src 'self' data:; "
            "style-src 'self' 'unsafe-inline'; "
            "script-src 'self'; "
            "connect-src 'self'; "
            "base-uri 'self'; "
            "frame-ancestors 'none'; "
            "form-action 'self'; "
            "object-src 'none'"
        )

        return resp


app.add_middleware(SecurityHeadersMiddleware)

# -----------------------------
# Rate limiting (best-effort in-memory, per-client IP)
# -----------------------------
# Tune these for your audit posture / UX tolerance.
# - Burst: 30 requests immediately
# - Sustained: 60 requests per minute (1/sec)
RL_BURST = 30
RL_PER_MIN = 60.0
_rl_refill_per_sec = RL_PER_MIN / 60.0

# key -> {"tokens": float, "ts": float}
_rl_buckets: Dict[str, Dict[str, float]] = {}

# Best-effort instance identifier for debugging rate limiting in Lambda.
# - AWS_LAMBDA_LOG_STREAM_NAME is stable per execution environment.
# - Fallback to a process-unique UUID.
_INSTANCE_ID = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME") or str(uuid.uuid4())


def _client_ip(request: Request) -> str:
    # If behind CloudFront/API GW, X-Forwarded-For is typically present.
    # Use the first IP in the list (originating client).
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",")[0].strip()
        if first:
            return first
    # fallback
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _rate_limit_allow(key: str) -> bool:
    now = time.time()
    b = _rl_buckets.get(key)
    if not b:
        _rl_buckets[key] = {"tokens": float(RL_BURST - 1), "ts": now}
        return True

    # Refill tokens
    elapsed = now - b["ts"]
    b["tokens"] = min(float(RL_BURST), b["tokens"] + elapsed * _rl_refill_per_sec)

    # Spend one token if available
    if b["tokens"] < 1.0:
        b["ts"] = now
        return False

    b["tokens"] = b["tokens"] - 1.0
    b["ts"] = now
    return True


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Only rate limit API endpoints (leave static/index alone).
        if request.url.path.startswith("/api/"):
            ip = _client_ip(request)
            allowed = _rate_limit_allow(ip)

            # Observability headers to prove limiter is on-path and detect multi-instance.
            # Safe for audits: no secrets, just control-plane debugging.
            base_headers = {
                "X-RateLimit-Path": "1",
                "X-RateLimited": "0" if allowed else "1",
                "X-RateLimit-Client-IP": ip,
                "X-Instance-Id": _INSTANCE_ID,
            }

            if not allowed:
                # Keep response shape compatible + HTTP 200 (can tighten later).
                msg = "Rate limit exceeded. Please retry shortly."
                payload = {
                    "query": None,
                    "resolved": {"type": "unknown", "name": "", "site_id": None},
                    "provider": provider_info(None),
                    "weather": empty_weather(error=msg),
                    "power": empty_power(None, msg, ok=False),
                    "probe": None,
                }
                resp = JSONResponse(status_code=200, content=payload)
                resp.headers.update(base_headers)
                return resp

            resp = await call_next(request)
            resp.headers.update(base_headers)
            return resp

        return await call_next(request)


app.add_middleware(RateLimitMiddleware)

# -----------------------------
# Data / lookups
# -----------------------------
DATA_DIR = Path(__file__).resolve().parent / "data"
SITES_PATH = DATA_DIR / "sites.json"

UTILITY_ALLOWLIST = {"PSO", "OGE", "EVERGY", "ONCOR", "AUSTIN"}


def load_sites() -> Dict[str, Any]:
    if not SITES_PATH.exists():
        return {}
    return json.loads(SITES_PATH.read_text(encoding="utf-8"))


SITES = load_sites()


def provider_info(utility: Optional[str]) -> Dict[str, Any]:
    return {"utility": utility or None}


def empty_weather(error: Optional[str] = None) -> Dict[str, Any]:
    return {
        "ok": False,
        "error": error,
        "current": None,
        "forecast": None,
    }


def empty_power(utility: Optional[str], error: Optional[str], ok: bool = False) -> Dict[str, Any]:
    return {
        "ok": ok,
        "error": error,
        "utility": utility,
        "status": None,
    }


def resolve_site(site_id: str) -> Optional[Dict[str, Any]]:
    if not site_id:
        return None
    s = SITES.get(site_id)
    return s


def resolve_utility_override(raw_utility: Optional[str]) -> Optional[str]:
    if not raw_utility:
        return None
    u = raw_utility.strip().upper()
    if len(u) > 16:
        return None
    if u not in UTILITY_ALLOWLIST:
        return None
    return u


# -----------------------------
# External calls / timeouts
# -----------------------------
DEFAULT_TIMEOUT = (3.0, 8.0)  # (connect, read)

# Thread pool for any blocking requests usage (requests library)
pool = ThreadPoolExecutor(max_workers=12)


def fetch_json(url: str, timeout: Tuple[float, float] = DEFAULT_TIMEOUT) -> Any:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


# -----------------------------
# Routes
# -----------------------------
@app.get("/")
def index():
    # Serve index.html if present (local dev). In prod, index is from S3 website.
    p = FRONTEND_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return JSONResponse({"ok": True, "message": "Frontend not found in this environment."})


# Mount /static for local dev if exists
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/api/status")
def api_status(
    request: Request,
    # Input controls hardened:
    site_id: Optional[str] = Query(default=None, max_length=128),
    lat: Optional[float] = Query(default=None),
    lon: Optional[float] = Query(default=None),
    q: Optional[str] = Query(default=None, max_length=128),
    query: Optional[str] = Query(default=None, max_length=128),
    utility: Optional[str] = Query(default=None, max_length=16),
):
    """
    Returns a single status snapshot combining weather + power context.
    Supports site_id OR lat/lon.
    """

    # Normalize "q" vs "query" (UI can send either)
    effective_query = query or q

    # Validate lat/lon if provided
    if lat is not None and (lat < -90.0 or lat > 90.0):
        return JSONResponse(status_code=200, content={"error": "Invalid latitude range."})
    if lon is not None and (lon < -180.0 or lon > 180.0):
        return JSONResponse(status_code=200, content={"error": "Invalid longitude range."})

    resolved = {"type": "unknown", "name": "", "site_id": None}
    utility_override = resolve_utility_override(utility)

    try:
        # Resolve location
        if site_id:
            site = resolve_site(site_id)
            if not site:
                msg = "Unknown site_id."
                payload = {
                    "query": effective_query,
                    "resolved": resolved,
                    "provider": provider_info(utility_override),
                    "weather": empty_weather(error=msg),
                    "power": empty_power(utility_override, msg, ok=False),
                    "probe": None,
                }
                return JSONResponse(status_code=200, content=payload)

            resolved = {
                "type": "site_id",
                "name": site.get("name") or "",
                "site_id": site_id,
            }
            lat = float(site["lat"])
            lon = float(site["lon"])
            if not utility_override:
                utility_override = (site.get("utility") or "").strip().upper() or None

        elif lat is not None and lon is not None:
            resolved = {"type": "latlon", "name": "", "site_id": None}
        else:
            msg = "Provide site_id or lat/lon."
            payload = {
                "query": effective_query,
                "resolved": resolved,
                "provider": provider_info(utility_override),
                "weather": empty_weather(error=msg),
                "power": empty_power(utility_override, msg, ok=False),
                "probe": None,
            }
            return JSONResponse(status_code=200, content=payload)

        # Power status (may call provider routers)
        power = get_power_status(lat=lat, lon=lon, utility_override=utility_override)

        # Optional probe diagnostics
        probe = probe_power_status(lat=lat, lon=lon, utility_override=utility_override)

        # Weather (likely handled in another module in your repo; placeholder shape here)
        # If your repo has a real weather fetcher, keep using it.
        weather = {"ok": True, "error": None, "current": None, "forecast": None}

        payload = {
            "query": effective_query,
            "resolved": resolved,
            "provider": provider_info(utility_override),
            "weather": weather,
            "power": power,
            "probe": probe,
        }
        return JSONResponse(status_code=200, content=payload)

    except Exception:
        # No internal error leakage
        log.exception("Unhandled error in /api/status")
        msg = "Internal error."
        payload = {
            "query": effective_query,
            "resolved": resolved,
            "provider": provider_info(utility_override),
            "weather": empty_weather(error=msg),
            "power": empty_power(utility_override, msg, ok=False),
            "probe": None,
        }
        return JSONResponse(status_code=200, content=payload)
