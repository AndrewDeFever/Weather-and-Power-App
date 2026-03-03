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
from starlette.responses import Response

from app.power_router import get_power_status, probe_power_status

log = logging.getLogger("wnp")

app = FastAPI(title="Weather & Power Status", version="0.8.2")

# ----------------------------
# Security headers (browser + API hardening)
# ----------------------------
CSP = (
    "default-src 'self'; "
    "img-src 'self' data:; "
    "style-src 'self' 'unsafe-inline'; "
    "script-src 'self'; "
    "connect-src 'self'; "
    "base-uri 'none'; "
    "frame-ancestors 'none'"
)

# Only enable HSTS if the app is served exclusively over HTTPS in production.
HSTS = "max-age=31536000; includeSubDomains"


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response: Response = await call_next(request)

        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        response.headers["Content-Security-Policy"] = CSP
        response.headers["Strict-Transport-Security"] = HSTS

        return response


app.add_middleware(SecurityHeadersMiddleware)

# ----------------------------
# Rate limiting (token bucket, per client IP)
# ----------------------------
# Defaults: allow short bursts while limiting sustained abuse.
# - Burst: 30 requests immediately
# - Sustained: 60 requests per minute (1/sec)
RL_BURST = 30
RL_PER_MIN = 60.0
_rl_refill_per_sec = RL_PER_MIN / 60.0

# key -> {"tokens": float, "ts": float}
_rl_buckets: Dict[str, Dict[str, float]] = {}

# Best-effort instance identifier for debugging rate limiting in Lambda.
# AWS_LAMBDA_LOG_STREAM_NAME is stable per execution environment; fallback to UUID.
_INSTANCE_ID = os.environ.get("AWS_LAMBDA_LOG_STREAM_NAME") or str(uuid.uuid4())


def _client_ip(request: Request) -> str:
    # If behind CloudFront/API GW, X-Forwarded-For is typically present.
    # Use the first IP in the list (original client).
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _rate_limit_allow(key: str) -> bool:
    now = time.time()
    b = _rl_buckets.get(key)
    if not b:
        _rl_buckets[key] = {"tokens": float(RL_BURST - 1), "ts": now}
        return True

    tokens = float(b.get("tokens", RL_BURST))
    last = float(b.get("ts", now))

    # refill
    tokens = min(float(RL_BURST), tokens + (now - last) * _rl_refill_per_sec)

    if tokens < 1.0:
        b["tokens"] = tokens
        b["ts"] = now
        return False

    b["tokens"] = tokens - 1.0
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
            rl_headers = {
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
                resp.headers.update(rl_headers)
                return resp

            resp = await call_next(request)
            resp.headers.update(rl_headers)
            return resp

        return await call_next(request)


# Add rate limiter AFTER security headers (either order is fine)
app.add_middleware(RateLimitMiddleware)

# ----------------------------
# Global error handling (guarantee JSON responses)
# ----------------------------
@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    log.exception("Unhandled exception path=%s", request.url.path)

    err_client = "Internal server error"
    payload = {
        "query": None,
        "resolved": {"type": "unknown", "name": "", "site_id": None},
        "provider": provider_info(None),
        "weather": empty_weather(error=err_client),
        "power": empty_power(None, err_client, ok=False),
        "probe": None,
    }
    return JSONResponse(status_code=200, content=payload)


# ----------------------------
# Budgets / timeouts (keep under CloudFront/origin timeouts)
# ----------------------------
WEATHER_TOTAL_BUDGET_S = 8.0
POWER_TOTAL_BUDGET_S = 14.0
HTTP_TIMEOUT_S = 5.0


# ----------------------------
# Power cache (best-effort fallback on timeouts)
# ----------------------------
POWER_CACHE_TTL_S = 120  # seconds
_power_cache: Dict[str, Dict[str, Any]] = {}  # key -> {"ts": float, "payload": dict}


def _power_cache_key(resolved: Dict[str, Any]) -> str:
    sid = resolved.get("site_id")
    if sid:
        return f"site:{sid}"
    lat = to_float(resolved.get("lat"))
    lon = to_float(resolved.get("lon"))
    if lat is None or lon is None:
        return "unknown"
    return f"ll:{lat:.3f},{lon:.3f}"


def _cache_power_if_ok(resolved: Dict[str, Any], power_payload: Any) -> None:
    try:
        if not isinstance(power_payload, dict):
            return
        meta = power_payload.get("meta")
        if isinstance(meta, dict) and meta.get("ok") is True:
            _power_cache[_power_cache_key(resolved)] = {"ts": time.time(), "payload": power_payload}
    except Exception:
        pass


def _cached_power_on_timeout(resolved: Dict[str, Any], site_utility: Optional[str]) -> Dict[str, Any]:
    try:
        key = _power_cache_key(resolved)
        cached = _power_cache.get(key)
        if cached:
            age = time.time() - float(cached.get("ts", 0.0))
            if age <= POWER_CACHE_TTL_S:
                payload = cached.get("payload")
                if isinstance(payload, dict):
                    meta = payload.get("meta")
                    if not isinstance(meta, dict):
                        meta = {}
                        payload["meta"] = meta
                    meta["cached"] = True
                    meta["cache_age_s"] = round(age, 1)
                    meta["error"] = "Live power lookup timed out; serving cached result"
                    meta["ok"] = True
                    return payload
    except Exception:
        pass

    return empty_power(site_utility, "Power lookup timed out", ok=False)


# ----------------------------
# Helpers
# ----------------------------
ALLOWED_UTILITIES = {"PSO", "OGE", "EVERGY", "ONCOR", "AUSTIN"}


def to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if s == "":
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def parse_latlon(q: str) -> Optional[Tuple[float, float]]:
    if not q or "," not in q:
        return None
    a, b = q.split(",", 1)
    lat = to_float(a)
    lon = to_float(b)
    if lat is None or lon is None:
        return None
    return (lat, lon)


def provider_info(utility: Optional[str]) -> Dict[str, Any]:
    u = (utility or "").strip().upper()
    if u == "PSO":
        return {
            "utility": "PSO",
            "name": "PSO",
            "outage_map": "https://outagemap.psoklahoma.com/",
            "platform": "KUBRA",
        }
    if u == "OGE":
        return {
            "utility": "OGE",
            "name": "OG&E",
            "outage_map": "https://www.oge.com/wps/portal/oge/outages/system-watch",
            "platform": "ESRI",
        }
    if u == "EVERGY":
        return {
            "utility": "EVERGY",
            "name": "Evergy",
            "outage_map": "https://outagemap.evergy.com/",
            "platform": "KUBRA",
        }
    if u == "ONCOR":
        return {
            "utility": "ONCOR",
            "name": "Oncor",
            "outage_map": "https://stormcenter.oncor.com/",
            "platform": "KUBRA",
        }
    if u == "AUSTIN":
        return {
            "utility": "AUSTIN",
            "name": "Austin Energy",
            "outage_map": "https://outagemap.austinenergy.com/",
            "platform": "KUBRA",
        }
    return {"utility": u or None}


def empty_weather(error: Optional[str] = None) -> Dict[str, Any]:
    return {
        "meta": {"ok": False, "error": error},
        "current": None,
        "forecast": None,
        "hourly": None,
        "alerts": None,
    }


def empty_power(utility: Optional[str], error: Optional[str], ok: bool = False) -> Dict[str, Any]:
    return {"meta": {"ok": ok, "error": error}, "utility": (utility or None), "status": None}


# ----------------------------
# Sites file
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
SITES_PATH = BASE_DIR / "data" / "sites.json"


def load_sites() -> Dict[str, Any]:
    try:
        if not SITES_PATH.exists():
            return {}
        return json.loads(SITES_PATH.read_text(encoding="utf-8"))
    except Exception:
        log.exception("Failed to load sites.json")
        return {}


SITES = load_sites()

# ----------------------------
# Weather (NWS)
# ----------------------------
NWS_POINTS = "https://api.weather.gov/points/{lat},{lon}"
NWS_OBS = "https://api.weather.gov/stations/{station}/observations/latest"
NWS_FORECAST = "https://api.weather.gov/gridpoints/{wfo}/{x},{y}/forecast"
NWS_FORECAST_HOURLY = "https://api.weather.gov/gridpoints/{wfo}/{x},{y}/forecast/hourly"
NWS_ALERTS = "https://api.weather.gov/alerts/active?point={lat},{lon}"

DEFAULT_HEADERS = {
    "User-Agent": "WeatherPowerStatus/1.0 (contact: subrealstudios.com)",
    "Accept": "application/geo+json, application/json",
}


def fetch_weather(lat: float, lon: float) -> Dict[str, Any]:
    points_url = NWS_POINTS.format(lat=lat, lon=lon)
    r = requests.get(points_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    pts = r.json()

    props = (pts.get("properties") or {})
    stations_url = props.get("observationStations")
    forecast_url = props.get("forecast")
    forecast_hourly_url = props.get("forecastHourly")

    station_id = None
    if stations_url:
        rs = requests.get(stations_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
        rs.raise_for_status()
        stations = rs.json()
        features = (stations.get("features") or [])
        if features:
            station_id = ((features[0] or {}).get("properties") or {}).get("stationIdentifier")

    current = None
    if station_id:
        ro = requests.get(
            NWS_OBS.format(station=station_id), headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S
        )
        ro.raise_for_status()
        obs = ro.json()
        current = (obs.get("properties") or {})

    forecast = None
    if forecast_url:
        rf = requests.get(forecast_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
        rf.raise_for_status()
        forecast = (rf.json().get("properties") or {})

    hourly = None
    if forecast_hourly_url:
        rh = requests.get(forecast_hourly_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
        rh.raise_for_status()
        hourly = (rh.json().get("properties") or {})

    alerts = None
    ra = requests.get(NWS_ALERTS.format(lat=lat, lon=lon), headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
    if ra.status_code == 200:
        alerts = ra.json()

    return {
        "meta": {"ok": True, "error": None},
        "current": current,
        "forecast": forecast,
        "hourly": hourly,
        "alerts": alerts,
    }


# ----------------------------
# Frontend static mount (local dev)
# ----------------------------
FRONTEND_DIR = BASE_DIR.parent / "frontend"
STATIC_DIR = FRONTEND_DIR / "static"

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    idx = FRONTEND_DIR / "index.html"
    if idx.exists():
        return FileResponse(str(idx))
    return JSONResponse({"ok": True, "message": "Frontend not found in this environment."})


# ----------------------------
# Main API endpoint
# ----------------------------
@app.get("/api/status")
def api_status(
    request: Request,
    site_id: Optional[str] = Query(default=None, max_length=128),
    lat: Optional[float] = Query(default=None),
    lon: Optional[float] = Query(default=None),
    q: Optional[str] = Query(default=None, max_length=128),
    query: Optional[str] = Query(default=None, max_length=128),
    utility: Optional[str] = Query(default=None, max_length=16),
):
    # prefer explicit query param if both present
    effective_q = query if query is not None else q

    # utility override allowlist
    utility_override = None
    if utility:
        u = utility.strip().upper()
        if u in ALLOWED_UTILITIES:
            utility_override = u

    resolved: Dict[str, Any] = {"type": "unknown", "name": "", "site_id": None, "lat": None, "lon": None}

    # resolve input -> lat/lon
    if site_id:
        site = SITES.get(site_id)
        if not site:
            msg = "Unknown site_id"
            return JSONResponse(
                status_code=200,
                content={
                    "query": effective_q,
                    "resolved": resolved,
                    "provider": provider_info(utility_override),
                    "weather": empty_weather(error=msg),
                    "power": empty_power(utility_override, msg, ok=False),
                    "probe": None,
                },
            )
        resolved = {
            "type": "site_id",
            "name": site.get("name") or "",
            "site_id": site_id,
            "lat": site.get("lat"),
            "lon": site.get("lon"),
        }
        lat = to_float(site.get("lat"))
        lon = to_float(site.get("lon"))
        if not utility_override:
            util = (site.get("utility") or "").strip().upper()
            if util in ALLOWED_UTILITIES:
                utility_override = util

    elif lat is not None and lon is not None:
        # range validation
        if lat < -90.0 or lat > 90.0:
            msg = "Invalid latitude range"
            return JSONResponse(
                status_code=200,
                content={
                    "query": effective_q,
                    "resolved": resolved,
                    "provider": provider_info(utility_override),
                    "weather": empty_weather(error=msg),
                    "power": empty_power(utility_override, msg, ok=False),
                    "probe": None,
                },
            )
        if lon < -180.0 or lon > 180.0:
            msg = "Invalid longitude range"
            return JSONResponse(
                status_code=200,
                content={
                    "query": effective_q,
                    "resolved": resolved,
                    "provider": provider_info(utility_override),
                    "weather": empty_weather(error=msg),
                    "power": empty_power(utility_override, msg, ok=False),
                    "probe": None,
                },
            )

        resolved = {"type": "latlon", "name": "", "site_id": None, "lat": lat, "lon": lon}

    elif effective_q:
        # attempt parse "lat,lon" from q/query
        ll = parse_latlon(effective_q)
        if ll:
            lat, lon = ll
            if lat < -90.0 or lat > 90.0 or lon < -180.0 or lon > 180.0:
                msg = "Invalid lat/lon range"
                return JSONResponse(
                    status_code=200,
                    content={
                        "query": effective_q,
                        "resolved": resolved,
                        "provider": provider_info(utility_override),
                        "weather": empty_weather(error=msg),
                        "power": empty_power(utility_override, msg, ok=False),
                        "probe": None,
                    },
                )
            resolved = {"type": "latlon", "name": "", "site_id": None, "lat": lat, "lon": lon}
        else:
            # fuzzy match against site names
            try:
                names = {sid: (SITES[sid].get("name") or "") for sid in SITES.keys()}
                choices = list(names.values())
                matches = difflib.get_close_matches(effective_q, choices, n=1, cutoff=0.5)
                if matches:
                    match_name = matches[0]
                    match_sid = next((sid for sid, nm in names.items() if nm == match_name), None)
                    if match_sid:
                        site = SITES.get(match_sid) or {}
                        resolved = {
                            "type": "site_id",
                            "name": site.get("name") or "",
                            "site_id": match_sid,
                            "lat": site.get("lat"),
                            "lon": site.get("lon"),
                        }
                        lat = to_float(site.get("lat"))
                        lon = to_float(site.get("lon"))
                        if not utility_override:
                            util = (site.get("utility") or "").strip().upper()
                            if util in ALLOWED_UTILITIES:
                                utility_override = util
            except Exception:
                pass

    if lat is None or lon is None:
        msg = "Provide site_id or lat/lon"
        return JSONResponse(
            status_code=200,
            content={
                "query": effective_q,
                "resolved": resolved,
                "provider": provider_info(utility_override),
                "weather": empty_weather(error=msg),
                "power": empty_power(utility_override, msg, ok=False),
                "probe": None,
            },
        )

    # ----------------------------
    # Execute weather + power with time budgets
    # ----------------------------
    resolved["lat"] = lat
    resolved["lon"] = lon

    def _call_weather() -> Dict[str, Any]:
        return fetch_weather(lat, lon)

    def _call_power() -> Dict[str, Any]:
        return get_power_status(lat=lat, lon=lon, utility_override=utility_override)

    def _call_probe() -> Dict[str, Any]:
        return probe_power_status(lat=lat, lon=lon, utility_override=utility_override)

    weather_payload: Dict[str, Any] = empty_weather(error="Weather unavailable")
    power_payload: Dict[str, Any] = empty_power(utility_override, "Power unavailable", ok=False)
    probe_payload: Optional[Dict[str, Any]] = None

    with ThreadPoolExecutor(max_workers=3) as ex:
        wf = ex.submit(_call_weather)
        pf = ex.submit(_call_power)
        prf = ex.submit(_call_probe)

        try:
            weather_payload = wf.result(timeout=WEATHER_TOTAL_BUDGET_S)
        except FuturesTimeout:
            weather_payload = empty_weather(error="Weather lookup timed out")
        except Exception:
            log.exception("Weather lookup failed")
            weather_payload = empty_weather(error="Weather lookup failed")

        try:
            power_payload = pf.result(timeout=POWER_TOTAL_BUDGET_S)
            _cache_power_if_ok(resolved, power_payload)
        except FuturesTimeout:
            power_payload = _cached_power_on_timeout(resolved, utility_override)
        except Exception:
            log.exception("Power lookup failed")
            power_payload = empty_power(utility_override, "Power lookup failed", ok=False)

        try:
            probe_payload = prf.result(timeout=POWER_TOTAL_BUDGET_S)
        except FuturesTimeout:
            probe_payload = None
        except Exception:
            log.exception("Probe failed")
            probe_payload = None

    payload = {
        "query": effective_q,
        "resolved": resolved,
        "provider": provider_info(utility_override),
        "weather": weather_payload,
        "power": power_payload,
        "probe": probe_payload,
    }
    return JSONResponse(status_code=200, content=payload)
