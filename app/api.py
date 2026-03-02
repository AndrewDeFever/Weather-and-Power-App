from __future__ import annotations

import difflib
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import FastAPI, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.power_router import get_power_status, probe_power_status

log = logging.getLogger("wnp")

app = FastAPI(title="Weather & Power Status", version="0.8.2")

# ----------------------------
# Global error handling (guarantee JSON responses)
# ----------------------------
@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    # Never let an exception bubble into an HTML 500 page.
    # Keep response shape backward compatible (do not remove fields).
    #
    # SECURITY: Do NOT leak internal exception details to clients (audit finding).
    # Log full exception server-side instead.
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
    # Keep HTTP 200 to preserve current client expectations (we can tighten later).
    return JSONResponse(status_code=200, content=payload)


# ----------------------------
# Budgets / timeouts (keep under CloudFront/origin timeouts)
# ----------------------------
WEATHER_TOTAL_BUDGET_S = 8.0

# Increased slightly to allow more work while still staying under your ~15s end-to-end SLA.
# Note: If provider calls exceed this, we will serve cached power if available.
POWER_TOTAL_BUDGET_S = 14.0

# Per-request HTTP timeouts (connect+read). Keep these smaller than the total budget.
HTTP_TIMEOUT_S = 5.0


# ----------------------------
# Power cache (best-effort fallback on timeouts)
# ----------------------------
# Goal: if a live power lookup times out (common for some providers),
# return last-known-good power payload instead of empty timeouts.
POWER_CACHE_TTL_S = 120  # seconds
_power_cache: Dict[str, Dict[str, Any]] = {}  # key -> {"ts": float, "payload": dict}


def _power_cache_key(resolved: Dict[str, Any]) -> str:
    # Prefer stable site_id when available; otherwise bucket by rounded lat/lon.
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
        # Never allow cache logic to break the endpoint
        pass


def _cached_power_on_timeout(resolved: Dict[str, Any], site_utility: Optional[str]) -> Dict[str, Any]:
    """
    Return cached power payload if available and fresh enough, otherwise an empty_power() timeout response.
    Adds non-breaking meta fields: cached, cache_age_s.
    """
    try:
        key = _power_cache_key(resolved)
        cached = _power_cache.get(key)
        if cached:
            age = time.time() - float(cached.get("ts", 0.0))
            if age <= POWER_CACHE_TTL_S:
                payload = cached.get("payload")
                if isinstance(payload, dict):
                    # Annotate meta without removing any existing fields
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
            "outage_map": "https://outagemap.oge.com/",
            "platform": "KUBRA",
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
    if u:
        return {"utility": u, "name": u, "outage_map": None, "platform": ""}
    return {"utility": None, "name": "Unknown", "outage_map": None, "platform": ""}


def empty_weather(error: Optional[str] = None) -> Dict[str, Any]:
    # Backward compatible keys + new NOC keys
    w: Dict[str, Any] = {
        "temperature_f": None,
        "condition": None,
        "wind_speed_mph": None,
        "wind_gust_mph": None,
        "wind_direction_deg": None,
        "wind_direction_cardinal": None,
        "precip_last_hour_in": None,
        "wind_chill_f": None,
        "heat_index_f": None,
        "observation_time": None,
        "station_id": None,
        "temp_kind": None,  # observed | forecast_fallback | None
        "temp_source": None,  # NWS_OBSERVATION | NWS_FORECAST | None
        "temp_source_url": None,  # endpoint actually used
        "detailedForecast": None,  # full text, suitable for expandable UI
        "has_weather_alert": False,
        "max_alert_severity": "none",
        "alerts": [],
    }
    if error:
        w["error"] = error
    return w


def empty_power(utility: Optional[str], error: str, ok: bool = False) -> Dict[str, Any]:
    return {
        "utility": (utility or "").strip().upper() or None,
        "has_outage_nearby": False,
        "nearest": None,
        "outages": [],
        "meta": {"source": "app.api", "ok": ok, "error": error},
    }


def c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0


def mps_to_mph(mps: float) -> float:
    return mps * 2.2369362920544


def kmh_to_mph(kmh: float) -> float:
    return kmh * 0.621371192237334


def mm_to_in(mm: float) -> float:
    return mm / 25.4


def deg_to_cardinal(deg: Optional[float]) -> Optional[str]:
    if deg is None:
        return None
    try:
        d = float(deg) % 360.0
    except Exception:
        return None
    dirs = [
        "N",
        "NNE",
        "NE",
        "ENE",
        "E",
        "ESE",
        "SE",
        "SSE",
        "S",
        "SSW",
        "SW",
        "WSW",
        "W",
        "WNW",
        "NW",
        "NNW",
    ]
    idx = int((d + 11.25) // 22.5) % 16
    return dirs[idx]


# ----------------------------
# Load sites
# ----------------------------
SITES_PATH = Path(__file__).resolve().parent / "data" / "sites.json"
try:
    SITES = json.loads(SITES_PATH.read_text(encoding="utf-8"))
except Exception:
    SITES = {}


# ----------------------------
# Weather (NWS)
# ----------------------------
NWS_POINTS = "https://api.weather.gov/points/{lat},{lon}"
NWS_OBSERVATION = "https://api.weather.gov/stations/{station}/observations/latest"
NWS_ALERTS = "https://api.weather.gov/alerts/active?point={lat},{lon}"

DEFAULT_HEADERS = {
    "User-Agent": "WeatherPowerStatus/1.0 (contact: subrealstudios.com)",
    "Accept": "application/geo+json, application/json",
}


def fetch_weather(lat: float, lon: float) -> Dict[str, Any]:
    # Step 1: points lookup
    points_url = NWS_POINTS.format(lat=lat, lon=lon)
    r = requests.get(points_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
    r.raise_for_status()
    pts = r.json()

    props = (pts.get("properties") or {})
    stations_url = props.get("observationStations")
    forecast_url = props.get("forecast")
    forecast_hourly_url = props.get("forecastHourly")

    # Step 2: stations list (best effort)
    station_id = None
    if stations_url:
        try:
            rs = requests.get(stations_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
            rs.raise_for_status()
            st = rs.json()
            feats = st.get("features") or []
            if feats and isinstance(feats, list):
                station_id = (feats[0].get("properties") or {}).get("stationIdentifier")
        except Exception:
            station_id = None

    # Step 3: observation (best effort). If it fails, fallback to forecast for temp/condition.
    out = empty_weather()
    out["temp_source_url"] = None

    if station_id:
        obs_url = NWS_OBSERVATION.format(station=station_id)
        out["temp_source_url"] = obs_url
        try:
            ro = requests.get(obs_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
            ro.raise_for_status()
            obs = ro.json()
            oprops = (obs.get("properties") or {})
            t_c = (oprops.get("temperature") or {}).get("value")
            if isinstance(t_c, (int, float)):
                out["temperature_f"] = round(c_to_f(float(t_c)), 1)
                out["temp_kind"] = "observed"
                out["temp_source"] = "NWS_OBSERVATION"
            out["station_id"] = station_id
            out["observation_time"] = oprops.get("timestamp")
            # winds
            wspd = (oprops.get("windSpeed") or {}).get("value")
            wgst = (oprops.get("windGust") or {}).get("value")
            wdir = (oprops.get("windDirection") or {}).get("value")
            if isinstance(wspd, (int, float)):
                out["wind_speed_mph"] = round(mps_to_mph(float(wspd)), 1)
            if isinstance(wgst, (int, float)):
                out["wind_gust_mph"] = round(mps_to_mph(float(wgst)), 1)
            if isinstance(wdir, (int, float)):
                out["wind_direction_deg"] = float(wdir)
                out["wind_direction_cardinal"] = deg_to_cardinal(float(wdir))
            # precip
            p = (oprops.get("precipitationLastHour") or {}).get("value")
            if isinstance(p, (int, float)):
                out["precip_last_hour_in"] = round(mm_to_in(float(p)), 3)
            # wind chill / heat index
            wc = (oprops.get("windChill") or {}).get("value")
            hi = (oprops.get("heatIndex") or {}).get("value")
            if isinstance(wc, (int, float)):
                out["wind_chill_f"] = round(c_to_f(float(wc)), 1)
            if isinstance(hi, (int, float)):
                out["heat_index_f"] = round(c_to_f(float(hi)), 1)
            # condition
            out["condition"] = oprops.get("textDescription")
        except Exception:
            pass

    # Step 4: forecast fallback for temp/condition (best effort)
    if out.get("temperature_f") is None and forecast_url:
        try:
            rf = requests.get(forecast_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
            rf.raise_for_status()
            fc = rf.json()
            periods = ((fc.get("properties") or {}).get("periods") or [])
            if periods and isinstance(periods, list):
                p0 = periods[0] or {}
                t = p0.get("temperature")
                if isinstance(t, (int, float)):
                    out["temperature_f"] = float(t)
                    out["temp_kind"] = "forecast_fallback"
                    out["temp_source"] = "NWS_FORECAST"
                    out["temp_source_url"] = forecast_url
                out["condition"] = p0.get("shortForecast") or out.get("condition")
                out["detailedForecast"] = p0.get("detailedForecast")
        except Exception:
            pass

    # Step 5: alerts (best effort)
    alerts_url = NWS_ALERTS.format(lat=lat, lon=lon)
    try:
        ra = requests.get(alerts_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
        ra.raise_for_status()
        aj = ra.json()
        feats = aj.get("features") or []
    except Exception:
        feats = []

    alerts = []
    max_alert_severity = "none"
    has_weather_alert = False

    sev_rank = {"none": 0, "minor": 1, "moderate": 2, "severe": 3, "extreme": 4}

    for f in feats:
        props = (f.get("properties") or {})
        event = props.get("event")
        severity = (props.get("severity") or "").lower()
        effective = props.get("effective")
        expires = props.get("expires")
        headline = props.get("headline")
        description = props.get("description")

        alerts.append(
            {
                "event": event,
                "severity": severity,
                "effective": effective,
                "expires": expires,
                "headline": headline,
                "description": description,
            }
        )
        has_weather_alert = True
        # Map NWS severity words to our banner values
        mapped = "none"
        if severity == "minor":
            mapped = "minor"
        elif severity in ("moderate", "unknown"):
            mapped = "moderate"
        elif severity == "severe":
            mapped = "severe"
        elif severity == "extreme":
            mapped = "extreme"

        if sev_rank[mapped] > sev_rank[max_alert_severity]:
            max_alert_severity = mapped

    out["alerts"] = alerts
    out["has_weather_alert"] = has_weather_alert
    out["max_alert_severity"] = max_alert_severity

    return out


# ----------------------------
# Frontend
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_PATH = STATIC_DIR / "index.html"

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(INDEX_PATH)


# ----------------------------
# API
# ----------------------------
@app.get("/api/status")
def api_status(
    query: Optional[str] = Query(None, max_length=128, description="Site ID or lat,lon"),
    q: Optional[str] = Query(None, max_length=128, description="Alias for 'query' (Site ID or lat,lon)"),
    utility: Optional[str] = Query(
        None,
        max_length=16,
        description="Optional utility/provider override (e.g., EVERGY, PSO, OGE, ONCOR). "
        "If provided with lat,lon queries, probing is skipped.",
    ),
) -> Dict[str, Any]:
    raw_in = (query if query is not None else q)
    q_str = (raw_in or "").strip()
    utility_override = (utility or "").strip().upper() or None

    # Validate utility override (only applies to the override param, not sites.json values)
    if utility_override and utility_override not in ALLOWED_UTILITIES:
        msg = f"Invalid utility '{utility_override}'. Allowed: {', '.join(sorted(ALLOWED_UTILITIES))}"
        return {
            "query": raw_in,
            "resolved": {"type": "unknown", "name": "", "site_id": None},
            "provider": provider_info(None),
            "weather": empty_weather(error=msg),
            "power": empty_power(None, msg, ok=False),
            "probe": None,
        }

    if not q_str:
        return {
            "query": raw_in,
            "resolved": {"type": "unknown", "name": "", "site_id": None},
            "provider": provider_info(None),
            "weather": empty_weather(error="Missing query parameter. Provide ?query= or ?q="),
            "power": empty_power(None, "Missing query parameter. Provide ?query= or ?q=", ok=False),
            "probe": None,
        }

    latlon = parse_latlon(q_str)

    resolved: Dict[str, Any]
    site_utility: Optional[str] = None

    if latlon:
        lat, lon = latlon

        # Range validation (audit-friendly input validation)
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            msg = "Invalid lat/lon range. Expected lat [-90..90], lon [-180..180]."
            return {
                "query": raw_in,
                "resolved": {"type": "unknown", "name": q_str, "site_id": None},
                "provider": provider_info(None),
                "weather": empty_weather(error=msg),
                "power": empty_power(None, msg, ok=False),
                "probe": None,
            }

        # If frontend supplies utility, skip probe.
        site_utility = utility_override
        resolved = {
            "type": "latlon",
            "name": f"{lat:.7f}, {lon:.7f}",
            "site_id": None,
            "lat": lat,
            "lon": lon,
            "utility": site_utility,
        }
    else:
        sid = q_str.upper()
        site = SITES.get(sid)
        if not site:
            close = difflib.get_close_matches(sid, list(SITES.keys()), n=3, cutoff=0.6)
            if close:
                sid = close[0]
                site = SITES.get(sid)

        if not site:
            return {
                "query": raw_in,
                "resolved": {"type": "unknown", "name": q_str, "site_id": None},
                "provider": provider_info(None),
                "weather": empty_weather(error="Site not found"),
                "power": empty_power(None, "Site not found", ok=False),
                "probe": None,
            }

        site_utility = site.get("utility")
        resolved = {
            "type": "site",
            "name": site.get("name") or sid,
            "site_id": sid,
            "address": site.get("address"),
            "city": site.get("city"),
            "state": site.get("state"),
            "zip": site.get("zip"),
            "lat": site.get("lat"),
            "lon": site.get("lon"),
            "utility": site_utility,
        }

    lat = to_float(resolved.get("lat"))
    lon = to_float(resolved.get("lon"))
    if lat is None or lon is None:
        return {
            "query": raw_in,
            "resolved": resolved,
            "provider": provider_info(site_utility),
            "weather": empty_weather(error="Missing latitude/longitude; weather lookup unavailable."),
            "power": empty_power(site_utility, "Missing latitude/longitude; power lookup unavailable.", ok=False),
            "probe": None,
        }

    probe_payload = None
    power_obj: Any = None
    attempts = []

    # Run in parallel but NEVER allow the request to exceed CloudFront/origin timeouts.
    ex = ThreadPoolExecutor(max_workers=2)
    try:
        f_weather = ex.submit(fetch_weather, lat, lon)

        if site_utility:
            # Deterministic: no probe, call the chosen provider directly.
            f_power = ex.submit(get_power_status, lat, lon, site_utility)
        else:

            def do_probe():
                chosen, atts = probe_power_status(lat, lon)
                return chosen, atts

            f_power = ex.submit(do_probe)

        # Weather: best-effort under budget (always return JSON)
        try:
            weather = f_weather.result(timeout=WEATHER_TOTAL_BUDGET_S)
        except FuturesTimeout:
            try:
                f_weather.cancel()
            except Exception:
                pass
            weather = empty_weather(error="Weather lookup timed out")
        except Exception as e:
            weather = empty_weather(error=f"Weather lookup failed: {type(e).__name__}: {e}")

        # Power: best-effort under budget (always return JSON)
        try:
            if site_utility:
                power_obj = f_power.result(timeout=POWER_TOTAL_BUDGET_S)
                attempts = []
            else:
                power_obj, attempts = f_power.result(timeout=POWER_TOTAL_BUDGET_S)
        except FuturesTimeout:
            try:
                f_power.cancel()
            except Exception:
                pass
            # FIX: correct function name (_cached_power_on_timeout)
            power_obj = _cached_power_on_timeout(resolved, site_utility)
            attempts = []
        except Exception as e:
            power_obj = empty_power(site_utility, f"Power lookup failed: {type(e).__name__}: {e}", ok=False)
            attempts = []
    finally:
        # Never block shutdown; timed-out provider threads may still be running.
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            ex.shutdown(wait=False)

    # Normalize power object to dict
    power_payload = power_obj.model_dump() if hasattr(power_obj, "model_dump") else power_obj

    # Cache successful power payloads (best-effort, never breaks endpoint)
    _cache_power_if_ok(resolved, power_payload)

    # Provider banner: if probed, show the chosen utility when available
    banner_utility = site_utility
    if not banner_utility and isinstance(power_payload, dict):
        banner_utility = (power_payload.get("utility") or None)

    provider_banner = provider_info(banner_utility)

    # Probe payload (only when probing)
    if not site_utility and attempts:
        winner_utility = None
        if isinstance(power_payload, dict) and power_payload.get("has_outage_nearby"):
            winner_utility = power_payload.get("utility")

        # Make resolved.utility informative for lat/lon probe cases (non-breaking)
        if isinstance(resolved, dict) and resolved.get("utility") is None and winner_utility:
            resolved["utility"] = winner_utility

        probe_payload = {
            "mode": "probe",
            "winner": winner_utility,
            "attempts": [
                {
                    "provider": getattr(a, "utility", None),
                    "ok": getattr(getattr(a, "meta", None), "ok", None),
                    "error": getattr(getattr(a, "meta", None), "error", None),
                    "has_outage_nearby": getattr(a, "has_outage_nearby", None),
                    "nearest_distance_miles": (getattr(getattr(a, "nearest", None), "distance_miles", None)),
                    "nearest_customers_out": (getattr(getattr(a, "nearest", None), "customers_out", None)),
                }
                for a in attempts
            ],
        }

    return {
        "query": raw_in,
        "resolved": resolved,
        "provider": provider_banner,
        "weather": weather,
        "power": power_payload,
        "probe": probe_payload,
    }
