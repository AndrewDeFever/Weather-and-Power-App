from __future__ import annotations

import difflib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from app.netguard import limited_requests_get
from app.power_router import get_power_status, probe_power_status

log = logging.getLogger("weather_power")

app = FastAPI(title="Weather & Power Status", version="0.9.0")

CSP = (
    "default-src 'self'; "
    "img-src 'self' data:; "
    "style-src 'self' 'unsafe-inline'; "
    "script-src 'self'; "
    "connect-src 'self'; "
    "base-uri 'none'; "
    "frame-ancestors 'none'"
)
HSTS = "max-age=31536000; includeSubDomains"


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        response: Response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
        response.headers["Content-Security-Policy"] = CSP
        response.headers["Strict-Transport-Security"] = HSTS
        return response


app.add_middleware(SecurityHeadersMiddleware)

RL_BURST = int(os.getenv("RL_BURST", "30"))
RL_PER_MIN = float(os.getenv("RL_PER_MIN", "60"))
_rl_refill_per_sec = RL_PER_MIN / 60.0
_rl_buckets: Dict[str, Dict[str, float]] = {}
_rl_lock = Lock()


def _client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


def _rate_limit_allow(key: str) -> bool:
    now = time.time()
    with _rl_lock:
        bucket = _rl_buckets.get(key)
        if not bucket:
            _rl_buckets[key] = {"tokens": float(max(RL_BURST - 1, 0)), "ts": now}
            return True

        tokens = float(bucket.get("tokens", RL_BURST))
        last = float(bucket.get("ts", now))
        tokens = min(float(RL_BURST), tokens + (now - last) * _rl_refill_per_sec)

        if tokens < 1.0:
            bucket["tokens"] = tokens
            bucket["ts"] = now
            return False

        bucket["tokens"] = tokens - 1.0
        bucket["ts"] = now
        return True


class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.url.path.startswith("/api/"):
            if not _rate_limit_allow(_client_ip(request)):
                msg = "Rate limit exceeded. Please retry shortly."
                payload = {
                    "query": None,
                    "resolved": {"type": "unknown", "name": "", "site_id": None},
                    "provider": provider_info(None),
                    "weather": empty_weather(error=msg),
                    "power": empty_power(None, msg, ok=False),
                    "probe": None,
                }
                return JSONResponse(status_code=429, content=payload)
        return await call_next(request)


app.add_middleware(RateLimitMiddleware)


@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    log.exception("Unhandled exception path=%s", request.url.path)
    msg = "Internal server error"
    payload = {
        "query": None,
        "resolved": {"type": "unknown", "name": "", "site_id": None},
        "provider": provider_info(None),
        "weather": empty_weather(error=msg),
        "power": empty_power(None, msg, ok=False),
        "probe": None,
    }
    return JSONResponse(status_code=500, content=payload)


WEATHER_TOTAL_BUDGET_S = float(os.getenv("WEATHER_TOTAL_BUDGET_S", "8"))
POWER_TOTAL_BUDGET_S = float(os.getenv("POWER_TOTAL_BUDGET_S", "14"))
HTTP_TIMEOUT_S = float(os.getenv("HTTP_TIMEOUT_S", "5"))
POWER_CACHE_TTL_S = int(os.getenv("POWER_CACHE_TTL_S", "300"))
WEATHER_CACHE_TTL_S = int(os.getenv("WEATHER_CACHE_TTL_S", "300"))
STATUS_CACHE_TTL_S = int(os.getenv("STATUS_CACHE_TTL_S", "0"))

_power_cache: Dict[str, Dict[str, Any]] = {}
_weather_cache: Dict[str, Dict[str, Any]] = {}

ALLOWED_UTILITIES = {
    "PSO",
    "OGE",
    "EVERGY",
    "ONCOR",
    "AUSTIN",
    "PEC",
    "AEP",
    "CENTERPOINT",
    "EPE",
    "EL_PASO_ELECTRIC",
    "CITY_OF_CONCORDIA_ELECTRIC",
    "PRAIRIE_LAND_ELECTRIC",
    "NINNESCAH_RURAL_ELECTRIC",
}

_PROVIDER_CATALOG: Dict[str, Dict[str, Any]] = {
    "PSO": {
        "utility": "PSO",
        "name": "PSO",
        "outage_map": "https://outagemap.psoklahoma.com/",
        "platform": "KUBRA",
    },
    "OGE": {
        "utility": "OGE",
        "name": "OG&E",
        "outage_map": "https://outagemap.oge.com/",
        "platform": "KUBRA",
    },
    "EVERGY": {
        "utility": "EVERGY",
        "name": "Evergy",
        "outage_map": "https://outagemap.evergy.com/",
        "platform": "KUBRA",
    },
    "ONCOR": {
        "utility": "ONCOR",
        "name": "Oncor",
        "outage_map": "https://stormcenter.oncor.com/",
        "platform": "KUBRA",
    },
    "AUSTIN": {
        "utility": "AUSTIN",
        "name": "Austin Energy",
        "outage_map": "https://outagemap.austinenergy.com/",
        "platform": "KUBRA",
    },
    "PEC": {
        "utility": "PEC",
        "name": "Pedernales Electric Cooperative",
        "outage_map": "https://map.mypec.com/",
        "platform": "KUBRA",
    },
    "AEP": {
        "utility": "AEP",
        "name": "AEP Texas",
        "outage_map": "https://outagemap.aeptexas.com/",
        "platform": "KUBRA",
    },
    "CENTERPOINT": {
        "utility": "CENTERPOINT",
        "name": "CenterPoint Energy",
        "outage_map": "https://tracker.centerpointenergy.com/map/texas",
        "platform": "ARCGIS",
    },
    "EL_PASO_ELECTRIC": {
        "utility": "EL_PASO_ELECTRIC",
        "name": "El Paso Electric",
        "outage_map": "https://outagemap.epelectric.com/",
        "platform": "STARLIT",
    },
    "CITY_OF_CONCORDIA_ELECTRIC": {
        "utility": "CITY_OF_CONCORDIA_ELECTRIC",
        "name": "City of Concordia Electric",
        "outage_map": "https://cecdata.com/outageMap.html",
        "platform": "TRPC",
    },
    "PRAIRIE_LAND_ELECTRIC": {
        "utility": "PRAIRIE_LAND_ELECTRIC",
        "name": "Prairie Land Electric Cooperative",
        "outage_map": "https://prairielandelectric.outagemap.coop/#/",
        "platform": "ARCGIS",
    },
    "NINNESCAH_RURAL_ELECTRIC": {
        "utility": "NINNESCAH_RURAL_ELECTRIC",
        "name": "Ninnescah Rural Electric Cooperative",
        "outage_map": "https://ninnescah.ebill.coop/maps/external_outage_web_map/",
        "platform": "WEB_MAP_SUMMARY",
    },
}


def provider_info(utility: Optional[str]) -> Dict[str, Any]:
    u = (utility or "").strip().upper()
    if u == "EPE":
        u = "EL_PASO_ELECTRIC"
    if u in _PROVIDER_CATALOG:
        return dict(_PROVIDER_CATALOG[u])
    if u:
        return {"utility": u, "name": u, "outage_map": None, "platform": ""}
    return {"utility": None, "name": "Unknown", "outage_map": None, "platform": ""}


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def parse_latlon(q: str) -> Optional[Tuple[float, float]]:
    if not q or "," not in q:
        return None
    left, right = q.split(",", 1)
    lat = to_float(left)
    lon = to_float(right)
    if lat is None or lon is None:
        return None
    return lat, lon


def empty_weather(error: Optional[str] = None) -> Dict[str, Any]:
    weather: Dict[str, Any] = {
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
        "temp_kind": None,
        "temp_source": None,
        "temp_source_url": None,
        "detailedForecast": None,
        "has_weather_alert": False,
        "max_alert_severity": "none",
        "alerts": [],
    }
    if error:
        weather["error"] = error
    return weather


def empty_power(utility: Optional[str], error: str, ok: bool = False) -> Dict[str, Any]:
    return {
        "utility": (utility or "").strip().upper() or None,
        "has_outage_nearby": False,
        "nearest": None,
        "outages": [],
        "meta": {"source": "app.api", "ok": ok, "error": error},
    }


def c_to_f(celsius: float) -> float:
    return (celsius * 9.0 / 5.0) + 32.0


def mps_to_mph(mps: float) -> float:
    return mps * 2.2369362920544


def mm_to_in(mm: float) -> float:
    return mm / 25.4


def deg_to_cardinal(deg: Optional[float]) -> Optional[str]:
    if deg is None:
        return None
    try:
        value = float(deg) % 360.0
    except Exception:
        return None
    directions = [
        "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
        "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
    ]
    return directions[int((value + 11.25) // 22.5) % 16]


SITES_PATH = Path(__file__).resolve().parent / "data" / "sites.json"
SITES: Dict[str, Dict[str, Any]] = {}
SITE_LOOKUP: Dict[str, str] = {}
_SITES_MTIME_NS: Optional[int] = None
_SITES_LOCK = Lock()


def _normalize_site_lookup(value: str) -> str:
    return re.sub(r"[^A-Z0-9]+", " ", (value or "").upper()).strip()


def _rebuild_site_index() -> None:
    global SITE_LOOKUP
    index: Dict[str, str] = {}
    for site_id, site in SITES.items():
        candidates = [site_id, str(site.get("name") or "")]
        aliases = site.get("aliases")
        if isinstance(aliases, list):
            candidates.extend(str(alias) for alias in aliases)
        for candidate in candidates:
            normalized = _normalize_site_lookup(candidate)
            if normalized and normalized not in index:
                index[normalized] = site_id
    SITE_LOOKUP = index


def _reload_sites_if_needed(force: bool = False) -> None:
    global SITES, _SITES_MTIME_NS
    try:
        mtime_ns = SITES_PATH.stat().st_mtime_ns
    except OSError:
        mtime_ns = None

    if not force and mtime_ns == _SITES_MTIME_NS:
        return

    with _SITES_LOCK:
        try:
            current_mtime = SITES_PATH.stat().st_mtime_ns
        except OSError:
            current_mtime = None
        if not force and current_mtime == _SITES_MTIME_NS:
            return
        try:
            raw = json.loads(SITES_PATH.read_text(encoding="utf-8"))
            SITES = raw if isinstance(raw, dict) else {}
        except Exception as exc:
            log.warning("Failed to load sites file %s: %s", SITES_PATH, exc)
            SITES = {}
        _SITES_MTIME_NS = current_mtime
        _rebuild_site_index()


_reload_sites_if_needed(force=True)


def _resolve_site(query_text: str) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
    _reload_sites_if_needed()
    direct = (query_text or "").strip().upper()
    if direct in SITES:
        return direct, SITES[direct]

    normalized = _normalize_site_lookup(query_text)
    indexed = SITE_LOOKUP.get(normalized)
    if indexed:
        return indexed, SITES.get(indexed)

    choices = list(SITE_LOOKUP.keys())
    close = difflib.get_close_matches(normalized, choices, n=1, cutoff=0.72)
    if close:
        site_id = SITE_LOOKUP.get(close[0])
        if site_id:
            return site_id, SITES.get(site_id)
    return None, None


NWS_POINTS = "https://api.weather.gov/points/{lat},{lon}"
NWS_OBSERVATION = "https://api.weather.gov/stations/{station}/observations/latest"
NWS_ALERTS = "https://api.weather.gov/alerts/active?point={lat},{lon}"
DEFAULT_HEADERS = {
    "User-Agent": "WeatherPowerStatus/1.0",
    "Accept": "application/geo+json, application/json",
}


def fetch_weather(lat: float, lon: float) -> Dict[str, Any]:
    key = f"{lat:.4f},{lon:.4f}"
    now = time.time()
    cached = _weather_cache.get(key)
    if cached and now - float(cached.get("ts", 0.0)) < WEATHER_CACHE_TTL_S:
        payload = cached.get("payload")
        if isinstance(payload, dict):
            return payload

    points_url = NWS_POINTS.format(lat=lat, lon=lon)
    response = limited_requests_get(points_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S)
    response.raise_for_status()
    points = response.json()
    props = points.get("properties") or {}
    stations_url = props.get("observationStations")
    forecast_url = props.get("forecast")

    station_id = None
    if stations_url:
        try:
            stations_response = limited_requests_get(
                stations_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S
            )
            stations_response.raise_for_status()
            features = stations_response.json().get("features") or []
            if features:
                station_id = (features[0].get("properties") or {}).get("stationIdentifier")
        except Exception:
            station_id = None

    out = empty_weather()

    if station_id:
        observation_url = NWS_OBSERVATION.format(station=station_id)
        out["temp_source_url"] = observation_url
        try:
            obs_response = limited_requests_get(
                observation_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S
            )
            obs_response.raise_for_status()
            obs_props = obs_response.json().get("properties") or {}
            temp_c = (obs_props.get("temperature") or {}).get("value")
            if isinstance(temp_c, (int, float)):
                out["temperature_f"] = round(c_to_f(float(temp_c)), 1)
                out["temp_kind"] = "observed"
                out["temp_source"] = "NWS_OBSERVATION"
            out["station_id"] = station_id
            out["observation_time"] = obs_props.get("timestamp")

            wind_speed = (obs_props.get("windSpeed") or {}).get("value")
            wind_gust = (obs_props.get("windGust") or {}).get("value")
            wind_direction = (obs_props.get("windDirection") or {}).get("value")
            if isinstance(wind_speed, (int, float)):
                out["wind_speed_mph"] = round(mps_to_mph(float(wind_speed)), 1)
            if isinstance(wind_gust, (int, float)):
                out["wind_gust_mph"] = round(mps_to_mph(float(wind_gust)), 1)
            if isinstance(wind_direction, (int, float)):
                out["wind_direction_deg"] = float(wind_direction)
                out["wind_direction_cardinal"] = deg_to_cardinal(float(wind_direction))

            precipitation = (obs_props.get("precipitationLastHour") or {}).get("value")
            if isinstance(precipitation, (int, float)):
                out["precip_last_hour_in"] = round(mm_to_in(float(precipitation)), 3)

            wind_chill = (obs_props.get("windChill") or {}).get("value")
            heat_index = (obs_props.get("heatIndex") or {}).get("value")
            if isinstance(wind_chill, (int, float)):
                out["wind_chill_f"] = round(c_to_f(float(wind_chill)), 1)
            if isinstance(heat_index, (int, float)):
                out["heat_index_f"] = round(c_to_f(float(heat_index)), 1)
            out["condition"] = obs_props.get("textDescription")
        except Exception:
            pass

    if forecast_url:
        try:
            forecast_response = limited_requests_get(
                forecast_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S
            )
            forecast_response.raise_for_status()
            periods = (forecast_response.json().get("properties") or {}).get("periods") or []
            if periods:
                period = periods[0] or {}
                temperature = period.get("temperature")
                if out.get("temperature_f") is None and isinstance(temperature, (int, float)):
                    out["temperature_f"] = float(temperature)
                    out["temp_kind"] = "forecast_fallback"
                    out["temp_source"] = "NWS_FORECAST"
                    out["temp_source_url"] = forecast_url
                out["condition"] = period.get("shortForecast") or out.get("condition")
                out["detailedForecast"] = period.get("detailedForecast")
        except Exception:
            pass

    alerts_url = NWS_ALERTS.format(lat=lat, lon=lon)
    try:
        alerts_response = limited_requests_get(
            alerts_url, headers=DEFAULT_HEADERS, timeout=HTTP_TIMEOUT_S
        )
        alerts_response.raise_for_status()
        features = alerts_response.json().get("features") or []
    except Exception:
        features = []

    alerts = []
    max_severity = "none"
    severity_rank = {"none": 0, "minor": 1, "moderate": 2, "severe": 3, "extreme": 4}
    for feature in features:
        alert_props = feature.get("properties") or {}
        severity = (alert_props.get("severity") or "").lower()
        mapped = severity if severity in severity_rank else "moderate" if severity == "unknown" else "none"
        alerts.append(
            {
                "event": alert_props.get("event"),
                "severity": severity,
                "effective": alert_props.get("effective"),
                "expires": alert_props.get("expires"),
                "headline": alert_props.get("headline"),
                "description": alert_props.get("description"),
            }
        )
        if severity_rank[mapped] > severity_rank[max_severity]:
            max_severity = mapped

    out["alerts"] = alerts
    out["has_weather_alert"] = bool(alerts)
    out["max_alert_severity"] = max_severity
    _weather_cache[key] = {"ts": now, "payload": out}
    return out


def _power_cache_key(resolved: Dict[str, Any]) -> str:
    site_id = resolved.get("site_id")
    if site_id:
        return f"site:{site_id}"
    lat = to_float(resolved.get("lat"))
    lon = to_float(resolved.get("lon"))
    if lat is None or lon is None:
        return "unknown"
    return f"ll:{lat:.3f},{lon:.3f}"


def _cache_power_if_ok(resolved: Dict[str, Any], power_payload: Any) -> None:
    if not isinstance(power_payload, dict):
        return
    meta = power_payload.get("meta")
    if isinstance(meta, dict) and meta.get("ok") is True:
        _power_cache[_power_cache_key(resolved)] = {
            "ts": time.time(),
            "payload": json.loads(json.dumps(power_payload)),
        }


def _cached_power_on_timeout(
    resolved: Dict[str, Any], site_utility: Optional[str]
) -> Dict[str, Any]:
    cached = _power_cache.get(_power_cache_key(resolved))
    if cached:
        age = time.time() - float(cached.get("ts", 0.0))
        payload = cached.get("payload")
        if age <= POWER_CACHE_TTL_S and isinstance(payload, dict):
            payload = json.loads(json.dumps(payload))
            meta = payload.setdefault("meta", {})
            meta["cached"] = True
            meta["cache_age_s"] = round(age, 1)
            meta["error"] = "Live power lookup timed out; serving cached result"
            meta["ok"] = True
            return payload
    return empty_power(site_utility, "Power lookup timed out", ok=False)


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/api/status")
def api_status(
    response: Response,
    query: Optional[str] = Query(None, max_length=128, description="Site ID/name/alias or lat,lon"),
    q: Optional[str] = Query(None, max_length=128, description="Alias for query"),
    utility: Optional[str] = Query(
        None,
        max_length=40,
        description="Optional utility/provider override. If supplied, provider probing is skipped.",
    ),
) -> Dict[str, Any]:
    response.headers["Cache-Control"] = (
        f"public, max-age={STATUS_CACHE_TTL_S}" if STATUS_CACHE_TTL_S > 0 else "no-store"
    )

    raw_in = query if query is not None else q
    query_text = (raw_in or "").strip()
    utility_override = (utility or "").strip().upper() or None
    if utility_override == "EPE":
        utility_override = "EL_PASO_ELECTRIC"

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

    if not query_text:
        msg = "Missing query parameter. Provide ?query= or ?q="
        return {
            "query": raw_in,
            "resolved": {"type": "unknown", "name": "", "site_id": None},
            "provider": provider_info(None),
            "weather": empty_weather(error=msg),
            "power": empty_power(None, msg, ok=False),
            "probe": None,
        }

    latlon = parse_latlon(query_text)
    resolved: Dict[str, Any]
    site_utility: Optional[str] = None

    if latlon:
        lat, lon = latlon
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            msg = "Invalid lat/lon range. Expected lat [-90..90], lon [-180..180]."
            return {
                "query": raw_in,
                "resolved": {"type": "unknown", "name": query_text, "site_id": None},
                "provider": provider_info(None),
                "weather": empty_weather(error=msg),
                "power": empty_power(None, msg, ok=False),
                "probe": None,
            }

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
        site_id, site = _resolve_site(query_text)
        if not site_id or not site:
            msg = "Site not found"
            return {
                "query": raw_in,
                "resolved": {"type": "unknown", "name": query_text, "site_id": None},
                "provider": provider_info(None),
                "weather": empty_weather(error=msg),
                "power": empty_power(None, msg, ok=False),
                "probe": None,
            }

        if site.get("enabled") is False:
            msg = "Site is disabled"
            return {
                "query": raw_in,
                "resolved": {"type": "site", "name": site.get("name") or site_id, "site_id": site_id},
                "provider": provider_info(site.get("utility")),
                "weather": empty_weather(error=msg),
                "power": empty_power(site.get("utility"), msg, ok=False),
                "probe": None,
            }

        site_utility = utility_override or (site.get("utility") or None)
        if site_utility == "EPE":
            site_utility = "EL_PASO_ELECTRIC"

        resolved = {
            "type": "site",
            "name": site.get("name") or site_id,
            "site_id": site_id,
            "address": site.get("address"),
            "city": site.get("city"),
            "state": site.get("state"),
            "zip": site.get("zip"),
            "lat": site.get("lat"),
            "lon": site.get("lon"),
            "utility": site_utility,
            "severity": site.get("severity"),
            "tz": site.get("tz"),
            "ops_profile": site.get("ops_profile"),
        }

    lat = to_float(resolved.get("lat"))
    lon = to_float(resolved.get("lon"))
    if lat is None or lon is None:
        weather_error = "Missing latitude/longitude; weather lookup unavailable."
        power_error = "Missing latitude/longitude; power lookup unavailable."
        return {
            "query": raw_in,
            "resolved": resolved,
            "provider": provider_info(site_utility),
            "weather": empty_weather(error=weather_error),
            "power": empty_power(site_utility, power_error, ok=False),
            "probe": None,
        }

    probe_payload = None
    power_obj: Any = None
    attempts: List[Any] = []

    executor = ThreadPoolExecutor(max_workers=2)
    try:
        weather_future = executor.submit(fetch_weather, lat, lon)

        if site_utility:
            power_future = executor.submit(get_power_status, lat, lon, site_utility)
        else:
            power_future = executor.submit(probe_power_status, lat, lon)

        try:
            weather = weather_future.result(timeout=WEATHER_TOTAL_BUDGET_S)
        except FuturesTimeout:
            weather_future.cancel()
            weather = empty_weather(error="Weather lookup timed out")
        except Exception as exc:
            weather = empty_weather(error=f"Weather lookup failed: {type(exc).__name__}: {exc}")

        try:
            if site_utility:
                power_obj = power_future.result(timeout=POWER_TOTAL_BUDGET_S)
            else:
                power_obj, attempts = power_future.result(timeout=POWER_TOTAL_BUDGET_S)
        except FuturesTimeout:
            power_future.cancel()
            power_obj = _cached_power_on_timeout(resolved, site_utility)
            attempts = []
        except Exception as exc:
            power_obj = empty_power(
                site_utility,
                f"Power lookup failed: {type(exc).__name__}: {exc}",
                ok=False,
            )
            attempts = []
    finally:
        try:
            executor.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            executor.shutdown(wait=False)

    power_payload = power_obj.model_dump() if hasattr(power_obj, "model_dump") else power_obj
    _cache_power_if_ok(resolved, power_payload)

    banner_utility = site_utility
    if not banner_utility and isinstance(power_payload, dict):
        banner_utility = power_payload.get("utility") or None
    provider_banner = provider_info(banner_utility)

    if not site_utility and attempts:
        winner_utility = None
        if isinstance(power_payload, dict) and power_payload.get("has_outage_nearby"):
            winner_utility = power_payload.get("utility")
        if resolved.get("utility") is None and winner_utility:
            resolved["utility"] = winner_utility

        probe_payload = {
            "mode": "probe",
            "winner": winner_utility,
            "attempts": [
                {
                    "provider": getattr(attempt, "utility", None),
                    "ok": getattr(getattr(attempt, "meta", None), "ok", None),
                    "error": getattr(getattr(attempt, "meta", None), "error", None),
                    "has_outage_nearby": getattr(attempt, "has_outage_nearby", None),
                    "nearest_distance_miles": getattr(
                        getattr(attempt, "nearest", None), "distance_miles", None
                    ),
                    "nearest_customers_out": getattr(
                        getattr(attempt, "nearest", None), "customers_out", None
                    ),
                }
                for attempt in attempts
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
