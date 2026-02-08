from __future__ import annotations

import difflib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.power_router import get_power_status, probe_power_status

app = FastAPI(title="Weather & Power Status", version="0.8.1")


# ----------------------------
# Helpers
# ----------------------------
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
    u = (utility or "").strip().upper() or None
    if u == "OGE":
        return {
            "utility": "OGE",
            "name": "OG&E",
            "outage_map": "https://oge.com/wps/portal/oge/outages/systemwatch/",
            "platform": "KUBRA",
        }
    if u == "PSO":
        return {"utility": "PSO", "name": "PSO", "outage_map": "https://outagemap.psoklahoma.com/", "platform": "KUBRA"}
    if u == "EVERGY":
        return {"utility": "EVERGY", "name": "Evergy", "outage_map": "https://outagemap.evergy.com/", "platform": "KUBRA"}
    if u == "ONCOR":
        return {"utility": "ONCOR", "name": "Oncor", "outage_map": "https://stormcenter.oncor.com/", "platform": "KUBRA"}
    if u in {"AUSTIN", "AUSTINENERGY", "AUSTIN_ENERGY", "AE"}:
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
        "temp_kind": None,        # observed | forecast_fallback | None
        "temp_source": None,      # NWS_OBSERVATION | NWS_FORECAST | None
        "temp_source_url": None,  # endpoint actually used
        "detailedForecast": None, # full text, suitable for expandable UI
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
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    idx = int((d + 11.25) // 22.5) % 16
    return dirs[idx]


def to_mph(value: Any, unit_code: Any) -> Optional[float]:
    """
    Convert an NWS observation numeric 'value' to mph using the provided unitCode.

    Known seen in the wild:
      - wmoUnit:m_s-1
      - wmoUnit:km_h-1

    If the unit is unknown, return None rather than emitting a wrong mph value.
    """
    v = to_float(value)
    if v is None:
        return None
    uc = (unit_code or "").strip()
    if "m_s-1" in uc:
        return mps_to_mph(v)
    if "km_h-1" in uc:
        return kmh_to_mph(v)
    return None


# ----------------------------
# Site registry
# ----------------------------
def load_sites() -> Dict[str, Dict[str, Any]]:
    p = Path(__file__).parent / "data" / "sites.json"
    raw = p.read_text(encoding="utf-8-sig")
    data = json.loads(raw)

    if isinstance(data, dict) and "sites" in data and isinstance(data["sites"], list):
        sites_list = data["sites"]
    elif isinstance(data, list):
        sites_list = data
    elif isinstance(data, dict):
        sites_list = list(data.values())
    else:
        sites_list = []

    out: Dict[str, Dict[str, Any]] = {}
    for s in sites_list:
        if not isinstance(s, dict):
            continue
        sid = (s.get("site_id") or s.get("id") or "").strip()
        if not sid:
            continue

        out[sid.upper()] = {
            "site_id": sid.upper(),
            "name": s.get("name") or sid.upper(),
            "lat": to_float(s.get("lat")),
            "lon": to_float(s.get("lon")),
            "utility": (s.get("utility") or "").strip().upper() or None,
            "sev": s.get("sev") or s.get("severity"),
            # --- Added: address fields passthrough from sites.json
            "address": s.get("address"),
            "city": s.get("city"),
            "state": s.get("state"),
            "zip": s.get("zip"),
            "enabled": s.get("enabled", True),
            "tz": s.get("tz"),
        }
    return out


SITES: Dict[str, Dict[str, Any]] = load_sites()


# ----------------------------
# Weather (NWS)
# ----------------------------
def fetch_weather(lat: float, lon: float) -> Dict[str, Any]:
    headers = {"User-Agent": "NOCTriage/1.0 (weather-power-status)"}
    points_url = f"https://api.weather.gov/points/{lat:.4f},{lon:.4f}"

    # Defaults (backward compatible)
    out = empty_weather()

    # --- points (used only to discover station + forecast URL) ---
    try:
        r = requests.get(points_url, headers=headers, timeout=5)
        r.raise_for_status()
        points_data = r.json()
    except Exception as e:
        return empty_weather(error=f"NWS points fetch failed: {type(e).__name__}: {e}")

    props = points_data.get("properties") or {}
    stations_url = props.get("observationStations")
    forecast_url = props.get("forecast")  # used ONLY for detailedForecast text

    # --- observed (preferred for all A-fields) ---
    station_id: Optional[str] = None
    obs_url: Optional[str] = None
    try:
        if stations_url:
            sr = requests.get(stations_url, headers=headers, timeout=5)
            sr.raise_for_status()
            feats = sr.json().get("features") or []
            if feats:
                station_id = (feats[0].get("properties") or {}).get("stationIdentifier")

        if station_id:
            obs_url = f"https://api.weather.gov/stations/{station_id}/observations/latest"
            orq = requests.get(obs_url, headers=headers, timeout=5)
            orq.raise_for_status()
            obs_props = orq.json().get("properties") or {}

            # Temperature (C -> F)
            temp_c = (obs_props.get("temperature") or {}).get("value")
            if temp_c is not None:
                out["temperature_f"] = int(round(c_to_f(float(temp_c))))
                out["temp_kind"] = "observed"
                out["temp_source"] = "NWS_OBSERVATION"
                out["temp_source_url"] = obs_url

            # Condition text
            out["condition"] = obs_props.get("textDescription") or out["condition"]

            # Wind speed / gust (UNIT-AWARE -> mph)
            ws = obs_props.get("windSpeed") or {}
            ws_mph = to_mph(ws.get("value"), ws.get("unitCode"))
            if ws_mph is not None:
                out["wind_speed_mph"] = int(round(ws_mph))

            wg = obs_props.get("windGust") or {}
            wg_mph = to_mph(wg.get("value"), wg.get("unitCode"))
            if wg_mph is not None:
                out["wind_gust_mph"] = int(round(wg_mph))

            # Wind direction (degrees)
            wind_dir = (obs_props.get("windDirection") or {}).get("value")
            if wind_dir is not None:
                out["wind_direction_deg"] = int(round(float(wind_dir)))
                out["wind_direction_cardinal"] = deg_to_cardinal(float(wind_dir))

            # Precipitation last hour (mm -> inches) (often null / absent)
            p1 = (obs_props.get("precipitationLastHour") or {}).get("value")
            if p1 is not None:
                out["precip_last_hour_in"] = round(mm_to_in(float(p1)), 2)

            # Wind chill / heat index (C -> F) (may be null)
            wc_c = (obs_props.get("windChill") or {}).get("value")
            if wc_c is not None:
                out["wind_chill_f"] = int(round(c_to_f(float(wc_c))))

            hi_c = (obs_props.get("heatIndex") or {}).get("value")
            if hi_c is not None:
                out["heat_index_f"] = int(round(c_to_f(float(hi_c))))

            # Provenance
            out["observation_time"] = obs_props.get("timestamp")
            out["station_id"] = station_id
    except Exception:
        # If observations fail, we do NOT try to "fill in" A-fields from forecast.
        # Only temperature may fallback (explicitly labeled) to avoid an empty card.
        pass

    # --- temperature fallback ONLY (clearly labeled) ---
    if out.get("temperature_f") is None and forecast_url:
        try:
            fc = requests.get(forecast_url, headers=headers, timeout=5)
            fc.raise_for_status()
            periods = (fc.json().get("properties") or {}).get("periods") or []
            if periods:
                p0 = periods[0]
                out["temperature_f"] = p0.get("temperature")
                # if we have no condition from obs, fallback for condition too
                if out.get("condition") is None:
                    out["condition"] = p0.get("shortForecast")
                out["temp_kind"] = "forecast_fallback"
                out["temp_source"] = "NWS_FORECAST"
                out["temp_source_url"] = forecast_url
                # Detailed forecast text requested
                out["detailedForecast"] = p0.get("detailedForecast")
        except Exception:
            pass

    # --- detailedForecast text (requested) ---
    # If we already set it from fallback, we keep it. Otherwise try to grab it.
    if out.get("detailedForecast") is None and forecast_url:
        try:
            fc = requests.get(forecast_url, headers=headers, timeout=5)
            fc.raise_for_status()
            periods = (fc.json().get("properties") or {}).get("periods") or []
            if periods:
                out["detailedForecast"] = periods[0].get("detailedForecast")
        except Exception:
            pass

    # --- alerts (C) ---
    alerts = []
    has_weather_alert = False
    max_alert_severity = "none"
    try:
        alerts_url = f"https://api.weather.gov/alerts/active?point={lat:.4f},{lon:.4f}"
        ar = requests.get(alerts_url, headers=headers, timeout=5)
        ar.raise_for_status()
        feats = ar.json().get("features") or []
        for f in feats:
            p = f.get("properties") or {}
            alerts.append(
                {
                    "event": p.get("event"),
                    "severity": p.get("severity"),
                    "certainty": p.get("certainty"),
                    "urgency": p.get("urgency"),
                    "headline": p.get("headline"),
                    "sent": p.get("sent"),
                    "onset": p.get("onset"),
                    "effective": p.get("effective"),
                    "ends": p.get("ends"),
                    "expires": p.get("expires"),
                    "description": p.get("description"),
                    "instruction": p.get("instruction"),
                }
            )
        if alerts:
            has_weather_alert = True
            # Keep your simple approach (first alert). You can improve later by ranking severities.
            max_alert_severity = (alerts[0].get("severity") or "unknown").lower()
    except Exception:
        pass

    out["alerts"] = alerts
    out["has_weather_alert"] = has_weather_alert
    out["max_alert_severity"] = max_alert_severity

    return out


# ----------------------------
# Frontend (Option A)
# ----------------------------
BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
INDEX_PATH = STATIC_DIR / "index.html"

# Serve /static/* (styles.css, app.js, images, etc.)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def index():
    return FileResponse(INDEX_PATH)


# ----------------------------
# API
# ----------------------------
@app.get("/api/status")
def api_status(
    # Backward/forward compatible: accept either ?query= (existing) or ?q= (frontend convention)
    query: Optional[str] = Query(None, description="Site ID or lat,lon"),
    q: Optional[str] = Query(None, description="Alias for 'query' (Site ID or lat,lon)"),
) -> Dict[str, Any]:
    raw_in = (query if query is not None else q)
    q_str = (raw_in or "").strip()

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
        resolved = {"type": "latlon", "name": f"{lat:.7f}, {lon:.7f}", "site_id": None, "lat": lat, "lon": lon, "utility": None}
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
        # --- Added: include address fields in resolved payload for site lookups
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

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_weather = ex.submit(fetch_weather, lat, lon)

        if site_utility:
            f_power = ex.submit(get_power_status, lat, lon, site_utility)
        else:

            def do_probe():
                chosen, attempts = probe_power_status(lat, lon)
                return chosen, attempts

            f_power = ex.submit(do_probe)

        weather = f_weather.result()

        if site_utility:
            power_obj = f_power.result()
        else:
            power_obj, attempts = f_power.result()
            probe_payload = {
                "mode": "probe",
                "winner": getattr(power_obj, "utility", None) if getattr(power_obj, "has_outage_nearby", False) else None,
                "attempts": [
                    {
                        "provider": a.utility,
                        "ok": a.meta.ok,
                        "error": a.meta.error,
                        "has_outage_nearby": a.has_outage_nearby,
                        "nearest_distance_miles": (a.nearest.distance_miles if a.nearest else None),
                        "nearest_customers_out": (a.nearest.customers_out if a.nearest else None),
                    }
                    for a in attempts
                ],
            }

    provider_banner = provider_info(site_utility)

    # Normalize power object to dict
    power_payload = power_obj.model_dump() if hasattr(power_obj, "model_dump") else power_obj

    return {"query": raw_in, "resolved": resolved, "provider": provider_banner, "weather": weather, "power": power_payload, "probe": probe_payload}
