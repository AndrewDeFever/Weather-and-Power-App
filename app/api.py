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

app = FastAPI(title="Weather & Power Status", version="0.7.8")


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
    w = {"temperature_f": None, "condition": None, "has_weather_alert": False, "max_alert_severity": "none", "alerts": []}
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

    try:
        r = requests.get(points_url, headers=headers, timeout=5)
        r.raise_for_status()
        points_data = r.json()
    except Exception as e:
        return empty_weather(error=f"NWS points fetch failed: {type(e).__name__}: {e}")

    props = points_data.get("properties") or {}
    forecast_url = props.get("forecast")

    temperature_f = None
    condition = None
    if forecast_url:
        try:
            fc = requests.get(forecast_url, headers=headers, timeout=5)
            fc.raise_for_status()
            periods = (fc.json().get("properties") or {}).get("periods") or []
            if periods:
                p0 = periods[0]
                temperature_f = p0.get("temperature")
                condition = p0.get("shortForecast")
        except Exception:
            pass

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
                    "ends": p.get("ends"),
                    "description": p.get("description"),
                    "instruction": p.get("instruction"),
                }
            )
        if alerts:
            has_weather_alert = True
            # Basic severity signal; you can enhance later with a mapping
            max_alert_severity = (alerts[0].get("severity") or "unknown").lower()
    except Exception:
        pass

    return {
        "temperature_f": temperature_f,
        "condition": condition,
        "has_weather_alert": has_weather_alert,
        "max_alert_severity": max_alert_severity,
        "alerts": alerts,
    }


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
        resolved = {"type": "site", "name": site.get("name") or sid, "site_id": sid, "lat": site.get("lat"), "lon": site.get("lon"), "utility": site_utility}

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
