from __future__ import annotations

import difflib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse

from app.power_router import get_power_status

app = FastAPI(title="Weather & Power Status", version="0.7.4")


# ---------------------------------------------------------------------
# Provider map (display + outage map link)
# ---------------------------------------------------------------------
PROVIDERS: Dict[str, Dict[str, str]] = {
    "OGE": {
        "name": "OG&E",
        "outage_map": "https://www.oge.com/wps/portal/oge/outages/systemwatch",
        "platform": "Kubra StormCenter",
    },
    "PSO": {
        "name": "PSO",
        "outage_map": "https://www.psoklahoma.com/outages/",
        "platform": "Kubra StormCenter",
    },
    "ONCOR": {
        "name": "Oncor",
        "outage_map": "https://stormcenter.oncor.com/",
        "platform": "Kubra StormCenter",
    },
    "EVERGY": {
        "name": "Evergy",
        "outage_map": "https://outagemap.evergy.com/",
        "platform": "Kubra StormCenter",
    },
}


def provider_info(utility: Optional[str]) -> Dict[str, str]:
    u = (utility or "").strip().upper()
    return PROVIDERS.get(u, {"name": u or "Unknown", "outage_map": "n/a", "platform": ""})


# ---------------------------------------------------------------------
# Site registry
# ---------------------------------------------------------------------
def load_sites() -> Dict[str, Dict[str, Any]]:
    p = Path(__file__).parent / "data" / "sites.json"
    if p.exists():
        data = json.loads(p.read_text(encoding="utf-8"))

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
            sid = (s.get("site_id") or s.get("id") or "").strip().upper()
            sid = (s.get("site_id") or s.get("id") or "").strip().upper()
            if not sid:
                continue

            out[sid] = {
                "site_id": sid,
                "name": s.get("name") or sid,
                "lat": float(s["lat"]),
                "lon": float(s["lon"]),
                "utility": (s.get("utility") or "").strip().upper() or None,
                "sev": s.get("sev"),
                "city": s.get("city"),
                "state": s.get("state"),
            }
        return out

    # Fallback for first-run dev
    return {
        "CRPOKOKCYIC": {
            "site_id": "CRPOKOKCYIC",
            "name": "Oklahoma City Information Center",
            "lat": 35.47121,
            "lon": -97.5181726,
            "utility": "OGE",
        }
    }


SITES: Dict[str, Dict[str, Any]] = load_sites()


# ---------------------------------------------------------------------
# Weather (NWS)  -- kept inside api.py (no app.weather module required)
# ---------------------------------------------------------------------
NWS_BASE = "https://api.weather.gov"
NWS_USER_AGENT = "onegas-noc-tool/0.1"


def get_weather(lat: float, lon: float) -> Dict[str, Any]:
    headers = {"User-Agent": NWS_USER_AGENT, "Accept": "application/geo+json"}

    try:
        points = requests.get(
            f"{NWS_BASE}/points/{lat:.4f},{lon:.4f}",
            headers=headers,
            timeout=5,
        )
        points.raise_for_status()
        points_data = points.json()
    except Exception:
        return {
            "temperature_f": None,
            "condition": None,
            "has_weather_alert": False,
            "max_alert_severity": "none",
            "alerts": [],
        }

    props = (points_data.get("properties") or {})
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
                temp = p0.get("temperature")
                unit = p0.get("temperatureUnit")
                if isinstance(temp, (int, float)):
                    if unit == "F":
                        temperature_f = float(temp)
                    elif unit == "C":
                        temperature_f = float(temp) * 9 / 5 + 32
                    else:
                        temperature_f = float(temp)
                condition = p0.get("shortForecast")
        except Exception:
            pass

    severity_rank = {"none": 0, "minor": 1, "moderate": 2, "severe": 3, "extreme": 4}
    max_sev = "none"
    alerts: List[Dict[str, Any]] = []

    try:
        a = requests.get(
            f"{NWS_BASE}/alerts",
            headers=headers,
            params={
                "point": f"{lat:.4f},{lon:.4f}",
                "status": "actual",
                "message_type": "alert",
                "limit": 10,
            },
            timeout=5,
        )
        a.raise_for_status()
        feats = a.json().get("features") or []
        for f in feats:
            ap = f.get("properties") or {}
            sev_raw = (ap.get("severity") or "none").lower()
            sev = sev_raw if sev_raw in severity_rank else "none"
            if severity_rank[sev] > severity_rank[max_sev]:
                max_sev = sev
            alerts.append(
                {
                    "event": ap.get("event"),
                    "severity": sev,
                    "headline": ap.get("headline"),
                    "effective": ap.get("effective"),
                    "expires": ap.get("expires"),
                }
            )
    except Exception:
        alerts = []
        max_sev = "none"

    return {
        "temperature_f": temperature_f,
        "condition": condition,
        "has_weather_alert": bool(alerts),
        "max_alert_severity": max_sev,
        "alerts": alerts,
    }


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def parse_latlon(q: str) -> Optional[Tuple[float, float]]:
    if "," not in q:
        return None
    parts = [p.strip() for p in q.split(",")]
    if len(parts) != 2:
        return None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None


# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def index() -> str:
    # Keep your embedded UI. If you already have a richer HTML version, you can paste it back in here.
    return """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Weather & Power Status</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; background: #f6f7fb; }
    .card { background: #fff; border: 1px solid #e6e7ee; border-radius: 12px; padding: 16px; margin-bottom: 16px; box-shadow: 0 1px 2px rgba(0,0,0,.04); }
    h1 { margin: 0 0 12px; }
    .row { display: flex; gap: 12px; }
    input { flex: 1; padding: 12px; border-radius: 10px; border: 1px solid #cfd2dc; font-size: 16px; }
    button { padding: 12px 18px; border-radius: 10px; border: 1px solid #111827; background: #111827; color: #fff; cursor: pointer; }
    button[disabled] { opacity: .7; cursor: not-allowed; }
    .muted { color: #6b7280; font-size: 14px; }
    .title { font-size: 22px; font-weight: 700; margin: 0 0 8px; }
    .section { font-weight: 700; margin-top: 12px; }
    ul { margin: 8px 0 0 18px; }
    a { color: #2563eb; text-decoration: none; }
    a:hover { text-decoration: underline; }
    pre { white-space: pre-wrap; word-wrap: break-word; background: #0b1020; color: #e5e7eb; padding: 12px; border-radius: 10px; overflow: auto; }
  </style>
</head>
<body>
  <div class="card">
    <h1>Weather & Power Status</h1>
    <div class="row">
      <input id="q" placeholder="SITE_ID or lat,lon" />
      <button id="btn" onclick="run()">Search</button>
    </div>
    <div class="muted" style="margin-top:8px;">Tip: Try CRPOKOKCYIC</div>
  </div>

  <div id="out" class="card" style="display:none;"></div>

<script>
function fmtCT(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  if (isNaN(d)) return iso;
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: true,
    timeZone: 'America/Chicago'
  }) + ' CT';
}

async function run() {
  const q = document.getElementById('q').value.trim();
  if (!q) return;

  const btn = document.getElementById('btn');
  const out = document.getElementById('out');

  btn.disabled = true;
  btn.textContent = 'Loading...';
  out.style.display = 'block';
  out.innerHTML = '<div class="title">Loading...</div><div class="muted">Fetching weather + power status.</div>';

  try {
    const r = await fetch('/api/status?query=' + encodeURIComponent(q));
    const data = await r.json();

    if (!r.ok) {
      out.innerHTML = '<div class="title">Error</div><pre>' + JSON.stringify(data, null, 2) + '</pre>';
      return;
    }

    const resolved = data.resolved || {};
    const provider = data.provider || {};
    const weather = data.weather || {};
    const power = data.power || {};

    const title = (resolved.name ? resolved.name + ' (' + (resolved.site_id || q) + ')' : q);

    let html = '';
    html += '<div class="title">' + title + '</div>';
    html += '<div class="muted">Type: ' + resolved.type +
            ' | Lat/Lon: ' + resolved.lat + ', ' + resolved.lon +
            ' | Utility: ' + (resolved.utility || 'n/a') + '</div>';

    html += '<div class="section">Weather</div>';
    html += '<div>' + (weather.condition || 'n/a') + ', ' + (weather.temperature_f ?? 'n/a') + ' °F' +
            ' (Alert: ' + (weather.max_alert_severity || 'none') + ')</div>';

    html += '<div class="section">Power</div>';
    const outageMap = provider.outage_map || 'n/a';
    html += '<div class="muted"><b>Provider:</b> ' + (provider.name || 'Unknown') +
            ' | ' + (provider.platform || '') +
            ' | <a href="' + outageMap + '" target="_blank">Outage map</a></div>';

    if (power.meta && power.meta.error) {
      html += '<div style="margin-top:8px;">' + power.meta.error + '</div>';
    } else if (!power.has_outage_nearby) {
      html += '<div style="margin-top:8px;">No outages reported nearby.</div>';
    } else {
      const n = power.nearest || {};
      html += '<ul>';
      if (n.customers_out != null) html += '<li><b>Customers out:</b> ' + n.customers_out + '</li>';

      // Crew status is the operational status we want on the UI
      if (n.raw && n.raw.crew_status) html += '<li><b>Status:</b> ' + n.raw.crew_status + '</li>';
      else if (n.crew_status) html += '<li><b>Status:</b> ' + n.crew_status + '</li>';

      if (n.start_time) html += '<li><b>Start (CT):</b> ' + fmtCT(n.start_time) + '</li>';
      if (n.etr) html += '<li><b>ETR (CT):</b> ' + fmtCT(n.etr) + '</li>';
      if (n.distance_miles != null) html += '<li><b>Distance:</b> ' + n.distance_miles.toFixed(2) + ' mi</li>';
      if (n.outage_id) html += '<li><b>Outage ID:</b> ' + n.outage_id + '</li>';

      // Cause is cause, not status
      if (n.cause) html += '<li><b>Cause:</b> ' + n.cause + '</li>';

      html += '</ul>';
    }

    html += '<div class="muted" style="margin-top:10px;">Data source: ' +
            (power.meta && power.meta.source ? power.meta.source : 'n/a') + '</div>';

    html += '<details style="margin-top:8px;"><summary>Raw response</summary><pre>' +
            JSON.stringify(data, null, 2) + '</pre></details>';

    out.innerHTML = html;

  } catch (e) {
    out.innerHTML = '<div class="title">Error</div><pre>' + String(e) + '</pre>';
  } finally {
    btn.disabled = false;
    btn.textContent = 'Search';
  }
}
</script>
</body>
</html>
"""


@app.get("/api/status")
def api_status(query: str = Query(..., description="Site ID or lat,lon")) -> Dict[str, Any]:
    q = (query or "").strip()

    latlon = parse_latlon(q)
    resolved: Dict[str, Any]

    if latlon:
        lat, lon = latlon
        resolved = {
            "type": "latlon",
            "name": f"{lat:.7f}, {lon:.7f}",
            "site_id": None,
            "lat": lat,
            "lon": lon,
            "utility": None,
        }
    else:
        sid = q.strip().upper()
        site = SITES.get(sid)
        if not site:
            close = difflib.get_close_matches(sid, list(SITES.keys()), n=1)
            hint = f" Did you mean {close[0]}?" if close else ""
            raise HTTPException(status_code=404, detail=f"Unknown site_id: {sid}.{hint}")

        resolved = {
            "type": "site",
            "name": site.get("name") or sid,
            "site_id": sid,
            "lat": site["lat"],
            "lon": site["lon"],
            "utility": site.get("utility"),
        }

    lat = float(resolved["lat"])
    lon = float(resolved["lon"])
    utility = resolved.get("utility")  # None for lat/lon

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_weather = ex.submit(get_weather, lat, lon)

        # ✅ FIX: do NOT force OG&E. Allow router/probing on None.
        f_power = ex.submit(get_power_status, lat, lon, utility)

        weather = f_weather.result()
        power_obj = f_power.result()

    # ✅ Provider attribution should reflect the provider actually used
    prov = provider_info(getattr(power_obj, "utility", utility))

    return {
        "query": query,
        "resolved": resolved,
        "provider": prov,
        "weather": weather,
        "power": power_obj.model_dump() if hasattr(power_obj, "model_dump") else power_obj,
    }
