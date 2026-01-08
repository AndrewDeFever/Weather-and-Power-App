from typing import Dict, Any, Optional, List

import requests
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="Weather & Power Status API",
    version="0.5.0",
)

# ---------------------------------------------------------------------------
# Site inventory (hardcoded for now)
# Keys are UPPERCASE so we can do case-insensitive lookups.
# Each site has: site_id, name, lat, lon, utility
# ---------------------------------------------------------------------------

SITES: Dict[str, Dict[str, Any]] = {
    "CRPTXAUSTOF": {
        "site_id": "CRPTXAUSTOF",
        "name": "Austin Texas Office",
        "lat": 30.26206,
        "lon": -97.7880626,
        "utility": "AUSTIN_ENERGY",   # Austin Energy
    },
    "CRPOKTULSHQ": {
        "site_id": "CRPOKTULSHQ",
        "name": "Corporate OK Tulsa HQ",
        "lat": 36.15224,
        "lon": -95.98956,
        "utility": "PSO",             # Public Service Company of Oklahoma (AEP)
    },
    "CRPOKOKCUIC": {
        "site_id": "CRPOKOKCUIC",
        "name": "Oklahoma City Information Center",
        "lat": 35.47121,
        "lon": -97.5181726,
        "utility": "OGE",             # Oklahoma Gas & Electric
    },
    # Extra sample sites
    "TUL-01": {
        "site_id": "TUL-01",
        "name": "Tulsa Operations Center",
        "lat": 36.15398,
        "lon": -95.99277,
        "utility": "PSO",
    },
    "AMA-02": {
        "site_id": "AMA-02",
        "name": "Amarillo District Office",
        "lat": 35.221997,
        "lon": -101.831297,
        "utility": "ONCOR",           # Example only
    },
}

# ---------------------------------------------------------------------------
# KUBRA configuration
# stormcenters[instance_id] + views[view_id] for each utility
# ---------------------------------------------------------------------------

KUBRA_CONFIG: Dict[str, Dict[str, str]] = {
    "OGE": {
        # OG&E Storm Center IDs you found
        "instance_id": "dc85f79f-59f9-4e9e-9557-b3a9bee7e0ce",
        "view_id": "8fe9d356-96bc-41f1-b353-6720eb408936",
    },
    # Later: add PSO / ONCOR / EVERGY / etc. here
}

# ---------------------------------------------------------------------------
# Geocoding (OpenStreetMap Nominatim)
# ---------------------------------------------------------------------------

GEOCODER_URL = "https://nominatim.openstreetmap.org/search"
GEOCODER_USER_AGENT = "onegas-noc-tool/0.1 (andrewdefever@gmail.com)"


def geocode_location(query: str) -> Optional[Dict[str, Any]]:
    """
    Use OpenStreetMap Nominatim to turn a free-text query into lat/lon.
    Returns a dict with lat, lon, and display_name, or None if not found.
    """
    params = {
        "q": query,
        "format": "json",
        "limit": 1,
    }
    headers = {
        "User-Agent": GEOCODER_USER_AGENT,
    }

    try:
        resp = requests.get(GEOCODER_URL, params=params, headers=headers, timeout=5)
        resp.raise_for_status()
        results = resp.json()
    except Exception:
        return None

    if not results:
        return None

    first = results[0]
    try:
        lat = float(first["lat"])
        lon = float(first["lon"])
    except (KeyError, ValueError):
        return None

    return {
        "lat": lat,
        "lon": lon,
        "display_name": first.get("display_name", query),
    }


# ---------------------------------------------------------------------------
# Simple Web UI
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index() -> str:
    """
    Very simple web UI: search box that calls /api/status and prints the result.
    """
    return """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Weather & Power Status</title>
    <style>
      body {
        font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 0;
        padding: 1.5rem;
        background: #f3f4f6;
      }
      h1 {
        margin-top: 0;
      }
      .card {
        background: #ffffff;
        border-radius: 8px;
        padding: 1rem 1.25rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        margin-top: 1rem;
      }
      .row {
        display: flex;
        gap: 0.5rem;
        margin-bottom: 0.75rem;
      }
      input[type="text"] {
        flex: 1;
        padding: 0.5rem 0.75rem;
        border-radius: 4px;
        border: 1px solid #d1d5db;
      }
      button {
        padding: 0.5rem 0.9rem;
        border-radius: 4px;
        border: none;
        background: #2563eb;
        color: #ffffff;
        cursor: pointer;
      }
      button:disabled {
        background: #9ca3af;
        cursor: default;
      }
      .error {
        color: #b91c1c;
        margin-top: 0.5rem;
      }
      pre {
        white-space: pre-wrap;
        word-wrap: break-word;
        font-size: 0.8rem;
        background: #f9fafb;
        padding: 0.75rem;
        border-radius: 6px;
        border: 1px solid #e5e7eb;
      }
    </style>
  </head>
  <body>
    <h1>Weather & Power Status</h1>
    <div class="card">
      <div class="row">
        <input id="query" type="text" placeholder="Enter site ID (e.g. CRPOKTULSHQ) or city/address" />
        <button id="searchBtn" onclick="search()">Search</button>
      </div>
      <div id="error" class="error"></div>
    </div>

    <div id="resultCard" class="card" style="display:none;">
      <h2 id="resolvedTitle"></h2>
      <p id="resolvedSub"></p>

      <h3>Weather</h3>
      <p id="weatherLine"></p>

      <h3>Power</h3>
      <p id="powerLine"></p>

      <details style="margin-top:0.5rem;">
        <summary>Raw response</summary>
        <pre id="rawJson"></pre>
      </details>
    </div>

    <script>
      async function search() {
        const queryInput = document.getElementById("query");
        const errorEl = document.getElementById("error");
        const resultCard = document.getElementById("resultCard");
        const searchBtn = document.getElementById("searchBtn");

        errorEl.textContent = "";
        resultCard.style.display = "none";

        const q = queryInput.value.trim();
        if (!q) {
          errorEl.textContent = "Please enter a site ID or location.";
          return;
        }

        searchBtn.disabled = true;
        searchBtn.textContent = "Searching...";

        try {
          const res = await fetch("/api/status?query=" + encodeURIComponent(q));
          if (!res.ok) {
            const body = await res.json().catch(() => ({}));
            errorEl.textContent = "Error " + res.status + (body.detail ? ": " + body.detail : "");
            return;
          }
          const data = await res.json();
          renderResult(data);
        } catch (e) {
          errorEl.textContent = "Error: " + e.message;
        } finally {
          searchBtn.disabled = false;
          searchBtn.textContent = "Search";
        }
      }

      function renderResult(data) {
        const resultCard = document.getElementById("resultCard");
        const resolvedTitle = document.getElementById("resolvedTitle");
        const resolvedSub = document.getElementById("resolvedSub");
        const weatherLine = document.getElementById("weatherLine");
        const powerLine = document.getElementById("powerLine");
        const rawJson = document.getElementById("rawJson");

        const r = data.resolved || {};
        const w = data.weather || {};
        const p = data.power || {};

        let title = r.name || "(unknown location)";
        if (r.site_id) {
          title += " (" + r.site_id + ")";
        }
        resolvedTitle.textContent = title;

        let utility = r.utility || "n/a";
        resolvedSub.textContent = `Type: ${r.type || "n/a"}  |  Lat/Lon: ${r.lat ?? "?"}, ${r.lon ?? "?"}  |  Utility: ${utility}`;

        // Weather line
        let weatherText = "";
        if (w.condition || w.temperature_f != null) {
          weatherText = `${w.condition || "Unknown"}`;
          if (w.temperature_f != null) {
            weatherText += `, ${w.temperature_f} °F`;
          }
          if (w.has_weather_alert) {
            weatherText += ` (Alert: ${w.max_alert_severity || "yes"})`;
          }
        } else {
          weatherText = "No weather data.";
        }
        weatherLine.textContent = weatherText;

        // Power line
        let powerText = "";
        if (p.has_outage_nearby === true) {
          powerText = `Outages nearby`;
          if (p.percent_out != null) {
            powerText += ` (${p.percent_out}% affected`;
            if (p.scope_name) {
              powerText += ` in ${p.scope_name}`;
            }
            powerText += `)`;
          }
          if (p.estimated_restoration) {
            powerText += `. ETR: ${p.estimated_restoration}`;
          } else {
            powerText += `. ETR: not available`;
          }
          if (p.cause) {
            powerText += `. Cause: ${p.cause}`;
          }
        } else if (p.has_outage_nearby === false) {
          powerText = "No outages reported nearby.";
        } else {
          powerText = p.status_text || "No power data.";
        }
        powerLine.textContent = powerText;

        rawJson.textContent = JSON.stringify(data, null, 2);
        resultCard.style.display = "block";
      }

      // Allow Enter key to trigger search
      document.addEventListener("DOMContentLoaded", () => {
        const input = document.getElementById("query");
        input.addEventListener("keydown", (e) => {
          if (e.key === "Enter") {
            search();
          }
        });
      });
    </script>
  </body>
</html>
    """


# ---------------------------------------------------------------------------
# API: /api/status
# ---------------------------------------------------------------------------

@app.get("/api/status")
def get_status(query: str = Query(..., description="Site ID or location search string")) -> Dict[str, Any]:
    """
    Status endpoint.
    - If query matches a known site ID, use that site's lat/lon + utility.
    - Otherwise, geocode the query to lat/lon (no utility mapping yet).
    - Returns NWS-based weather and utility-based power.
    """
    normalized = query.strip().upper()

    # 1) Try site lookup
    site = SITES.get(normalized)

    if site:
        lat, lon = site["lat"], site["lon"]
        resolved = {
            "type": "site",
            "name": site["name"],
            "site_id": site["site_id"],
            "lat": lat,
            "lon": lon,
            "utility": site.get("utility"),
        }
        utility = site.get("utility")
    else:
        # 2) Geocode free-text location
        geo = geocode_location(query)
        if not geo:
            raise HTTPException(status_code=404, detail="Could not resolve location")

        lat, lon = geo["lat"], geo["lon"]
        resolved = {
            "type": "address",
            "name": geo["display_name"],
            "site_id": None,
            "lat": lat,
            "lon": lon,
            "utility": None,
        }
        utility = None

    # 3) Weather (real from NWS) + power (utility-aware)
    weather = get_weather(lat, lon)
    power = get_power(lat, lon, utility)

    return {
        "query": query,
        "resolved": resolved,
        "weather": weather,
        "power": power,
    }


# ---------------------------------------------------------------------------
# NWS Weather Integration
# ---------------------------------------------------------------------------

NWS_BASE = "https://api.weather.gov"
NWS_USER_AGENT = "onegas-noc-tool/0.1 (andrewdefever@gmail.com)"


def get_weather(lat: float, lon: float) -> Dict[str, Any]:
    """
    Fetch current-ish weather + alerts from NWS for the given lat/lon.
    1) Call /points/{lat},{lon} to get metadata + forecast URL.
    2) Call the forecast URL and take the first period as "current conditions".
    3) Call /alerts?point=lat,lon to get active alerts.
    """
    headers = {
        "User-Agent": NWS_USER_AGENT,
        "Accept": "application/geo+json",
    }

    # Step 1: /points/{lat},{lon}
    points_url = f"{NWS_BASE}/points/{lat:.4f},{lon:.4f}"

    try:
        resp_points = requests.get(points_url, headers=headers, timeout=5)
        resp_points.raise_for_status()
        points_data = resp_points.json()
    except Exception:
        return {
            "temperature_f": None,
            "condition": None,
            "has_weather_alert": False,
            "max_alert_severity": "none",
            "alerts": [],
        }

    props = points_data.get("properties", {})
    forecast_url = props.get("forecast")

    # Step 2: forecast
    temperature_f: Optional[float] = None
    condition: Optional[str] = None

    if forecast_url:
        try:
            resp_forecast = requests.get(forecast_url, headers=headers, timeout=5)
            resp_forecast.raise_for_status()
            forecast_data = resp_forecast.json()
            periods: List[Dict[str, Any]] = forecast_data.get("properties", {}).get("periods", [])
            if periods:
                p0 = periods[0]
                temp = p0.get("temperature")
                unit = p0.get("temperatureUnit")
                if temp is not None:
                    if unit == "F":
                        temperature_f = float(temp)
                    elif unit == "C":
                        temperature_f = float(temp) * 9.0 / 5.0 + 32.0
                    else:
                        temperature_f = float(temp)
                condition = p0.get("shortForecast")
        except Exception:
            pass

    # Step 3: alerts for point
    alerts_url = f"{NWS_BASE}/alerts"
    params = {
        "point": f"{lat:.4f},{lon:.4f}",
        "status": "actual",
        "message_type": "alert",
        "limit": 10,
    }

    alerts: List[Dict[str, Any]] = []
    max_severity = "none"

    severity_rank = {
        "none": 0,
        "minor": 1,
        "moderate": 2,
        "severe": 3,
        "extreme": 4,
    }

    try:
        resp_alerts = requests.get(alerts_url, headers=headers, params=params, timeout=5)
        resp_alerts.raise_for_status()
        alerts_data = resp_alerts.json()
        features: List[Dict[str, Any]] = alerts_data.get("features", [])
        for f in features:
            ap = f.get("properties", {})
            sev_raw = (ap.get("severity") or "none").lower()
            sev = sev_raw if sev_raw in severity_rank else "none"
            alert_obj = {
                "id": ap.get("id") or ap.get("event"),
                "type": ap.get("event"),
                "severity": sev,
                "starts_at": ap.get("onset") or ap.get("effective"),
                "expires_at": ap.get("ends") or ap.get("expires"),
                "headline": ap.get("headline"),
                "description": ap.get("description"),
            }
            alerts.append(alert_obj)

            if severity_rank[sev] > severity_rank[max_severity]:
                max_severity = sev
    except Exception:
        alerts = []
        max_severity = "none"

    return {
        "temperature_f": temperature_f,
        "condition": condition,
        "has_weather_alert": len(alerts) > 0,
        "max_alert_severity": max_severity,
        "alerts": alerts,
    }


# ---------------------------------------------------------------------------
# KUBRA helpers for Storm Center-backed utilities
# ---------------------------------------------------------------------------

def fetch_kubra_current_state(instance_id: str, view_id: str) -> Optional[Dict[str, Any]]:
    """
    Call KUBRA Storm Center currentState API and return the JSON manifest.
    """
    url = (
        f"https://kubra.io/stormcenter/api/v1/stormcenters/"
        f"{instance_id}/views/{view_id}/currentState"
    )
    params = {"preview": "false"}

    try:
        resp = requests.get(url, params=params, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def fetch_kubra_summary_for_interval(interval_path: str) -> Optional[Dict[str, Any]]:
    """
    Given an interval_generation_data path from currentState, e.g.:

        "data/69e637c8-6f1b-4b80-9499-c125e7a29400"

    try to load its summary totals from:

        https://kubra.io/<interval_path>/public/summary-1/data.json

    Returns parsed JSON, or None on error/404.
    """
    interval_path = interval_path.lstrip("/")
    base_url = f"https://kubra.io/{interval_path}"
    summary_url = f"{base_url}/public/summary-1/data.json"

    try:
        resp = requests.get(summary_url, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def fetch_kubra_serviceareas(current_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Use the `datastatic` section of currentState to locate serviceareas.json
    with per-outage details (ETR, cause, start_time, customers_out).

    datastatic example:

        "datastatic": {
          "638880d8-38ea-4a2e-a11d-823cbc72703f": "regions/05f35c16-871b-42a3-9c53-a6dd623f822a"
        }

    Which maps to:

        https://kubra.io/regions/05f35c16-871b-42a3-9c53-a6dd623f822a/638880d8-38ea-4a2e-a11d-823cbc72703f/serviceareas.json
    """
    datastatic = current_state.get("datastatic") or {}
    if not datastatic:
        return None

    static_id, region_path = next(iter(datastatic.items()))
    region_path = (region_path or "").strip("/")

    if not static_id or not region_path:
        return None

    serviceareas_url = (
        f"https://kubra.io/{region_path}/{static_id}/serviceareas.json"
    )

    try:
        resp = requests.get(serviceareas_url, timeout=5)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Power / Outage Integration – utility-aware dispatcher
# ---------------------------------------------------------------------------

def get_power(lat: float, lon: float, utility: Optional[str]) -> Dict[str, Any]:
    """
    Power/outage provider dispatcher.
    - OGE: real KUBRA-backed stats
    - Others: stubbed for now
    """
    if utility == "OGE":
        return get_power_oge(lat, lon)
    elif utility == "PSO":
        return get_power_pso(lat, lon)
    elif utility == "AUSTIN_ENERGY":
        return get_power_austin(lat, lon)
    elif utility == "ONCOR":
        return get_power_oncor(lat, lon)
    elif utility == "EVERGY":
        return get_power_evergy(lat, lon)
    else:
        return {
            "has_outage_nearby": None,
            "customers_out": None,
            "customers_total": None,
            "percent_out": None,
            "scope": None,
            "scope_name": None,
            "last_updated": None,
            "estimated_restoration": None,
            "estimated_restoration_source": "none",
            "status_text": "No utility mapping for this site/location",
            "cause": None,
        }


def get_power_oge(lat: float, lon: float) -> Dict[str, Any]:
    """
    OG&E outage integration via KUBRA Storm Center.

    Flow:
      1) Call currentState (stable URL via instance/view IDs).
      2) Use data.interval_generation_data  -> summary-1/data.json for system totals.
      3) Use datastatic                     -> serviceareas.json for per-outage details.
      4) Select a 'primary' outage, ideally the one nearest the site.
         (For now we fall back to largest customers_out until geometry is wired.)
    """
    cfg = KUBRA_CONFIG.get("OGE")
    if not cfg:
        return {
            "has_outage_nearby": None,
            "customers_out": None,
            "customers_total": None,
            "percent_out": None,
            "scope": "utility",
            "scope_name": "OG&E",
            "last_updated": None,
            "estimated_restoration": None,
            "estimated_restoration_source": "none",
            "status_text": "No KUBRA configuration for OG&E",
            "cause": None,
        }

    # ---- 1) currentState manifest ----
    current_state = fetch_kubra_current_state(cfg["instance_id"], cfg["view_id"])
    if not current_state:
        return {
            "has_outage_nearby": None,
            "customers_out": None,
            "customers_total": None,
            "percent_out": None,
            "scope": "utility",
            "scope_name": "OG&E",
            "last_updated": None,
            "estimated_restoration": None,
            "estimated_restoration_source": "none",
            "status_text": "Unable to reach OG&E KUBRA currentState",
            "cause": None,
        }

    data_section = current_state.get("data") or {}
    interval_path = data_section.get("interval_generation_data")
    if not interval_path:
        return {
            "has_outage_nearby": None,
            "customers_out": None,
            "customers_total": None,
            "percent_out": None,
            "scope": "utility",
            "scope_name": "OG&E",
            "last_updated": None,
            "estimated_restoration": None,
            "estimated_restoration_source": "none",
            "status_text": "OG&E currentState has no interval_generation_data",
            "cause": None,
        }

    # ---- 2) summary totals for this interval ----
    summary_json = fetch_kubra_summary_for_interval(interval_path)
    if not summary_json:
        return {
            "has_outage_nearby": None,
            "customers_out": None,
            "customers_total": None,
            "percent_out": None,
            "scope": "utility",
            "scope_name": "OG&E",
            "last_updated": None,
            "estimated_restoration": None,
            "estimated_restoration_source": "none",
            "status_text": "OG&E summary data not available for current interval",
            "cause": None,
        }

    summary_data = (
        summary_json.get("summaryFileData")
        or summary_json.get("summary")
        or {}
    )
    totals_list = summary_data.get("totals") or []
    totals = totals_list[0] if totals_list else {}

    def _val(obj: Any) -> Optional[float]:
        if isinstance(obj, dict):
            v = obj.get("val")
            if isinstance(v, (int, float)):
                return float(v)
        return None

    total_cust_a = _val(totals.get("total_cust_a"))
    total_cust_s = _val(totals.get("total_cust_s"))
    total_percent_cust_a = _val(totals.get("total_percent_cust_a"))
    total_outages = totals.get("total_outages")
    last_generated = summary_data.get("date_generated") or summary_data.get("last_updated")

    customers_out = int(total_cust_a) if total_cust_a is not None else 0
    customers_total = int(total_cust_s) if total_cust_s is not None else None

    if total_percent_cust_a is not None:
        percent_out = float(total_percent_cust_a)
    elif customers_total and customers_total > 0:
        percent_out = customers_out / customers_total * 100.0
    else:
        percent_out = None

    has_outage = customers_out > 0

    # ---- 3) per-outage details from serviceareas.json ----
    serviceareas_json = fetch_kubra_serviceareas(current_state)
    outages_slim: List[Dict[str, Any]] = []

    if serviceareas_json:
        file_data = serviceareas_json.get("file_data") or []
        for o in file_data:
            desc = o.get("desc", {}) or {}
            cust_a_obj = desc.get("cust_a", {}) or {}
            n_out = desc.get("n_out")

            cust_val = _val(cust_a_obj)
            cust_int = int(cust_val) if cust_val is not None else None

            etr = o.get("etr")
            cause = o.get("cause")
            start_time = o.get("start_time")
            geom = o.get("geom") or {}

            outages_slim.append(
                {
                    "id": o.get("id"),
                    "title": o.get("title"),
                    "customers_out": cust_int,
                    "n_out": n_out,
                    "etr": etr,
                    "cause": cause,
                    "start_time": start_time,
                    "geom": geom,  # contains 'p': [poly_id, ...]
                }
            )

    # ---- 4) pick primary outage (designed for "nearest", falls back to largest impact) ----

    def select_primary_outage(outages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Intended behavior: choose outage nearest the site using geometry.

        Current behavior (until we decode polygon -> lat/lon):
          - Choose outage with the largest customers_out.
        """
        if not outages:
            return None

        def impact_key(o: Dict[str, Any]) -> int:
            co = o.get("customers_out")
            return co if isinstance(co, int) and co >= 0 else 0

        return max(outages, key=impact_key)

    best_outage = select_primary_outage(outages_slim)
    estimated_restoration: Optional[str] = None
    best_cause: Optional[str] = None

    if best_outage:
        estimated_restoration = best_outage.get("etr")
        if best_outage.get("cause"):
            best_cause = str(best_outage["cause"])
        else:
            # fallback: any cause from any outage
            for o in outages_slim:
                if o.get("cause"):
                    best_cause = str(o["cause"])
                    break

    if totals_list:
        status = (
            f"OG&E (KUBRA): {customers_out} customers out of "
            f"{customers_total if customers_total is not None else 'unknown'}; "
            f"total_outages={total_outages}"
        )
    else:
        status = "OG&E summary returned no totals data"

    if not has_outage:
        status += " (no customers currently out)"

    result: Dict[str, Any] = {
        "has_outage_nearby": has_outage,
        "customers_out": customers_out if has_outage else 0,
        "customers_total": customers_total,
        "percent_out": round(percent_out, 2) if percent_out is not None else None,
        "scope": "utility",
        "scope_name": "OG&E",
        "last_updated": last_generated,
        "estimated_restoration": estimated_restoration,
        "estimated_restoration_source": "utility" if estimated_restoration else "none",
        "status_text": status,
        "cause": best_cause,
    }

    if outages_slim:
        result["outages"] = outages_slim
        if best_outage:
            result["best_outage"] = best_outage

    return result


# ---- Stub implementations for other utilities (PSO, Austin, Oncor, Evergy) ----

def get_power_austin(lat: float, lon: float) -> Dict[str, Any]:
    customers_total = 100000
    customers_out = 500
    percent_out = customers_out / customers_total * 100.0
    return {
        "has_outage_nearby": customers_out > 0,
        "customers_out": customers_out,
        "customers_total": customers_total,
        "percent_out": round(percent_out, 2),
        "scope": "utility",
        "scope_name": "Austin Energy",
        "last_updated": None,
        "estimated_restoration": None,
        "estimated_restoration_source": "stub",
        "status_text": "Austin Energy stub: example outages in the area",
        "cause": "Weather-related (stub)",
    }


def get_power_pso(lat: float, lon: float) -> Dict[str, Any]:
    customers_total = 30000
    customers_out = 0
    percent_out = 0.0
    return {
        "has_outage_nearby": customers_out > 0,
        "customers_out": customers_out,
        "customers_total": customers_total,
        "percent_out": round(percent_out, 1),
        "scope": "utility",
        "scope_name": "Public Service Company of Oklahoma",
        "last_updated": None,
        "estimated_restoration": None,
        "estimated_restoration_source": "stub",
        "status_text": "PSO stub: no outages reported",
        "cause": None,
    }


def get_power_oncor(lat: float, lon: float) -> Dict[str, Any]:
    customers_total = 80000
    customers_out = 120
    percent_out = customers_out / customers_total * 100.0
    return {
        "has_outage_nearby": customers_out > 0,
        "customers_out": customers_out,
        "customers_total": customers_total,
        "percent_out": round(percent_out, 1),
        "scope": "utility",
        "scope_name": "Oncor",
        "last_updated": None,
        "estimated_restoration": None,
        "estimated_restoration_source": "stub",
        "status_text": "Oncor stub: localized outages in the area",
        "cause": "High winds (stub)",
    }


def get_power_evergy(lat: float, lon: float) -> Dict[str, Any]:
    customers_total = 60000
    customers_out = 10
    percent_out = customers_out / customers_total * 100.0
    return {
        "has_outage_nearby": customers_out > 0,
        "customers_out": customers_out,
        "customers_total": customers_total,
        "percent_out": round(percent_out, 2),
        "scope": "utility",
        "scope_name": "Evergy",
        "last_updated": None,
        "estimated_restoration": None,
        "estimated_restoration_source": "stub",
        "status_text": "Evergy stub: minimal outages in the area",
        "cause": None,
    }
