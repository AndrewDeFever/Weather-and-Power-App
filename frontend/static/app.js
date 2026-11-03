/**
 * SubReal Studios Weather & Power Status - Frontend
 * Notes:
 * - API is same-origin: /api/status?q=...
 * - Actions: Open in Google Maps, Open Power Outage Map, Open Weather Map (NWS)
 * - Preserves the production lookup/cards/tabs/maps/theme interaction model using synthetic portfolio data
 *
 * Hardening:
 * - safeUrl() blocks javascript:/data: etc from backend-provided URLs (XSS mitigation)
 * - validateQueryInput() tightens user input hygiene (allowlist + bounds checks)
 */

const API_ENDPOINT = "/api/status";
const SUGGESTION_MIN_QUERY_LENGTH = 2;
const SUGGESTION_COORD_MIN_CHARS = 6;
const DEMO_SITES = [
  {
    "site_id": "DEMO_KCK_01",
    "name": "Kansas City Demo Site",
    "city": "Kansas City",
    "state": "KS",
    "utility": "EVERGY",
    "lat": 39.1147,
    "lon": -94.6276,
    "aliases": []
  },
  {
    "site_id": "DEMO_TOP_01",
    "name": "Topeka Demo Site",
    "city": "Topeka",
    "state": "KS",
    "utility": "EVERGY",
    "lat": 39.0473,
    "lon": -95.6752,
    "aliases": []
  },
  {
    "site_id": "DEMO_TUL_01",
    "name": "Tulsa Demo Site",
    "city": "Tulsa",
    "state": "OK",
    "utility": "PSO",
    "lat": 36.154,
    "lon": -95.9928,
    "aliases": []
  },
  {
    "site_id": "DEMO_BA_01",
    "name": "Broken Arrow Demo Site",
    "city": "Broken Arrow",
    "state": "OK",
    "utility": "PSO",
    "lat": 36.0526,
    "lon": -95.7908,
    "aliases": []
  },
  {
    "site_id": "DEMO_OKC_01",
    "name": "Oklahoma City Demo Site",
    "city": "Oklahoma City",
    "state": "OK",
    "utility": "OGE",
    "lat": 35.4676,
    "lon": -97.5164,
    "aliases": []
  },
  {
    "site_id": "DEMO_NRM_01",
    "name": "Norman Demo Site",
    "city": "Norman",
    "state": "OK",
    "utility": "OGE",
    "lat": 35.2226,
    "lon": -97.4395,
    "aliases": []
  },
  {
    "site_id": "DEMO_DAL_01",
    "name": "Dallas Demo Site",
    "city": "Dallas",
    "state": "TX",
    "utility": "ONCOR",
    "lat": 32.7767,
    "lon": -96.797,
    "aliases": []
  },
  {
    "site_id": "DEMO_FTW_01",
    "name": "Fort Worth Demo Site",
    "city": "Fort Worth",
    "state": "TX",
    "utility": "ONCOR",
    "lat": 32.7555,
    "lon": -97.3308,
    "aliases": []
  },
  {
    "site_id": "DEMO_AUS_01",
    "name": "Austin Central Demo Site",
    "city": "Austin",
    "state": "TX",
    "utility": "AUSTIN",
    "lat": 30.2672,
    "lon": -97.7431,
    "aliases": []
  },
  {
    "site_id": "DEMO_AUS_02",
    "name": "Austin North Demo Site",
    "city": "Austin",
    "state": "TX",
    "utility": "AUSTIN",
    "lat": 30.3072,
    "lon": -97.755,
    "aliases": []
  },
  {
    "site_id": "DEMO_AEP_01",
    "name": "AEP Texas Demo Site",
    "city": "Corpus Christi",
    "state": "TX",
    "utility": "AEP",
    "lat": 27.8006,
    "lon": -97.3964,
    "aliases": []
  },
  {
    "site_id": "DEMO_CNP_01",
    "name": "CenterPoint Demo Site",
    "city": "Houston",
    "state": "TX",
    "utility": "CENTERPOINT",
    "lat": 29.7604,
    "lon": -95.3698,
    "aliases": []
  },
  {
    "site_id": "DEMO_EPE_01",
    "name": "El Paso Electric Demo Site",
    "city": "El Paso",
    "state": "TX",
    "utility": "EPE",
    "lat": 31.7619,
    "lon": -106.485,
    "aliases": []
  },
  {
    "site_id": "DEMO_PEC_01",
    "name": "PEC Demo Site",
    "city": "Johnson City",
    "state": "TX",
    "utility": "PEC",
    "lat": 30.2769,
    "lon": -98.4114,
    "aliases": []
  },
  {
    "site_id": "DEMO_PLE_01",
    "name": "Prairie Land Electric Demo Site",
    "city": "Norton",
    "state": "KS",
    "utility": "PRAIRIE_LAND_ELECTRIC",
    "lat": 39.8339,
    "lon": -99.8915,
    "aliases": []
  },
  {
    "site_id": "DEMO_NRE_01",
    "name": "Ninnescah Rural Electric Demo Site",
    "city": "Pratt",
    "state": "KS",
    "utility": "NINNESCAH_RURAL_ELECTRIC",
    "lat": 37.6439,
    "lon": -98.7376,
    "aliases": []
  },
  {
    "site_id": "DEMO_CEC_01",
    "name": "Concordia Electric Demo Site",
    "city": "Ferriday",
    "state": "LA",
    "utility": "CITY_OF_CONCORDIA_ELECTRIC",
    "lat": 31.6302,
    "lon": -91.5546,
    "aliases": []
  }
];


const $ = (id) => document.getElementById(id);

const q = $("q");
const btn = $("btn");
const toast = $("toast");
const suggestions = $("suggestions");

// Client timeout should be above backend response budget (~14s) to avoid
// client-side aborts before API timeout handling completes.
const CLIENT_TIMEOUT_MS = 17000;

const statusDot = $("statusDot");
const statusText = $("statusText");

const headline = $("headline");
const subhead = $("subhead");
const mapIntelTitle = $("mapIntelTitle");
const mapIntelMeta = $("mapIntelMeta");
const forecastMapEl = $("forecastMap");
const mapStatus = $("mapStatus");
const forecastTimeSlider = $("forecastTimeSlider");
const forecastTimeReadout = $("forecastTimeReadout");
const forecastTimeNotches = $("forecastTimeNotches");
const forecastCurrentTime = $("forecastCurrentTime");
const forecastPrevTime = $("forecastPrevTime");
const forecastNextTime = $("forecastNextTime");
const opsForecastTitle = $("opsForecastTitle");
const opsCurrentWeatherIcon = $("opsCurrentWeatherIcon");
const opsForecastSummary = $("opsForecastSummary");
const opsWeatherMode = $("opsWeatherMode");
const opsWeatherMetrics = $("opsWeatherMetrics");
const opsForecastStrip = $("opsForecastStrip");
const opsWeatherTrend = $("opsWeatherTrend");
const opsWeatherTrendRange = $("opsWeatherTrendRange");
const opsHazardTitle = $("opsHazardTitle");
const opsHazardStatus = $("opsHazardStatus");
const opsHazardList = $("opsHazardList");
const opsPowerTitle = $("opsPowerTitle");
const opsPowerStatus = $("opsPowerStatus");
const opsPowerMeta = $("opsPowerMeta");
const opsPowerSiteMeta = $("opsPowerSiteMeta");
const opsOpenGoogleMaps = $("opsOpenGoogleMaps");
const opsOpenOutageMap = $("opsOpenOutageMap");

const c_loc = $("c_loc");
const c_loc_meta = $("c_loc_meta");
const c_util = $("c_util");
const c_util_meta = $("c_util_meta");
const c_wx = $("c_wx");
const c_wx_meta = $("c_wx_meta");
const c_pwr = $("c_pwr");
const c_pwr_meta = $("c_pwr_meta");

const wxDot = $("wxDot");
const pwrDot = $("pwrDot");

const ov_kv = $("ov_kv");
const pwr_kv = $("pwr_kv");
const wx_kv = $("wx_kv");

// Buttons
const openGoogleMaps = $("openGoogleMaps");
const openOutageMap = $("openOutageMap");
const openNwsMap = $("openNwsMap");

let lastPayload = null;
let ndfdHazardSeries = [];
let ndfdHazardError = null;
let ndfdHazardLocationKey = null;
let currentSuggestions = [];
let activeSuggestionIndex = -1;
let suggestionAbortController = null;
let suggestionDebounceTimer = null;
// Dark-only product mode for the current north-star phase.
document.documentElement.setAttribute("data-theme", "dark");

// --------------------
// Formatting helpers
// --------------------

function formatDateTimeLocal(d) {
  const time = new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(d);

  const date = new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "2-digit",
  }).format(d);

  const today = new Date();
  const isToday =
    d.getFullYear() === today.getFullYear() &&
    d.getMonth() === today.getMonth() &&
    d.getDate() === today.getDate();

  return isToday ? time : `${date} ${time}`;
}

function formatCentralTimestamp() {
  try {
    const now = new Date();
    const timeParts = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Chicago",
      hour: "2-digit",
      minute: "2-digit",
      hour12: true,
    }).formatToParts(now);
    const hour = timeParts.find((p) => p.type === "hour")?.value || "--";
    const minute = timeParts.find((p) => p.type === "minute")?.value || "--";
    const dayPeriod = (timeParts.find((p) => p.type === "dayPeriod")?.value || "").toUpperCase();
    const time = `${hour}:${minute}${dayPeriod}`;
    const date = new Intl.DateTimeFormat("en-US", {
      timeZone: "America/Chicago",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).format(now);
    return `${time} on ${date}`;
  } catch {
    return `${formatDateTimeLocal(new Date())} CT`;
  }
}

function formatIsoLocal(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (!Number.isNaN(d.getTime())) return formatDateTimeLocal(d);
  return String(iso);
}

function formatETR(etr) {
  if (etr === null || etr === undefined || etr === "") return null;

  // Epoch seconds/ms
  if (typeof etr === "number" && Number.isFinite(etr)) {
    const ms = etr < 10_000_000_000 ? etr * 1000 : etr;
    const d = new Date(ms);
    if (!Number.isNaN(d.getTime())) return formatDateTimeLocal(d);
    return String(etr);
  }

  // ISO or provider string
  if (typeof etr === "string") {
    const s = etr.trim();
    if (!s) return null;
    const d = new Date(s);
    if (!Number.isNaN(d.getTime())) return formatDateTimeLocal(d);
    return s;
  }

  return String(etr);
}

function isBlank(v) {
  return v === null || v === undefined || v === "";
}

function displayOrDash(v) {
  return isBlank(v) ? "—" : String(v);
}

function numOrDash(v, suffix = "") {
  if (v === null || v === undefined || v === "" || Number.isNaN(Number(v))) return "—";
  return `${v}${suffix}`;
}

function normalizeCityDisplay(city) {
  const text = String(city || "").trim();
  if (!text) return "";
  return text.replace(/\s*\([^)]*\)\s*$/, "").trim();
}

function formatPostalAddress(address, city, state, zip) {
  const street = String(address || "").trim();
  const cityName = normalizeCityDisplay(city);
  const stateCode = String(state || "").trim();
  const zipCode = String(zip || "").trim();
  const line2 = [cityName, stateCode, zipCode].filter(Boolean).join(", ");
  return [street, line2].filter(Boolean).join(", ") || "Synthetic demo location";
}

/**
 * XSS mitigation for backend-provided links:
 * Only allow http/https URLs. Blocks javascript:, data:, file:, etc.
 */
const WEATHER_ICON_BASE = "/static/assets/weather-icons/weather/";

function meteoconSlug(condition, isDaytime = true) {
  const text = String(condition || "").toLowerCase();
  const day = isDaytime !== false;
  if (text.includes("thunder") || text.includes("storm")) return "thunderstorms";
  if (text.includes("snow") || text.includes("sleet") || text.includes("flurr") || text.includes("freezing rain") || text.includes("ice")) return "snow";
  if (text.includes("shower")) return day ? "partly-cloudy-day-rain" : "rain";
  if (text.includes("rain") || text.includes("drizzle")) return "rain";
  if (text.includes("fog") || text.includes("mist") || text.includes("haze")) return "fog";
  if (text.includes("overcast")) return "overcast";
  if (text.includes("cloudy")) {
    if (text.includes("partly") || text.includes("mostly")) return day ? "partly-cloudy-day" : "partly-cloudy-night";
    return "cloudy";
  }
  if (text.includes("clear") || text.includes("sunny") || text.includes("fair")) return day ? "clear-day" : "clear-night";
  if (text.includes("wind") || text.includes("breez")) return "wind";
  return day ? "partly-cloudy-day" : "partly-cloudy-night";
}

function ndfdHazardClass(hazards) {
  const significance = (Array.isArray(hazards) ? hazards : [])
    .map((hazard) => String(hazard?.significance || "").toLowerCase());
  if (significance.some((value) => value.includes("warning"))) return "severe";
  if (significance.some((value) => value.includes("watch"))) return "moderate";
  if (significance.some((value) => value.includes("advisory"))) return "minor";
  return hazards?.length ? "minor" : "none";
}

function selectedForecastHazardPeriod() {
  const selected = forecastTimes[selectedForecastTimeIndex] || null;
  if (!selected || !Array.isArray(ndfdHazardSeries) || !ndfdHazardSeries.length) return null;
  const selectedMs = Date.parse(selected);
  if (!Number.isFinite(selectedMs)) return null;

  for (const period of ndfdHazardSeries) {
    const startMs = Date.parse(period?.start_time || "");
    const endMs = Date.parse(period?.end_time || "");
    if (!Number.isFinite(startMs)) continue;
    if (Number.isFinite(endMs) && selectedMs >= startMs && selectedMs < endMs) return period;
    if (!Number.isFinite(endMs) && startMs === selectedMs) return period;
  }
  return null;
}

function renderHazardCard() {
  if (!opsHazardTitle || !opsHazardStatus || !opsHazardList) return;

  const currentAlerts = Array.isArray(lastPayload?.weather?.alerts) ? lastPayload.weather.alerts : [];
  const selectedTime = forecastTimes[selectedForecastTimeIndex] || null;
  const period = selectedForecastHazardPeriod();
  const forecastHazards = Array.isArray(period?.hazards) ? period.hazards : [];
  const forecastClass = ndfdHazardClass(forecastHazards);
  const activeSeverity = String(lastPayload?.weather?.max_alert_severity || "").toLowerCase();
  const hasActiveAlert = currentAlerts.length > 0;
  const insights = operationalInsightsForPeriod(selectedWeatherPeriod());
  const hazardsCard = opsHazardStatus.closest(".ops-card");

  const severityRank = { info: 0, minor: 1, elevated: 2, moderate: 2, high: 3, severe: 3, extreme: 4 };
  const rows = [];

  if (hasActiveAlert) {
    const primary = currentAlerts[0] || {};
    const expires = primary?.expires ? formatIsoLocal(primary.expires) : null;
    const level = ["minor", "moderate", "severe", "extreme"].includes(activeSeverity) ? activeSeverity : "moderate";
    rows.push({
      level,
      priority: 100 + (severityRank[level] || 0),
      title: primary?.event || "Weather alert in effect",
      detail: expires ? `In effect until ${expires}` : "Currently in effect",
      official: true,
    });
  }

  forecastHazards.slice(0, 2).forEach((hazard) => {
    rows.push({
      level: forecastClass === "none" ? "minor" : forecastClass,
      priority: 70 + (severityRank[forecastClass] || 0),
      title: [hazard?.phenomena, hazard?.significance].filter(Boolean).join(" ") || "Forecast hazard",
      detail: selectedTime ? `Expected around ${formatIsoLocal(selectedTime) || selectedTime}` : "Forecast hazard expected",
      official: true,
    });
  });

  insights.forEach((insight) => {
    let title = String(insight.title || "Weather concern").replace(/^Operational insight\s*·\s*/i, "").trim();
    if (/^Severe storms$/i.test(title)) title = "Severe storms possible";
    if (/^Rain chance$/i.test(title)) {
      const precip = Number(selectedWeatherPeriod()?.precip_probability);
      title = Number.isFinite(precip) ? `${Math.round(precip)}% chance of rain` : "Elevated rain chance";
    }
    rows.push({
      level: insight.level || "info",
      priority: 50 + (severityRank[insight.level] || 0),
      title,
      detail: insight.text || "",
      official: false,
    });
  });

  if (!hasActiveAlert) {
    rows.push({ level: "info", priority: 10, title: "No active alerts", detail: "", official: true });
  }

  if (!ndfdHazardError && ndfdHazardLocationKey && selectedTime && !forecastHazards.length) {
    rows.push({
      level: "info",
      priority: 5,
      title: "No additional forecast hazards expected",
      detail: `At ${formatIsoLocal(selectedTime) || selectedTime}`,
      official: true,
    });
  } else if (ndfdHazardError) {
    rows.push({
      level: "info",
      priority: 5,
      title: "Forecast hazard data unavailable",
      detail: "Current alerts and weather conditions are still shown.",
      official: true,
    });
  }

  rows.sort((a, b) => b.priority - a.priority);
  opsHazardList.innerHTML = "";

  rows.slice(0, 5).forEach((item) => {
    const row = document.createElement("div");
    row.className = `impact-brief-row ${item.level || "info"}${item.official && item.priority >= 100 ? " active-official" : ""}`;

    const body = document.createElement("div");
    body.className = "impact-brief-copy";
    const title = document.createElement("strong");
    title.textContent = item.title;
    body.appendChild(title);

    if (item.detail) {
      const detail = document.createElement("span");
      detail.textContent = item.detail;
      body.appendChild(detail);
    }

    row.appendChild(body);
    opsHazardList.appendChild(row);
  });

  const strongest = rows[0] || null;
  const meaningfulConcern = rows.find((item) => (severityRank[item.level] || 0) >= 2);
  opsHazardTitle.textContent = hasActiveAlert
    ? (currentAlerts[0]?.event || "Weather alert in effect")
    : meaningfulConcern
      ? "Weather concerns expected"
      : "No significant weather impacts expected";

  if (hazardsCard) {
    hazardsCard.classList.remove("severity-none", "severity-minor", "severity-moderate", "severity-severe", "severity-extreme");
    const cardLevel = strongest?.level || "none";
    const mapped = cardLevel === "high" ? "severe" : cardLevel === "elevated" ? "moderate" : cardLevel;
    hazardsCard.classList.add(`severity-${["minor", "moderate", "severe", "extreme"].includes(mapped) ? mapped : "none"}`);
  }

  opsHazardStatus.className = "ops-status";
  opsHazardStatus.textContent = "";
}

async function loadNdfdHazards(latValue, lonValue) {
  const lat = Number(latValue);
  const lon = Number(lonValue);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

  const key = `${lat.toFixed(2)},${lon.toFixed(2)}`;
  if (ndfdHazardLocationKey === key && ndfdHazardSeries.length && !ndfdHazardError) {
    renderHazardCard();
    return;
  }

  ndfdHazardLocationKey = key;
  ndfdHazardSeries = [];
  ndfdHazardError = null;
  if (opsHazardTitle) opsHazardTitle.textContent = "Loading forecast impacts…";
  if (opsHazardStatus) {
    opsHazardStatus.className = "ops-status severity-none";
    opsHazardStatus.textContent = "Loading forecast hazard guidance for this location.";
  }

  try {
    const response = await fetch(
      `/api/forecast/point-hazards?lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lon)}`,
      { headers: { Accept: "application/json" } }
    );
    if (!response.ok) throw new Error(`Forecast hazards failed (${response.status})`);
    const payload = await response.json();
    if (payload?.error) throw new Error(String(payload.error));
    ndfdHazardSeries = Array.isArray(payload?.periods) ? payload.periods : [];
  } catch (error) {
    ndfdHazardError = "Forecast hazard data unavailable";
    console.error(error);
  }
  renderHazardCard();
}

function meteoconUrl(condition, isDaytime = true) {
  return `${WEATHER_ICON_BASE}${encodeURIComponent(meteoconSlug(condition, isDaytime))}.svg`;
}

function isCurrentForecastSelection() {
  return forecastTimes.length > 0 && selectedForecastTimeIndex === currentForecastTimeIndex;
}

function selectedWeatherPeriod() {
  const periods = Array.isArray(lastPayload?.weather?.forecast_periods)
    ? lastPayload.weather.forecast_periods
    : [];
  if (!periods.length) return null;

  const selected = forecastTimes[selectedForecastTimeIndex] || null;
  const selectedMs = Date.parse(selected || "");
  if (!Number.isFinite(selectedMs)) return periods[0] || null;

  let nearestFuture = null;
  for (const period of periods) {
    const startMs = Date.parse(period?.start_time || "");
    const endMs = Date.parse(period?.end_time || "");
    if (!Number.isFinite(startMs)) continue;
    if (Number.isFinite(endMs) && selectedMs >= startMs && selectedMs < endMs) return period;
    if (startMs >= selectedMs && !nearestFuture) nearestFuture = period;
  }
  return nearestFuture || periods[periods.length - 1] || null;
}

function extractMaxWindMph(text) {
  const values = String(text || "").match(/\d+(?:\.\d+)?/g);
  if (!values?.length) return null;
  const nums = values.map(Number).filter(Number.isFinite);
  return nums.length ? Math.max(...nums) : null;
}

function operationalInsightsForPeriod(period) {
  if (!period) return [];
  const shortText = String(period?.short_forecast || "");
  const detailText = String(period?.detailed_forecast || "");
  const combined = `${shortText} ${detailText}`.toLowerCase();
  const insights = [];

  const maxWind = extractMaxWindMph(period?.wind_speed || detailText);
  if (Number.isFinite(maxWind) && maxWind >= 35) {
    insights.push({
      level: maxWind >= 50 ? "high" : "elevated",
      title: "Operational insight · Wind",
      text: `Winds may reach about ${Math.round(maxWind)} mph, increasing risk for debris and line stress.`,
    });
  }

  if (combined.includes("thunderstorm") || combined.includes("severe")) {
    insights.push({
      level: combined.includes("severe") ? "high" : "elevated",
      title: "Operational insight · Severe storms",
      text: combined.includes("severe")
        ? "Severe thunderstorm wording appears in the selected forecast period."
        : "Thunderstorms are forecast during the selected period.",
    });
  }

  const precip = Number(period?.precip_probability);
  if (Number.isFinite(precip) && precip >= 60) {
    insights.push({
      level: precip >= 80 ? "elevated" : "info",
      title: "Operational insight · Rain chance",
      text: "Rain is likely during this period.",
    });
  }

  return insights.slice(0, 3);
}

function formatCheckpointLabel(iso) {
  const d = new Date(iso || "");
  if (Number.isNaN(d.getTime())) return { date: "", time: "—" };
  const timeZone = String(lastPayload?.resolved?.tz || "").trim() || undefined;
  const options = timeZone ? { timeZone } : {};
  const date = new Intl.DateTimeFormat("en-US", {
    ...options,
    month: "numeric",
    day: "numeric",
  }).format(d);
  const time = new Intl.DateTimeFormat("en-US", {
    ...options,
    hour: "numeric",
    hour12: true,
  }).format(d).replace(":00", "");
  return { date, time };
}

function pickHourlyCheckpoints(hourlyPeriods) {
  const rows = (Array.isArray(hourlyPeriods) ? hourlyPeriods : [])
    .filter((period) =>
      Number.isFinite(Date.parse(period?.start_time || "")) &&
      Number.isFinite(Number(period?.temperature))
    )
    .sort((a, b) => Date.parse(a.start_time) - Date.parse(b.start_time));
  if (!rows.length) return [];

  const nowMs = Date.now();
  const first = rows.find((row) => Date.parse(row.start_time) >= nowMs - 60 * 60 * 1000) || rows[0];
  const baseMs = Date.parse(first.start_time);
  const selected = [];
  const used = new Set();

  for (let hours = 0; hours <= 24; hours += 3) {
    const targetMs = baseMs + hours * 60 * 60 * 1000;
    let best = null;
    let bestDelta = Infinity;
    rows.forEach((row) => {
      const startMs = Date.parse(row.start_time);
      const endMs = Date.parse(row.end_time || "");
      const coversTarget = Number.isFinite(endMs)
        ? targetMs >= startMs && targetMs < endMs
        : Math.abs(startMs - targetMs) <= 60 * 60 * 1000;
      if (!coversTarget || used.has(row.start_time)) return;
      const delta = Math.abs(startMs - targetMs);
      if (delta < bestDelta) {
        best = row;
        bestDelta = delta;
      }
    });
    if (best) {
      selected.push(best);
      used.add(best.start_time);
    }
  }
  return selected;
}

function smoothPath(points) {
  if (points.length < 2) return "";
  let d = `M ${points[0][0].toFixed(1)} ${points[0][1].toFixed(1)}`;
  for (let i = 0; i < points.length - 1; i += 1) {
    const [x0, y0] = points[i];
    const [x1, y1] = points[i + 1];
    const midX = (x0 + x1) / 2;
    d += ` C ${midX.toFixed(1)} ${y0.toFixed(1)}, ${midX.toFixed(1)} ${y1.toFixed(1)}, ${x1.toFixed(1)} ${y1.toFixed(1)}`;
  }
  return d;
}

function renderWeatherTrend(periods, selectedTime) {
  if (!opsWeatherTrend) return;
  opsWeatherTrend.innerHTML = "";
  const rows = pickHourlyCheckpoints(periods);

  if (opsWeatherTrendRange) {
    opsWeatherTrendRange.textContent = rows.length ? "Next 24 hours" : "No hourly trend data";
  }
  if (rows.length < 2) return;

  const width = 720;
  const height = 158;
  const padX = 42;
  const padTop = 20;
  const padBottom = 42;
  const temps = rows.map((period) => Number(period.temperature));
  const minTemp = Math.min(...temps);
  const maxTemp = Math.max(...temps);
  const spread = Math.max(8, maxTemp - minTemp);
  const low = minTemp - Math.max(2, spread * 0.16);
  const high = maxTemp + Math.max(2, spread * 0.16);
  const selectedMs = Date.parse(selectedTime || "");

  const xFor = (index) => padX + (index * (width - padX * 2)) / Math.max(1, rows.length - 1);
  const yFor = (temp) => padTop + ((high - temp) / (high - low)) * (height - padTop - padBottom);

  const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
  svg.setAttribute("preserveAspectRatio", "none");
  svg.classList.add("weather-trend-svg");

  [0.25, 0.5, 0.75].forEach((ratio) => {
    const line = document.createElementNS(svg.namespaceURI, "line");
    const y = padTop + ratio * (height - padTop - padBottom);
    line.setAttribute("x1", String(padX));
    line.setAttribute("x2", String(width - padX));
    line.setAttribute("y1", String(y));
    line.setAttribute("y2", String(y));
    line.setAttribute("class", "weather-trend-grid");
    svg.appendChild(line);
  });

  const points = rows.map((period, index) => [xFor(index), yFor(Number(period.temperature))]);
  const path = document.createElementNS(svg.namespaceURI, "path");
  path.setAttribute("d", smoothPath(points));
  path.setAttribute("class", "weather-trend-line");
  svg.appendChild(path);

  rows.forEach((period, index) => {
    const [x, y] = points[index];
    const startMs = Date.parse(period?.start_time || "");
    const endMs = Date.parse(period?.end_time || "");
    const isSelected = Number.isFinite(selectedMs) &&
      selectedMs >= startMs &&
      (!Number.isFinite(endMs) || selectedMs < endMs);

    const marker = document.createElementNS(svg.namespaceURI, "circle");
    marker.setAttribute("cx", String(x));
    marker.setAttribute("cy", String(y));
    marker.setAttribute("r", isSelected ? "5.5" : "4");
    marker.setAttribute("class", isSelected ? "weather-trend-dot selected" : "weather-trend-dot");
    svg.appendChild(marker);

    const tempText = document.createElementNS(svg.namespaceURI, "text");
    tempText.setAttribute("x", String(x));
    tempText.setAttribute("y", String(y - 10));
    tempText.setAttribute("text-anchor", "middle");
    tempText.setAttribute("class", "weather-trend-temp");
    tempText.textContent = `${Math.round(Number(period.temperature))}°`;
    svg.appendChild(tempText);

    const parts = formatCheckpointLabel(period?.start_time);
    const dateLabel = document.createElementNS(svg.namespaceURI, "text");
    dateLabel.setAttribute("x", String(x));
    dateLabel.setAttribute("y", String(height - 19));
    dateLabel.setAttribute("text-anchor", "middle");
    dateLabel.setAttribute("class", "weather-trend-date");
    dateLabel.textContent = parts.date;
    svg.appendChild(dateLabel);

    const timeLabel = document.createElementNS(svg.namespaceURI, "text");
    timeLabel.setAttribute("x", String(x));
    timeLabel.setAttribute("y", String(height - 6));
    timeLabel.setAttribute("text-anchor", "middle");
    timeLabel.setAttribute("class", "weather-trend-label");
    timeLabel.textContent = parts.time;
    svg.appendChild(timeLabel);
  });

  opsWeatherTrend.appendChild(svg);
  return rows;
}

function conciseCondition(condition) {
  const text = String(condition || "Weather unavailable").trim();
  const first = text.split(/\s+then\s+/i)[0].trim();
  if (first.length <= 34) return first;
  if (/thunderstorm/i.test(first)) return "Thunderstorms";
  if (/showers|rain/i.test(first)) return "Rain Showers";
  if (/partly sunny/i.test(first)) return "Partly Sunny";
  if (/mostly cloudy/i.test(first)) return "Mostly Cloudy";
  return first.slice(0, 31).trimEnd() + "…";
}

function formatEtrForDisplay(rawValue, outageNearby) {
  if (!outageNearby) return "Not applicable";
  const raw = String(rawValue || "").trim();
  if (!raw) return "Not provided";
  const parsed = Date.parse(raw);
  if (Number.isFinite(parsed)) return formatIsoLocal(raw) || raw;
  if (/^[A-Z0-9_-]{3,}$/i.test(raw)) return "Not provided";
  return raw;
}

function renderSelectedTimeWeather() {
  if (!lastPayload) return;
  const wx = lastPayload?.weather || {};
  const current = isCurrentForecastSelection();
  const period = selectedWeatherPeriod();

  let condition = wx?.condition || period?.short_forecast || "Weather unavailable";
  let temperature = wx?.temperature_f;
  let summary = wx?.detailedForecast || period?.detailed_forecast || "Forecast details unavailable.";
  let iconCondition = condition;
  let isDaytime = true;
  const metrics = [];

  if (current) {
    if (opsWeatherMode) {
      opsWeatherMode.textContent = "OBSERVED · NOW";
      opsWeatherMode.className = "time-domain-badge observed";
    }
    const observedAt = formatIsoLocal(wx?.observation_time);
    if (observedAt) metrics.push(["Observation", observedAt]);
    if (Number.isFinite(Number(wx?.wind_speed_mph))) {
      const gust = Number.isFinite(Number(wx?.wind_gust_mph)) ? ` · Gust ${Math.round(Number(wx.wind_gust_mph))}` : "";
      metrics.push(["Wind", `${wx?.wind_direction_cardinal || ""} ${Math.round(Number(wx.wind_speed_mph))} mph${gust}`.trim()]);
    }
    if (Number.isFinite(Number(wx?.relative_humidity_pct))) metrics.push(["Humidity", `${Math.round(Number(wx.relative_humidity_pct))}%`]);
    const feelsLike = Number.isFinite(Number(wx?.heat_index_f))
      ? Number(wx.heat_index_f)
      : Number.isFinite(Number(wx?.wind_chill_f))
        ? Number(wx.wind_chill_f)
        : null;
    if (Number.isFinite(feelsLike)) {
      metrics.push(["Feels like", `${Math.round(feelsLike)}°F`]);
    } else if (period && Number.isFinite(Number(period?.precip_probability))) {
      metrics.push(["Next precip", `${Math.round(Number(period.precip_probability))}%`]);
    }
  } else if (period) {
    condition = period?.short_forecast || condition;
    temperature = period?.temperature;
    summary = period?.detailed_forecast || summary;
    iconCondition = condition;
    isDaytime = period?.is_daytime !== false;
    if (opsWeatherMode) {
      opsWeatherMode.textContent = "FORECAST";
      opsWeatherMode.className = "time-domain-badge forecast";
    }
    metrics.push(["Valid", formatIsoLocal(forecastTimes[selectedForecastTimeIndex]) || "—"]);
    if (period?.wind_speed) metrics.push(["Wind", [period?.wind_direction, period.wind_speed].filter(Boolean).join(" ")]);
    if (Number.isFinite(Number(period?.precip_probability))) metrics.push(["Precip", `${Math.round(Number(period.precip_probability))}%`]);
    if (Number.isFinite(Number(period?.relative_humidity))) metrics.push(["Humidity", `${Math.round(Number(period.relative_humidity))}%`]);
  }

  const displayCondition = conciseCondition(condition);
  if (opsForecastTitle) {
    opsForecastTitle.textContent = temperature !== null && temperature !== undefined
      ? `${temperature}°F • ${displayCondition}`
      : displayCondition;
  }
  if (opsForecastSummary) {
    opsForecastSummary.textContent = current && period?.detailed_forecast
      ? `Next period: ${period.detailed_forecast}`
      : summary;
  }
  if (opsCurrentWeatherIcon) opsCurrentWeatherIcon.src = meteoconUrl(iconCondition, isDaytime);

  if (opsWeatherMetrics) {
    opsWeatherMetrics.innerHTML = "";
    metrics.slice(0, 4).forEach(([label, value]) => {
      const item = document.createElement("div");
      item.className = "weather-metric";
      const k = document.createElement("span");
      k.textContent = label;
      const v = document.createElement("strong");
      v.textContent = String(value || "—");
      item.append(k, v);
      opsWeatherMetrics.appendChild(item);
    });
  }

  const hourlyPeriods = Array.isArray(wx?.hourly_periods) ? wx.hourly_periods : [];
  const selectedTime = forecastTimes[selectedForecastTimeIndex] || null;
  const checkpoints = renderWeatherTrend(hourlyPeriods, selectedTime) || [];

  if (opsForecastStrip) {
    opsForecastStrip.innerHTML = "";
    checkpoints.forEach((entry) => {
      const item = document.createElement("div");
      item.className = "forecast-period hourly-forecast-period";

      const parts = formatCheckpointLabel(entry?.start_time);
      const label = document.createElement("span");
      label.className = "forecast-period-label";
      label.textContent = [parts.date, parts.time].filter(Boolean).join(" · ");

      const icon = document.createElement("img");
      icon.className = "meteocon-small";
      icon.src = meteoconUrl(entry?.short_forecast, entry?.is_daytime);
      icon.alt = "";
      icon.setAttribute("aria-hidden", "true");

      const condition = document.createElement("span");
      condition.className = "forecast-period-condition";
      condition.textContent = conciseCondition(entry?.short_forecast || "Forecast");

      const precip = Number(entry?.precip_probability);
      const meta = document.createElement("span");
      meta.className = "forecast-period-meta";
      meta.textContent = Number.isFinite(precip) && precip > 0 ? `${Math.round(precip)}% rain` : "";

      item.append(label, icon, condition);
      if (meta.textContent) item.appendChild(meta);
      opsForecastStrip.appendChild(item);
    });
  }
}

function renderOpsCards(data) {
  renderSelectedTimeWeather();

  renderHazardCard();

  const utility = data?.provider?.name || data?.provider?.utility || data?.resolved?.utility || "Utility";
  const outageNearby = !!data?.power?.has_outage_nearby;
  const nearest = data?.power?.nearest || {};
  const powerOk = data?.power?.meta?.ok !== false;
  const powerCard = opsPowerStatus?.closest(".ops-card");
  if (powerCard) {
    powerCard.classList.remove("power-normal", "power-outage", "power-unknown");
    powerCard.classList.add(!powerOk ? "power-unknown" : outageNearby ? "power-outage" : "power-normal");
  }
  if (opsPowerTitle) opsPowerTitle.textContent = utility;
  if (opsPowerStatus) {
    opsPowerStatus.className = "ops-status " + (!powerOk ? "state-unknown" : outageNearby ? "state-outage" : "state-good");
    opsPowerStatus.textContent = !powerOk
      ? "Live power status unavailable"
      : outageNearby
        ? "Nearby outage detected"
        : "No nearby outage detected";
  }
  if (opsPowerMeta) {
    opsPowerMeta.innerHTML = "";
    const etrRaw = nearest?.etr;
    const etrFormatted = formatEtrForDisplay(etrRaw, outageNearby);
    const metrics = [
      ["Customers out", nearest?.customers_out ?? (outageNearby ? "Unknown" : "0")],
      ["Distance", Number.isFinite(Number(nearest?.distance_miles)) ? `${Number(nearest.distance_miles).toFixed(2)} mi` : "—"],
      ["ETR", etrFormatted],
    ];
    metrics.forEach(([label, value]) => {
      const metric = document.createElement("div");
      metric.className = "ops-metric";
      const k = document.createElement("span");
      k.textContent = label;
      const v = document.createElement("strong");
      v.textContent = String(value);
      metric.append(k, v);
      opsPowerMeta.appendChild(metric);
    });
  }

  if (opsPowerSiteMeta) {
    const siteName = data?.resolved?.name || data?.resolved?.site_id || "Selected location";
    const city = normalizeCityDisplay(data?.resolved?.city);
    const state = String(data?.resolved?.state || "").trim();
    const locality = [city, state].filter(Boolean).join(", ");
    const coords = Number.isFinite(Number(data?.resolved?.lat)) && Number.isFinite(Number(data?.resolved?.lon))
      ? `${Number(data.resolved.lat).toFixed(3)}, ${Number(data.resolved.lon).toFixed(3)}`
      : "";
    opsPowerSiteMeta.innerHTML = "";
    const name = document.createElement("strong");
    name.textContent = siteName;
    const meta = document.createElement("span");
    meta.textContent = [locality, coords].filter(Boolean).join(" · ");
    opsPowerSiteMeta.append(name, meta);
  }

  const hasSiteCoords = Number.isFinite(Number(data?.resolved?.lat)) && Number.isFinite(Number(data?.resolved?.lon));
  const outageMapUrl = safeUrl(data?.provider?.outage_map);
  if (opsOpenGoogleMaps) opsOpenGoogleMaps.disabled = !hasSiteCoords;
  if (opsOpenOutageMap) opsOpenOutageMap.disabled = !outageMapUrl;
}

function safeUrl(url) {
  if (!url) return null;
  try {
    const u = new URL(String(url), window.location.origin);
    if (u.protocol === "http:" || u.protocol === "https:") return u.toString();
    return null;
  } catch {
    return null;
  }
}

/**
 * Tight input validation / normalization for the search box.
 * - Max length: 128
 * - Accept:
 *   A) Site ID: letters/numbers/_/- (1..40 chars) -> normalized to UPPERCASE
 *   B) Lat,Lon: numeric with optional decimals -> range checked -> normalized "lat,lon"
 *   C) Friendly names / aliases with letters, numbers, spaces, and light punctuation
 *
 * This is frontend hygiene; it reduces log noise and blocks obvious garbage input.
 */
function validateQueryInput(raw) {
  const value = String(raw || "").trim();

  if (!value) return { ok: false, error: "Enter a Site ID or coordinates before searching." };
  if (value.length > 128) return { ok: false, error: "Query too long. Keep it under 128 characters." };

  // lat,lon pattern (allow spaces)
  const latlonMatch = value.match(/^\s*(-?\d{1,3}(?:\.\d+)?)\s*,\s*(-?\d{1,3}(?:\.\d+)?)\s*$/);
  if (latlonMatch) {
    const lat = Number(latlonMatch[1]);
    const lon = Number(latlonMatch[2]);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      return { ok: false, error: "Coordinates must be valid numbers." };
    }
    if (lat < -90 || lat > 90) {
      return { ok: false, error: "Latitude must be between -90 and 90." };
    }
    if (lon < -180 || lon > 180) {
      return { ok: false, error: "Longitude must be between -180 and 180." };
    }

    // Normalize formatting (no spaces)
    return { ok: true, normalized: `${lat},${lon}` };
  }

  // Site ID allowlist: 1-40 chars, letters/numbers/_/-
  if (/^[A-Za-z0-9_-]{1,40}$/.test(value)) {
    return { ok: true, normalized: value.toUpperCase() };
  }

  // Friendly names / aliases: allow normal punctuation used in site names.
  if (/^[A-Za-z0-9 .,&()'\/-]{1,128}$/.test(value)) {
    return { ok: true, normalized: value.replace(/\s+/g, " ").trim() };
  }

  return {
    ok: false,
    error: "Invalid input. Use a Site ID, a friendly site name, or coordinates like 36.15,-95.99.",
  };
}

// --------------------
// UI helpers
// --------------------

function setStatus(state, msg) {
  statusDot.className = "dot";
  if (state === "loading") statusDot.classList.add("live");
  if (state === "ok") statusDot.classList.add("ok");
  if (state === "warn") statusDot.classList.add("warn");
  statusText.textContent = msg;
}

function showToast(message) {
  toast.textContent = message;
  toast.classList.add("show");
  window.clearTimeout(showToast._t);
  showToast._t = window.setTimeout(() => toast.classList.remove("show"), 3500);
}

/**
 * KV row that supports either:
 * - primitive value (string/number/etc) OR
 * - a DOM Node as the value
 */
function kvRow(k, v) {
  const wrap = document.createElement("div");
  wrap.className = "kv";

  const kk = document.createElement("div");
  kk.className = "k";
  kk.textContent = k;

  const vv = document.createElement("div");
  vv.className = "v";

  if (v instanceof Node) {
    vv.appendChild(v);
  } else {
    vv.textContent = v === null || v === undefined || v === "" ? "—" : String(v);
  }

  wrap.appendChild(kk);
  wrap.appendChild(vv);
  return wrap;
}

function sectionTitle(text) {
  const el = document.createElement("div");
  el.style.margin = "16px 0 6px 0";
  el.style.fontSize = "12px";
  el.style.color = "var(--muted)";
  el.style.fontWeight = "800";
  el.style.letterSpacing = ".2px";
  el.textContent = text;
  return el;
}

function makeLink(url, label) {
  const safe = safeUrl(url);
  if (!safe) return document.createTextNode("—");

  const a = document.createElement("a");
  a.href = safe;
  a.target = "_blank";
  a.rel = "noopener noreferrer";
  a.textContent = label || safe;
  a.style.color = "var(--og-blue)";
  a.style.textDecoration = "none";
  a.addEventListener("mouseover", () => (a.style.textDecoration = "underline"));
  a.addEventListener("mouseout", () => (a.style.textDecoration = "none"));
  return a;
}

function detailsBlock(summaryText, bodyNode, open = false) {
  const d = document.createElement("details");
  d.open = !!open;

  const s = document.createElement("summary");
  s.textContent = summaryText;
  s.style.cursor = "pointer";
  s.style.fontWeight = "800";
  s.style.color = "var(--text)";
  s.style.fontSize = "13px";

  const body = document.createElement("div");
  body.style.marginTop = "8px";
  body.appendChild(bodyNode);

  d.appendChild(s);
  d.appendChild(body);
  return d;
}

function preWrap(text) {
  const div = document.createElement("div");
  div.style.whiteSpace = "pre-wrap";
  div.style.fontFamily = "var(--sans)";
  div.style.fontSize = "13px";
  div.style.color = "var(--text)";
  div.textContent = text || "—";
  return div;
}

function isCoordinateLikeInput(value) {
  const text = String(value || "").trim();
  if (!text) return false;
  if (!/^[0-9\s,.-]+$/.test(text)) return false;
  return /\d/.test(text);
}

function formatSuggestionMeta(entry) {
  const aliasList = Array.isArray(entry?.aliases) ? entry.aliases : [];
  const cleanAliases = aliasList
    .map((a) => String(a || "").trim())
    .filter(Boolean);
  const aliasPreview = cleanAliases.slice(0, 2).join(", ") + (cleanAliases.length > 2 ? ", ..." : "");

  const address = String(entry?.address || "").trim();
  const city = String(entry?.city || "").trim();
  const state = String(entry?.state || "").trim();
  const lat = Number(entry?.lat);
  const lon = Number(entry?.lon);

  const addressBits = [address, [city, state].filter(Boolean).join(", ")]
    .filter(Boolean)
    .join(" | ");
  const gps = Number.isFinite(lat) && Number.isFinite(lon) ? `${lat.toFixed(5)}, ${lon.toFixed(5)}` : "";

  return [aliasPreview ? `Aliases: ${aliasPreview}` : "", addressBits, gps]
    .filter(Boolean)
    .join(" | ");
}

function clearSuggestions() {
  currentSuggestions = [];
  activeSuggestionIndex = -1;
  suggestions.innerHTML = "";
  suggestions.hidden = true;
}

function setActiveSuggestion(index) {
  if (!currentSuggestions.length) {
    activeSuggestionIndex = -1;
    return;
  }

  const max = currentSuggestions.length - 1;
  if (index < 0) index = max;
  if (index > max) index = 0;
  activeSuggestionIndex = index;

  const rows = suggestions.querySelectorAll(".suggestion-item");
  rows.forEach((row, rowIndex) => {
    const isActive = rowIndex === activeSuggestionIndex;
    row.classList.toggle("active", isActive);
    if (isActive) {
      row.setAttribute("aria-selected", "true");
      row.scrollIntoView({ block: "nearest" });
    } else {
      row.setAttribute("aria-selected", "false");
    }
  });
}

function applySuggestion(entry) {
  const siteId = String(entry?.site_id || "").trim();
  if (!siteId) return;
  q.value = siteId;
  clearSuggestions();
  void executeSearch(siteId);
}

function renderSuggestions(items) {
  currentSuggestions = Array.isArray(items) ? items : [];
  activeSuggestionIndex = -1;
  suggestions.innerHTML = "";

  if (!currentSuggestions.length) {
    suggestions.hidden = true;
    return;
  }

  currentSuggestions.forEach((entry, index) => {
    const siteId = String(entry?.site_id || "").trim();
    if (!siteId) return;

    const button = document.createElement("button");
    button.type = "button";
    button.className = "suggestion-item";
    button.setAttribute("role", "option");
    button.setAttribute("aria-selected", "false");
    button.dataset.index = String(index);

    const title = document.createElement("span");
    title.className = "suggestion-primary";
    const name = String(entry?.name || "").trim();
    title.textContent = name && name.toUpperCase() !== siteId.toUpperCase()
      ? `${siteId} - ${name}`
      : siteId;

    const meta = document.createElement("span");
    meta.className = "suggestion-meta";
    meta.textContent = formatSuggestionMeta(entry) || "No additional details";

    button.appendChild(title);
    button.appendChild(meta);

    button.addEventListener("mouseenter", () => {
      setActiveSuggestion(index);
    });

    button.addEventListener("mousedown", (e) => {
      e.preventDefault();
      applySuggestion(entry);
    });

    suggestions.appendChild(button);
  });

  suggestions.hidden = false;
}

async function fetchSuggestions(rawInput) {
  const query = String(rawInput || "").trim();
  if (query.length < SUGGESTION_MIN_QUERY_LENGTH) {
    clearSuggestions();
    return;
  }
  if (isCoordinateLikeInput(query) && query.replace(/\s+/g, "").length < SUGGESTION_COORD_MIN_CHARS) {
    clearSuggestions();
    return;
  }
  if (isCoordinateLikeInput(query)) {
    clearSuggestions();
    return;
  }

  const needle = query.toUpperCase();
  const ranked = DEMO_SITES
    .map((site) => {
      const haystack = [site.site_id, site.name, site.city, site.state, site.utility].join(" ").toUpperCase();
      let score = haystack.includes(needle) ? 1 : 0;
      if (site.site_id.startsWith(needle)) score += 3;
      if (String(site.name || "").toUpperCase().startsWith(needle)) score += 2;
      return { site, score };
    })
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score || a.site.site_id.localeCompare(b.site.site_id))
    .slice(0, 8)
    .map((item) => item.site);

  renderSuggestions(ranked);
}

function clearPanels() {
  ov_kv.innerHTML = "";
  pwr_kv.innerHTML = "";
  wx_kv.innerHTML = "";

  openGoogleMaps.disabled = true;
  openOutageMap.disabled = true;
  if (openNwsMap) openNwsMap.disabled = true;
  lastPayload = null;

  c_loc.textContent = "—";
  c_loc_meta.textContent = "—";
  c_util.textContent = "—";
  c_util_meta.textContent = "—";
  c_wx.textContent = "—";
  c_wx_meta.textContent = "—";
  c_pwr.textContent = "—";
  c_pwr_meta.textContent = "—";

  if (opsForecastTitle) opsForecastTitle.textContent = "Weather outlook";
  if (opsForecastSummary) opsForecastSummary.textContent = "Search a site to load forecast context.";
  if (opsWeatherMode) {
    opsWeatherMode.textContent = "CURRENT";
    opsWeatherMode.className = "time-domain-badge";
  }
  if (opsWeatherMetrics) opsWeatherMetrics.innerHTML = "";
  if (opsForecastStrip) opsForecastStrip.innerHTML = "";
  if (opsCurrentWeatherIcon) opsCurrentWeatherIcon.src = `${WEATHER_ICON_BASE}partly-cloudy-day.svg`;
  ndfdHazardSeries = [];
  ndfdHazardError = null;
  ndfdHazardLocationKey = null;
  if (opsHazardTitle) opsHazardTitle.textContent = "No location selected";
  if (opsHazardStatus) {
    opsHazardStatus.className = "ops-status good";
    opsHazardStatus.textContent = "No active hazard context loaded.";
  }
  if (opsHazardList) opsHazardList.innerHTML = "";
  if (opsPowerTitle) opsPowerTitle.textContent = "Regional overview";
  if (opsPowerStatus) {
    opsPowerStatus.className = "ops-status good";
    opsPowerStatus.textContent = "Search a site to load power conditions.";
  }
  if (opsPowerMeta) opsPowerMeta.innerHTML = "";
  if (opsPowerSiteMeta) opsPowerSiteMeta.innerHTML = "";
  if (opsOpenGoogleMaps) opsOpenGoogleMaps.disabled = true;
  if (opsOpenOutageMap) opsOpenOutageMap.disabled = true;

  if (mapIntelTitle) mapIntelTitle.textContent = "No location selected";
  if (mapIntelMeta) {
    mapIntelMeta.textContent = "Search a demo site or coordinates to load operational context.";
  }

  wxDot.className = "dot live";
  pwrDot.className = "dot";
}

function setTabs(activeId) {
  const tabs = ["overview", "power", "weather"];
  for (const t of tabs) {
    $("t_" + t).classList.remove("active");
    $("t_" + t).setAttribute("aria-selected", "false");
    $("tab_" + t).hidden = true;
  }
  $("t_" + activeId).classList.add("active");
  $("t_" + activeId).setAttribute("aria-selected", "true");
  $("tab_" + activeId).hidden = false;
}

["overview", "power", "weather"].forEach((t) => {
  $("t_" + t).addEventListener("click", () => setTabs(t));
});

// --------------------
// Map foundation
// --------------------
let forecastMap = null;
let selectedLocationMarker = null;
let forecastWmsLayer = null;
let activeForecastLayer = "weather";
let forecastRefreshTimer = null;
let forecastTimes = [];
let selectedForecastTimeIndex = 0;
let currentForecastTimeIndex = 0;
let selectedDemoSiteId = null;
const demoSiteMarkers = new Map();

const DEMO_MARKER_STYLE = {
  radius: 5,
  weight: 2,
  color: "#72e9ff",
  fillColor: "#163e73",
  fillOpacity: 0.86,
};

const SELECTED_MARKER_STYLE = {
  radius: 8,
  weight: 3,
  color: "#b9f5ff",
  fillColor: "#665cf6",
  fillOpacity: 0.96,
};

function initForecastMap() {
  if (!forecastMapEl) return;

  if (typeof window.L === "undefined") {
    if (mapStatus) mapStatus.textContent = "Map library failed to load.";
    return;
  }

  forecastMap = window.L.map(forecastMapEl, {
    zoomControl: true,
    attributionControl: true,
    minZoom: 3,
    maxZoom: 18,
  }).setView([37.5, -96.5], 4);

  window.L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}", {
    maxZoom: 16,
    attribution: "Tiles © Esri — Esri, DeLorme, NAVTEQ",
  }).addTo(forecastMap);

  forecastMap.createPane("forecastReferencePane").style.zIndex = "325";
  window.L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Reference/MapServer/tile/{z}/{y}/{x}", {
    maxZoom: 16,
    opacity: 0.9,
    pane: "forecastReferencePane",
    attribution: "Tiles © Esri — Esri, DeLorme, NAVTEQ",
  }).addTo(forecastMap);

  renderDemoSiteMarkers();
  void setForecastLayer(activeForecastLayer);
  forecastMap.on("moveend", refreshForecastOverlay);

  document.querySelectorAll("[data-forecast-layer]").forEach((button) => {
    button.addEventListener("click", () => {
      const layer = button.getAttribute("data-forecast-layer");
      if (!layer) return;
      document.querySelectorAll("[data-forecast-layer]").forEach((item) => {
        item.classList.toggle("active", item === button);
      });
      setForecastLayer(layer);
    });
  });

  if (forecastTimeSlider) {
    forecastTimeSlider.addEventListener("input", () => {
      selectedForecastTimeIndex = Math.max(currentForecastTimeIndex, Number(forecastTimeSlider.value) || currentForecastTimeIndex);
      updateForecastTimeControls();
      refreshForecastOverlay();
    });
  }
  if (forecastCurrentTime) {
    forecastCurrentTime.addEventListener("click", () => {
      selectedForecastTimeIndex = currentForecastTimeIndex;
      updateForecastTimeControls();
      refreshForecastOverlay();
    });
  }
  if (forecastPrevTime) {
    forecastPrevTime.addEventListener("click", () => {
      selectedForecastTimeIndex = Math.max(currentForecastTimeIndex, selectedForecastTimeIndex - 1);
      updateForecastTimeControls();
      refreshForecastOverlay();
    });
  }
  if (forecastNextTime) {
    forecastNextTime.addEventListener("click", () => {
      selectedForecastTimeIndex = Math.min(forecastTimes.length - 1, selectedForecastTimeIndex + 1);
      updateForecastTimeControls();
      refreshForecastOverlay();
    });
  }

  window.setTimeout(() => forecastMap.invalidateSize(), 0);
}

function latLonToWebMercator(lat, lon) {
  const clampedLat = Math.max(-85.05112878, Math.min(85.05112878, Number(lat)));
  const x = Number(lon) * 20037508.34 / 180;
  let y = Math.log(Math.tan((90 + clampedLat) * Math.PI / 360)) / (Math.PI / 180);
  y = y * 20037508.34 / 180;
  return [x, y];
}

function refreshForecastOverlay() {
  if (!forecastMap || typeof window.L === "undefined") return;

  window.clearTimeout(forecastRefreshTimer);
  forecastRefreshTimer = window.setTimeout(() => {
    const bounds = forecastMap.getBounds();
    const southwest = latLonToWebMercator(bounds.getSouth(), bounds.getWest());
    const northeast = latLonToWebMercator(bounds.getNorth(), bounds.getEast());
    const size = forecastMap.getSize();
    const width = Math.max(256, Math.min(1024, Math.round(size.x)));
    const height = Math.max(256, Math.min(1024, Math.round(size.y)));
    const bbox = [southwest[0], southwest[1], northeast[0], northeast[1]].join(",");

    const url = new URL("/api/forecast/wms", window.location.origin);
    url.searchParams.set("layer", activeForecastLayer);
    url.searchParams.set("bbox", bbox);
    url.searchParams.set("width", String(width));
    url.searchParams.set("height", String(height));
    const selectedVtit = forecastTimes[selectedForecastTimeIndex] || null;
    if (!selectedVtit) {
      if (mapStatus) mapStatus.textContent = "Forecast valid times unavailable";
      return;
    }
    url.searchParams.set("vtit", selectedVtit);

    if (forecastWmsLayer) {
      forecastMap.removeLayer(forecastWmsLayer);
      forecastWmsLayer = null;
    }

    const labels = {
      weather: "weather",
      temperature: "temperature",
      pop12: "precipitation probability",
      wind: "wind speed",
      hazards: "hazards",
    };
    if (mapStatus) {
      mapStatus.textContent = `Loading NDFD ${labels[activeForecastLayer] || activeForecastLayer}…`;
    }

    forecastWmsLayer = window.L.imageOverlay(url.toString(), bounds, {
      opacity: activeForecastLayer === "hazards" ? 0.72 : 0.58,
      interactive: false,
      zIndex: 250,
    });

    forecastWmsLayer.on("load", () => {
      if (mapStatus) {
        mapStatus.textContent = "";
      }
    });
    forecastWmsLayer.on("error", () => {
      if (mapStatus) {
        mapStatus.textContent = `NDFD ${labels[activeForecastLayer] || activeForecastLayer} unavailable`;
      }
    });
    forecastWmsLayer.addTo(forecastMap);
  }, 180);
}

function findCurrentForecastTimeIndex(times) {
  if (!Array.isArray(times) || !times.length) return 0;
  const now = Date.now();
  for (let index = 0; index < times.length; index += 1) {
    const timestamp = Date.parse(times[index]);
    if (Number.isFinite(timestamp) && timestamp >= now) return index;
  }
  return Math.max(0, times.length - 1);
}

function renderForecastTimeNotches() {
  if (!forecastTimeNotches) return;
  forecastTimeNotches.innerHTML = "";
  const futureTimes = forecastTimes.slice(currentForecastTimeIndex);
  futureTimes.forEach((value, offset) => {
    const notch = document.createElement("span");
    notch.className = "forecast-time-notch";
    const absoluteIndex = currentForecastTimeIndex + offset;
    const isSelected = absoluteIndex === selectedForecastTimeIndex;
    const isMajor = offset === 0 || offset === futureTimes.length - 1 || offset % 4 === 0;
    notch.classList.toggle("selected", isSelected);
    notch.classList.toggle("major", isMajor);
    notch.title = formatIsoLocal(value) || String(value);
    forecastTimeNotches.appendChild(notch);
  });
}

function updateForecastTimeControls() {
  if (!forecastTimeSlider || !forecastTimeReadout) return;
  const hasTimes = forecastTimes.length > 0;
  if (hasTimes) {
    selectedForecastTimeIndex = Math.max(currentForecastTimeIndex, Math.min(selectedForecastTimeIndex, forecastTimes.length - 1));
  }
  forecastTimeSlider.disabled = !hasTimes;
  forecastTimeSlider.min = hasTimes ? String(currentForecastTimeIndex) : "0";
  forecastTimeSlider.max = hasTimes ? String(forecastTimes.length - 1) : "0";
  forecastTimeSlider.value = hasTimes ? String(selectedForecastTimeIndex) : "0";
  const selected = hasTimes ? forecastTimes[selectedForecastTimeIndex] : null;
  const formatted = selected ? (formatIsoLocal(selected) || selected) : "";
  const isCurrent = hasTimes && selectedForecastTimeIndex === currentForecastTimeIndex;
  forecastTimeReadout.textContent = selected
    ? (isCurrent ? `Current · ${formatted}` : formatted)
    : "Forecast times unavailable";
  if (forecastCurrentTime) {
    forecastCurrentTime.disabled = !hasTimes;
    forecastCurrentTime.classList.toggle("active", isCurrent);
    forecastCurrentTime.setAttribute("aria-pressed", isCurrent ? "true" : "false");
  }
  if (forecastPrevTime) forecastPrevTime.disabled = !hasTimes || selectedForecastTimeIndex <= currentForecastTimeIndex;
  if (forecastNextTime) forecastNextTime.disabled = !hasTimes || selectedForecastTimeIndex >= forecastTimes.length - 1;
  renderForecastTimeNotches();
  renderSelectedTimeWeather();
  renderHazardCard();
}

async function loadForecastTimes(layerKey) {
  if (mapStatus) mapStatus.textContent = "Loading forecast valid times…";
  const previousSelectedTime = forecastTimes[selectedForecastTimeIndex] || null;
  const response = await fetch(`/api/forecast/times?layer=${encodeURIComponent(layerKey)}`, {
    headers: { Accept: "application/json" },
  });
  if (!response.ok) throw new Error(`Forecast times failed (${response.status})`);
  const payload = await response.json();
  if (payload?.error) throw new Error(String(payload.error));
  forecastTimes = Array.isArray(payload?.times) ? payload.times : [];
  currentForecastTimeIndex = findCurrentForecastTimeIndex(forecastTimes);

  if (previousSelectedTime && forecastTimes.length) {
    const targetMs = Date.parse(previousSelectedTime);
    let closestIndex = currentForecastTimeIndex;
    let closestDelta = Infinity;
    forecastTimes.forEach((value, index) => {
      const valueMs = Date.parse(value);
      if (!Number.isFinite(valueMs) || index < currentForecastTimeIndex) return;
      const delta = Math.abs(valueMs - targetMs);
      if (delta < closestDelta) {
        closestDelta = delta;
        closestIndex = index;
      }
    });
    selectedForecastTimeIndex = closestIndex;
  } else {
    selectedForecastTimeIndex = currentForecastTimeIndex;
  }
  updateForecastTimeControls();
}

async function setForecastLayer(layerKey) {
  activeForecastLayer = layerKey;
  try {
    await loadForecastTimes(layerKey);
    refreshForecastOverlay();
  } catch (error) {
    forecastTimes = [];
    selectedForecastTimeIndex = 0;
    updateForecastTimeControls();
    if (mapStatus) mapStatus.textContent = "Forecast valid times unavailable";
    console.error(error);
  }
}

function renderDemoSiteMarkers() {
  if (!forecastMap || typeof window.L === "undefined") return;

  DEMO_SITES.forEach((site) => {
    const lat = Number(site?.lat);
    const lon = Number(site?.lon);
    const siteId = String(site?.site_id || "").trim();
    if (!siteId || !Number.isFinite(lat) || !Number.isFinite(lon)) return;

    const marker = window.L.circleMarker([lat, lon], { ...DEMO_MARKER_STYLE }).addTo(forecastMap);
    marker.on("click", () => {
      void executeSearch(siteId);
    });
    demoSiteMarkers.set(siteId, marker);
  });
}

function clearSelectedDemoMarker() {
  if (!selectedDemoSiteId) return;
  const previous = demoSiteMarkers.get(selectedDemoSiteId);
  if (previous) {
    previous.setStyle({ ...DEMO_MARKER_STYLE });
    previous.unbindTooltip();
  }
  selectedDemoSiteId = null;
}

function updateForecastMapLocation(latValue, lonValue, label, siteIdValue = "") {
  if (!forecastMap || typeof window.L === "undefined") return;

  const lat = Number(latValue);
  const lon = Number(lonValue);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

  forecastMap.setView([lat, lon], Math.max(forecastMap.getZoom(), 8), {
    animate: true,
  });

  clearSelectedDemoMarker();

  const safeLabel = String(label || "Selected location");
  const siteId = String(siteIdValue || "").trim();
  const demoMarker = siteId ? demoSiteMarkers.get(siteId) : null;

  if (demoMarker) {
    if (selectedLocationMarker) {
      forecastMap.removeLayer(selectedLocationMarker);
      selectedLocationMarker = null;
    }
    selectedDemoSiteId = siteId;
    demoMarker.setStyle({ ...SELECTED_MARKER_STYLE });
    demoMarker.bringToFront();
    demoMarker.bindTooltip(safeLabel, {
      permanent: true,
      direction: "right",
      offset: [12, 0],
      opacity: 1,
      className: "site-label-tooltip",
    }).openTooltip();
  } else {
    if (selectedLocationMarker) {
      selectedLocationMarker.setLatLng([lat, lon]);
      selectedLocationMarker.setStyle({ ...SELECTED_MARKER_STYLE });
    } else {
      selectedLocationMarker = window.L.circleMarker([lat, lon], { ...SELECTED_MARKER_STYLE }).addTo(forecastMap);
    }
    selectedLocationMarker.bindTooltip(safeLabel, {
      permanent: true,
      direction: "right",
      offset: [12, 0],
      opacity: 1,
      className: "site-label-tooltip",
    }).openTooltip();
  }

  if (mapStatus) mapStatus.textContent = `Selected: ${safeLabel}`;
}

// --------------------
// Main search
// --------------------

async function runSearch() {
  const check = validateQueryInput(q.value);
  if (!check.ok) {
    showToast(check.error);
    return;
  }

  await executeSearch(check.normalized);
}

async function executeSearch(value) {
  q.value = value;

  setStatus("loading", "Searching…");
  btn.disabled = true;

  const controller = new AbortController();
  const timeoutId = window.setTimeout(() => controller.abort(), CLIENT_TIMEOUT_MS);

  try {
    const url = new URL(API_ENDPOINT, window.location.origin);
    url.searchParams.set("q", value);

    const res = await fetch(url.toString(), {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });

    const contentType = (res.headers.get("content-type") || "").toLowerCase();

    if (!res.ok) {
      // Don’t surface raw body text (could contain internal details).
      throw new Error(`HTTP ${res.status} ${res.statusText}`);
    }

    if (!contentType.includes("application/json")) {
      throw new Error("Service returned a non-JSON response.");
    }

    const data = await res.json();
    lastPayload = data;

    render(value, data);
    void loadNdfdHazards(data?.resolved?.lat, data?.resolved?.lon);
    setStatus("ok", "OK");
  } catch (err) {
    console.error(err);
    setStatus("warn", "Error");

    const isAbort =
      err &&
      (err.name === "AbortError" ||
        String(err.message || "").toLowerCase().includes("aborted"));

    showToast(isAbort ? "Request timed out. Try again." : (err?.message || "Request failed."));
  } finally {
    window.clearTimeout(timeoutId);
    btn.disabled = false;
  }
}

// --------------------
// Render
// --------------------

function render(query, data) {
  renderOpsCards(data);
  // Headline
  const resolvedName =
    data?.resolved?.name || data?.resolved?.site_id || data?.resolved?.id || "Result";
  if (headline) headline.textContent = `${resolvedName}`;
  if (subhead) subhead.textContent = `Query: ${query}`;

  // Location summary
  const siteId = data?.resolved?.site_id || data?.resolved?.id || "—";
  const lat = data?.resolved?.lat ?? "—";
  const lon = data?.resolved?.lon ?? "—";
  const matchedSites = Array.isArray(data?.resolved?.matched_sites)
    ? data.resolved.matched_sites
    : [];
  const matchedSiteIds = matchedSites
    .map((m) => String(m?.site_id || "").trim())
    .filter(Boolean);
  c_loc.textContent = data?.resolved?.name ? `${data.resolved.name}` : siteId;
  c_loc_meta.textContent = `Site ID: ${siteId} • ${lat}, ${lon}`;
  updateForecastMapLocation(lat, lon, data?.resolved?.name || siteId, siteId);

  if (mapIntelTitle) {
    mapIntelTitle.textContent = data?.resolved?.name || siteId;
  }
  if (mapIntelMeta) {
    const intelParts = [];
    const intelCity = normalizeCityDisplay(data?.resolved?.city);
    const intelState = String(data?.resolved?.state || "").trim();
    if (intelCity || intelState) intelParts.push([intelCity, intelState].filter(Boolean).join(", "));
    if (Number.isFinite(Number(lat)) && Number.isFinite(Number(lon))) intelParts.push(`${lat}, ${lon}`);
    mapIntelMeta.textContent = intelParts.join(" • ") || "Operational context loaded.";
  }

  // Utility/provider summary
  const utility = data?.provider?.name || data?.provider?.utility || data?.resolved?.utility || "—";
  const providerName = data?.provider?.name || data?.provider?.platform || "—";
  c_util.textContent = utility;
  c_util_meta.textContent = `Provider: ${providerName}`;

  // --------------------
  // Weather summary (minimal but trustworthy)
  // --------------------
  const wx = data?.weather || {};
  const temp = wx?.temperature_f;
  const cond = wx?.condition || "—";
  const hasAlert = !!wx?.has_weather_alert;
  const severity = wx?.max_alert_severity || "none";

  c_wx.textContent =
    temp !== null && temp !== undefined ? `${temp}°F • ${cond}` : `${cond}`;

  const obsLocal = formatIsoLocal(wx?.observation_time);
  const tempKind = wx?.temp_kind;
  const kindLabel =
    tempKind === "forecast_fallback"
      ? "Forecast (fallback)"
      : tempKind === "observed"
      ? "Observed"
      : null;

  const wxMetaParts = [];
  if (kindLabel) wxMetaParts.push(kindLabel);
  if (obsLocal) wxMetaParts.push(`Obs: ${obsLocal}`);
  wxMetaParts.push(hasAlert ? `Alerts: YES (${severity})` : "Alerts: No");

  c_wx_meta.textContent = wxMetaParts.join(" • ");
  wxDot.className = "dot " + (hasAlert ? "warn" : "live");

  // --------------------
  // Power summary
  // --------------------
  const outageDataStatus = String(data?.provider?.outage_data_status || "").toUpperCase();
  const isMapOnly = outageDataStatus === "MAP_ONLY";
  const mapOnlyFriendly = "Automation unavailable; please check the Outage Map.";
  const outageNearby = !!data?.power?.has_outage_nearby;
  const nearest = data?.power?.nearest || {};
  const customersOut = nearest?.customers_out ?? "—";
  const miles = nearest?.distance_miles;
  const etrRaw = nearest?.etr || nearest?.raw?.etr || null;
  const etr = formatETR(etrRaw);
  const crew = nearest?.raw?.crew_status || "—";

  if (isMapOnly) {
    c_pwr.textContent = "Automation unavailable";
  } else {
    c_pwr.textContent = outageNearby ? "Outage nearby: YES" : "Outage nearby: No";
  }
  const pwrMetaWarning = data?.power?.meta?.warning;
  if (isMapOnly) {
    c_pwr_meta.textContent = mapOnlyFriendly;
  } else if (outageNearby) {
    const dist =
      miles !== null && miles !== undefined ? miles.toFixed(2) + " mi" : "—";
    const baseMeta = etr
      ? `ETR: ${etr} • Customers out: ${customersOut} • Distance: ${dist}`
      : `Customers out: ${customersOut} • Distance: ${dist}`;
    c_pwr_meta.textContent = pwrMetaWarning
      ? baseMeta + " • ⚠ County-level data"
      : baseMeta;
  } else {
    c_pwr_meta.textContent = pwrMetaWarning
      ? "No outage detected (county-level data only)"
      : "No nearby outage detected";
  }

  if (isMapOnly) {
    pwrDot.className = "dot warn";
  } else {
    pwrDot.className = "dot " + (outageNearby ? "warn" : "ok");
  }

  // --------------------
  // Overview KV (keep minimal; order matters)
  // --------------------
  ov_kv.innerHTML = "";
  ov_kv.appendChild(kvRow("Site ID", siteId));
  if (matchedSiteIds.length > 0) {
    ov_kv.appendChild(kvRow("Matched Site IDs", matchedSiteIds.join(", ")));
  }

  // Address
  const addr = data?.resolved?.address;
  const city = data?.resolved?.city;
  const state = data?.resolved?.state;
  const zip = data?.resolved?.zip;

  const addressLine = formatPostalAddress(addr, city, state, zip);
  ov_kv.appendChild(kvRow("Address", addressLine));

  ov_kv.appendChild(kvRow("Utility", utility));
  ov_kv.appendChild(
    kvRow("Weather", temp !== null && temp !== undefined ? `${temp}°F, ${cond}` : cond)
  );
  ov_kv.appendChild(kvRow("Weather alert", hasAlert ? "YES" : "No"));
  ov_kv.appendChild(kvRow("Outage nearby", isMapOnly ? mapOnlyFriendly : (outageNearby ? "YES" : "No")));

  ov_kv.appendChild(sectionTitle("SubReal Status Context"));
  const powerMeta = data?.power?.meta || {};
  const providerPlatform = data?.provider?.platform || "—";
  ov_kv.appendChild(kvRow("Provider platform", providerPlatform));
  ov_kv.appendChild(kvRow("Power data source", powerMeta?.source || "—"));
  if (powerMeta?.cached) {
    const age = powerMeta?.cache_age_s;
    ov_kv.appendChild(kvRow("Power result", age !== null && age !== undefined ? `Cached (${age}s old)` : "Cached"));
  }
  if (powerMeta?.warning) ov_kv.appendChild(kvRow("Power warning", powerMeta.warning));
  if (powerMeta?.error) ov_kv.appendChild(kvRow("Power status detail", powerMeta.error));
  if (wx?.error) ov_kv.appendChild(kvRow("Weather status detail", wx.error));
  if (data?.probe?.mode === "probe") {
    ov_kv.appendChild(kvRow("Provider probe winner", data?.probe?.winner || "No outage-source winner"));
  }

  // Power KV
  // --------------------
  pwr_kv.innerHTML = "";
  pwr_kv.appendChild(kvRow("Utility", utility));

  // Provider disclaimer (e.g. Rio Grande county-level precision notice)
  const pwrWarning = data?.power?.meta?.warning;
  const pwrError = data?.power?.meta?.error;
  if (pwrWarning) {
    const warnBanner = document.createElement("div");
    warnBanner.style.cssText =
      "margin:8px 0;padding:8px 10px;background:var(--warn-bg,#fff3cd);color:var(--warn-fg,#664d03);border-left:3px solid #ffc107;border-radius:3px;font-size:12px;line-height:1.5;";
    warnBanner.textContent = "⚠ " + pwrWarning;
    pwr_kv.appendChild(warnBanner);
  }
  if (pwrError && (!pwrWarning || pwrError !== pwrWarning)) {
    const errNote = document.createElement("div");
    errNote.style.cssText =
      "margin:4px 0 8px;padding:6px 10px;background:var(--muted-bg,#f8f9fa);color:var(--muted,#6c757d);border-left:3px solid #6c757d;border-radius:3px;font-size:11px;line-height:1.5;";
    errNote.textContent = "ⓘ " + pwrError;
    pwr_kv.appendChild(errNote);
  }

  pwr_kv.appendChild(kvRow("Outage nearby", isMapOnly ? mapOnlyFriendly : (outageNearby ? "YES" : "No")));
  pwr_kv.appendChild(kvRow("Customers out (nearest)", customersOut));
  pwr_kv.appendChild(
    kvRow(
      "Distance (miles)",
      miles !== null && miles !== undefined ? miles.toFixed(3) : "—"
    )
  );
  pwr_kv.appendChild(kvRow("ETR (nearest)", etr ?? "—"));
  pwr_kv.appendChild(kvRow("Crew status", crew));

  // --------------------
  // Weather KV
  // --------------------
  wx_kv.innerHTML = "";

  wx_kv.appendChild(sectionTitle("Weather Alerts"));

  const alertsArr = Array.isArray(wx?.alerts) ? wx.alerts : [];
  wx_kv.appendChild(kvRow("Has alert", hasAlert ? "YES" : "No"));
  wx_kv.appendChild(kvRow("Max severity", severity));
  wx_kv.appendChild(kvRow("Alerts count", alertsArr.length));

  if (alertsArr.length > 0) {
    const alertsWrap = document.createElement("div");

    for (const a of alertsArr) {
      const event = a?.event || "Alert";
      const sev = a?.severity || "—";
      const headlineTxt = a?.headline || "";
      const sent = formatIsoLocal(a?.sent) || a?.sent || null;
      const ends = formatIsoLocal(a?.ends) || a?.ends || null;
      const expires = formatIsoLocal(a?.expires) || a?.expires || null;

      const summary = `${event} (${sev})${headlineTxt ? " — " + headlineTxt : ""}`;

      const body = document.createElement("div");
      body.style.whiteSpace = "normal";

      const meta = [];
      if (a?.urgency) meta.push(`Urgency: ${a.urgency}`);
      if (a?.certainty) meta.push(`Certainty: ${a.certainty}`);
      if (sent) meta.push(`Sent: ${sent}`);
      if (a?.effective) meta.push(`Effective: ${formatIsoLocal(a.effective) || a.effective}`);
      if (a?.onset) meta.push(`Onset: ${formatIsoLocal(a.onset) || a.onset}`);
      if (ends) meta.push(`Ends: ${ends}`);
      if (expires) meta.push(`Expires: ${expires}`);

      const metaDiv = document.createElement("div");
      metaDiv.style.fontSize = "12px";
      metaDiv.style.color = "var(--muted)";
      metaDiv.style.marginBottom = "8px";
      metaDiv.textContent = meta.join(" • ") || "—";

      const desc = preWrap(a?.description || "—");
      const instr = preWrap(a?.instruction || "");

      body.appendChild(metaDiv);

      const descLabel = document.createElement("div");
      descLabel.style.fontSize = "12px";
      descLabel.style.color = "var(--muted)";
      descLabel.style.fontWeight = "800";
      descLabel.style.margin = "10px 0 6px 0";
      descLabel.textContent = "Description";
      body.appendChild(descLabel);
      body.appendChild(desc);

      if (!isBlank(a?.instruction)) {
        const instrLabel = document.createElement("div");
        instrLabel.style.fontSize = "12px";
        instrLabel.style.color = "var(--muted)";
        instrLabel.style.fontWeight = "800";
        instrLabel.style.margin = "10px 0 6px 0";
        instrLabel.textContent = "Instruction";
        body.appendChild(instrLabel);
        body.appendChild(instr);
      }

      const block = detailsBlock(summary, body, false);
      block.style.margin = "10px 0";
      alertsWrap.appendChild(block);
    }

    wx_kv.appendChild(kvRow("Alert details", alertsWrap));
  }

  wx_kv.appendChild(sectionTitle("Forecast Text"));
  const dfText = wx?.detailedForecast || "—";
  wx_kv.appendChild(
    kvRow(
      "Detailed forecast",
      dfText.length > 400
        ? detailsBlock("Detailed forecast", preWrap(dfText), false)
        : preWrap(dfText)
    )
  );

  wx_kv.appendChild(sectionTitle("Current Observations"));

  wx_kv.appendChild(kvRow("Observed temp (°F)", isBlank(temp) ? "—" : temp));
  wx_kv.appendChild(kvRow("Condition", displayOrDash(cond)));

  const windSpeed = wx?.wind_speed_mph;
  const windGust = wx?.wind_gust_mph;
  const windDirCard = wx?.wind_direction_cardinal;
  const windDirDeg = wx?.wind_direction_deg;

  let windLine = "—";
  if (!isBlank(windSpeed) || !isBlank(windDirCard) || !isBlank(windDirDeg) || !isBlank(windGust)) {
    const parts = [];
    if (!isBlank(windSpeed)) parts.push(`${windSpeed} mph`);
    if (!isBlank(windGust)) parts.push(`gust ${windGust} mph`);
    if (!isBlank(windDirCard) || !isBlank(windDirDeg)) {
      const dir =
        !isBlank(windDirCard) && !isBlank(windDirDeg)
          ? `${windDirCard} (${windDirDeg}°)`
          : !isBlank(windDirCard)
          ? `${windDirCard}`
          : `${windDirDeg}°`;
      parts.push(dir);
    }
    windLine = parts.join(" • ");
  }

  wx_kv.appendChild(kvRow("Wind", windLine));
  wx_kv.appendChild(kvRow("Wind chill (°F)", numOrDash(wx?.wind_chill_f)));
  wx_kv.appendChild(kvRow("Heat index (°F)", numOrDash(wx?.heat_index_f)));
  wx_kv.appendChild(
    kvRow(
      "Precip last hour (in)",
      wx?.precip_last_hour_in === null || wx?.precip_last_hour_in === undefined
        ? "—"
        : String(wx?.precip_last_hour_in)
    )
  );

  wx_kv.appendChild(
    kvRow(
      "Observation time",
      displayOrDash(formatIsoLocal(wx?.observation_time) || wx?.observation_time)
    )
  );


  // --------------------
  // Actions
  // --------------------
  const latNum = Number(data?.resolved?.lat);
  const lonNum = Number(data?.resolved?.lon);
  const hasCoords = Number.isFinite(latNum) && Number.isFinite(lonNum);

  openGoogleMaps.disabled = !hasCoords;

  const outageMapSafe = safeUrl(data?.provider?.outage_map);
  openOutageMap.disabled = !outageMapSafe;

  if (openNwsMap) openNwsMap.disabled = !hasCoords;

  setTabs("overview");
}

// --------------------
// Events
// --------------------

btn.addEventListener("click", runSearch);

q.addEventListener("keydown", (e) => {
  if (e.key === "ArrowDown") {
    if (!suggestions.hidden && currentSuggestions.length > 0) {
      e.preventDefault();
      setActiveSuggestion(activeSuggestionIndex + 1);
    }
    return;
  }

  if (e.key === "ArrowUp") {
    if (!suggestions.hidden && currentSuggestions.length > 0) {
      e.preventDefault();
      setActiveSuggestion(activeSuggestionIndex - 1);
    }
    return;
  }

  if (e.key === "Enter") {
    if (!suggestions.hidden && currentSuggestions.length > 0) {
      e.preventDefault();
      const pickIndex = activeSuggestionIndex >= 0 ? activeSuggestionIndex : 0;
      const pick = currentSuggestions[pickIndex];
      if (pick) {
        applySuggestion(pick);
        return;
      }
    }
    runSearch();
    return;
  }

  if (e.key === "Tab") {
    clearSuggestions();
    return;
  }

  if (e.key === "Escape") {
    if (!suggestions.hidden) {
      clearSuggestions();
      return;
    }
    q.value = "";
    clearSuggestions();
    clearPanels();
    headline.textContent = "No query loaded";
    subhead.textContent =
      "Submit a synthetic Site ID or coordinates to retrieve weather and outage proximity.";
    setStatus("ok", "Idle");
  }
});

q.addEventListener("input", () => {
  window.clearTimeout(suggestionDebounceTimer);
  suggestionDebounceTimer = window.setTimeout(() => {
    void fetchSuggestions(q.value);
  }, 120);
});

// Optional: normalize input when leaving the box (no toast spam)
q.addEventListener("blur", () => {
  // Let click handlers run before the dropdown is closed.
  window.setTimeout(() => clearSuggestions(), 120);
  const check = validateQueryInput(q.value);
  if (check.ok) q.value = check.normalized;
});

if (opsOpenGoogleMaps) {
  opsOpenGoogleMaps.addEventListener("click", () => openGoogleMaps.click());
}
if (opsOpenOutageMap) {
  opsOpenOutageMap.addEventListener("click", () => openOutageMap.click());
}

openGoogleMaps.addEventListener("click", () => {
  if (!lastPayload) return;

  const lat = Number(lastPayload?.resolved?.lat);
  const lon = Number(lastPayload?.resolved?.lon);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    showToast("No coordinates available for this result.");
    return;
  }

  const url = `https://www.google.com/maps?q=${encodeURIComponent(`${lat},${lon}`)}`;
  window.open(url, "_blank", "noopener,noreferrer");
});

openOutageMap.addEventListener("click", () => {
  const url = safeUrl(lastPayload?.provider?.outage_map);
  if (!url) {
    showToast("Invalid power outage map URL.");
    return;
  }
  window.open(url, "_blank", "noopener,noreferrer");
});

// Open NWS point forecast page (includes map + point forecast)
if (openNwsMap) {
  openNwsMap.addEventListener("click", () => {
    if (!lastPayload) return;

    const lat = Number(lastPayload?.resolved?.lat);
    const lon = Number(lastPayload?.resolved?.lon);

    if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
      showToast("No coordinates available for this result.");
      return;
    }

    const url = `https://forecast.weather.gov/MapClick.php?lat=${encodeURIComponent(lat)}&lon=${encodeURIComponent(lon)}`;
    window.open(url, "_blank", "noopener,noreferrer");
  });
}

// Initial state
initForecastMap();
clearPanels();
setStatus("ok", "Idle");
