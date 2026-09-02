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
const themeToggle = $("themeToggle");

const headline = $("headline");
const subhead = $("subhead");

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
const raw = $("raw");

// Buttons
const openGoogleMaps = $("openGoogleMaps");
const openOutageMap = $("openOutageMap");
const openNwsMap = $("openNwsMap");

let lastPayload = null;
let currentSuggestions = [];
let activeSuggestionIndex = -1;
let suggestionAbortController = null;
let suggestionDebounceTimer = null;
const THEME_STORAGE_KEY = "subreal.weather-power.theme";

function applyTheme(theme) {
  const mode = theme === "dark" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", mode);

  if (themeToggle) {
    const isDark = mode === "dark";
    themeToggle.setAttribute("aria-checked", isDark ? "true" : "false");
    themeToggle.setAttribute("title", isDark ? "Switch to light mode" : "Switch to dark mode");
  }
}

function resolveInitialTheme() {
  const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
  if (stored === "light" || stored === "dark") return stored;

  const prefersDark =
    typeof window.matchMedia === "function" &&
    window.matchMedia("(prefers-color-scheme: dark)").matches;
  return prefersDark ? "dark" : "light";
}

function initThemeToggle() {
  const initialTheme = resolveInitialTheme();
  applyTheme(initialTheme);

  if (themeToggle) {
    themeToggle.addEventListener("click", () => {
      const isDark = document.documentElement.getAttribute("data-theme") === "dark";
      const nextTheme = isDark ? "light" : "dark";
      applyTheme(nextTheme);
      window.localStorage.setItem(THEME_STORAGE_KEY, nextTheme);
    });
  }
}

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
  raw.textContent = "(no data)";

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

  wxDot.className = "dot live";
  pwrDot.className = "dot";
}

function setTabs(activeId) {
  const tabs = ["overview", "power", "weather", "raw"];
  for (const t of tabs) {
    $("t_" + t).classList.remove("active");
    $("t_" + t).setAttribute("aria-selected", "false");
    $("tab_" + t).hidden = true;
  }
  $("t_" + activeId).classList.add("active");
  $("t_" + activeId).setAttribute("aria-selected", "true");
  $("tab_" + activeId).hidden = false;
}

["overview", "power", "weather", "raw"].forEach((t) => {
  $("t_" + t).addEventListener("click", () => setTabs(t));
});

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
  // Headline
  const resolvedName =
    data?.resolved?.name || data?.resolved?.site_id || data?.resolved?.id || "Result";
  headline.textContent = `${resolvedName}`;
  subhead.textContent = `Query: ${query}`;

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

  // Raw JSON tab
  raw.textContent = JSON.stringify(data, null, 2);

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
initThemeToggle();
clearPanels();
setStatus("ok", "Idle");
