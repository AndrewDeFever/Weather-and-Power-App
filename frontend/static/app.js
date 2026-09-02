/**
 * Weather & Power Status - Frontend
 * Notes:
 * - API is same-origin: /api/status?q=...
 * - Actions: Open in Google Maps, Open Outage Map, Open Weather Map (NWS)
 * - Overview stays minimal; Weather tab is NOC-focused (observations + alerts + forecast text)
 *
 * Hardening:
 * - safeUrl() blocks javascript:/data: etc from backend-provided URLs (XSS mitigation)
 * - validateQueryInput() tightens user input hygiene (allowlist + bounds checks)
 */

const API_ENDPOINT = "/api/status";

const $ = (id) => document.getElementById(id);

const q = $("q");
const btn = $("btn");
const toast = $("toast");

// Client-side time budget: keep below backend ~15s SLA (CloudFront/origin timeouts).
const CLIENT_TIMEOUT_MS = 13000;

const statusDot = $("statusDot");
const statusText = $("statusText");

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
 * - Max length: 80
 * - Accept:
 *   A) Site ID: letters/numbers/_/- (1..40 chars) -> normalized to UPPERCASE
 *   B) Lat,Lon: numeric with optional decimals -> range checked -> normalized "lat,lon"
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

  return {
    ok: false,
    error: "Invalid input. Use a Site ID (letters/numbers/_/-) or coordinates like 36.15,-95.99.",
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

  const value = check.normalized;

  // Show normalized input (helps operators spot typos immediately)
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
  c_loc.textContent = data?.resolved?.name ? `${data.resolved.name}` : siteId;
  c_loc_meta.textContent = `Site ID: ${siteId} • ${lat}, ${lon}`;

  // Utility/provider summary
  const utility = data?.provider?.utility || data?.resolved?.utility || "—";
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
  const outageNearby = !!data?.power?.has_outage_nearby;
  const nearest = data?.power?.nearest || {};
  const customersOut = nearest?.customers_out ?? "—";
  const miles = nearest?.distance_miles;
  const etrRaw = nearest?.etr || nearest?.raw?.etr || null;
  const etr = formatETR(etrRaw);
  const crew = nearest?.raw?.crew_status || "—";

  c_pwr.textContent = outageNearby ? "Outage nearby: YES" : "Outage nearby: No";
  if (outageNearby) {
    const dist =
      miles !== null && miles !== undefined ? miles.toFixed(2) + " mi" : "—";
    c_pwr_meta.textContent = etr
      ? `ETR: ${etr} • Customers out: ${customersOut} • Distance: ${dist}`
      : `Customers out: ${customersOut} • Distance: ${dist}`;
  } else {
    c_pwr_meta.textContent = "No nearby outage detected";
  }

  pwrDot.className = "dot " + (outageNearby ? "warn" : "ok");

  // --------------------
  // Overview KV (keep minimal; order matters)
  // --------------------
  ov_kv.innerHTML = "";
  ov_kv.appendChild(kvRow("Resolved type", data?.resolved?.type ?? "—"));
  ov_kv.appendChild(kvRow("Site ID", siteId));

  // Address
  const addr = data?.resolved?.address;
  const city = data?.resolved?.city;
  const state = data?.resolved?.state;
  const zip = data?.resolved?.zip;

  let addressLine = "—";
  if (addr || city || state || zip) {
    const line2 = [city, state, zip].filter(Boolean).join(" ");
    addressLine = [addr, line2].filter(Boolean).join(", ");
  }
  ov_kv.appendChild(kvRow("Address", addressLine));

  ov_kv.appendChild(kvRow("Coordinates", `${lat}, ${lon}`));
  ov_kv.appendChild(kvRow("Utility", utility));
  ov_kv.appendChild(
    kvRow("Weather", temp !== null && temp !== undefined ? `${temp}°F, ${cond}` : cond)
  );
  ov_kv.appendChild(kvRow("Weather alert", hasAlert ? "YES" : "No"));
  ov_kv.appendChild(kvRow("Outage nearby", outageNearby ? "YES" : "No"));

  // --------------------
  // Power KV
  // --------------------
  pwr_kv.innerHTML = "";
  pwr_kv.appendChild(kvRow("Utility", utility));
  pwr_kv.appendChild(kvRow("Platform", data?.provider?.platform ?? "—"));
  pwr_kv.appendChild(kvRow("Outage map", data?.provider?.outage_map ?? "—"));
  pwr_kv.appendChild(kvRow("Outage nearby", outageNearby ? "YES" : "No"));
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
  // Weather KV (NOC-focused)
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

    wx_kv.appendChild(
      kvRow("Alert details", detailsBlock("Expand alert list", alertsWrap, false))
    );
  }

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
    if (!isBlank(windGust)) parts.push(`gust ${windGust}`);
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

  wx_kv.appendChild(sectionTitle("Forecast Text"));
  const dfText = wx?.detailedForecast || "—";
  wx_kv.appendChild(
    kvRow("Detailed forecast", detailsBlock("Expand detailedForecast", preWrap(dfText), false))
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
  if (e.key === "Enter") runSearch();

  if (e.key === "Escape") {
    q.value = "";
    clearPanels();
    headline.textContent = "No query loaded";
    subhead.textContent =
      "Submit a Site ID or coordinates to retrieve weather and outage proximity.";
    setStatus("ok", "Idle");
  }
});

// Optional: normalize input when leaving the box (no toast spam)
q.addEventListener("blur", () => {
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
    showToast("Invalid outage map URL.");
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
clearPanels();
setStatus("ok", "Idle");
