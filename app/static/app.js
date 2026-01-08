/**
     * Adjust this to match your backend route.
     * Common choices:
     *  - "/api/status"
     *  - "/status"
     *  - "/lookup"
     */
    const API_ENDPOINT = "/api/status";

    const $ = (id) => document.getElementById(id);

    const q = $("q");
    const btn = $("btn");
    const toast = $("toast");

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

    const copyJson = $("copyJson");
    const openOutageMap = $("openOutageMap");

    let lastPayload = null;

    // Format ETR (Estimated Time of Restoration) into a human-friendly local string.
    // Input may be ISO8601, epoch ms, or a provider-specific string.
    function formatETR(etr){
      if (etr === null || etr === undefined || etr === "") return null;

      // If it's epoch milliseconds (or seconds) as a number.
      if (typeof etr === "number" && Number.isFinite(etr)){
        const ms = etr < 10_000_000_000 ? etr * 1000 : etr;
        const d = new Date(ms);
        if (!Number.isNaN(d.getTime())) return formatDateTimeLocal(d);
        return String(etr);
      }

      // If it's a string, try ISO first.
      if (typeof etr === "string"){
        const s = etr.trim();
        if (!s) return null;
        const d = new Date(s);
        if (!Number.isNaN(d.getTime())) return formatDateTimeLocal(d);
        return s; // fall back to raw provider text
      }

      return String(etr);
    }

    function formatDateTimeLocal(d){
      // Use the user's local timezone (browser). For you, this should be America/Chicago.
      const time = new Intl.DateTimeFormat(undefined, {
        hour: "numeric",
        minute: "2-digit",
      }).format(d);

      const date = new Intl.DateTimeFormat(undefined, {
        year: "numeric",
        month: "short",
        day: "2-digit",
      }).format(d);

      // If it's today, show only the time; otherwise include the date.
      const today = new Date();
      const isToday = d.getFullYear() === today.getFullYear()
        && d.getMonth() === today.getMonth()
        && d.getDate() === today.getDate();

      return isToday ? time : `${date} ${time}`;
    }

    function setStatus(state, msg){
      statusDot.className = "dot";
      if (state === "loading") statusDot.classList.add("live");
      if (state === "ok") statusDot.classList.add("ok");
      if (state === "warn") statusDot.classList.add("warn");
      statusText.textContent = msg;
    }

    function showToast(message){
      toast.textContent = message;
      toast.classList.add("show");
      window.clearTimeout(showToast._t);
      showToast._t = window.setTimeout(() => toast.classList.remove("show"), 3500);
    }

    function kvRow(k, v){
      const wrap = document.createElement("div");
      wrap.className = "kv";
      const kk = document.createElement("div");
      kk.className = "k";
      kk.textContent = k;
      const vv = document.createElement("div");
      vv.className = "v";
      vv.textContent = (v === null || v === undefined || v === "") ? "—" : String(v);
      wrap.appendChild(kk);
      wrap.appendChild(vv);
      return wrap;
    }

    function clearPanels(){
      ov_kv.innerHTML = "";
      pwr_kv.innerHTML = "";
      wx_kv.innerHTML = "";
      raw.textContent = "(no data)";
      copyJson.disabled = true;
      openOutageMap.disabled = true;
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

    function setTabs(activeId){
      const tabs = ["overview","power","weather","raw"];
      for (const t of tabs){
        $("t_"+t).classList.remove("active");
        $("t_"+t).setAttribute("aria-selected", "false");
        $("tab_"+t).hidden = true;
      }
      $("t_"+activeId).classList.add("active");
      $("t_"+activeId).setAttribute("aria-selected","true");
      $("tab_"+activeId).hidden = false;
    }

    ["overview","power","weather","raw"].forEach(t=>{
      $("t_"+t).addEventListener("click", ()=>setTabs(t));
    });

    async function runSearch(){
      const value = (q.value || "").trim();
      if (!value){
        showToast("Enter a Site ID or coordinates before searching.");
        return;
      }

      setStatus("loading", "Searching…");
      btn.disabled = true;

      try{
        const url = new URL(API_ENDPOINT, window.location.origin);
        url.searchParams.set("q", value);

        const res = await fetch(url.toString(), { headers: { "Accept": "application/json" } });

        if (!res.ok){
          const text = await res.text().catch(()=> "");
          throw new Error(`HTTP ${res.status} ${res.statusText}${text ? " — " + text : ""}`);
        }

        const data = await res.json();
        lastPayload = data;

        render(value, data);
        setStatus("ok", "OK");
        btn.disabled = false;
      }catch(err){
        console.error(err);
        setStatus("warn", "Error");
        btn.disabled = false;
        showToast(err?.message || "Request failed.");
      }
    }

    function render(query, data){
      // Headline
      const resolvedName = data?.resolved?.name || data?.resolved?.site_id || data?.resolved?.id || "Result";
      headline.textContent = `${resolvedName}`;
      subhead.textContent = `Query: ${query}`;

      // Summary: Location
      const siteId = data?.resolved?.site_id || data?.resolved?.id || "—";
      const lat = data?.resolved?.lat ?? "—";
      const lon = data?.resolved?.lon ?? "—";
      c_loc.textContent = data?.resolved?.name ? `${data.resolved.name}` : siteId;
      c_loc_meta.textContent = `Site ID: ${siteId} • ${lat}, ${lon}`;

      // Summary: Utility/Provider
      const utility = data?.provider?.utility || data?.resolved?.utility || "—";
      const providerName = data?.provider?.name || data?.provider?.platform || "—";
      c_util.textContent = utility;
      c_util_meta.textContent = `Provider: ${providerName}`;

      // Summary: Weather
      const temp = data?.weather?.temperature_f;
      const cond = data?.weather?.condition || "—";
      const hasAlert = !!data?.weather?.has_weather_alert;
      const severity = data?.weather?.max_alert_severity || "none";

      c_wx.textContent = (temp !== null && temp !== undefined) ? `${temp}°F • ${cond}` : `${cond}`;
      c_wx_meta.textContent = hasAlert ? `Weather alerts: YES • Severity: ${severity}` : `Weather alerts: No`;

      wxDot.className = "dot " + (hasAlert ? "warn" : "live");

      // Summary: Power
      const outageNearby = !!data?.power?.has_outage_nearby;
      const nearest = data?.power?.nearest || {};
      const customersOut = nearest?.customers_out ?? "—";
      const miles = nearest?.distance_miles;
      const etrRaw = nearest?.etr || nearest?.raw?.etr || null; // ETR, not ETA
      const etr = formatETR(etrRaw);
      const crew = nearest?.raw?.crew_status || "—";

      c_pwr.textContent = outageNearby ? "Outage nearby: YES" : "Outage nearby: No";
      if (outageNearby){
        const dist = (miles !== null && miles !== undefined) ? miles.toFixed(2) + " mi" : "—";
        // Keep it concise; surface ETR when present.
        c_pwr_meta.textContent = etr
          ? `ETR: ${etr} • Customers out: ${customersOut} • Distance: ${dist}`
          : `Customers out: ${customersOut} • Distance: ${dist}`;
      } else {
        c_pwr_meta.textContent = "No nearby outage detected";
      }

      pwrDot.className = "dot " + (outageNearby ? "warn" : "ok");

      // Panel: Overview KV
      ov_kv.innerHTML = "";
      ov_kv.appendChild(kvRow("Resolved type", data?.resolved?.type ?? "—"));
      ov_kv.appendChild(kvRow("Site ID", siteId));
      ov_kv.appendChild(kvRow("Coordinates", `${lat}, ${lon}`));
      ov_kv.appendChild(kvRow("Utility", utility));
      ov_kv.appendChild(kvRow("Weather", (temp !== null && temp !== undefined) ? `${temp}°F, ${cond}` : cond));
      ov_kv.appendChild(kvRow("Weather alert", hasAlert ? "YES" : "No"));
      ov_kv.appendChild(kvRow("Outage nearby", outageNearby ? "YES" : "No"));

      // Panel: Power KV
      pwr_kv.innerHTML = "";
      pwr_kv.appendChild(kvRow("Utility", utility));
      pwr_kv.appendChild(kvRow("Platform", data?.provider?.platform ?? "—"));
      pwr_kv.appendChild(kvRow("Outage map", data?.provider?.outage_map ?? "—"));
      pwr_kv.appendChild(kvRow("Outage nearby", outageNearby ? "YES" : "No"));
      pwr_kv.appendChild(kvRow("Customers out (nearest)", customersOut));
      pwr_kv.appendChild(kvRow("Distance (miles)", (miles !== null && miles !== undefined) ? miles.toFixed(3) : "—"));
      pwr_kv.appendChild(kvRow("ETR (nearest)", etr ?? "—"));
      pwr_kv.appendChild(kvRow("Crew status", crew));

      // Panel: Weather KV
      wx_kv.innerHTML = "";
      wx_kv.appendChild(kvRow("Temperature (°F)", (temp !== null && temp !== undefined) ? temp : "—"));
      wx_kv.appendChild(kvRow("Condition", cond));
      wx_kv.appendChild(kvRow("Has alert", hasAlert ? "YES" : "No"));
      wx_kv.appendChild(kvRow("Max severity", severity));
      wx_kv.appendChild(kvRow("Alerts count", Array.isArray(data?.weather?.alerts) ? data.weather.alerts.length : "—"));

      // Raw JSON
      raw.textContent = JSON.stringify(data, null, 2);

      // Enable actions
      copyJson.disabled = false;
      const outageMap = data?.provider?.outage_map;
      openOutageMap.disabled = !outageMap;

      setTabs("overview");
    }

    // Button & keyboard behaviors
    btn.addEventListener("click", runSearch);
    q.addEventListener("keydown", (e) => {
      if (e.key === "Enter") runSearch();
      if (e.key === "Escape") {
        q.value = "";
        clearPanels();
        headline.textContent = "No query loaded";
        subhead.textContent = "Submit a Site ID or coordinates to retrieve weather and outage proximity.";
        setStatus("ok", "Idle");
      }
    });

    copyJson.addEventListener("click", async () => {
      if (!lastPayload) return;
      try{
        await navigator.clipboard.writeText(JSON.stringify(lastPayload, null, 2));
        showToast("Copied JSON to clipboard.");
      }catch{
        showToast("Copy failed (clipboard permission).");
      }
    });

    openOutageMap.addEventListener("click", () => {
      const url = lastPayload?.provider?.outage_map;
      if (!url) return;
      window.open(url, "_blank", "noopener,noreferrer");
    });

    // Initial state
    clearPanels();
    setStatus("ok", "Idle");
