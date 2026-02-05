from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable, Dict, List, Optional

from app.models import Outage, PowerBlock, PowerMeta

# ------------------------------------------------------------
# Operational tuning knobs
# ------------------------------------------------------------

# 10 miles radius -> kilometers
MAX_RADIUS_MILES = 20
MAX_RADIUS_KM = MAX_RADIUS_MILES * 1.609344  # 16.09344

# Timeouts
DIRECT_TIMEOUT_S = 30  # known utility
PROBE_TIMEOUT_S = 15    # per-provider when utility is unknown

# Metadata-only caching (provider health)
HEALTH_TTL_S = 90  # seconds to "cool down" a provider after a failure/timeout

SUPPORTED_PROVIDERS = {"OGE", "PSO", "EVERGY", "ONCOR", "AUSTIN"}
PROBE_ORDER = ("PSO", "OGE", "EVERGY", "ONCOR", "AUSTIN")  # keep deterministic


# ------------------------------------------------------------
# Provider health cache (metadata only)
# ------------------------------------------------------------
_provider_health: Dict[str, Dict[str, Any]] = {}
# shape:
# {
#   "PSO": {"ts": 1700000000.0, "ok": True/False, "error": "Timeout"...}
# }

def _health_mark(provider: str, ok: bool, error: Optional[str]) -> None:
    _provider_health[provider] = {"ts": time.time(), "ok": ok, "error": error}


def _health_should_skip(provider: str) -> bool:
    rec = _provider_health.get(provider)
    if not rec:
        return False
    age = time.time() - float(rec.get("ts", 0.0))
    if age > HEALTH_TTL_S:
        return False
    # Only skip if last result was NOT ok
    return not bool(rec.get("ok", True))


# ------------------------------------------------------------
# Normalization
# ------------------------------------------------------------
def _coerce_outage(d: Dict[str, Any]) -> Outage:
    lat = d.get("lat")
    lon = d.get("lon")

    if lat is None:
        lat = d.get("latitude")
    if lon is None:
        lon = d.get("longitude")

    dkm = d.get("distance_km")
    dm = d.get("distance_miles")

    miles: Optional[float]
    if isinstance(dm, (int, float)):
        miles = float(dm)
    elif isinstance(dkm, (int, float)):
        miles = float(dkm) * 0.621371
    else:
        miles = None

    return Outage(
        customers_out=d.get("customers_out"),
        etr=d.get("etr"),
        start_time=d.get("start_time"),
        cause=d.get("cause"),
        lat=lat,
        lon=lon,
        distance_km=float(dkm) if isinstance(dkm, (int, float)) else None,
        distance_miles=miles,
        outage_id=d.get("outage_id") or d.get("outage_id".upper()) or d.get("id"),
        provider=d.get("provider") or "KUBRA",
        raw=d,
    )


# ------------------------------------------------------------
# Timeout wrapper
# ------------------------------------------------------------
def _run_with_timeout(provider: str, fn: Callable[[], PowerBlock], timeout_s: int) -> PowerBlock:
    """
    Runs provider call with a hard timeout. Updates provider health (metadata only).
    """
    meta_source = f"app.power_router.{provider.lower()}.timeout_wrapper"

    # IMPORTANT: do NOT use a context manager for ThreadPoolExecutor here.
    # If the future times out, the executor's __exit__ would block waiting for the
    # worker thread to finish (which defeats the point of the timeout and causes
    # multi-minute "hangs" in the UI). We shut down without waiting.
    ex = ThreadPoolExecutor(max_workers=1)
    fut = ex.submit(fn)
    try:
        res = fut.result(timeout=timeout_s)
        _health_mark(provider, bool(res.meta.ok), res.meta.error)
        return res
    except FuturesTimeout:
        _health_mark(provider, False, f"Timeout after {timeout_s}s")
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            # Older Python fallback
            ex.shutdown(wait=False)
        meta = PowerMeta(source=meta_source, ok=False, error=f"Timeout after {timeout_s}s")
        return PowerBlock(utility=provider, has_outage_nearby=False, nearest=None, outages=[], meta=meta)
    except Exception as e:
        _health_mark(provider, False, f"{type(e).__name__}: {e}")
        try:
            ex.shutdown(wait=False, cancel_futures=True)
        except TypeError:
            ex.shutdown(wait=False)
        meta = PowerMeta(source=meta_source, ok=False, error=f"{type(e).__name__}: {e}")
        return PowerBlock(utility=provider, has_outage_nearby=False, nearest=None, outages=[], meta=meta)
    finally:
        # If we got here without timing out, ensure proper shutdown.
        try:
            ex.shutdown(wait=False)
        except Exception:
            pass


# ------------------------------------------------------------
# Provider calls (all constrained to 10 miles radius)
# ------------------------------------------------------------
def _call_oge(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.oge_kubra.fetch_oge_outages", ok=True, error=None)
    try:
        from app.providers.oge_kubra import fetch_oge_outages

        result: Dict[str, Any] = fetch_oge_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,  # <= 10 miles
            debug=False,
        )

        nearest_raw = result.get("nearest") or None
        outages_raw = result.get("outages") or []

        nearest = _coerce_outage(nearest_raw) if isinstance(nearest_raw, dict) else None
        outages = [_coerce_outage(o) for o in outages_raw if isinstance(o, dict)]

        return PowerBlock(utility="OGE", has_outage_nearby=bool(nearest), nearest=nearest, outages=outages, meta=meta)
    except Exception as e:
        meta.ok = False
        meta.error = f"{type(e).__name__}: {e}"
        return PowerBlock(utility="OGE", has_outage_nearby=False, nearest=None, outages=[], meta=meta)


def _call_pso(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.pso_kubra.fetch_pso_outages", ok=True, error=None)
    try:
        from app.providers.pso_kubra import fetch_pso_outages

        result: Dict[str, Any] = fetch_pso_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,  # <= 10 miles
            debug=False,
        )

        nearest_raw = result.get("nearest") or None
        outages_raw = result.get("outages") or []

        nearest = _coerce_outage(nearest_raw) if isinstance(nearest_raw, dict) else None
        outages = [_coerce_outage(o) for o in outages_raw if isinstance(o, dict)]

        return PowerBlock(utility="PSO", has_outage_nearby=bool(nearest), nearest=nearest, outages=outages, meta=meta)
    except Exception as e:
        meta.ok = False
        meta.error = f"{type(e).__name__}: {e}"
        return PowerBlock(utility="PSO", has_outage_nearby=False, nearest=None, outages=[], meta=meta)


def _call_evergy(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.evergy_kubra.fetch_evergy_outages", ok=True, error=None)
    try:
        from app.providers.evergy_kubra import fetch_evergy_outages

        result: Dict[str, Any] = fetch_evergy_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,  # <= 10 miles
            debug=False,
        )

        nearest_raw = result.get("nearest") or None
        outages_raw = result.get("outages") or []

        nearest = _coerce_outage(nearest_raw) if isinstance(nearest_raw, dict) else None
        outages = [_coerce_outage(o) for o in outages_raw if isinstance(o, dict)]

        return PowerBlock(utility="EVERGY", has_outage_nearby=bool(nearest), nearest=nearest, outages=outages, meta=meta)
    except Exception as e:
        meta.ok = False
        meta.error = f"{type(e).__name__}: {e}"
        return PowerBlock(utility="EVERGY", has_outage_nearby=False, nearest=None, outages=[], meta=meta)


def _call_oncor(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.oncor_kubra.fetch_oncor_outages", ok=True, error=None)
    try:
        from app.providers.oncor_kubra import fetch_oncor_outages

        result: Dict[str, Any] = fetch_oncor_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=0,
            drill_neighbor_depth=0,
            max_radius_km=MAX_RADIUS_KM,  # <= 10 miles
            debug=False,
        )

        nearest_raw = result.get("nearest") or None
        outages_raw = result.get("outages") or []

        nearest = _coerce_outage(nearest_raw) if isinstance(nearest_raw, dict) else None
        outages = [_coerce_outage(o) for o in outages_raw if isinstance(o, dict)]

        return PowerBlock(utility="ONCOR", has_outage_nearby=bool(nearest), nearest=nearest, outages=outages, meta=meta)
    except Exception as e:
        meta.ok = False
        meta.error = f"{type(e).__name__}: {e}"
        return PowerBlock(utility="ONCOR", has_outage_nearby=False, nearest=None, outages=[], meta=meta)


def _call_austin(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.austin_energy_kubra.fetch_austin_energy_outages", ok=True, error=None)
    try:
        from app.providers.austin_energy_kubra import fetch_austin_energy_outages

        result: Dict[str, Any] = fetch_austin_energy_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,  # <= 10 miles
            fallback_radius_km=MAX_RADIUS_KM * 2.5,  # bounded fallback (~25 miles)
            debug=False,
        )

        nearest_raw = result.get("nearest") or None
        outages_raw = result.get("outages") or []

        # Tag provider for downstream coercion
        if isinstance(nearest_raw, dict):
            nearest_raw.setdefault("provider", "AUSTIN")
        for o in outages_raw:
            if isinstance(o, dict):
                o.setdefault("provider", "AUSTIN")

        nearest = _coerce_outage(nearest_raw) if isinstance(nearest_raw, dict) else None
        outages = [_coerce_outage(o) for o in outages_raw if isinstance(o, dict)]

        return PowerBlock(utility="AUSTIN", has_outage_nearby=bool(nearest), nearest=nearest, outages=outages, meta=meta)
    except Exception as e:
        meta.ok = False
        meta.error = f"{type(e).__name__}: {e}"
        return PowerBlock(utility="AUSTIN", has_outage_nearby=False, nearest=None, outages=[], meta=meta)



_call_map: Dict[str, Callable[[float, float], PowerBlock]] = {
    "OGE": _call_oge,
    "PSO": _call_pso,
    "EVERGY": _call_evergy,
    "ONCOR": _call_oncor,
    "AUSTIN": _call_austin,
}


# ------------------------------------------------------------
# Public API
# ------------------------------------------------------------
def get_power_status(lat: float, lon: float, utility: Optional[str] = None) -> PowerBlock:
    """
    Dispatcher:
    - Known supported utility: call it directly (DIRECT_TIMEOUT_S)
    - Known but unsupported utility: return "unsupported" (no probing)
    - Unknown utility: probe providers with per-provider timeouts (PROBE_TIMEOUT_S),
      but DO NOT impersonate a utility when no outages are found.
    """
    utility_norm = (utility or "").strip().upper() or None

    # Known but unsupported
    if utility_norm and utility_norm not in SUPPORTED_PROVIDERS:
        meta = PowerMeta(source="app.power_router.unsupported", ok=True, error=None)
        return PowerBlock(
            utility=utility_norm,
            has_outage_nearby=False,
            nearest=None,
            outages=[],
            meta=meta,
        )

    # Direct call (known supported)
    if utility_norm in SUPPORTED_PROVIDERS:
        fn = _call_map[utility_norm]
        return _run_with_timeout(utility_norm, lambda: fn(lat, lon), DIRECT_TIMEOUT_S)

    # Probe mode (unknown utility): metadata-cache skip + timeouts + radius already constrained
    attempts: List[PowerBlock] = []

    for key in PROBE_ORDER:
        if _health_should_skip(key):
            # metadata-only caching skip
            meta = PowerMeta(
                source="app.power_router.health_cache",
                ok=False,
                error=f"Skipped due to recent failure: {_provider_health.get(key, {}).get('error')}",
            )
            attempts.append(PowerBlock(utility=key, has_outage_nearby=False, nearest=None, outages=[], meta=meta))
            continue

        fn = _call_map[key]
        res = _run_with_timeout(key, lambda: fn(lat, lon), PROBE_TIMEOUT_S)
        attempts.append(res)

        # If any provider finds an outage nearby, return it immediately (deterministic).
        if res.meta.ok and res.has_outage_nearby:
            return res

    # No outages found anywhere: do NOT return PSO/OGE/etc. as the "utility"
    any_ok = any(a.meta.ok for a in attempts)
    meta = PowerMeta(source="app.power_router.probe", ok=any_ok, error=None if any_ok else "All probes failed/timeout")
    return PowerBlock(
        utility="UNKNOWN",
        has_outage_nearby=False,
        nearest=None,
        outages=[],
        meta=meta,
    )

from typing import Tuple

def probe_power_status(lat: float, lon: float) -> Tuple[PowerBlock, List[PowerBlock]]:
    """
    Probe providers when site.utility is unknown.

    Returns:
      chosen: PowerBlock (utility="UNKNOWN" unless an outage is found)
      attempts: list of provider PowerBlocks (ok/error + findings)
    """
    attempts: List[PowerBlock] = []

    for key in PROBE_ORDER:
        if _health_should_skip(key):
            meta = PowerMeta(
                source="app.power_router.health_cache",
                ok=False,
                error=f"Skipped due to recent failure: {_provider_health.get(key, {}).get('error')}",
            )
            attempts.append(
                PowerBlock(
                    utility=key,
                    has_outage_nearby=False,
                    nearest=None,
                    outages=[],
                    meta=meta,
                )
            )
            continue

        fn = _call_map[key]
        res = _run_with_timeout(key, lambda: fn(lat, lon), PROBE_TIMEOUT_S)
        attempts.append(res)

        # Deterministic winner: first provider that finds an outage nearby.
        if res.meta.ok and res.has_outage_nearby:
            return res, attempts

    # No outages found anywhere: do NOT impersonate a utility.
    any_ok = any(a.meta.ok for a in attempts)
    meta = PowerMeta(
        source="app.power_router.probe",
        ok=any_ok,
        error=None if any_ok else "All probes failed/timeout",
    )
    chosen = PowerBlock(
        utility="UNKNOWN",
        has_outage_nearby=False,
        nearest=None,
        outages=[],
        meta=meta,
    )
    return chosen, attempts
