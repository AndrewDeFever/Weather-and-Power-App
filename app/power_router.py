from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.models import Outage, PowerBlock, PowerMeta

MAX_RADIUS_MILES = 20
MAX_RADIUS_KM = MAX_RADIUS_MILES * 1.609344

# Keep direct calls comfortably below the API's total power budget.
DIRECT_TIMEOUT_S = 13
PROBE_TIMEOUT_S = 2
HEALTH_TTL_S = 90

SUPPORTED_PROVIDERS = {
    "OGE",
    "PSO",
    "EVERGY",
    "ONCOR",
    "AUSTIN",
    "PEC",
    "AEP",
    "EPE",
    "EL_PASO_ELECTRIC",
    "CENTERPOINT",
    "CITY_OF_CONCORDIA_ELECTRIC",
    "PRAIRIE_LAND_ELECTRIC",
    "NINNESCAH_RURAL_ELECTRIC",
}

PROBE_ORDER = (
    "PSO",
    "OGE",
    "EVERGY",
    "ONCOR",
    "AUSTIN",
    "PEC",
    "CENTERPOINT",
    "AEP",
    "EPE",
    "CITY_OF_CONCORDIA_ELECTRIC",
    "PRAIRIE_LAND_ELECTRIC",
    "NINNESCAH_RURAL_ELECTRIC",
)

_provider_health: Dict[str, Dict[str, Any]] = {}


def _health_mark(provider: str, ok: bool, error: Optional[str]) -> None:
    _provider_health[provider] = {"ts": time.time(), "ok": ok, "error": error}


def _health_should_skip(provider: str) -> bool:
    rec = _provider_health.get(provider)
    if not rec:
        return False
    age = time.time() - float(rec.get("ts", 0.0))
    if age > HEALTH_TTL_S:
        return False
    return not bool(rec.get("ok", True))


def _coerce_outage(d: Dict[str, Any]) -> Outage:
    lat = d.get("lat")
    lon = d.get("lon")
    if lat is None:
        lat = d.get("latitude")
    if lon is None:
        lon = d.get("longitude")

    dkm = d.get("distance_km")
    dm = d.get("distance_miles")
    if isinstance(dm, (int, float)):
        miles: Optional[float] = float(dm)
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
        outage_id=d.get("outage_id") or d.get("OUTAGE_ID") or d.get("id"),
        provider=d.get("provider") or "KUBRA",
        raw=d,
    )


def _run_with_timeout(provider: str, fn: Callable[[], PowerBlock], timeout_s: int) -> PowerBlock:
    meta_source = f"app.power_router.{provider.lower()}.timeout_wrapper"
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
        try:
            ex.shutdown(wait=False)
        except Exception:
            pass


def _normalize_result(provider: str, result: Dict[str, Any], meta: PowerMeta) -> PowerBlock:
    nearest_raw = result.get("nearest") or None
    outages_raw = result.get("outages") or []

    if isinstance(nearest_raw, dict):
        nearest_raw.setdefault("provider", provider)
    for item in outages_raw:
        if isinstance(item, dict):
            item.setdefault("provider", provider)

    nearest = _coerce_outage(nearest_raw) if isinstance(nearest_raw, dict) else None
    outages = [_coerce_outage(item) for item in outages_raw if isinstance(item, dict)]
    return PowerBlock(
        utility=provider,
        has_outage_nearby=bool(nearest),
        nearest=nearest,
        outages=outages,
        meta=meta,
    )


def _provider_error(provider: str, meta: PowerMeta, exc: Exception) -> PowerBlock:
    meta.ok = False
    meta.error = f"{type(exc).__name__}: {exc}"
    return PowerBlock(utility=provider, has_outage_nearby=False, nearest=None, outages=[], meta=meta)


def _call_oge(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.oge_kubra.fetch_oge_outages", ok=True, error=None)
    try:
        from app.providers.oge_kubra import fetch_oge_outages
        result = fetch_oge_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result("OGE", result, meta)
    except Exception as exc:
        return _provider_error("OGE", meta, exc)


def _call_pso(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.pso_kubra.fetch_pso_outages", ok=True, error=None)
    try:
        from app.providers.pso_kubra import fetch_pso_outages
        result = fetch_pso_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result("PSO", result, meta)
    except Exception as exc:
        return _provider_error("PSO", meta, exc)


def _call_evergy(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.evergy_kubra.fetch_evergy_outages", ok=True, error=None)
    try:
        from app.providers.evergy_kubra import fetch_evergy_outages
        result = fetch_evergy_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result("EVERGY", result, meta)
    except Exception as exc:
        return _provider_error("EVERGY", meta, exc)


def _call_oncor(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.oncor_kubra.fetch_oncor_outages", ok=True, error=None)
    try:
        from app.providers.oncor_kubra import fetch_oncor_outages
        result = fetch_oncor_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=0,
            drill_neighbor_depth=0,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result("ONCOR", result, meta)
    except Exception as exc:
        return _provider_error("ONCOR", meta, exc)


def _call_austin(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.austin_energy_kubra.fetch_austin_energy_outages", ok=True, error=None)
    try:
        from app.providers.austin_energy_kubra import fetch_austin_energy_outages
        result = fetch_austin_energy_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,
            fallback_radius_km=MAX_RADIUS_KM * 2.5,
            debug=False,
        )
        return _normalize_result("AUSTIN", result, meta)
    except Exception as exc:
        return _provider_error("AUSTIN", meta, exc)


def _call_pec(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.pec_kubra.fetch_pec_outages", ok=True, error=None)
    try:
        from app.providers.pec_kubra import fetch_pec_outages
        result = fetch_pec_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result("PEC", result, meta)
    except Exception as exc:
        return _provider_error("PEC", meta, exc)


def _call_aep(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.aep_kubra.fetch_aep_outages", ok=True, error=None)
    try:
        from app.providers.aep_kubra import fetch_aep_outages
        result = fetch_aep_outages(
            lat=lat,
            lon=lon,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result("AEP", result, meta)
    except Exception as exc:
        return _provider_error("AEP", meta, exc)


def _call_epe(lat: float, lon: float) -> PowerBlock:
    provider = "EL_PASO_ELECTRIC"
    meta = PowerMeta(source="app.providers.epe_starlit.fetch_epe_outages", ok=True, error=None)
    try:
        from app.providers.epe_starlit import fetch_epe_outages
        result = fetch_epe_outages(
            lat=lat,
            lon=lon,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result(provider, result, meta)
    except Exception as exc:
        return _provider_error(provider, meta, exc)


def _call_centerpoint(lat: float, lon: float) -> PowerBlock:
    meta = PowerMeta(source="app.providers.centerpoint_arcgis.fetch_centerpoint_outages", ok=True, error=None)
    try:
        from app.providers.centerpoint_arcgis import fetch_centerpoint_outages
        result = fetch_centerpoint_outages(
            lat=lat,
            lon=lon,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result("CENTERPOINT", result, meta)
    except Exception as exc:
        return _provider_error("CENTERPOINT", meta, exc)


def _call_city_of_concordia_electric(lat: float, lon: float) -> PowerBlock:
    provider = "CITY_OF_CONCORDIA_ELECTRIC"
    meta = PowerMeta(
        source="app.providers.concordia_electric_trpc.fetch_city_of_concordia_outages",
        ok=True,
        error=None,
    )
    try:
        from app.providers.concordia_electric_trpc import fetch_city_of_concordia_outages
        result = fetch_city_of_concordia_outages(
            lat=lat,
            lon=lon,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result(provider, result, meta)
    except Exception as exc:
        return _provider_error(provider, meta, exc)


def _call_prairie_land_electric(lat: float, lon: float) -> PowerBlock:
    provider = "PRAIRIE_LAND_ELECTRIC"
    meta = PowerMeta(
        source="app.providers.prairie_land_arcgis.fetch_prairie_land_outages",
        ok=True,
        error=None,
    )
    try:
        from app.providers.prairie_land_arcgis import fetch_prairie_land_outages
        result = fetch_prairie_land_outages(
            lat=lat,
            lon=lon,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result(provider, result, meta)
    except Exception as exc:
        return _provider_error(provider, meta, exc)


def _call_ninnescah_rural_electric(lat: float, lon: float) -> PowerBlock:
    provider = "NINNESCAH_RURAL_ELECTRIC"
    meta = PowerMeta(
        source="app.providers.ninnescah_webmap.fetch_ninnescah_outages",
        ok=True,
        error=None,
    )
    try:
        from app.providers.ninnescah_webmap import fetch_ninnescah_outages
        result = fetch_ninnescah_outages(
            lat=lat,
            lon=lon,
            max_radius_km=MAX_RADIUS_KM,
            debug=False,
        )
        return _normalize_result(provider, result, meta)
    except Exception as exc:
        return _provider_error(provider, meta, exc)


_call_map: Dict[str, Callable[[float, float], PowerBlock]] = {
    "OGE": _call_oge,
    "PSO": _call_pso,
    "EVERGY": _call_evergy,
    "ONCOR": _call_oncor,
    "AUSTIN": _call_austin,
    "PEC": _call_pec,
    "AEP": _call_aep,
    "CENTERPOINT": _call_centerpoint,
    "EPE": _call_epe,
    "EL_PASO_ELECTRIC": _call_epe,
    "CITY_OF_CONCORDIA_ELECTRIC": _call_city_of_concordia_electric,
    "PRAIRIE_LAND_ELECTRIC": _call_prairie_land_electric,
    "NINNESCAH_RURAL_ELECTRIC": _call_ninnescah_rural_electric,
}


def get_power_status(lat: float, lon: float, utility: Optional[str] = None) -> PowerBlock:
    utility_norm = (utility or "").strip().upper() or None

    if utility_norm and utility_norm not in SUPPORTED_PROVIDERS:
        meta = PowerMeta(source="app.power_router.unsupported", ok=True, error=None)
        return PowerBlock(
            utility=utility_norm,
            has_outage_nearby=False,
            nearest=None,
            outages=[],
            meta=meta,
        )

    if utility_norm in SUPPORTED_PROVIDERS:
        fn = _call_map[utility_norm]
        return _run_with_timeout(utility_norm, lambda: fn(lat, lon), DIRECT_TIMEOUT_S)

    attempts: List[PowerBlock] = []
    for key in PROBE_ORDER:
        if _health_should_skip(key):
            meta = PowerMeta(
                source="app.power_router.health_cache",
                ok=False,
                error=f"Skipped due to recent failure: {_provider_health.get(key, {}).get('error')}",
            )
            attempts.append(
                PowerBlock(utility=key, has_outage_nearby=False, nearest=None, outages=[], meta=meta)
            )
            continue

        fn = _call_map[key]
        res = _run_with_timeout(key, lambda fn=fn, key=key: fn(lat, lon), PROBE_TIMEOUT_S)
        attempts.append(res)
        if res.meta.ok and res.has_outage_nearby:
            return res

    any_ok = any(a.meta.ok for a in attempts)
    meta = PowerMeta(
        source="app.power_router.probe",
        ok=any_ok,
        error=None if any_ok else "All probes failed/timeout",
    )
    return PowerBlock(
        utility="UNKNOWN",
        has_outage_nearby=False,
        nearest=None,
        outages=[],
        meta=meta,
    )


def probe_power_status(lat: float, lon: float) -> Tuple[PowerBlock, List[PowerBlock]]:
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
        res = _run_with_timeout(key, lambda fn=fn, key=key: fn(lat, lon), PROBE_TIMEOUT_S)
        attempts.append(res)
        if res.meta.ok and res.has_outage_nearby:
            return res, attempts

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
