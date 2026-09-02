# scripts/check_sites_austin_outages.py
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from app.providers.austin_energy_kubra import fetch_austin_energy_outages


DEFAULT_SITES_PATH = Path("app/data/sites.json")
DEFAULT_RADIUS_KM = 16.1
DEFAULT_UTILITY = "AUSTIN"


def _coerce_sites(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, dict):
        if "sites" in data and isinstance(data["sites"], list):
            return data["sites"]
        out: List[Dict[str, Any]] = []
        for k, v in data.items():
            if isinstance(v, dict):
                vv = dict(v)
                vv.setdefault("site_id", vv.get("site_id") or k)
                vv.setdefault("name", vv.get("name") or k)
                out.append(vv)
        return out
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    raise ValueError("sites.json must be a dict keyed by site_id, a list of sites, or an object with a 'sites' list")


def load_sites(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(f"Could not find {path.resolve()}")
    data = json.loads(path.read_text(encoding="utf-8"))
    return _coerce_sites(data)


def site_lat_lon(site: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    lat = site.get("lat")
    lon = site.get("lon")
    if lat is None or lon is None:
        return None
    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None


def is_expected_no_outage_error(e: Exception) -> bool:
    msg = (str(e) or "").lower()
    needles = [
        "no outages",
        "no non-cluster outage records",
        "outside max_radius_km",
        "timeout",
        "could not discover entry zoom",
    ]
    return any(n in msg for n in needles)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sites", default=str(DEFAULT_SITES_PATH))
    p.add_argument("--utility", default=DEFAULT_UTILITY)
    p.add_argument("--radius-km", type=float, default=DEFAULT_RADIUS_KM)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--show-misses", action="store_true")
    p.add_argument("--debug", action="store_true")
    p.add_argument("--neighbor-depth", type=int, default=0)
    p.add_argument("--drill-neighbor-depth", type=int, default=1)
    p.add_argument("--max-zoom", type=int, default=12)
    args = p.parse_args()

    sites = load_sites(Path(args.sites))
    utility = (args.utility or "").strip().upper()

    filtered = [
        s for s in sites
        if s.get("enabled", True)
        and (str(s.get("utility") or "").strip().upper() == utility)
    ]
    if args.limit and args.limit > 0:
        filtered = filtered[: args.limit]

    print(f"Checking {len(filtered)} sites for {utility} outages within {args.radius_km:.1f} km...")

    hits = misses = skipped = errs = 0

    for s in filtered:
        sid = s.get("site_id") or s.get("name") or "UNKNOWN_SITE"
        loc = site_lat_lon(s)
        if not loc:
            skipped += 1
            if args.show_misses:
                print(f"[SKIP] {sid}: missing/invalid lat/lon")
            continue

        lat, lon = loc
        try:
            res = fetch_austin_energy_outages(
                lat,
                lon,
                max_radius_km=float(args.radius_km),
                max_zoom=int(args.max_zoom),
                neighbor_depth=int(args.neighbor_depth),
                drill_neighbor_depth=int(args.drill_neighbor_depth),
                debug=bool(args.debug),
            )
            outages = res.get("outages") or []
            nearest = res.get("nearest")

            if nearest or outages:
                if not nearest and outages:
                    nearest = outages[0]
                hits += 1
                cust = nearest.get("customers_out") or nearest.get("n_out")
                dist = nearest.get("distance_km")
                oid = nearest.get("id")
                print(
                    f"[HIT ] {sid} ({lat:.5f},{lon:.5f}) -> "
                    f"{len(outages)} outage(s) within {args.radius_km:.1f} km | "
                    f"nearest={None if dist is None else float(dist):.3f} km customers={cust} id={oid}"
                )
            else:
                misses += 1
                if args.show_misses:
                    print(f"[MISS] {sid}: no outages within {args.radius_km:.1f} km")

        except KeyboardInterrupt:
            print("\nInterrupted.")
            break
        except Exception as e:
            if is_expected_no_outage_error(e):
                misses += 1
                if args.show_misses:
                    print(f"[MISS] {sid}: {e}")
            else:
                errs += 1
                if args.show_misses:
                    print(f"[ERR ] {sid}: {e}")

    print("\n=== SUMMARY ===")
    print(f"Sites checked: {len(filtered)}")
    print(f"Hits: {hits}")
    print(f"Misses: {misses}")
    print(f"Skipped (no coords): {skipped}")
    print(f"Errors: {errs}")


if __name__ == "__main__":
    main()
