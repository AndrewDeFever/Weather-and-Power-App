"""
Check OG&E outages near sites.

Defaults:
- reads sites from: app/data/sites.json
- filters to utility == "OGE"
- prints ONLY sites with an outage within radius (HITs)

Run:
  python -m scripts.check_sites_oge_outages
Optional:
  python -m scripts.check_sites_oge_outages --radius-km 10
  python -m scripts.check_sites_oge_outages --utility OGE --show-misses
  python -m scripts.check_sites_oge_outages --limit 50
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

from app.providers.oge_kubra import OgeKubraClient


DEFAULT_SITES_PATH = Path("app/data/sites.json")
DEFAULT_RADIUS_KM = 16.0
DEFAULT_UTILITY = "OGE"


def _coerce_sites(data: Any) -> List[Dict[str, Any]]:
    """
    Supports:
      1) dict keyed by site_id (your current format)
      2) list of sites
      3) {"sites": [...]} wrapper
    """
    if isinstance(data, dict):
        if "sites" in data and isinstance(data["sites"], list):
            return data["sites"]
        # dict keyed by site_id
        # keep the key around if it's not in the object
        out = []
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
    msg = str(e) or ""
    needles = [
        "Tiles fetched successfully, but no non-cluster outage records were produced",
        "outside max_radius_km",
    ]
    return any(n in msg for n in needles)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--sites", default=str(DEFAULT_SITES_PATH), help="Path to sites.json (default: app/data/sites.json)")
    p.add_argument("--utility", default=DEFAULT_UTILITY, help="Utility filter (default: OGE)")
    p.add_argument("--radius-km", type=float, default=DEFAULT_RADIUS_KM, help="Search radius in km (default: 16)")
    p.add_argument("--limit", type=int, default=0, help="Limit number of sites processed (0 = no limit)")
    p.add_argument("--show-misses", action="store_true", help="Also print MISS/ERR lines (default prints only HITs)")
    p.add_argument("--debug", action="store_true", help="Enable provider debug logging")
    args = p.parse_args()

    sites_path = Path(args.sites)
    sites = load_sites(sites_path)

    # Filter: enabled + utility match
    utility = (args.utility or "").strip().upper()
    filtered = [
        s for s in sites
        if s.get("enabled", True)
        and (str(s.get("utility") or "").strip().upper() == utility)
    ]

    if args.limit and args.limit > 0:
        filtered = filtered[: args.limit]

    print(f"Checking {len(filtered)} sites for {utility} outages within {args.radius_km:.1f} km...")

    client = OgeKubraClient(debug=args.debug)

    hits = 0
    misses = 0
    skipped = 0
    errs = 0

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
            res = client.fetch_outages_for_point(
                lat,
                lon,
                max_radius_km=args.radius_km,
                # keep defaults from provider: entry_zoom autodiscovery and max zoom cap
            )
            outages = res.get("outages") or []
            if outages:
                hits += 1
                nearest = outages[0]
                cust = nearest.get("customers_out") or nearest.get("n_out")
                dist = nearest.get("distance_km")
                oid = nearest.get("id")
                print(
                    f"[HIT ] {sid} ({lat:.5f},{lon:.5f}) -> "
                    f"{len(outages)} outage(s) within {args.radius_km:.1f} km | "
                    f"nearest={dist:.3f} km customers={cust} id={oid}"
                )
            else:
                misses += 1
                if args.show_misses:
                    print(f"[MISS] {sid}: no outages within {args.radius_km:.1f} km")

        except KeyboardInterrupt:
            print("\nInterrupted by user.")
            break
        except Exception as e:
            # Treat common OG&E/Kubra "no data" situations as a MISS, not a hard error
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
