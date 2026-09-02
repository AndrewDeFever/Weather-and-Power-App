import random
import time
import math
from dataclasses import dataclass
from typing import Dict, Any, List, Tuple, Optional

import mercantile

from app.providers.oge_kubra import fetch_oge_outages


# --------------------------- CONFIG ---------------------------

# Area seed points to harvest outages (OKC metro + a few around it)
SEED_POINTS = [
    (35.4676, -97.5164),  # OKC
    (35.5150, -97.5600),  # NW OKC
    (35.4050, -97.5400),  # SW OKC
    (35.2226, -97.4395),  # Norman
    (35.3733, -96.9253),  # Shawnee-ish
]

# How far to roam around each seed point to discover outages (in tiles at entry zoom)
NEIGHBOR_DEPTH_HARVEST = 2  # 5x5 tiles around each seed

# Max zoom for provider (your desired cap)
MAX_ZOOM = 12

# Test generation
TESTS_PER_OUTAGE = 3           # number of near-point tests per outage
MAX_OUTAGES_TO_TEST = 50       # cap to keep runtime reasonable
NEAR_RADIUS_METERS = 750       # query points within this distance of outage point

# Scoring rules
TOP_N_ACCEPTABLE = 1           # require it to be nearest; set to 3 if you want leniency

# Reliability checks
REPEAT_CALLS = 2               # run same query multiple times and ensure stable decision
SLEEP_BETWEEN_CALLS_SEC = 0.25 # reduce chance of transient differences

# Request knobs (match production)
MAX_RADIUS_KM = 50.0
NEIGHBOR_DEPTH = 1
DRILL_NEIGHBOR_DEPTH = 1

# Randomness
RANDOM_SEED = 42


# --------------------------- UTIL ---------------------------

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def jitter_point(lat: float, lon: float, max_meters: float) -> Tuple[float, float]:
    """
    Random point within ~max_meters of (lat, lon).
    Uses a simple equirectangular approximation; good enough for sub-km jitter.
    """
    # random polar offset
    r = random.random() * max_meters
    theta = random.random() * 2 * math.pi

    dlat = (r * math.cos(theta)) / 111320.0
    dlon = (r * math.sin(theta)) / (111320.0 * math.cos(math.radians(lat)))
    return lat + dlat, lon + dlon


@dataclass
class OutagePoint:
    outage_id: str
    lat: float
    lon: float


# --------------------------- HARVEST ---------------------------

def harvest_outages() -> List[OutagePoint]:
    """
    Calls fetch_oge_outages around seed points, aggregates unique outages.
    This is our 'snapshot' of known outages for the reliability test.
    """
    seen: Dict[str, OutagePoint] = {}

    for (lat, lon) in SEED_POINTS:
        # Expand tiles around the seed by moving the query point slightly per tile.
        # We'll do a grid of query points in ~tile-sized steps using mercantile tiles.
        # Entry zoom is discovered internally by your provider, but we can approximate
        # by using multiple nearby points.
        seed_tile = mercantile.tile(lon, lat, 11)  # OG&E entry zoom known today; used only for harvest grid
        for dx in range(-NEIGHBOR_DEPTH_HARVEST, NEIGHBOR_DEPTH_HARVEST + 1):
            for dy in range(-NEIGHBOR_DEPTH_HARVEST, NEIGHBOR_DEPTH_HARVEST + 1):
                t = mercantile.Tile(seed_tile.x + dx, seed_tile.y + dy, seed_tile.z)
                b = mercantile.bounds(t)
                mid_lat = (b.north + b.south) / 2
                mid_lon = (b.east + b.west) / 2

                try:
                    res = fetch_oge_outages(
                        mid_lat,
                        mid_lon,
                        max_radius_km=MAX_RADIUS_KM,
                        max_zoom=MAX_ZOOM,
                        neighbor_depth=NEIGHBOR_DEPTH,
                        drill_neighbor_depth=DRILL_NEIGHBOR_DEPTH,
                        debug=False,
                    )
                except Exception:
                    continue

                for o in res.get("outages", [])[:200]:
                    oid = o.get("id")
                    if not oid:
                        continue
                    if oid not in seen:
                        seen[oid] = OutagePoint(
                            outage_id=oid,
                            lat=o["latitude"],
                            lon=o["longitude"],
                        )

                if len(seen) >= MAX_OUTAGES_TO_TEST:
                    break
            if len(seen) >= MAX_OUTAGES_TO_TEST:
                break
        if len(seen) >= MAX_OUTAGES_TO_TEST:
            break

    return list(seen.values())


# --------------------------- ASSERTIONS ---------------------------

def rank_of_outage(outages: List[Dict[str, Any]], outage_id: str) -> Optional[int]:
    """
    Returns 1-based rank of outage_id in outages list (assumes already distance-sorted),
    or None if not present.
    """
    for i, o in enumerate(outages, start=1):
        if o.get("id") == outage_id:
            return i
    return None


def run_tests(outage_points: List[OutagePoint]) -> Dict[str, Any]:
    """
    Generates tests around known outage points and validates the provider
    returns that outage as nearest (or within top N).
    """
    total = 0
    pass_count = 0
    topn_pass = 0
    stable_pass = 0
    failures: List[Dict[str, Any]] = []

    for op in outage_points[:MAX_OUTAGES_TO_TEST]:
        for _ in range(TESTS_PER_OUTAGE):
            total += 1

            qlat, qlon = jitter_point(op.lat, op.lon, NEAR_RADIUS_METERS)

            # Repeated calls to check stability/determinism
            nearest_ids = []
            last_res = None
            err = None

            for _rc in range(REPEAT_CALLS):
                try:
                    last_res = fetch_oge_outages(
                        qlat,
                        qlon,
                        max_radius_km=MAX_RADIUS_KM,
                        max_zoom=MAX_ZOOM,
                        neighbor_depth=NEIGHBOR_DEPTH,
                        drill_neighbor_depth=DRILL_NEIGHBOR_DEPTH,
                        debug=False,
                    )
                    nearest = (last_res.get("nearest") or {})
                    nearest_ids.append(nearest.get("id"))
                except Exception as e:
                    err = str(e)
                    nearest_ids.append(None)

                time.sleep(SLEEP_BETWEEN_CALLS_SEC)

            # If any call errored, count as failure (reliability)
            if err is not None or last_res is None:
                failures.append({
                    "type": "error",
                    "expected_id": op.outage_id,
                    "query": (qlat, qlon),
                    "error": err,
                    "nearest_ids": nearest_ids,
                })
                continue

            outages = last_res.get("outages", [])
            expected_rank = rank_of_outage(outages, op.outage_id)

            # Primary pass condition: expected outage is nearest (rank 1)
            if expected_rank == 1:
                pass_count += 1

            # Secondary pass: within top N
            if expected_rank is not None and expected_rank <= TOP_N_ACCEPTABLE:
                topn_pass += 1
            else:
                failures.append({
                    "type": "mismatch",
                    "expected_id": op.outage_id,
                    "query": (qlat, qlon),
                    "expected_rank": expected_rank,
                    "nearest_id": (last_res.get("nearest") or {}).get("id"),
                    "nearest_distance_km": (last_res.get("nearest") or {}).get("distance_km"),
                    "outage_count": len(outages),
                })

            # Stability: repeated calls returned same nearest ID
            if len(nearest_ids) == REPEAT_CALLS and len(set(nearest_ids)) == 1 and nearest_ids[0] is not None:
                stable_pass += 1

    return {
        "tests_total": total,
        "strict_pass_nearest_rate": (pass_count / total) if total else 0.0,
        "topn_pass_rate": (topn_pass / total) if total else 0.0,
        "stability_rate": (stable_pass / total) if total else 0.0,
        "failures_sample": failures[:10],
        "failures_count": len(failures),
    }


def main():
    random.seed(RANDOM_SEED)

    print("Harvesting outage snapshot...")
    outages = harvest_outages()
    print(f"Harvested {len(outages)} unique outages for testing.")

    if not outages:
        print("No outages harvested. Either there are no outages right now, or seed region did not hit them.")
        return

    print("Running reliability tests...")
    results = run_tests(outages)

    print("\n=== OG&E KUBRA RELIABILITY REPORT ===")
    print(f"Total tests: {results['tests_total']}")
    print(f"Strict nearest pass rate: {results['strict_pass_nearest_rate']:.2%}")
    print(f"Top-{TOP_N_ACCEPTABLE} pass rate: {results['topn_pass_rate']:.2%}")
    print(f"Stability rate (repeat calls): {results['stability_rate']:.2%}")
    print(f"Failures: {results['failures_count']}")

    if results["failures_sample"]:
        print("\nSample failures (first 10):")
        for f in results["failures_sample"]:
            print(f)

    print("\nDone.")


if __name__ == "__main__":
    main()
