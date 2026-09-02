import json
from pathlib import Path

from app.providers.oncor_kubra import fetch_oncor_outages

ROOT = Path(__file__).resolve().parents[1]
SITES_PATH = ROOT / "app" / "data" / "sites.json"

def main():
    sites = json.loads(SITES_PATH.read_text(encoding="utf-8"))

    print(f"Loaded {len(sites)} sites from {SITES_PATH}\n")
    print("Testing Oncor outages...\n")

    hit_count = 0
    checked = 0

    for site_id, s in sites.items():
        # Optional: only test TX (fast + relevant)
        if s.get("state") != "TX":
            continue

        lat = s.get("lat")
        lon = s.get("lon")
        if lat is None or lon is None:
            continue

        checked += 1
        try:
            result = fetch_oncor_outages(
                float(lat),
                float(lon),
                max_radius_km=50,
                max_zoom=12,
                neighbor_depth=0,
                drill_neighbor_depth=1,
                debug=False,
            )
        except Exception as e:
            print(f"{site_id}: ERROR {e}")
            continue

        if result.get("nearest"):
            hit_count += 1
            n = result["nearest"]
            miles = float(n["distance_km"]) * 0.621371 if n.get("distance_km") is not None else None
            print(
                f"OUTAGE | {site_id} | "
                f"cust={n.get('customers_out')} | "
                f"crew={n.get('crew_status')} | "
                f"etr={n.get('etr')} | "
                f"dist_mi={None if miles is None else round(miles, 2)}"
            )

    print(f"\nChecked TX sites: {checked}")
    print(f"Outage hits: {hit_count}")

if __name__ == "__main__":
    main()
