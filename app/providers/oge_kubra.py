"""
OG&E KUBRA Storm Center outage integration
Auto-discovers tile scheme (layer, zoom, qkh sharding) and fetches outages near a point.
Adds drill-down granularity by expanding cluster tiles to higher zooms, capped at max zoom 12.
"""

import math
import time
import requests
import mercantile
import polyline
from typing import List, Dict, Any, Optional, Tuple, Set, Callable

# -------------------------- OG&E IDENTIFIERS --------------------------

BASE_URL = "https://kubra.io/"
INSTANCE_ID = "dc85f79f-59f9-4e9e-9557-b3a9bee7e0ce"
VIEW_ID = "8fe9d356-96bc-41f1-b353-6720eb408936"

# -------------------------- PROBING CONSTANTS --------------------------

PROBE_ZOOMS = [11, 12, 13, 14]

PROBE_POINTS = [
    (35.4676, -97.5164),  # OKC
    (36.1540, -95.9928),  # Tulsa-ish
    (35.2226, -97.4395),  # Norman-ish
    (35.5150, -97.5600),  # NW OKC
    (35.3733, -96.9253),  # Shawnee-ish
]

# initial neighborhood search around the site tile
NEIGHBOR_DEPTH = 1

# ✅ capped granularity
DEFAULT_MAX_ZOOM = 12

# when drilling into a cluster at higher zoom, fetch a neighborhood too
DEFAULT_DRILL_NEIGHBOR_DEPTH = 1


class OgeKubraClient:
    def __init__(self, debug: bool = False) -> None:
        self.session = requests.Session()
        self.debug = debug

        state = self._get_current_state()
        self.cluster_data_path = state["data"]["cluster_interval_generation_data"]
        self.deployment_id = state["stormcenterDeploymentId"]

        config = self._get_configuration()
        self.cluster_layers = self._extract_cluster_layers(config)

        (
            self.layer_name,
            self.entry_zoom,
            self._qkh_func,
            self._url_builder,
        ) = self._autodiscover_tile_scheme()

        if self.debug:
            print("AUTO-DISCOVERED SETTINGS")
            print("  layer_name:", self.layer_name)
            print("  entry_zoom:", self.entry_zoom)
            print("  cluster_data_path:", self.cluster_data_path)

    # -------------------------- PUBLIC --------------------------

    def fetch_outages_for_point(
        self,
        lat: float,
        lon: float,
        max_radius_km: float = 50.0,
        max_zoom: int = DEFAULT_MAX_ZOOM,
        neighbor_depth: int = NEIGHBOR_DEPTH,
        drill_neighbor_depth: int = DEFAULT_DRILL_NEIGHBOR_DEPTH,
    ) -> Dict[str, Any]:
        """
        Returns:
          {
            "nearest": <nearest outage dict>,
            "outages": [<outage dict>, ...]   # sorted by distance
          }

        Granularity behavior:
          - Start at entry_zoom (auto-discovered; OG&E currently = 11)
          - Fetch site tile + neighbors
          - If any features are clusters, drill into them:
              zoom+1 ... up to max_zoom (✅ default 12)
              fetching child tile neighborhoods at each level
        """

        if max_zoom < self.entry_zoom:
            raise ValueError(f"max_zoom ({max_zoom}) must be >= entry_zoom ({self.entry_zoom}).")

        # 1) Base quadkey at entry zoom
        base_tile = mercantile.tile(lon, lat, self.entry_zoom)
        base_q = mercantile.quadkey(base_tile)

        if self.debug:
            print(f"Base tile: {base_tile}")
            print(f"Base quadkey (z={self.entry_zoom}): {base_q}")

        # 2) Initial quadkey neighborhood around site
        seeds = self._expand_quadkeys(base_q, depth=neighbor_depth)

        outages_by_id: Dict[str, Dict[str, Any]] = {}
        seen_urls: Set[str] = set()
        seen_quadkeys: Set[Tuple[int, str]] = set()

        # 3) Crawl: seed tiles + drill clusters
        cluster_queue: List[Tuple[int, Dict[str, Any]]] = []  # (zoom, raw feature)

        for q in seeds:
            raw_features = self._fetch_tile_features(q, self.entry_zoom, seen_urls, seen_quadkeys)
            for feat in raw_features:
                if self._is_cluster(feat):
                    cluster_queue.append((self.entry_zoom, feat))
                else:
                    o = self._normalize_outage(feat)
                    if o:
                        outages_by_id[o["id"]] = o

        # 4) Drill clusters for granularity (✅ stops at max_zoom=12 by default)
        while cluster_queue:
            z, cluster_feat = cluster_queue.pop(0)
            if z >= max_zoom:
                o = self._normalize_outage(cluster_feat)
                if o:
                    outages_by_id[o["id"]] = o
                continue

            loc = self._extract_location(cluster_feat)
            if not loc:
                o = self._normalize_outage(cluster_feat)
                if o:
                    outages_by_id[o["id"]] = o
                continue

            clat, clon = loc
            child_z = z + 1
            child_tile = mercantile.tile(clon, clat, child_z)
            child_q = mercantile.quadkey(child_tile)

            # Fetch neighborhood around child tile to capture multiple outages
            child_keys = self._expand_quadkeys(child_q, depth=drill_neighbor_depth)

            if self.debug:
                print(f"DRILL cluster z={z} -> z={child_z}, center_q={child_q}, neighborhood={len(child_keys)} tiles")

            for cq in child_keys:
                raw_features = self._fetch_tile_features(cq, child_z, seen_urls, seen_quadkeys)
                for feat in raw_features:
                    if self._is_cluster(feat):
                        cluster_queue.append((child_z, feat))
                    else:
                        o = self._normalize_outage(feat)
                        if o:
                            outages_by_id[o["id"]] = o

        if not outages_by_id:
            # Common during “no outages” or transition states; return clean empty.
            return {"nearest": None, "outages": []}

        # 5) Attach distance and sort
        enriched = []
        for o in outages_by_id.values():
            d = haversine_km(lat, lon, o["latitude"], o["longitude"])
            oo = dict(o)
            oo["distance_km"] = d
            enriched.append(oo)

        enriched.sort(key=lambda x: x["distance_km"])

        # 6) nearest within radius
        nearest = enriched[0] if enriched else None
        if nearest and nearest["distance_km"] > max_radius_km:
            raise RuntimeError(
                f"Outages found ({len(enriched)}), but nearest is {nearest['distance_km']:.2f} km "
                f"which is outside max_radius_km={max_radius_km}."
            )

        return {
            "nearest": nearest,
            "outages": [o for o in enriched if o["distance_km"] <= max_radius_km],
        }

    # -------------------------- TILE FETCH --------------------------

    def _fetch_tile_features(
        self,
        quadkey: str,
        zoom: int,
        seen_urls: Set[str],
        seen_quadkeys: Set[Tuple[int, str]],
    ) -> List[Dict[str, Any]]:

        key = (zoom, quadkey)
        if key in seen_quadkeys:
            return []
        seen_quadkeys.add(key)

        qkh = self._qkh_func(quadkey)
        base = self.cluster_data_path.format(qkh=qkh)
        url = self._url_builder(base, self.layer_name, quadkey)

        if url in seen_urls:
            return []
        seen_urls.add(url)

        if self.debug:
            print(f"FETCH z={zoom} q={quadkey} -> {url}")

        try:
            r = self.session.get(url, timeout=5)
        except requests.RequestException:
            return []

        if r.status_code != 200:
            return []

        try:
            tile = r.json()
        except ValueError:
            return []

        feats = tile.get("file_data", [])
        if self.debug:
            print(f"  -> features: {len(feats)}")
        return feats

    # -------------------------- DISCOVERY --------------------------

    def _autodiscover_tile_scheme(self):
        def qkh_last3_rev(q: str) -> str: return q[-3:][::-1]
        def qkh_last3(q: str) -> str: return q[-3:]
        def qkh_first3(q: str) -> str: return q[:3]
        def qkh_first3_rev(q: str) -> str: return q[:3][::-1]
        def qkh_last4_rev(q: str) -> str: return q[-4:][::-1]

        qkh_strats: List[Tuple[str, Callable[[str], str]]] = [
            ("last3_rev", qkh_last3_rev),
            ("last3", qkh_last3),
            ("first3", qkh_first3),
            ("first3_rev", qkh_first3_rev),
            ("last4_rev", qkh_last4_rev),
        ]

        def url_simple(base: str, layer: str, q: str) -> str:
            return f"{BASE_URL}{base}/public/{layer}/{q}.json"

        def url_prefix2(base: str, layer: str, q: str) -> str:
            return f"{BASE_URL}{base}/public/{layer}/{q[:2]}/{q}.json"

        layouts: List[Tuple[str, Callable[[str, str, str], str]]] = [
            ("simple", url_simple),
            ("prefix2", url_prefix2),
        ]

        probe_keys: List[Tuple[str, int]] = []
        for plat, plon in PROBE_POINTS:
            for z in PROBE_ZOOMS:
                t = mercantile.tile(plon, plat, z)
                probe_keys.append((mercantile.quadkey(t), z))

        for layer in self.cluster_layers:
            layer_id = layer["id"]
            for q, z in probe_keys:
                for qkh_name, qkh_func in qkh_strats:
                    for layout_name, layout in layouts:
                        base = self.cluster_data_path.format(qkh=qkh_func(q))
                        url = layout(base, layer_id, q)

                        if self.debug:
                            print(f"PROBE layer={layer_id} zoom={z} qkh={qkh_name} layout={layout_name}")
                            print("  ", url)

                        try:
                            r = self.session.get(url, timeout=5)
                        except requests.RequestException:
                            continue

                        if r.status_code != 200:
                            continue

                        try:
                            js = r.json()
                        except ValueError:
                            continue

                        if isinstance(js, dict) and "file_data" in js:
                            if self.debug:
                                print("PROBE SUCCESS:", url)
                            return layer_id, z, qkh_func, layout

        raise RuntimeError("Failed to auto-discover OG&E tile scheme.")

    # -------------------------- NORMALIZATION / GEOMETRY --------------------------

    @staticmethod
    def _is_cluster(feature: Dict[str, Any]) -> bool:
        desc = feature.get("desc", {}) or {}
        return bool(desc.get("cluster"))

    def _extract_location(self, feature: Dict[str, Any]) -> Optional[Tuple[float, float]]:
        geom = feature.get("geom", {}) or {}
        pts = geom.get("p", []) or []
        if not pts:
            return None
        try:
            return polyline.decode(pts[0])[0]
        except Exception:
            return None

    def _normalize_outage(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        desc = feature.get("desc", {}) or {}
        loc = self._extract_location(feature)
        if not loc:
            return None

        lat, lon = loc[0], loc[1]

        # Prefer a real incident id if present
        inc_id = desc.get("inc_id")

        # Preserve existing id semantics for compatibility (falls back to lat/lon + start_time)
        outage_id = inc_id if inc_id else f"{loc}-{desc.get('start_time', 'unknown')}"

        # Stable identity for testing/deduping (does NOT replace `id`)
        canonical_id = inc_id if inc_id else f"{round(lat, 5)}|{round(lon, 5)}"

        cause = desc.get("cause")
        if isinstance(cause, dict):
            cause = cause.get("EN-US") or next(iter(cause.values()), None)

        customers_out = None
        cust_a = desc.get("cust_a")
        if isinstance(cust_a, dict):
            customers_out = cust_a.get("val")

        return {
            "id": outage_id,
            "canonical_id": canonical_id,
            "cluster": bool(desc.get("cluster")),
            "customers_out": customers_out,
            "n_out": desc.get("n_out"),
            "etr": desc.get("etr"),
            "etr_confidence": desc.get("etr_confidence"),
            "cause": cause,
            "comments": desc.get("comments"),
            "crew_status": desc.get("crew_status"),
            "start_time": desc.get("start_time"),
            "latitude": lat,
            "longitude": lon,
        }

# -------------------------- QUADKEY HELPERS --------------------------

    def _expand_quadkeys(self, base_quadkey: str, depth: int) -> List[str]:
        t = mercantile.quadkey_to_tile(base_quadkey)
        keys = []
        for dx in range(-depth, depth + 1):
            for dy in range(-depth, depth + 1):
                keys.append(
                    mercantile.quadkey(mercantile.Tile(t.x + dx, t.y + dy, t.z))
                )
        return list(set(keys))

    # -------------------------- METADATA --------------------------

    def _get_current_state(self) -> Dict[str, Any]:
        url = (
            f"{BASE_URL}stormcenter/api/v1/stormcenters/"
            f"{INSTANCE_ID}/views/{VIEW_ID}/currentState?preview=false"
        )
        r = self.session.get(url, timeout=5)
        r.raise_for_status()
        return r.json()

    def _get_configuration(self) -> Dict[str, Any]:
        url = (
            f"{BASE_URL}stormcenter/api/v1/stormcenters/"
            f"{INSTANCE_ID}/views/{VIEW_ID}/configuration/{self.deployment_id}?preview=false"
        )
        r = self.session.get(url, timeout=5)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _extract_cluster_layers(config: Dict[str, Any]) -> List[Dict[str, Any]]:
        layers = config["config"]["layers"]["data"]["interval_generation_data"]
        return [l for l in layers if str(l.get("type", "")).startswith("CLUSTER_LAYER")]


# -------------------------- UTIL --------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# -------------------------- CLIENT CACHE --------------------------

_CLIENT: Optional["OgeKubraClient"] = None
_CLIENT_TS: float = 0.0
_CLIENT_TTL_S: int = 60  # refresh discovery once per minute


def _get_client(debug: bool = False) -> "OgeKubraClient":
    global _CLIENT, _CLIENT_TS
    now = time.time()
    if _CLIENT is None or (now - _CLIENT_TS) > _CLIENT_TTL_S:
        _CLIENT = OgeKubraClient(debug=debug)
        _CLIENT_TS = now
    else:
        if debug:
            _CLIENT.debug = True
    return _CLIENT

# -------------------------- PUBLIC WRAPPER --------------------------

def fetch_oge_outages(
    lat: float,
    lon: float,
    max_radius_km: float = 50.0,
    max_zoom: int = DEFAULT_MAX_ZOOM,
    neighbor_depth: int = NEIGHBOR_DEPTH,
    drill_neighbor_depth: int = DEFAULT_DRILL_NEIGHBOR_DEPTH,
    debug: bool = False,
) -> Dict[str, Any]:
    client = _get_client(debug=debug)
    return client.fetch_outages_for_point(
        lat,
        lon,
        max_radius_km=max_radius_km,
        max_zoom=max_zoom,
        neighbor_depth=neighbor_depth,
        drill_neighbor_depth=drill_neighbor_depth,
    )


# -------------------------- SELF TEST --------------------------

if __name__ == "__main__":
    print("Testing OG&E outage fetch (debug on, max_zoom=12)...")
    try:
        res = fetch_oge_outages(
            35.4676, -97.5164,
            max_radius_km=50.0,
            max_zoom=12,
            neighbor_depth=1,
            drill_neighbor_depth=1,
            debug=True
        )
        print("NEAREST:", res["nearest"])
        print("COUNT (within radius):", len(res["outages"]))
        for o in res["outages"][:5]:
            print(" -", o["id"], "cust_out=", o["customers_out"], "km=", round(o["distance_km"], 3))
    except Exception as e:
        print("ERROR:", e)
