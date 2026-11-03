from fastapi.testclient import TestClient

from app import api as api_mod


DEMO_SITE_ID = "DEMO_TUL_01"


def test_api_status_returns_json_and_is_resilient(monkeypatch):
    # Avoid network calls in CI: stub weather + power.
    def fake_weather(lat: float, lon: float):
        return api_mod.empty_weather()

    class DummyPower:
        def model_dump(self):
            return api_mod.empty_power("PSO", error="", ok=True)

    def fake_power(lat: float, lon: float, utility=None):
        return DummyPower()

    monkeypatch.setattr(api_mod, "fetch_weather", fake_weather)
    monkeypatch.setattr(api_mod, "get_power_status", fake_power)

    client = TestClient(api_mod.app)

    r = client.get(f"/api/status?q={DEMO_SITE_ID}")
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith("application/json")
    data = r.json()

    # Backward-compatible shape (do not remove fields)
    for k in ("query", "resolved", "provider", "weather", "power", "probe"):
        assert k in data


def test_api_status_wraps_provider_exceptions(monkeypatch):
    # Force a provider failure and ensure endpoint still returns JSON with meta.error.
    def boom_weather(lat: float, lon: float):
        raise RuntimeError("weather explode")

    def boom_power(lat: float, lon: float, utility=None):
        raise RuntimeError("power explode")

    monkeypatch.setattr(api_mod, "fetch_weather", boom_weather)
    monkeypatch.setattr(api_mod, "get_power_status", boom_power)

    client = TestClient(api_mod.app)
    r = client.get(f"/api/status?q={DEMO_SITE_ID}")
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith("application/json")
    data = r.json()
    assert "weather" in data
    assert "power" in data
    # power.meta.error must exist on provider failure
    assert isinstance(data["power"], dict)
    assert isinstance(data["power"].get("meta"), dict)
    assert data["power"]["meta"].get("error")


def test_ndfd_layer_allowlist_is_explicit():
    from app.api import NDFD_ALLOWED_LAYERS

    assert NDFD_ALLOWED_LAYERS["weather"] == "ndfd.conus.wx"
    assert NDFD_ALLOWED_LAYERS["temperature"] == "ndfd.conus.t"
    assert NDFD_ALLOWED_LAYERS["pop12"] == "ndfd.conus.pop12"
    assert NDFD_ALLOWED_LAYERS["wind"] == "ndfd.conus.windspd"
    assert NDFD_ALLOWED_LAYERS["hazards"] == "ndfd.conus.wwa"


def test_public_weather_defaults_to_live_fetch():
    assert api_mod.WEATHER_CACHE_TTL_S == 0


def test_meteocon_allowlist_is_bounded():
    assert "clear-day" in api_mod.METEOCON_ALLOWED_SLUGS
    assert "thunderstorms-day-rain" in api_mod.METEOCON_ALLOWED_SLUGS


def test_ndfd_times_parser_extracts_vtit():
    import xml.etree.ElementTree as ET

    root = ET.fromstring(
        """
        <WMS_Capabilities>
          <Capability>
            <Layer>
              <Layer>
                <Name>ndfd.conus.wx</Name>
                <Dimension name="VTIT">2026-11-02T18:00:00Z,2026-11-02T21:00:00Z</Dimension>
              </Layer>
            </Layer>
          </Capability>
        </WMS_Capabilities>
        """
    )
    parsed = api_mod._extract_ndfd_times_by_layer(root)
    assert parsed["ndfd.conus.wx"] == [
        "2026-11-02T18:00:00Z",
        "2026-11-02T21:00:00Z",
    ]


def test_parse_ndfd_hazard_series_aligns_time_layout():
    import xml.etree.ElementTree as ET

    root = ET.fromstring(
        """
        <dwml>
          <data>
            <time-layout>
              <layout-key>haz-1</layout-key>
              <start-valid-time>2026-11-03T03:00:00-06:00</start-valid-time>
              <start-valid-time>2026-11-03T04:00:00-06:00</start-valid-time>
            </time-layout>
            <parameters>
              <hazards time-layout="haz-1">
                <hazard-conditions>
                  <hazard hazardCode="TO.A" phenomena="Tornado" significance="Watch" hazardType="long duration">
                    <hazardTextURL>https://example.test/tornado</hazardTextURL>
                  </hazard>
                </hazard-conditions>
                <hazard-conditions/>
              </hazards>
            </parameters>
          </data>
        </dwml>
        """
    )
    periods = api_mod._parse_ndfd_hazard_series(root)
    assert periods[0]["start_time"] == "2026-11-03T03:00:00-06:00"
    assert periods[0]["end_time"] == "2026-11-03T04:00:00-06:00"
    assert periods[0]["hazards"][0]["code"] == "TO.A"
    assert periods[0]["hazards"][0]["significance"] == "Watch"
    assert periods[1]["hazards"] == []


def test_ndfd_hazard_cache_key_rounds_point():
    assert api_mod._ndfd_hazard_cache_key(36.1541, -95.9928) == "36.15,-95.99"


def test_weather_payload_exposes_selected_time_fields():
    weather = api_mod.empty_weather()
    assert "relative_humidity_pct" in weather
    assert "hourly_periods" in weather



def test_temperature_season_mapping_matches_noaa_viewer_calendar():
    assert api_mod._temperature_season_code_for_month(1) == -1
    assert api_mod._temperature_season_code_for_month(3) == 0
    assert api_mod._temperature_season_code_for_month(7) == 1
    assert api_mod._temperature_season_code_for_month(10) == 0
    assert api_mod._temperature_season_code_for_month(12) == -1


def test_weather_legend_uses_verified_noaa_categories():
    payload = api_mod.forecast_legend(layer="weather", season=None)
    assert payload["kind"] == "categorical"
    precipitation = {item["label"]: item["color"] for item in payload["groups"]["precipitation"]}
    secondary = {item["label"]: item["color"] for item in payload["groups"]["secondary"]}
    assert precipitation["Rain chance"] == "#C6FFAD"
    assert precipitation["Ice likely"] == "#CE9CFF"
    assert secondary["Severe"] == "#FF0000"
    assert secondary["Other"] == "#FFEFE7"


def test_ndfd_colortable_parser_uses_authoritative_endpoint(monkeypatch):
    captured = {}

    class FakeResponse:
        text = '[["-10", "1", "2", "3"], ["32", "40", "55", "160"]]'
        content = text.encode("utf-8")
        headers = {"content-type": "text/html; charset=UTF-8"}

        def raise_for_status(self):
            return None

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["params"] = kwargs.get("params")
        return FakeResponse()

    api_mod._NDFD_LEGEND_CACHE.clear()
    monkeypatch.setattr(api_mod, "limited_requests_get", fake_get)
    rows = api_mod._load_ndfd_colortable("t", season_code=0)

    assert captured["url"] == api_mod.NDFD_COLORTABLE
    assert captured["params"]["element"] == "t"
    assert captured["params"]["season"] == "spring"
    assert rows == [
        {"value": -10.0, "color": "#010203"},
        {"value": 32.0, "color": "#2837A0"},
    ]


def test_temperature_wms_passes_explicit_season(monkeypatch):
    captured = {}

    class FakeResponse:
        content = b"png"
        headers = {"content-type": "image/png"}

        def raise_for_status(self):
            return None

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["params"] = kwargs.get("params")
        return FakeResponse()

    monkeypatch.setattr(api_mod, "limited_requests_get", fake_get)
    response = api_mod.forecast_wms(
        layer="temperature",
        bbox="-1000000,-1000000,1000000,1000000",
        vtit="2026-11-03T12:00",
        season=0,
        width=256,
        height=256,
    )

    assert response.status_code == 200
    assert captured["url"] == api_mod.NDFD_WMS
    assert captured["params"]["LAYERS"] == "ndfd.conus.t"
    assert captured["params"]["VTIT"] == "2026-11-03T12:00"
    assert captured["params"]["SEASON"] == "0"


def test_hazard_legend_proxy_uses_viewport_and_valid_time(monkeypatch):
    captured = {}

    class FakeResponse:
        content = b"png"
        headers = {"content-type": "image/png"}

        def raise_for_status(self):
            return None

    def fake_get(url, **kwargs):
        captured["url"] = url
        captured["params"] = kwargs.get("params")
        return FakeResponse()

    monkeypatch.setattr(api_mod, "limited_requests_get", fake_get)
    response = api_mod.forecast_legend_image(
        bbox="-11300000,4300000,-10600000,5000000",
        vtit="2026-11-03T18:00:00Z",
        width=900,
    )

    assert response.status_code == 200
    assert captured["url"] == api_mod.NDFD_VIEWER_LEGEND
    assert captured["params"]["element"] == "wwa"
    assert captured["params"]["vt"] == "2026-11-03T18:00"
    assert captured["params"]["left"] == "-11300000.0"
    assert captured["params"]["top"] == "5000000.0"
