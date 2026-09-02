from fastapi.testclient import TestClient

from app import api as api_mod


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

    r = client.get("/api/status?q=TULSATEST")
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
    r = client.get("/api/status?q=TULSATEST")
    assert r.status_code == 200
    assert r.headers.get("content-type", "").startswith("application/json")
    data = r.json()
    assert "weather" in data
    assert "power" in data
    # power.meta.error must exist on provider failure
    assert isinstance(data["power"], dict)
    assert isinstance(data["power"].get("meta"), dict)
    assert data["power"]["meta"].get("error")
