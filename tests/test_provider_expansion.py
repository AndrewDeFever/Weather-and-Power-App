import pytest

from app import api as api_mod
from app import power_router
from app.providers import centerpoint_arcgis as centerpoint
from app.providers import epe_starlit as epe


EXPECTED_PROVIDERS = {
    "OGE",
    "PSO",
    "EVERGY",
    "ONCOR",
    "AUSTIN",
    "PEC",
    "AEP",
    "CENTERPOINT",
    "EPE",
    "EL_PASO_ELECTRIC",
    "CITY_OF_CONCORDIA_ELECTRIC",
    "PRAIRIE_LAND_ELECTRIC",
    "NINNESCAH_RURAL_ELECTRIC",
}


def test_expanded_provider_set_is_routable():
    assert EXPECTED_PROVIDERS <= power_router.SUPPORTED_PROVIDERS
    assert EXPECTED_PROVIDERS <= set(power_router._call_map)


def test_api_catalog_covers_expanded_provider_set():
    for provider in EXPECTED_PROVIDERS:
        info = api_mod.provider_info(provider)
        assert info["utility"]
        assert info["name"]


def test_epe_requires_environment_provided_credentials(monkeypatch):
    monkeypatch.setattr(epe, "EPE_API_KEY", "")
    monkeypatch.setattr(epe, "EPE_ENCRYPTION_KEY", "")

    with pytest.raises(epe.EpeProviderError, match="Missing EPE_API_KEY"):
        epe._request_epe_outages()


def test_centerpoint_normalizes_public_event_shape():
    event = {
        "identifier": "DEMO-CP-1",
        "startTime": 1783016652895,
        "numPeople": 42,
        "status": "Crew Assigned",
        "cause": "Storm",
        "latitude": 29.7605,
        "longitude": -95.3697,
        "additionalProperties": [
            {"property": "AREA_ZIP", "value": ["77002"]},
        ],
    }

    outage = centerpoint._normalize_datacapable_event(event, 29.7604, -95.3698)
    assert outage is not None
    assert outage["outage_id"] == "DEMO-CP-1"
    assert outage["customers_out"] == 42
    assert outage["provider"] == "CENTERPOINT"
    assert outage["zipcode"] == "77002"


def test_health_endpoint_is_available():
    paths = {route.path for route in api_mod.app.routes}
    assert "/healthz" in paths
