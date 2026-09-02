import json
from pathlib import Path


SITES_PATH = Path(__file__).resolve().parents[1] / "app" / "data" / "sites.json"

REQUIRED_FIELDS = {
    "address",
    "aliases",
    "city",
    "enabled",
    "lat",
    "lon",
    "name",
    "notes",
    "ops_profile",
    "outage_data_status",
    "outage_map",
    "provider_module_supported",
    "severity",
    "site_id",
    "state",
    "tz",
    "utility",
    "utility_method",
    "utility_phone",
    "utility_source",
    "utility_tagged_at",
    "zip",
}

EXPECTED_CANONICAL_UTILITIES = {
    "OGE",
    "PSO",
    "EVERGY",
    "ONCOR",
    "AUSTIN",
    "PEC",
    "AEP",
    "CENTERPOINT",
    "EPE",
    "CITY_OF_CONCORDIA_ELECTRIC",
    "PRAIRIE_LAND_ELECTRIC",
    "NINNESCAH_RURAL_ELECTRIC",
}


def _load_sites():
    return json.loads(SITES_PATH.read_text(encoding="utf-8"))


def test_sites_dataset_is_nonempty_and_synthetic():
    sites = _load_sites()
    assert sites

    for key, site in sites.items():
        assert key.startswith("DEMO_")
        assert site["site_id"] == key
        assert site["ops_profile"] == "portfolio_demo"
        assert site["outage_data_status"] == "synthetic_demo"
        assert site["utility_source"] == "portfolio_dataset"
        assert "Synthetic portfolio demo location" in site["notes"]


def test_sites_preserve_portfolio_schema_contract():
    sites = _load_sites()

    for key, site in sites.items():
        assert REQUIRED_FIELDS <= set(site), f"{key} is missing required fields"
        assert isinstance(site["aliases"], list)
        assert isinstance(site["enabled"], bool)
        assert isinstance(site["provider_module_supported"], bool)
        assert isinstance(site["lat"], (int, float))
        assert isinstance(site["lon"], (int, float))
        assert -90 <= site["lat"] <= 90
        assert -180 <= site["lon"] <= 180
        assert isinstance(site["severity"], int)
        assert 1 <= site["severity"] <= 4


def test_sites_do_not_embed_operational_contact_or_outage_urls():
    sites = _load_sites()

    for site in sites.values():
        assert site["address"] == ""
        assert site["utility_phone"] is None
        assert site["outage_map"] is None
        assert site["zip"] is None


def test_synthetic_dataset_covers_every_canonical_provider():
    sites = _load_sites()
    utilities = {site["utility"] for site in sites.values() if site["enabled"]}

    assert EXPECTED_CANONICAL_UTILITIES <= utilities
    assert len(EXPECTED_CANONICAL_UTILITIES) == 12
