from __future__ import annotations

import os
from threading import BoundedSemaphore
from urllib.parse import urlparse

import requests
import urllib3

_DEFAULT_ALLOWED_HOSTS = (
    "api.weather.gov",
    "kubra.io",
    "outagemap.psoklahoma.com",
    "outagemap.oge.com",
    "outagemap.evergy.com",
    "stormcenter.oncor.com",
    "outagemap.austinenergy.com",
    "starlit.epelectric.com",
    "tracker.centerpointenergy.com",
    "centerpoint.datacapable.com",
    "cecdata.com",
    "prairielandelectric.outagemap.coop",
    "ninnescah.ebill.coop",
    "outagemap-data.cloud.coop",
    "services.arcgis.com",
    "services1.arcgis.com",
    "services2.arcgis.com",
    "services3.arcgis.com",
    "services4.arcgis.com",
    "services5.arcgis.com",
    "services6.arcgis.com",
    "services7.arcgis.com",
    "services8.arcgis.com",
    "services9.arcgis.com",
    "utility.arcgis.com",
)


def _parse_bool_env(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


def _parse_ca_env(name: str) -> str | None:
    value = os.getenv(name, "").strip()
    return value or None


# Global outbound TLS controls. Defaults remain secure verification enabled.
OUTBOUND_SSL_VERIFY = _parse_bool_env("OUTBOUND_SSL_VERIFY", True)
OUTBOUND_SSL_CA_BUNDLE = _parse_ca_env("OUTBOUND_SSL_CA_BUNDLE")

# Host-specific TLS overrides for outage providers.
_HOST_TLS_ENV = {
    "api.weather.gov": ("NWS_SSL_VERIFY", "NWS_SSL_CA_BUNDLE"),
    "kubra.io": ("KUBRA_SSL_VERIFY", "KUBRA_SSL_CA_BUNDLE"),
    "outagemap.psoklahoma.com": ("PSO_SSL_VERIFY", "PSO_SSL_CA_BUNDLE"),
    "outagemap.oge.com": ("OGE_SSL_VERIFY", "OGE_SSL_CA_BUNDLE"),
    "outagemap.evergy.com": ("EVERGY_SSL_VERIFY", "EVERGY_SSL_CA_BUNDLE"),
    "stormcenter.oncor.com": ("ONCOR_SSL_VERIFY", "ONCOR_SSL_CA_BUNDLE"),
    "outagemap.austinenergy.com": ("AUSTIN_MAP_SSL_VERIFY", "AUSTIN_MAP_SSL_CA_BUNDLE"),
    "starlit.epelectric.com": ("EPE_SSL_VERIFY", "EPE_SSL_CA_BUNDLE"),
    "tracker.centerpointenergy.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "centerpoint.datacapable.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "cecdata.com": ("CONCORDIA_SSL_VERIFY", "CONCORDIA_SSL_CA_BUNDLE"),
    "prairielandelectric.outagemap.coop": ("PRAIRIE_LAND_SSL_VERIFY", "PRAIRIE_LAND_SSL_CA_BUNDLE"),
    "ninnescah.ebill.coop": ("NINNESCAH_SSL_VERIFY", "NINNESCAH_SSL_CA_BUNDLE"),
    "outagemap-data.cloud.coop": ("OUTAGEMAP_DATA_SSL_VERIFY", "OUTAGEMAP_DATA_SSL_CA_BUNDLE"),
    "services.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services1.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services2.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services3.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services4.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services5.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services6.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services7.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services8.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "services9.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
    "utility.arcgis.com": ("CENTERPOINT_SSL_VERIFY", "CENTERPOINT_SSL_CA_BUNDLE"),
}


def _build_host_tls_overrides() -> dict[str, tuple[bool, str | None]]:
    overrides: dict[str, tuple[bool, str | None]] = {}
    for host, (verify_env, ca_env) in _HOST_TLS_ENV.items():
        verify = _parse_bool_env(verify_env, True)
        ca_bundle = _parse_ca_env(ca_env)
        if os.getenv(verify_env) is not None or ca_bundle:
            overrides[host] = (verify, ca_bundle)
    return overrides


HOST_TLS_OVERRIDES = _build_host_tls_overrides()

if (not OUTBOUND_SSL_VERIFY) or any(not verify for verify, _ in HOST_TLS_OVERRIDES.values()):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _parse_allowed_hosts(value: str | None) -> tuple[str, ...]:
    if not value:
        return _DEFAULT_ALLOWED_HOSTS
    hosts = [h.strip().lower() for h in value.split(",") if h.strip()]
    return tuple(hosts) if hosts else _DEFAULT_ALLOWED_HOSTS


ALLOWED_OUTBOUND_HOSTS = frozenset(_parse_allowed_hosts(os.getenv("ALLOWED_OUTBOUND_HOSTS")))
OUTBOUND_MAX_CONCURRENCY = max(1, int(os.getenv("OUTBOUND_MAX_CONCURRENCY", "8")))
_outbound_semaphore = BoundedSemaphore(OUTBOUND_MAX_CONCURRENCY)


class OutboundHostBlockedError(RuntimeError):
    pass


def assert_allowed_outbound_url(url: str) -> str:
    host = (urlparse(url).hostname or "").strip().lower()
    if not host:
        raise OutboundHostBlockedError(f"Outbound URL missing hostname: {url!r}")
    if host not in ALLOWED_OUTBOUND_HOSTS:
        allowed = ", ".join(sorted(ALLOWED_OUTBOUND_HOSTS))
        raise OutboundHostBlockedError(f"Outbound host blocked: {host}. Allowed hosts: {allowed}")
    return host


def _apply_tls_overrides(url: str, kwargs: dict) -> dict:
    # Respect explicit caller-provided verify values.
    if "verify" in kwargs:
        return kwargs

    host = (urlparse(url).hostname or "").strip().lower()
    verify = OUTBOUND_SSL_VERIFY
    ca_bundle = OUTBOUND_SSL_CA_BUNDLE

    host_override = HOST_TLS_OVERRIDES.get(host)
    if host_override:
        verify, host_ca_bundle = host_override
        if host_ca_bundle:
            ca_bundle = host_ca_bundle

    out = dict(kwargs)
    if not verify:
        out["verify"] = False
    elif ca_bundle:
        out["verify"] = ca_bundle
    return out


def limited_get(session: requests.Session, url: str, **kwargs):
    assert_allowed_outbound_url(url)
    kwargs = _apply_tls_overrides(url, kwargs)
    with _outbound_semaphore:
        return session.get(url, **kwargs)


def limited_post(session: requests.Session, url: str, **kwargs):
    assert_allowed_outbound_url(url)
    kwargs = _apply_tls_overrides(url, kwargs)
    with _outbound_semaphore:
        return session.post(url, **kwargs)


def limited_requests_get(url: str, **kwargs):
    assert_allowed_outbound_url(url)
    kwargs = _apply_tls_overrides(url, kwargs)
    with _outbound_semaphore:
        return requests.get(url, **kwargs)


def limited_requests_post(url: str, **kwargs):
    assert_allowed_outbound_url(url)
    kwargs = _apply_tls_overrides(url, kwargs)
    with _outbound_semaphore:
        return requests.post(url, **kwargs)
