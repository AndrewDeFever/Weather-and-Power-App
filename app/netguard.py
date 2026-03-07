from __future__ import annotations

import os
from threading import BoundedSemaphore
from urllib.parse import urlparse

import requests

_DEFAULT_ALLOWED_HOSTS = (
    "api.weather.gov",
    "kubra.io",
    "outagemap.psoklahoma.com",
    "outagemap.oge.com",
    "outagemap.evergy.com",
    "stormcenter.oncor.com",
    "outagemap.austinenergy.com",
)


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


def limited_get(session: requests.Session, url: str, **kwargs):
    assert_allowed_outbound_url(url)
    with _outbound_semaphore:
        return session.get(url, **kwargs)


def limited_requests_get(url: str, **kwargs):
    assert_allowed_outbound_url(url)
    with _outbound_semaphore:
        return requests.get(url, **kwargs)
