from __future__ import annotations

import os
from threading import BoundedSemaphore
import requests

OUTBOUND_MAX_CONCURRENCY = max(1, int(os.getenv("OUTBOUND_MAX_CONCURRENCY", "8")))
_outbound_semaphore = BoundedSemaphore(OUTBOUND_MAX_CONCURRENCY)


def limited_get(session: requests.Session, url: str, **kwargs):
    with _outbound_semaphore:
        return session.get(url, **kwargs)
