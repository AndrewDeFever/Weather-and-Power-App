from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Outage(BaseModel):
    """
    Normalized outage object. Keep fields flexible because upstream providers vary.
    Preserve unknown fields in `raw` so we don't lose data for RFO / troubleshooting.
    """
    customers_out: Optional[int] = None
    etr: Optional[str] = None
    start_time: Optional[str] = None
    cause: Optional[str] = None

    lat: Optional[float] = None
    lon: Optional[float] = None
    distance_km: Optional[float] = None
    distance_miles: Optional[float] = None

    outage_id: Optional[str] = None
    provider: Optional[str] = None

    raw: Dict[str, Any] = Field(default_factory=dict)


class PowerMeta(BaseModel):
    source: str = "app.power_router.get_power_status"
    ok: bool = True
    error: Optional[str] = None
    generated_at_utc: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat(timespec="seconds") + "Z"
    )


class PowerBlock(BaseModel):
    utility: str = "OGE"
    has_outage_nearby: bool = False
    nearest: Optional[Outage] = None
    outages: List[Outage] = Field(default_factory=list)
    meta: PowerMeta = Field(default_factory=PowerMeta)


class StatusResponse(BaseModel):
    lat: float
    lon: float
    power: PowerBlock
