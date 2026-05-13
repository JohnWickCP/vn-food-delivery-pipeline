from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class RiderEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    rider_id: UUID
    order_id: Optional[UUID] = None
    city: Literal["hanoi", "hcmc", "danang"]
    latitude: float
    longitude: float
    speed_kmh: float
    status: Literal["available", "on_delivery", "returning"]
    battery_pct: int
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")
