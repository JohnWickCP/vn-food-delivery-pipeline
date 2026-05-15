from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal, Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Payment(BaseModel):
    payment_id: UUID = Field(default_factory=uuid4)
    order_id: UUID
    amount_vnd: int
    method: Literal["cash", "momo", "vnpay", "zalopay", "bank_transfer"]
    status: Literal["success", "failed", "refunded"]
    gateway_transaction_id: Optional[str] = None
    processed_at: datetime
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")
