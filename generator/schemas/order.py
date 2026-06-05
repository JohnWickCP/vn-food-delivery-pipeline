from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import List, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class OrderItem(BaseModel):
    item_id: str
    name: str
    price_vnd: int
    quantity: int


class Order(BaseModel):
    order_id: UUID = Field(default_factory=uuid4)
    customer_id: UUID = Field(default_factory=uuid4)
    restaurant_id: UUID = Field(default_factory=uuid4)
    rider_id: UUID = Field(default_factory=uuid4)
    city: Literal["hanoi", "hcmc", "danang"]
    district: str
    status: Literal["placed", "confirmed", "preparing", "picked_up", "delivered", "cancelled"]
    items: List[OrderItem]
    subtotal_vnd: int
    delivery_fee_vnd: int
    discount_vnd: int
    total_vnd: int
    payment_method: Literal["cash", "momo", "vnpay", "zalopay", "bank_transfer"]
    platform: Literal["android", "ios", "web"]
    placed_at: datetime
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    producer_ts: float = Field(default_factory=time.time)

    def to_kafka_dict(self) -> dict:
        return self.model_dump(mode="json")
