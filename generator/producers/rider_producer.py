from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

import config
from producers.base_producer import BaseProducer
from schemas.rider_event import RiderEvent

logger = logging.getLogger(__name__)

# Bounding boxes from master plan
_GPS_BOUNDS: dict[str, dict[str, tuple[float, float]]] = {
    "hanoi": {"lat": (20.95, 21.10), "lng": (105.75, 105.90)},
    "hcmc":  {"lat": (10.65, 10.90), "lng": (106.55, 106.80)},
    "danang":{"lat": (15.95, 16.15), "lng": (108.15, 108.30)},
}

# City distribution roughly matches VN food delivery market share
_CITY_WEIGHTS = {"hanoi": 3, "hcmc": 5, "danang": 2}


class _Rider:
    """Internal state for a single simulated rider."""

    def __init__(self, rider_id: UUID, city: str) -> None:
        self.rider_id = rider_id
        self.city = city
        bounds = _GPS_BOUNDS[city]
        self.lat = random.uniform(*bounds["lat"])
        self.lng = random.uniform(*bounds["lng"])
        self.status: str = random.choices(
            ["available", "on_delivery", "returning"],
            weights=[40, 45, 15],
        )[0]
        self.order_id: Optional[UUID] = uuid4() if self.status == "on_delivery" else None
        self.battery_pct: int = random.randint(30, 100)

    def tick(self) -> None:
        """Advance position, battery, and status for one GPS interval."""
        bounds = _GPS_BOUNDS[self.city]
        # ~0.001 degree ≈ 100 m; clamp within city bounds
        self.lat = max(bounds["lat"][0], min(bounds["lat"][1], self.lat + random.uniform(-0.002, 0.002)))
        self.lng = max(bounds["lng"][0], min(bounds["lng"][1], self.lng + random.uniform(-0.002, 0.002)))
        self.battery_pct = max(10, self.battery_pct - random.randint(0, 1))

        # State transitions: ~10% chance each interval
        if random.random() < 0.10:
            if self.status == "available":
                self.status = "on_delivery"
                self.order_id = uuid4()
            elif self.status == "on_delivery":
                self.status = "returning"
                self.order_id = None
            else:
                self.status = "available"

    def to_event(self) -> RiderEvent:
        speed = 0.0 if self.status == "available" else random.uniform(10.0, 50.0)
        return RiderEvent(
            rider_id=self.rider_id,
            order_id=self.order_id,
            city=self.city,
            latitude=self.lat,
            longitude=self.lng,
            speed_kmh=speed,
            status=self.status,
            battery_pct=self.battery_pct,
        )


class RiderProducer(BaseProducer):
    def __init__(self) -> None:
        super().__init__()
        self.riders = self._init_riders()

    def _init_riders(self) -> list[_Rider]:
        cities = list(_CITY_WEIGHTS.keys())
        weights = list(_CITY_WEIGHTS.values())
        return [
            _Rider(rider_id, random.choices(cities, weights=weights)[0])
            for rider_id in config.RIDER_POOL
        ]

    async def run(self) -> None:
        logger.info("RiderProducer started — %d riders", len(self.riders))
        while True:
            for rider in self.riders:
                rider.tick()
                event = rider.to_event()
                self.produce(
                    config.TOPIC_RIDER_EVENTS,
                    event.to_kafka_dict(),
                    key=str(rider.rider_id),
                )
            await asyncio.sleep(config.RIDER_GPS_INTERVAL_SEC)
