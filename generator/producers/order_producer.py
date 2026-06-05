from __future__ import annotations

import asyncio
import logging
import os
import random
from datetime import datetime, timezone
from uuid import uuid4
from zoneinfo import ZoneInfo

_AVSC = os.path.join(os.path.dirname(__file__), "..", "schemas", "avro", "order.avsc")

_VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

from faker import Faker

import config
from producers.base_producer import BaseProducer
from schemas.order import Order, OrderItem

logger = logging.getLogger(__name__)
fake = Faker("vi_VN")

DISTRICTS: dict[str, list[str]] = {
    "hanoi": ["Hoàn Kiếm", "Đống Đa", "Ba Đình", "Cầu Giấy", "Hai Bà Trưng", "Thanh Xuân", "Long Biên", "Hoàng Mai"],
    "hcmc": ["Quận 1", "Quận 3", "Quận 5", "Quận 7", "Bình Thạnh", "Phú Nhuận", "Tân Bình", "Gò Vấp"],
    "danang": ["Hải Châu", "Thanh Khê", "Sơn Trà", "Ngũ Hành Sơn", "Liên Chiểu", "Cẩm Lệ"],
}

# (name, price_vnd) — representative VN street food menu
FOOD_MENU: list[tuple[str, int]] = [
    ("Phở bò tái", 65_000),
    ("Bún bò Huế", 55_000),
    ("Bánh mì thịt", 35_000),
    ("Cơm tấm sườn bì chả", 55_000),
    ("Gà rán giòn", 75_000),
    ("Bún chả Hà Nội", 50_000),
    ("Bánh xèo miền Trung", 60_000),
    ("Lẩu thái hải sản", 120_000),
    ("Trà sữa trân châu", 35_000),
    ("Cà phê sữa đá", 25_000),
    ("Cơm gà Hội An", 60_000),
    ("Mì Quảng", 45_000),
    ("Bún riêu cua", 50_000),
    ("Chả giò chiên", 40_000),
]

DELIVERY_FEES = [15_000, 20_000, 25_000, 30_000, 35_000]
DISCOUNT_OPTIONS = [0, 0, 0, 0, 10_000, 20_000, 30_000, 50_000]

# Realistic final status distribution for food delivery
# In production each transition would be a separate event; here we assign final state at emit time
_STATUS_WEIGHTS = {
    "delivered": 65,
    "cancelled": 12,
    "picked_up": 8,
    "preparing": 8,
    "confirmed": 5,
    "placed": 2,
}

# Pre-generate a pool of restaurant IDs — restaurants are reused across orders
RESTAURANT_POOL = [uuid4() for _ in range(500)]


class OrderProducer(BaseProducer):
    def __init__(self, order_queue: asyncio.Queue) -> None:
        super().__init__(avsc_path=_AVSC)
        self.order_queue = order_queue

    def _current_rate(self) -> int:
        hour = datetime.now(_VN_TZ).hour
        for start, end in config.PEAK_HOURS:
            if start <= hour < end:
                return config.ORDERS_PER_MIN_PEAK
        return config.ORDERS_PER_MIN_BASE

    def _make_order(self) -> Order:
        city = random.choice(["hanoi", "hcmc", "danang"])
        district = random.choice(DISTRICTS[city])

        chosen_items = random.sample(FOOD_MENU, k=random.randint(1, 4))
        items = [
            OrderItem(
                item_id=str(uuid4()),
                name=name,
                price_vnd=price,
                quantity=random.randint(1, 3),
            )
            for name, price in chosen_items
        ]
        subtotal = sum(i.price_vnd * i.quantity for i in items)
        delivery_fee = random.choice(DELIVERY_FEES)
        discount = random.choice(DISCOUNT_OPTIONS)
        now = datetime.now(timezone.utc)

        return Order(
            customer_id=uuid4(),
            restaurant_id=random.choice(RESTAURANT_POOL),
            rider_id=random.choice(config.RIDER_POOL),
            city=city,
            district=district,
            status=random.choices(
                list(_STATUS_WEIGHTS.keys()),
                weights=list(_STATUS_WEIGHTS.values()),
            )[0],
            items=items,
            subtotal_vnd=subtotal,
            delivery_fee_vnd=delivery_fee,
            discount_vnd=discount,
            total_vnd=max(0, subtotal + delivery_fee - discount),
            payment_method=random.choice(["cash", "momo", "vnpay", "zalopay", "bank_transfer"]),
            platform=random.choices(["android", "ios", "web"], weights=[55, 35, 10])[0],
            placed_at=now,
            event_timestamp=now,
        )

    async def run(self) -> None:
        logger.info("OrderProducer started")
        while True:
            rate = self._current_rate()
            interval = 60.0 / rate  # seconds per message

            order = self._make_order()
            self.produce(config.TOPIC_ORDERS, order.to_kafka_dict(), key=str(order.order_id))

            # Pass order info to PaymentProducer via shared queue
            if not self.order_queue.full():
                await self.order_queue.put((str(order.order_id), order.payment_method, order.total_vnd))

            await asyncio.sleep(interval)
