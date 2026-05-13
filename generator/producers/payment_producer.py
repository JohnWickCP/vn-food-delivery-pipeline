from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from uuid import UUID

import config
from producers.base_producer import BaseProducer
from schemas.payment import Payment

logger = logging.getLogger(__name__)

# Realistic payment outcome distribution
_STATUS_WEIGHTS = {"success": 93, "failed": 5, "refunded": 2}


class PaymentProducer(BaseProducer):
    def __init__(self, order_queue: asyncio.Queue) -> None:
        super().__init__()
        self.order_queue = order_queue

    def _make_payment(self, order_id: str, method: str, amount: int) -> Payment:
        status = random.choices(
            list(_STATUS_WEIGHTS.keys()),
            weights=list(_STATUS_WEIGHTS.values()),
        )[0]
        # Cash payments have no gateway transaction ID
        gateway_id = None if method == "cash" else f"TXN{random.randint(10**9, 10**10 - 1)}"
        now = datetime.now(timezone.utc)
        # Payment processed 1–5 min after order
        processed_at = now + timedelta(minutes=random.uniform(1, 5))

        return Payment(
            order_id=UUID(order_id),
            amount_vnd=amount,
            method=method,
            status=status,
            gateway_transaction_id=gateway_id,
            processed_at=processed_at,
            event_timestamp=now,
        )

    async def run(self) -> None:
        logger.info("PaymentProducer started")
        while True:
            order_id, method, amount = await self.order_queue.get()
            payment = self._make_payment(order_id, method, amount)
            self.produce(
                config.TOPIC_PAYMENTS,
                payment.to_kafka_dict(),
                key=str(payment.payment_id),
            )
            self.order_queue.task_done()
