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
        # Payment was processed 10s–3min before this event was emitted
        processed_at = now - timedelta(seconds=random.uniform(10, 180))

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
            try:
                order_id, method, amount = await asyncio.wait_for(
                    self.order_queue.get(), timeout=30.0
                )
            except asyncio.TimeoutError:
                logger.warning("No orders in 30s — OrderProducer may be down")
                continue
            payment = self._make_payment(order_id, method, amount)
            self.produce(
                config.TOPIC_PAYMENTS,
                payment.to_kafka_dict(),
                key=str(payment.payment_id),
            )
            self.order_queue.task_done()
