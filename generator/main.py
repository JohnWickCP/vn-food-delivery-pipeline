"""
Entry point — runs all three producers concurrently via asyncio.
Throughput is logged every 10 seconds to stdout.
"""
import asyncio
import logging

from producers.order_producer import OrderProducer
from producers.payment_producer import PaymentProducer
from producers.rider_producer import RiderProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

_THROUGHPUT_INTERVAL = 10  # seconds


async def _log_throughput(producers: list, interval: int = _THROUGHPUT_INTERVAL) -> None:
    while True:
        await asyncio.sleep(interval)
        total = sum(p._produced for p in producers)
        errors = sum(p._errors for p in producers)
        rate_per_min = total / interval * 60
        logger.info(
            "Throughput | produced=%d errors=%d rate=%.0f msg/min",
            total, errors, rate_per_min,
        )
        for p in producers:
            p._produced = 0
            p._errors = 0


async def main() -> None:
    order_queue: asyncio.Queue = asyncio.Queue(maxsize=20_000)

    order_prod = OrderProducer(order_queue)
    payment_prod = PaymentProducer(order_queue)
    rider_prod = RiderProducer()

    all_producers = [order_prod, payment_prod, rider_prod]
    logger.info("Starting all producers")

    try:
        await asyncio.gather(
            order_prod.run(),
            payment_prod.run(),
            rider_prod.run(),
            _log_throughput(all_producers),
        )
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutdown signal received")
    finally:
        logger.info("Flushing remaining messages...")
        for p in all_producers:
            p.flush()
        logger.info("Done")


if __name__ == "__main__":
    asyncio.run(main())
