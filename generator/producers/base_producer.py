from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod

from confluent_kafka import Producer

import config

logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    def __init__(self) -> None:
        self.producer = Producer(config.KAFKA_PRODUCER_CONFIG)
        self._produced = 0
        self._errors = 0

    def _delivery_callback(self, err, msg) -> None:
        if err:
            self._errors += 1
            logger.error("Delivery failed | topic=%s err=%s", msg.topic(), err)
        else:
            self._produced += 1

    def produce(self, topic: str, value: dict, key: str | None = None) -> None:
        payload = json.dumps(value, default=str).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None
        try:
            self.producer.produce(
                topic,
                value=payload,
                key=key_bytes,
                callback=self._delivery_callback,
            )
            self.producer.poll(0)
        except BufferError:
            # Internal queue full — flush a bit then retry once
            self.producer.poll(1)
            self.producer.produce(
                topic,
                value=payload,
                key=key_bytes,
                callback=self._delivery_callback,
            )

    def flush(self) -> None:
        self.producer.flush()

    @abstractmethod
    async def run(self) -> None: ...
