from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

import config

logger = logging.getLogger(__name__)

_SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


class BaseProducer(ABC):
    def __init__(self, avsc_path: str) -> None:
        sr_client = SchemaRegistryClient({"url": _SR_URL})
        with open(avsc_path) as f:
            schema_str = f.read()

        avro_serializer = AvroSerializer(
            sr_client,
            schema_str,
            lambda obj, ctx: obj,
        )

        producer_conf = {
            k: v for k, v in config.KAFKA_PRODUCER_CONFIG.items()
            if k not in ("compression.type",)
        }
        producer_conf["key.serializer"]   = StringSerializer("utf_8")
        producer_conf["value.serializer"] = avro_serializer

        self.producer  = SerializingProducer(producer_conf)
        self._produced = 0
        self._errors   = 0

    def _delivery_callback(self, err, msg) -> None:
        if err:
            self._errors += 1
            logger.error("Delivery failed | topic=%s err=%s", msg.topic(), err)
        else:
            self._produced += 1

    def produce(self, topic: str, value: dict, key: str | None = None) -> None:
        try:
            self.producer.produce(
                topic,
                value=value,
                key=key,
                on_delivery=self._delivery_callback,
            )
            self.producer.poll(0)
        except BufferError:
            self.producer.poll(1)
            self.producer.produce(
                topic,
                value=value,
                key=key,
                on_delivery=self._delivery_callback,
            )

    def flush(self) -> None:
        self.producer.flush()

    @abstractmethod
    async def run(self) -> None: ...
