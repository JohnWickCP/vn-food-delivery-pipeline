from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod

from google.cloud import pubsub_v1

import config

logger = logging.getLogger(__name__)


class BaseProducer(ABC):
    def __init__(self) -> None:
        self.publisher = pubsub_v1.PublisherClient()
        self._produced = 0
        self._errors = 0

    def _topic_path(self, topic_id: str) -> str:
        return self.publisher.topic_path(config.GCP_PROJECT_ID, topic_id)

    def _on_publish(self, future, topic: str) -> None:
        try:
            future.result()
            self._produced += 1
        except Exception as exc:
            self._errors += 1
            logger.error("Publish failed | topic=%s err=%s", topic, exc)

    def produce(self, topic: str, value: dict, key: str | None = None) -> None:
        data = json.dumps(value, default=str).encode("utf-8")
        future = self.publisher.publish(self._topic_path(topic), data)
        future.add_done_callback(lambda f: self._on_publish(f, topic))

    def flush(self) -> None:
        pass  # PublisherClient drains automatically when GC'd on process exit

    @abstractmethod
    async def run(self) -> None: ...
