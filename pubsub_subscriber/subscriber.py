"""
Pulls messages from Pub/Sub subscriptions and writes JSON Lines to GCS.
Acts as the bridge between Pub/Sub and Spark's file-based streaming source.

GCS output layout:
  gs://{bucket}/pubsub-raw/{topic}/year=YYYY/month=MM/day=DD/batch_{ts}.jsonl
"""
import json
import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCS_BUCKET     = os.environ["GCS_BUCKET"]
PULL_INTERVAL  = float(os.getenv("PULL_INTERVAL_SEC", "2"))
MAX_MESSAGES   = int(os.getenv("MAX_MESSAGES_PER_PULL", "1000"))

SUBSCRIPTIONS = {
    "raw-orders":       "raw-orders-sub",
    "raw-payments":     "raw-payments-sub",
    "raw-rider-events": "raw-rider-events-sub",
}

storage_client = storage.Client()
subscriber_client = pubsub_v1.SubscriberClient()
bucket = storage_client.bucket(GCS_BUCKET)


def _gcs_path(topic: str) -> str:
    now = datetime.now(timezone.utc)
    ts = int(now.timestamp() * 1000)
    uid = uuid.uuid4().hex[:8]
    return (
        f"pubsub-raw/{topic}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"batch_{ts}_{uid}.jsonl"
    )


def pull_and_write(topic: str, subscription_id: str) -> None:
    subscription_path = subscriber_client.subscription_path(GCP_PROJECT_ID, subscription_id)

    while True:
        try:
            response = subscriber_client.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": MAX_MESSAGES,
                },
                timeout=10.0,
            )

            if not response.received_messages:
                time.sleep(PULL_INTERVAL)
                continue

            lines = []
            ack_ids = []
            for msg in response.received_messages:
                try:
                    lines.append(msg.message.data.decode("utf-8"))
                    ack_ids.append(msg.ack_id)
                except Exception as exc:
                    logger.warning("Decode error topic=%s: %s", topic, exc)

            if lines:
                blob = bucket.blob(_gcs_path(topic))
                blob.upload_from_string(
                    "\n".join(lines) + "\n",
                    content_type="application/x-ndjson",
                )
                logger.info("topic=%s wrote %d records → gs://%s/%s",
                            topic, len(lines), GCS_BUCKET, blob.name)

            if ack_ids:
                subscriber_client.acknowledge(
                    request={
                        "subscription": subscription_path,
                        "ack_ids": ack_ids,
                    }
                )

        except Exception as exc:
            logger.error("Pull error topic=%s: %s", topic, exc)
            time.sleep(5)

        time.sleep(PULL_INTERVAL)


def main() -> None:
    logger.info("Starting Pub/Sub subscriber — project=%s bucket=%s", GCP_PROJECT_ID, GCS_BUCKET)
    threads = []
    for topic, sub_id in SUBSCRIPTIONS.items():
        t = threading.Thread(target=pull_and_write, args=(topic, sub_id), daemon=True)
        t.start()
        threads.append(t)
        logger.info("Started subscriber thread for %s → %s", topic, sub_id)

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down")


if __name__ == "__main__":
    main()
