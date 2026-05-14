from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

_KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
_TOPICS           = ["raw.orders", "raw.payments", "raw.rider_events"]
_LAG_THRESHOLD    = int(os.getenv("KAFKA_LAG_ALERT_THRESHOLD", "10000"))  # per topic
_SILENT_THRESHOLD = 0   # alert if 0 new messages since last check


def _get_high_watermarks() -> dict[str, int]:
    """Return {topic:partition → high_watermark} for all monitored topics."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": _KAFKA_SERVERS,
        "group.id": "__airflow_monitor__",
        "enable.auto.commit": False,
    })
    result = {}
    try:
        for topic in _TOPICS:
            meta = consumer.list_topics(topic, timeout=10)
            if topic not in meta.topics or meta.topics[topic].error:
                logger.warning("Topic %s not found or error", topic)
                continue
            for pid in meta.topics[topic].partitions:
                tp = TopicPartition(topic, pid)
                low, high = consumer.get_watermark_offsets(tp, timeout=10)
                result[f"{topic}:{pid}"] = high
    finally:
        consumer.close()
    return result


def _find_consumer_groups_for_topics() -> list[str]:
    """List consumer groups that are actively consuming our monitored topics."""
    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": _KAFKA_SERVERS})
    try:
        all_groups = admin.list_consumer_groups(request_timeout=10).result().valid
        group_ids = [g.group_id for g in all_groups if not g.group_id.startswith("__")]
    except Exception as exc:
        logger.warning("Could not list consumer groups: %s", exc)
        return []

    if not group_ids:
        return []

    relevant = set()
    try:
        described = admin.describe_consumer_groups(group_ids, request_timeout=15)
        for gid, future in described.items():
            try:
                group = future.result()
                for member in group.members:
                    if not member.assignment:
                        continue
                    for tp in member.assignment.topic_partitions:
                        if tp.topic in _TOPICS:
                            relevant.add(gid)
                            break
            except Exception:
                pass
    except Exception as exc:
        logger.warning("Could not describe consumer groups: %s", exc)

    return list(relevant)


def _get_consumer_lag(group_ids: list[str], high_watermarks: dict[str, int]) -> dict[str, int]:
    """Return {topic → total lag} across all partitions for the given consumer groups."""
    if not group_ids:
        return {t: -1 for t in _TOPICS}  # -1 = unknown (no active group found)

    from confluent_kafka.admin import AdminClient
    from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions

    admin = AdminClient({"bootstrap.servers": _KAFKA_SERVERS})

    # Build one request per group covering all monitored topic-partitions
    tps = [TopicPartition(key.split(":")[0], int(key.split(":")[1])) for key in high_watermarks]
    requests = [ConsumerGroupTopicPartitions(gid, tps) for gid in group_ids]

    lag_by_topic: dict[str, int] = {t: 0 for t in _TOPICS}
    for req in requests:
        try:
            offsets_result = admin.list_consumer_group_offsets([req], request_timeout=15)
            for _gid, future in offsets_result.items():
                try:
                    cgtp = future.result()
                    for tp in cgtp.topic_partitions:
                        if tp.error or tp.offset < 0:
                            continue
                        hw = high_watermarks.get(f"{tp.topic}:{tp.partition}", 0)
                        lag_by_topic[tp.topic] = lag_by_topic.get(tp.topic, 0) + max(0, hw - tp.offset)
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("Could not fetch offsets for group %s: %s", req.group_id, exc)

    return lag_by_topic


@dag(
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["pipeline", "monitor"],
)
def monitor_kafka_lag():
    """Every 5 min: check Kafka topic throughput and Spark consumer lag. Alert if thresholds exceeded."""

    @task
    def check_lag():
        # --- Throughput: compare current vs previous high watermarks ---
        current_hw = _get_high_watermarks()

        prev_json  = Variable.get("kafka_last_offsets", default_var="{}")
        prev_hw    = json.loads(prev_json)
        Variable.set("kafka_last_offsets", json.dumps(current_hw))

        throughput: dict[str, int] = {t: 0 for t in _TOPICS}
        for key, cur_val in current_hw.items():
            topic = key.rsplit(":", 1)[0]
            throughput[topic] += max(0, cur_val - prev_hw.get(key, cur_val))

        # --- Consumer lag ---
        groups    = _find_consumer_groups_for_topics()
        lag       = _get_consumer_lag(groups, current_hw)

        # --- Evaluate + log ---
        alerts = []
        for topic in _TOPICS:
            new_msgs = throughput[topic]
            topic_lag = lag[topic]

            logger.info(
                "[%s] throughput=+%d msgs | consumer_lag=%s | groups=%s",
                topic, new_msgs,
                topic_lag if topic_lag >= 0 else "unknown",
                groups or "none found",
            )

            if new_msgs == _SILENT_THRESHOLD and prev_hw:  # skip first run (no baseline)
                alerts.append(f"SILENT: {topic} — 0 new messages in last 5 min (producer down?)")

            if topic_lag > _LAG_THRESHOLD:
                alerts.append(f"LAG: {topic} — {topic_lag:,} messages behind (threshold={_LAG_THRESHOLD:,})")

        if alerts:
            for alert in alerts:
                logger.warning("[ALERT] %s", alert)
            # Raise so Airflow marks run as failed → triggers email/webhook if configured
            raise ValueError("Kafka health alerts:\n" + "\n".join(alerts))

        return {"throughput": throughput, "lag": {k: v for k, v in lag.items()}}

    check_lag()


dag = monitor_kafka_lag()
