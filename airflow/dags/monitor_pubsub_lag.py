from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger(__name__)

_PROJECT_ID    = os.getenv("GCP_PROJECT_ID", "project-739a3554-aa69-4eab-9e2")
_SUBSCRIPTIONS = ["raw-orders-sub", "raw-payments-sub", "raw-rider-events-sub"]
_BACKLOG_ALERT = int(os.getenv("PUBSUB_BACKLOG_ALERT_THRESHOLD", "50000"))
_ALERT_EMAIL   = os.getenv("AIRFLOW_ALERT_EMAIL", "")


def _get_backlogs(project_id: str, subscriptions: list[str]) -> dict[str, int]:
    from google.cloud import monitoring_v3

    client = monitoring_v3.MetricServiceClient()
    now = datetime.now(timezone.utc)
    result = {}

    for sub_id in subscriptions:
        ts_list = list(client.list_time_series(
            request={
                "name": f"projects/{project_id}",
                "filter": (
                    'metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages" '
                    f'AND resource.labels.subscription_id="{sub_id}"'
                ),
                "interval": monitoring_v3.TimeInterval({
                    "end_time":   {"seconds": int(now.timestamp())},
                    "start_time": {"seconds": int((now - timedelta(minutes=5)).timestamp())},
                }),
                "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            }
        ))
        backlog = int(ts_list[0].points[0].value.int64_value) if ts_list and ts_list[0].points else 0
        result[sub_id] = backlog

    return result


@dag(
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "email_on_failure": bool(_ALERT_EMAIL),
        "email_on_retry": False,
        "email": [_ALERT_EMAIL] if _ALERT_EMAIL else [],
    },
    tags=["pipeline", "monitor"],
)
def monitor_pubsub_lag():
    """Every 5 min: check Pub/Sub subscription backlog. Alert if > 50k or growing."""

    @task
    def check_lag() -> dict:
        current = _get_backlogs(_PROJECT_ID, _SUBSCRIPTIONS)

        prev = json.loads(Variable.get("pubsub_last_backlog", default_var="{}"))
        Variable.set("pubsub_last_backlog", json.dumps(current))

        alerts = []
        for sub_id, backlog in current.items():
            prev_backlog = prev.get(sub_id, 0)
            logger.info("[%s] backlog=%d prev=%d", sub_id, backlog, prev_backlog)

            if backlog > _BACKLOG_ALERT:
                alerts.append(
                    f"HIGH_BACKLOG: {sub_id} — {backlog:,} undelivered (threshold={_BACKLOG_ALERT:,})"
                )

            if prev and prev_backlog > 5_000 and backlog > prev_backlog:
                alerts.append(
                    f"GROWING_BACKLOG: {sub_id} — {prev_backlog:,} → {backlog:,} (Spark stuck?)"
                )

        if alerts:
            for a in alerts:
                logger.warning("[ALERT] %s", a)
            raise ValueError("Pub/Sub health alerts:\n" + "\n".join(alerts))

        return current

    check_lag()


dag = monitor_pubsub_lag()
