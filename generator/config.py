import os
from uuid import uuid4

from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "vn-food-delivery-2026")

TOPIC_ORDERS = "raw-orders"
TOPIC_PAYMENTS = "raw-payments"
TOPIC_RIDER_EVENTS = "raw-rider-events"

# Orders per minute at peak vs off-peak
ORDERS_PER_MIN_PEAK = int(os.getenv("ORDERS_PER_MIN_PEAK", "5000"))
ORDERS_PER_MIN_BASE = int(os.getenv("ORDERS_PER_MIN_BASE", "1200"))

# Peak hours (inclusive start, exclusive end) — VN meal times
PEAK_HOURS: list[tuple[int, int]] = [(11, 13), (18, 20)]

NUM_RIDERS = int(os.getenv("NUM_RIDERS", "200"))
RIDER_GPS_INTERVAL_SEC = float(os.getenv("RIDER_GPS_INTERVAL_SEC", "30"))

# Shared rider pool — same UUIDs used by both OrderProducer and RiderProducer
# so that raw_orders.rider_id can join raw_rider_events.rider_id in dbt
RIDER_POOL = [uuid4() for _ in range(NUM_RIDERS)]
