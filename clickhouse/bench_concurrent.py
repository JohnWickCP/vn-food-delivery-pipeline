import concurrent.futures
import time
from clickhouse_driver import Client

QUERY = """
    SELECT city, toStartOfHour(placed_at) AS h, count(), sum(total_vnd)
    FROM food_delivery.raw_orders
    WHERE placed_at >= now() - INTERVAL 7 DAY
    GROUP BY city, h ORDER BY h DESC
"""


def run_query(_):
    client = Client("localhost", port=9900)
    t0 = time.perf_counter()
    client.execute(QUERY)
    return (time.perf_counter() - t0) * 1000  # ms


print(f"{'N':>4}  {'P50 (ms)':>10}  {'P95 (ms)':>10}  {'Max (ms)':>10}  {'Queries':>8}")
for n in [1, 5, 10]:
    results = []
    for _ in range(3):  # 3 rounds per concurrency level
        with concurrent.futures.ThreadPoolExecutor(max_workers=n) as ex:
            times = list(ex.map(run_query, range(n)))
        results.extend(times)
    results.sort()
    p50 = results[len(results) // 2]
    p95 = results[int(len(results) * 0.95)]
    print(f"{n:>4}  {p50:>10.0f}  {p95:>10.0f}  {max(results):>10.0f}  {len(results):>8}")
