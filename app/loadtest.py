"""Load generator and result checker. Sends orders through the gateway."""
import os, statistics, threading, time, uuid
import psycopg2, redis, requests

URL = os.getenv("ORDER_URL", "http://gateway:8000")
N = int(os.getenv("ORDERS", "200"))
DUPS = int(os.getenv("DUPLICATES", "3"))        # concurrent identical requests per order (double clicks)
RETRIES = int(os.getenv("RETRIES", "3"))
DRAIN = int(os.getenv("DRAIN_SECONDS", "15"))
CRASH_MIDWAY = os.getenv("CRASH_MIDWAY", "0") == "1"   # crash order-service-1 while the test is running

db = psycopg2.connect(os.environ["DATABASE_URL"])
db.autocommit = True
rd = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)

for _ in range(40):
    try:
        mode = requests.get(URL + "/gateway/status", timeout=2).json()["mode"]
        requests.get(URL + "/health", timeout=2).raise_for_status()
        break
    except Exception:
        time.sleep(1)
else:
    raise SystemExit("gateway or order services not reachable")

with db.cursor() as cur:
    cur.execute("TRUNCATE orders, idempotency_keys, duplicate_attempts, outbox, payments, inbox RESTART IDENTITY")
    cur.execute("UPDATE products SET stock=initial_stock")
for k in rd.keys("idem:*"):
    rd.delete(k)
rd.delete("orders")
time.sleep(2)  # let workers recreate the consumer group

latencies, failed, lock = [], [0], threading.Lock()


def send(req_id):
    body = {"customer": "load", "product_id": 99, "quantity": 1}
    for _ in range(RETRIES + 1):
        t0 = time.time()
        try:
            resp = requests.post(URL + "/orders", json=body, headers={"Idempotency-Key": req_id}, timeout=8)
            ok = resp.status_code < 300
        except requests.RequestException:
            ok = False
        with lock:
            latencies.append((time.time() - t0) * 1000)
        if ok:
            return
    with lock:
        failed[0] += 1


def crash_later():
    time.sleep(1.0)
    try:
        requests.post(URL + "/admin/crash/1", timeout=3)
        print("-- crashed order-service-1 (docker restarts it)")
    except requests.RequestException:
        pass


if CRASH_MIDWAY:
    threading.Thread(target=crash_later, daemon=True).start()

start, threads = time.time(), []
for _ in range(N):
    rid = str(uuid.uuid4())
    for _ in range(DUPS):
        t = threading.Thread(target=send, args=(rid,))
        t.start()
        threads.append(t)
    if len(threads) > 120:
        for t in threads:
            t.join()
        threads = []
for t in threads:
    t.join()
elapsed = time.time() - start
time.sleep(DRAIN)  # let relay and workers drain, including redeliveries

with db.cursor() as cur:
    cur.execute("SELECT count(*), count(DISTINCT client_request_id) FROM orders")
    orders, distinct_orders = cur.fetchone()
    cur.execute("SELECT count(*), count(DISTINCT o.client_request_id) FROM payments p JOIN orders o ON o.id=p.order_id")
    payments, distinct_paid = cur.fetchone()
    cur.execute("SELECT count(*) FROM duplicate_attempts")
    blocked = cur.fetchone()[0]
    cur.execute("SELECT initial_stock - stock FROM products WHERE id=99")
    stock_used = cur.fetchone()[0]

p95 = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies)
print(f"\n=== RESULT (MODE={mode}, crash={CRASH_MIDWAY}) ===")
print(f"logical orders sent     : {N} (x{DUPS} concurrent duplicates)")
print(f"duplicate orders        : {orders - distinct_orders}")
print(f"duplicate charges       : {payments - distinct_paid}")
print(f"missing charges         : {N - distinct_paid}")
print(f"excess stock deducted   : {stock_used - N}")
print(f"duplicates blocked+logged: {blocked}")
print(f"requests that failed    : {failed[0]}")
print(f"p95 latency (ms)        : {p95:.1f}")
print(f"throughput (orders/s)   : {N / elapsed:.1f}")
