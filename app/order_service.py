"""Order service (run as 2 replicas behind the gateway).

MODE=baseline : no deduplication (for comparison).
MODE=proposed : layer 1 Redis fast path, layer 2 atomic key + stock + order + outbox in ONE transaction,
                request fingerprint (same key with different data -> 422), audit log of blocked duplicates.
"""
import hashlib, json, os, random, socket, threading, time
import psycopg2.pool
import redis
from flask import Flask, jsonify, request
from waitress import serve

MODE = os.getenv("MODE", "proposed")
DROP_RESPONSE_PROB = float(os.getenv("DROP_RESPONSE_PROB", "0"))
SERVER = socket.gethostname()
VALID_STATUSES = ("PLACED", "PAID", "SHIPPED", "CANCELLED")
state = {"delay_ms": 0}

app = Flask(__name__)
pool = psycopg2.pool.ThreadedConnectionPool(2, 40, os.environ["DATABASE_URL"])
cache = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


class OutOfStock(Exception):
    pass


class UnknownProduct(Exception):
    pass


def fingerprint(customer, pid, qty):
    return hashlib.sha256(f"{customer}|{pid}|{qty}".encode()).hexdigest()


def record_attempt(order_id, key, kind, customer, pid, qty):
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO duplicate_attempts(order_id,idempotency_key,kind,customer,product_id,quantity,server) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)", (order_id, key, kind, customer, pid, qty, SERVER))
    finally:
        pool.putconn(conn)


def query(sql, args=()):
    conn = pool.getconn()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, args)
            cols = [c[0] for c in cur.description] if cur.description else []
            return [dict(zip(cols, r)) for r in cur.fetchall()] if cols else []
    finally:
        pool.putconn(conn)


@app.before_request
def simulated_slow_network():
    if state["delay_ms"] > 0 and not request.path.startswith("/admin") and request.path != "/health":
        time.sleep(state["delay_ms"] / 1000.0)


@app.get("/health")
def health():
    return jsonify(status="ok", mode=MODE, server=SERVER)


@app.get("/products")
def products():
    return jsonify(query("SELECT id,name,price,stock FROM products WHERE id<>99 ORDER BY id"))


@app.get("/orders")
def orders():
    customer = request.args.get("customer")
    rows = query(
        "SELECT o.id, o.client_request_id AS key, o.customer, p.name AS item, o.quantity, o.status, o.version, "
        "o.created_at::text AS created_at, (SELECT count(*) FROM duplicate_attempts d WHERE d.order_id=o.id) AS duplicate_attempts "
        "FROM orders o LEFT JOIN products p ON p.id=o.product_id "
        "WHERE (%s IS NULL OR o.customer=%s) ORDER BY o.id DESC LIMIT 50", (customer, customer))
    return jsonify(rows)


@app.get("/duplicates")
def duplicates():
    return jsonify(query(
        "SELECT d.id, d.order_id, d.idempotency_key, d.kind, d.customer, p.name AS item, d.quantity, d.server, "
        "d.attempted_at::text AS attempted_at FROM duplicate_attempts d LEFT JOIN products p ON p.id=d.product_id "
        "ORDER BY d.id DESC LIMIT 50"))


@app.post("/orders")
def create_order():
    body = request.get_json(silent=True)
    key = request.headers.get("Idempotency-Key", "")
    proposed = MODE == "proposed"
    if not isinstance(body, dict):
        return jsonify(error="body must be a JSON object"), 400
    pid, qty = body.get("product_id", 99), body.get("quantity", 1)
    customer = str(body.get("customer", ""))[:40]
    if (not isinstance(pid, int) or not isinstance(qty, int) or isinstance(qty, bool) or qty <= 0
            or (proposed and not key.strip())):
        return jsonify(error="Idempotency-Key header, a product and a positive quantity are required"), 400
    h = fingerprint(customer, pid, qty)

    if proposed:  # Layer 1: Redis fast path (cache only, the database stays the source of truth)
        hit = cache.get("idem:" + key)
        if hit:
            c = json.loads(hit)
            if c["hash"] != h:
                record_attempt(c["order_id"], key, "KEY_REUSED", customer, pid, qty)
                return jsonify(error="Idempotency-Key already used with different order data"), 422
            record_attempt(c["order_id"], key, "DUPLICATE", customer, pid, qty)
            return jsonify(order_id=c["order_id"], duplicate=True), 200

    conn = pool.getconn()
    duplicate, stored_hash, order_id = False, None, None
    try:
        with conn:  # Layer 2: one transaction (commit on exit, rollback on any exception)
            with conn.cursor() as cur:
                if proposed:
                    cur.execute("INSERT INTO idempotency_keys(key,request_hash) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                                (key, h))
                    if cur.rowcount == 0:  # a concurrent twin waits on the primary key until the first commits
                        cur.execute("SELECT order_id, request_hash FROM idempotency_keys WHERE key=%s", (key,))
                        order_id, stored_hash = cur.fetchone()
                        duplicate = True
                if not duplicate:
                    cur.execute("SELECT price FROM products WHERE id=%s", (pid,))
                    row = cur.fetchone()
                    if row is None:
                        raise UnknownProduct()
                    cur.execute("UPDATE products SET stock=stock-%s WHERE id=%s AND stock>=%s", (qty, pid, qty))
                    if cur.rowcount == 0:
                        raise OutOfStock()  # rollback also removes the key, so a later retry is allowed
                    amount = row[0] * qty
                    cur.execute("INSERT INTO orders(client_request_id,customer,product_id,quantity,amount) "
                                "VALUES (%s,%s,%s,%s,%s) RETURNING id", (key, customer, pid, qty, amount))
                    order_id = cur.fetchone()[0]
                    if proposed:
                        cur.execute("UPDATE idempotency_keys SET order_id=%s WHERE key=%s", (order_id, key))
                    cur.execute("INSERT INTO outbox(order_id,payload) VALUES (%s,%s)",
                                (order_id, json.dumps({"order_id": order_id, "amount": float(amount)})))
    except UnknownProduct:
        return jsonify(error="unknown product"), 404
    except OutOfStock:
        return jsonify(error="out of stock"), 409
    finally:
        pool.putconn(conn)

    if duplicate:
        kind = "KEY_REUSED" if stored_hash != h else "DUPLICATE"
        record_attempt(order_id, key, kind, customer, pid, qty)
        if kind == "KEY_REUSED":
            return jsonify(error="Idempotency-Key already used with different order data"), 422
    elif proposed:
        cache.setex("idem:" + key, 3600, json.dumps({"order_id": order_id, "hash": h}))

    if random.random() < DROP_RESPONSE_PROB:
        return jsonify(error="response lost"), 504  # fault injection: committed, but the answer never arrives
    return jsonify(order_id=order_id, duplicate=duplicate), (200 if duplicate else 201)


@app.put("/orders/<int:oid>")
def update_order(oid):
    """Optimistic locking (idea borrowed from OrderSystemPy): succeeds only if the caller saw the latest version."""
    body = request.get_json(silent=True) or {}
    version, status = body.get("version"), body.get("status")
    if not isinstance(version, int) or status not in VALID_STATUSES:
        return jsonify(error="version (int) and a valid status are required"), 400
    rows = query("UPDATE orders SET status=%s, version=version+1 WHERE id=%s AND version=%s RETURNING version",
                 (status, oid, version))
    if rows:
        return jsonify(message="updated", newVersion=rows[0]["version"])
    if not query("SELECT 1 FROM orders WHERE id=%s", (oid,)):
        return jsonify(error="not found"), 404
    return jsonify(error="conflict: stale version, re-read and retry"), 409


@app.post("/admin/reset-stock")
def reset_stock():
    query("UPDATE products SET stock=initial_stock")
    return jsonify(message="stock reset")


@app.post("/admin/delay/<int:ms>")
def set_delay(ms):
    state["delay_ms"] = ms
    return jsonify(delay_ms=ms)


@app.post("/admin/crash")
def crash():
    threading.Timer(0.2, lambda: os._exit(1)).start()  # compose restarts the container: recovery demo
    return jsonify(message="crashing")


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=8000, threads=32)
