"""Payment consumer. MODE=proposed uses an inbox table (layer 3); baseline charges every delivery."""
import json, os, random, socket, time
import psycopg2, redis

MODE = os.getenv("MODE", "proposed")
NO_ACK_PROB = float(os.getenv("WORKER_NO_ACK_PROB", "0"))
STREAM, GROUP, CONSUMER = "orders", "payments", socket.gethostname()
r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


def connect():
    while True:
        try:
            return psycopg2.connect(os.environ["DATABASE_URL"])
        except psycopg2.OperationalError:
            time.sleep(1)


def ensure_group():
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


def process(conn, fields):
    payload = json.loads(fields["payload"])
    with conn:
        with conn.cursor() as cur:
            if MODE == "proposed":
                cur.execute(
                    "INSERT INTO inbox(message_id) VALUES (%s) ON CONFLICT DO NOTHING",
                    (fields["message_id"],),
                )
                if cur.rowcount == 0:
                    return  # already handled: skip
            cur.execute(
                "INSERT INTO payments(order_id, amount) VALUES (%s,%s)",
                (payload["order_id"], payload["amount"]),
            )
            # order lifecycle: PLACED -> PAID (version bump keeps optimistic locking consistent)
            cur.execute("UPDATE orders SET status='PAID', version=version+1 WHERE id=%s AND status='PLACED'",
                        (payload["order_id"],))


def handle(conn, messages):
    for mid, fields in messages:
        process(conn, fields)
        if random.random() < NO_ACK_PROB:
            continue  # fault injection: no ack, message will be redelivered
        r.xack(STREAM, GROUP, mid)


conn = connect()
ensure_group()
while True:
    try:
        resp = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=10, block=1000)
        for _, messages in resp or []:
            handle(conn, messages)
        claimed = r.xautoclaim(STREAM, GROUP, CONSUMER, min_idle_time=3000, start_id="0-0", count=10)
        handle(conn, claimed[1])
    except redis.ResponseError as e:
        if "NOGROUP" in str(e):
            ensure_group()
        else:
            raise
    except psycopg2.Error:
        conn = connect()
