"""Outbox relay: publishes unsent outbox rows to a Redis Stream (at-least-once)."""
import json, os, random, time
import psycopg2, redis

STREAM = "orders"
CRASH_PROB = float(os.getenv("RELAY_CRASH_PROB", "0"))
r = redis.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)


def connect():
    while True:
        try:
            return psycopg2.connect(os.environ["DATABASE_URL"])
        except psycopg2.OperationalError:
            time.sleep(1)


conn = connect()
while True:
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id, payload FROM outbox WHERE sent=FALSE "
                    "ORDER BY id LIMIT 50 FOR UPDATE SKIP LOCKED"
                )
                for msg_id, payload in cur.fetchall():
                    r.xadd(STREAM, {"message_id": str(msg_id), "payload": json.dumps(payload)})
                    if random.random() < CRASH_PROB:
                        raise RuntimeError("simulated relay crash before marking sent")
                    cur.execute("UPDATE outbox SET sent=TRUE WHERE id=%s", (msg_id,))
    except RuntimeError:
        pass  # transaction rolled back: rows stay unsent and are published again
    except psycopg2.Error:
        conn = connect()
    time.sleep(0.2)
