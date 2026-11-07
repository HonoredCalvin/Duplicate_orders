# Duplicate Order Processing: Prototype (v2)

Layered, fault-tolerant deduplication for an online shop order pipeline.

```
Browser / load test -> Gateway :8080 -> order-service-1 \
                       (round robin,    order-service-2  -> PostgreSQL (orders, keys, stock, outbox, audit)
                        failover,                          Redis (key cache + Streams)
                        circuit breaker)          relay -> Redis Streams -> payment-worker x2 (inbox) -> PAID
```

## Run
Needs Docker. In this folder:

    ./run_experiments.sh          # S1-S4, baseline vs proposed, prints result blocks
    MODE=proposed docker compose up -d --build   # then open http://localhost:8080 (web shop)
    docker compose --profile tools run --rm loadtest
    docker compose down -v

Scenarios: S1 duplicate requests; S2 + lost responses; S3 + relay crashes and missing acks; S4 = S3 + order-service-1 crashed mid-test.

## What changed in v2 (ideas borrowed from the uploaded OrderSystemPy project, then improved)
| Idea from OrderSystemPy | How it is used here | Improvement |
|---|---|---|
| Gateway with load balancing, 3 s timeout, failover, 5 s circuit breaker | `gateway.py` in front of 2 order replicas | Retries after timeouts and 5xx only when the request carries an idempotency key (safe retry); baseline mode retries only if the request never reached a server |
| `duplicate_attempts` audit table, DUPLICATE vs KEY_REUSED | Same table, written on both the Redis fast path and the database path | Works across replicas and survives restarts |
| Same key with different data is rejected (422) | SHA-256 fingerprint of buyer, product, quantity | Checked in Redis and Postgres |
| Order and stock updated in one transaction | Key, stock, order and outbox event in one Postgres transaction | Row-level locks instead of a global SQLite write lock; unique key makes concurrent twins wait, not fail |
| Optimistic locking on updates (version column) | `PUT /orders/<id>` | Worker moves PLACED to PAID with a version bump |
| Crash and slow-server demo buttons, web shop, one-click start | Web shop at :8080, `/admin/crash/<n>`, `/admin/delay/<n>/<ms>`, restart policy heals crashes | Same demos, plus a duplicate-charge check downstream |
| Shared SQLite file was a single point of failure (their own note) | PostgreSQL service | Still one Postgres instance; a replicated cluster is future work |

New metrics: excess stock deducted, duplicates blocked and logged, requests that failed.

## Files
- app/gateway.py, app/order_service.py, app/relay.py, app/payment_worker.py, app/loadtest.py, app/web/index.html
- db/init.sql, docker-compose.yml, run_experiments.sh

Copy the printed numbers into Section 8 of the report. Do not invent figures.
