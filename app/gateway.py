"""Gateway: round-robin load balancer with timeout, failover, circuit breaker and the web shop page.

Idea borrowed from OrderSystemPy. Improvement: retrying a request on another replica is only safe when the
operation is idempotent, so in MODE=proposed the gateway also retries after timeouts and 5xx answers (the same
Idempotency-Key makes the retry harmless). In MODE=baseline it only fails over when the request never reached a server.
"""
import itertools, os, threading, time
import requests
from flask import Flask, Response, request, send_from_directory
from waitress import serve

MODE = os.getenv("MODE", "proposed")
BACKENDS = [b.strip() for b in os.environ["BACKENDS"].split(",")]
TIMEOUT = float(os.getenv("TIMEOUT", "3"))
SAFE_RETRY = MODE == "proposed"
down_until = [0.0] * len(BACKENDS)
counter = itertools.count()
app = Flask(__name__)


def never_reached(err):
    m = str(err).lower()
    return "refused" in m or "resolve" in m or "name or service" in m


@app.get("/")
def index():
    return send_from_directory(os.path.join(os.path.dirname(__file__), "web"), "index.html")


@app.get("/gateway/status")
def status():
    now = time.time()
    return {"mode": MODE, "backends": [{"url": b, "circuit_open": down_until[i] > now} for i, b in enumerate(BACKENDS)]}


@app.route("/admin/crash/<int:n>", methods=["POST"])
@app.route("/admin/delay/<int:n>/<int:ms>", methods=["POST"])
def targeted_admin(n, ms=None):
    if not 1 <= n <= len(BACKENDS):
        return {"error": "no such server"}, 404
    path = "/admin/crash" if ms is None else f"/admin/delay/{ms}"
    try:
        r = requests.post(BACKENDS[n - 1] + path, timeout=2)
        return Response(r.content, r.status_code, {"Content-Type": "application/json"})
    except requests.RequestException as e:
        return {"error": str(e)}, 502


@app.route("/<path:path>", methods=["GET", "POST", "PUT"])
def proxy(path):
    if path.startswith("admin") and path != "admin/reset-stock":
        return {"error": "forbidden"}, 403
    start, now = next(counter), time.time()
    order = [(start + i) % len(BACKENDS) for i in range(len(BACKENDS))]
    candidates = [i for i in order if down_until[i] <= now] + [i for i in order if down_until[i] > now]
    headers = {k: v for k, v in request.headers if k in ("Content-Type", "Idempotency-Key")}
    retry_ok = SAFE_RETRY or request.method == "GET"
    last = None
    for idx in candidates:
        try:
            r = requests.request(request.method, f"{BACKENDS[idx]}/{path}", params=request.args,
                                 data=request.get_data(), headers=headers, timeout=TIMEOUT)
        except requests.RequestException as e:
            down_until[idx] = time.time() + 5  # circuit breaker: skip this server for 5 s
            if never_reached(e) or retry_ok:
                continue
            return {"error": "upstream timeout or reset"}, 504
        down_until[idx] = 0.0
        last = Response(r.content, r.status_code, {"Content-Type": r.headers.get("Content-Type", "application/json"),
                                                   "X-Served-By": BACKENDS[idx]})
        if r.status_code in (502, 503, 504) and retry_ok and idx != candidates[-1]:
            continue
        return last
    return last or ({"error": "all servers unavailable"}, 503)


if __name__ == "__main__":
    serve(app, host="0.0.0.0", port=8000, threads=64)
