import os
import socket
import time
from flask import Flask, jsonify, make_response, Response
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from redis.exceptions import RedisError
from redis.sentinel import Sentinel, MasterNotFoundError

app = Flask(__name__)

NODE_NAME = os.getenv("NODE_NAME", "unknown")
TASK_SLOT = os.getenv("TASK_SLOT", "0")
CONTAINER_ID = socket.gethostname()
REDIS_MASTER_NAME = os.getenv("REDIS_MASTER_NAME", "mymaster")
SENTINEL_PORT = int(os.getenv("SENTINEL_PORT", "26379"))
SENTINEL_HOSTS = [
    (h.strip(), SENTINEL_PORT)
    for h in os.getenv("SENTINEL_HOSTS", "").split(",")
    if h.strip()
]

sentinel = Sentinel(
    SENTINEL_HOSTS,
    socket_timeout=1.5,
    socket_connect_timeout=1.5,
)

HTTP_REQUESTS = Counter(
    "demo_app_http_requests_total",
    "Total HTTP requests handled by demo_app",
    ["endpoint", "status", "node"],
)
HTTP_LATENCY = Histogram(
    "demo_app_http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["endpoint", "node"],
)
REDIS_ERRORS = Counter(
    "demo_app_redis_errors_total",
    "Total Redis/Sentinel errors encountered by demo_app",
    ["node"],
)
REDIS_HITS = Gauge(
    "demo_app_visits_current",
    "Last observed visit counter value from Redis",
)


def get_master():
    return sentinel.master_for(
        REDIS_MASTER_NAME,
        socket_timeout=1.5,
        socket_connect_timeout=1.5,
        decode_responses=True,
    )


def build_response(payload, status_code=200):
    resp = make_response(jsonify(payload), status_code)
    resp.headers["X-Handled-By"] = f"{NODE_NAME}/slot-{TASK_SLOT}/{CONTAINER_ID}"
    return resp


@app.route("/")
def index():
    start = time.monotonic()
    try:
        master_addr = sentinel.discover_master(REDIS_MASTER_NAME)
        visits = get_master().incr("visits")
        REDIS_HITS.set(visits)
        HTTP_REQUESTS.labels("/", "200", NODE_NAME).inc()
        return build_response(
            {
                "status": "ok",
                "visits": visits,
                "node": NODE_NAME,
                "task_slot": TASK_SLOT,
                "container": CONTAINER_ID,
                "redis_master": f"{master_addr[0]}:{master_addr[1]}",
            },
            200,
        )
    except (RedisError, MasterNotFoundError, OSError) as exc:
        REDIS_ERRORS.labels(NODE_NAME).inc()
        HTTP_REQUESTS.labels("/", "503", NODE_NAME).inc()
        return build_response(
            {
                "status": "degraded",
                "error": str(exc),
                "node": NODE_NAME,
                "task_slot": TASK_SLOT,
                "container": CONTAINER_ID,
            },
            503,
        )
    finally:
        HTTP_LATENCY.labels("/", NODE_NAME).observe(time.monotonic() - start)


@app.route("/live")
def live():
    return build_response({"status": "alive", "node": NODE_NAME}, 200)


@app.route("/health")
def health():
    try:
        master_addr = sentinel.discover_master(REDIS_MASTER_NAME)
        get_master().ping()
        HTTP_REQUESTS.labels("/health", "200", NODE_NAME).inc()
        return build_response(
            {
                "status": "healthy",
                "node": NODE_NAME,
                "redis_master": f"{master_addr[0]}:{master_addr[1]}",
            },
            200,
        )
    except (RedisError, MasterNotFoundError, OSError) as exc:
        REDIS_ERRORS.labels(NODE_NAME).inc()
        HTTP_REQUESTS.labels("/health", "503", NODE_NAME).inc()
        return build_response({"status": "unhealthy", "error": str(exc)}, 503)


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)