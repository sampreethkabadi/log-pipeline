#!/usr/bin/env python3
"""
Log generator for the distributed log pipeline.
Produces fake microservice logs to Kafka topics.

Usage:
  python3 log_generator.py --broker kafka-broker:9092 --rate 100
"""

import json
import time
import random
import argparse
import threading
from datetime import datetime, timezone
from kafka import KafkaProducer

# Service topology: web -> auth -> db
TOPOLOGY = {
    "web":  {"topic": "webserver-logs", "upstream": None,   "base_latency": 20},
    "auth": {"topic": "auth-logs",      "upstream": "web",  "base_latency": 30},
    "db":   {"topic": "db-logs",        "upstream": "auth", "base_latency": 10},
}

# Realistic message templates per service
MESSAGES = {
    "web":  ["GET /api/orders", "POST /api/login", "GET /api/menu", "GET /healthz"],
    "auth": ["verify token", "issue session", "refresh token", "check permissions"],
    "db":   ["SELECT users", "SELECT orders", "UPDATE sessions", "INSERT audit_log"],
}

# Normal level distribution: 85% INFO, 10% WARN, 5% ERROR
LEVELS = ["INFO"] * 85 + ["WARN"] * 10 + ["ERROR"] * 5

# Injection state — set by the injection controller to simulate failures
INJECTION_FILE = "/tmp/injection_state.json"
INJECTION_STATE = {
    "web":  {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
    "auth": {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
    "db":   {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
}


def reload_injection_state():
    """Re-read injection file every 2 seconds in a background thread."""
    global INJECTION_STATE
    last_mtime = 0
    while True:
        try:
            import os
            if os.path.exists(INJECTION_FILE):
                mt = os.path.getmtime(INJECTION_FILE)
                if mt != last_mtime:
                    with open(INJECTION_FILE) as f:
                        new_state = json.load(f)
                    INJECTION_STATE.update(new_state)
                    last_mtime = mt
                    print(f"[injection] Reloaded: {INJECTION_STATE}")
        except Exception as e:
            print(f"[injection] reload error: {e}")
        time.sleep(2)



def make_record(service):
    """Build one fake log record for the given service."""
    cfg = TOPOLOGY[service]
    state = INJECTION_STATE[service]

    # Decide the severity level, considering any injection
    if random.random() < 0.05 * state["error_rate_multiplier"]:
        level = "ERROR"
    elif random.random() < 0.10:
        level = "WARN"
    else:
        level = "INFO"

    # Latency: base + noise, scaled by any latency injection
    base = cfg["base_latency"]
    latency = random.gauss(base, base * 0.3) * state["latency_multiplier"]
    latency = max(1, int(latency))  # at least 1ms

    # Status code derived from level
    if level == "ERROR":
        status_code = random.choice([500, 502, 503, 504])
    elif level == "WARN":
        status_code = random.choice([400, 404, 429])
    else:
        status_code = 200

    return {
        "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "service": service,
        "level": level,
        "status_code": status_code,
        "latency_ms": latency,
        "upstream_service": cfg["upstream"],
        "message": random.choice(MESSAGES[service]),
    }


def run_service(service, producer, rate):
    """Produce messages for one service at the given rate per second."""
    cfg = TOPOLOGY[service]
    interval = 1.0 / rate if rate > 0 else 1.0
    sent = 0
    last_print = time.time()

    print(f"[{service}] starting at {rate} events/sec -> topic {cfg['topic']}")

    while True:
        record = make_record(service)
        producer.send(cfg["topic"], json.dumps(record).encode("utf-8"))
        sent += 1

        # Print status every 5 seconds
        now = time.time()
        if now - last_print >= 5:
            print(f"[{service}] sent {sent} total, current rate ~{sent / (now - last_print + 0.001):.0f}/s")
            sent = 0
            last_print = now

        time.sleep(interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default="kafka-broker:9092",
                        help="Kafka bootstrap server")
    parser.add_argument("--rate", type=int, default=100,
                        help="Total events/sec across all 3 services")
    args = parser.parse_args()

    # Split rate across 3 services
    per_service_rate = max(1, args.rate // 3)

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        linger_ms=10,           # batch up to 10ms for efficiency
        batch_size=16384,       # 16KB batches
        compression_type="gzip" # reduce network load
    )

    print(f"Generator starting. Total rate: {args.rate}/s "
          f"({per_service_rate}/s per service). Broker: {args.broker}")
    threading.Thread(target=reload_injection_state, daemon=True).start()
    # Start 3 threads, one per service
    threads = []
    for service in TOPOLOGY.keys():
        t = threading.Thread(
            target=run_service,
            args=(service, producer, per_service_rate),
            daemon=True
        )
        t.start()
        threads.append(t)

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nShutting down...")
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
