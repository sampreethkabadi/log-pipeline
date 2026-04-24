#!/usr/bin/env python3
"""
Injection controller: orchestrate cascade scenarios for testing.

Usage:
  python3 inject.py reset           # restore normal
  python3 inject.py auth-fail       # auth starts failing
  python3 inject.py db-fail         # db starts failing (different scenario)
  python3 inject.py cascade         # full scripted cascade scenario
"""

import json
import sys
import time

INJECTION_FILE = "/tmp/injection_state.json"


def write_state(state):
    with open(INJECTION_FILE, "w") as f:
        json.dump(state, f, indent=2)
    print(f"Wrote injection state: {state}")


def normal():
    return {
        "web":  {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
        "auth": {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
        "db":   {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
    }


SCENARIOS = {
    "reset": normal(),
    "auth-fail": {
        "web":  {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
        "auth": {"error_rate_multiplier": 8.0, "latency_multiplier": 4.0},
        "db":   {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
    },
    "db-fail": {
        "web":  {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
        "auth": {"error_rate_multiplier": 2.0, "latency_multiplier": 2.0},
        "db":   {"error_rate_multiplier": 10.0, "latency_multiplier": 5.0},
    },
    "web-fail": {
        "web":  {"error_rate_multiplier": 6.0, "latency_multiplier": 3.0},
        "auth": {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
        "db":   {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
    },
}


def run_cascade():
    """Scripted cascade: auth degrades, then propagates."""
    print("Cascade scenario starting...")
    print("t=0s: auth starts degrading (error_rate 5x, latency 3x)")
    write_state({
        "web":  {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
        "auth": {"error_rate_multiplier": 5.0, "latency_multiplier": 3.0},
        "db":   {"error_rate_multiplier": 1.0, "latency_multiplier": 1.0},
    })
    time.sleep(20)
    print("t=20s: cascade — db starts seeing retry storm, web starts errors")
    write_state({
        "web":  {"error_rate_multiplier": 3.0, "latency_multiplier": 2.0},
        "auth": {"error_rate_multiplier": 8.0, "latency_multiplier": 4.0},
        "db":   {"error_rate_multiplier": 2.0, "latency_multiplier": 1.5},
    })
    time.sleep(60)
    print("t=80s: recovery")
    write_state(normal())
    print("Cascade scenario complete")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "cascade":
        run_cascade()
    elif cmd in SCENARIOS:
        write_state(SCENARIOS[cmd])
    else:
        print(f"Unknown scenario: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()

