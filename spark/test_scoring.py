#!/usr/bin/env python3
"""Quick smoke test for scoring module."""

from scoring import (
    WindowStats, load_config, score_window
)

# Pretend topology config (instead of loading the YAML)
config = {
    "services": {
        "web":  {"depends_on": ["auth"], "upstream_score": 1.0},
        "auth": {"depends_on": ["db"],   "upstream_score": 0.7},
        "db":   {"depends_on": [],       "upstream_score": 0.3},
    },
    "weights": {"persistence": 0.20, "upstream": 0.20,
                "propagation": 0.25, "magnitude": 0.20, "multi_signal": 0.15},
    "thresholds": {"error_rate_zscore": 2.5, "latency_zscore": 2.5,
                   "throughput_zscore": 2.5, "min_persistence_windows": 2},
}

# Baseline: each service's normal metrics
baselines = {
    "web":  {"error_rate_mean": 0.05, "error_rate_std": 0.02,
             "latency_mean": 20, "latency_std": 5,
             "throughput_mean": 100, "throughput_std": 10},
    "auth": {"error_rate_mean": 0.05, "error_rate_std": 0.02,
             "latency_mean": 30, "latency_std": 8,
             "throughput_mean": 100, "throughput_std": 10},
    "db":   {"error_rate_mean": 0.05, "error_rate_std": 0.02,
             "latency_mean": 10, "latency_std": 3,
             "throughput_mean": 100, "throughput_std": 10},
}

# Cascade scenario: auth is the actual root cause.
# auth: high errors + high latency.
# web: errors propagating from auth (ERROR appears in web because auth failed).
# db: stays normal.
current = {
    "web":  WindowStats("web",  "t1", total=100, errors=15, error_rate=0.15,
                       avg_latency_ms=80,  max_latency_ms=400),
    "auth": WindowStats("auth", "t1", total=100, errors=40, error_rate=0.40,
                       avg_latency_ms=120, max_latency_ms=500),
    "db":   WindowStats("db",   "t1", total=100, errors=5,  error_rate=0.05,
                       avg_latency_ms=11,  max_latency_ms=30),
}

# Pretend auth and web have been like this for 2 windows already
auth_history = [current["auth"], current["auth"]]
web_history  = [current["web"],  current["web"]]
db_history   = [current["db"]]

results = score_window(
    current_window_stats=current,
    history_per_service={"web": web_history, "auth": auth_history, "db": db_history},
    baseline_per_service=baselines,
    config=config,
)

print(f"{'Service':<8} {'Score':<8} Pers Up   Prop Mag  Multi  Anom?")
print("-" * 65)
for r in results:
    s = r.signals
    print(f"{r.service:<8} {r.score:<8.3f} {s.persistence:.2f} {s.upstream:.2f} "
          f"{s.propagation:.2f} {s.magnitude:.2f} {s.multi_signal:.2f}   "
          f"{r.is_root_cause_candidate}")
