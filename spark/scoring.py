#!/usr/bin/env python3
"""
Root cause scoring module.

Pure Python — no Spark dependencies. Easy to unit test.

Input: per-window stats per service, plus a rolling history of recent windows.
Output: a score per service per window, identifying root cause candidates.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
import yaml


@dataclass
class WindowStats:
    """Stats for one service in one time window."""
    service: str
    window_start: str
    total: int
    errors: int
    error_rate: float
    avg_latency_ms: float
    max_latency_ms: float


@dataclass
class Signals:
    """The 5 signals we score on."""
    persistence: float    # 0-1: fraction of last N windows where this service was anomalous
    upstream: float       # 0-1: from topology config
    propagation: float    # 0-1: fraction of downstream services also anomalous
    magnitude: float      # 0-1: scaled max z-score across error/latency/throughput
    multi_signal: float   # 0-1: how many of (error_rate, latency, throughput) are simultaneously off


@dataclass
class ScoreResult:
    service: str
    window_start: str
    score: float          # 0-1 final score
    signals: Signals
    is_root_cause_candidate: bool


def load_config(path="spark/topology.yaml"):
    with open(path) as f:
        return yaml.safe_load(f)


def zscore(value, mean, std):
    """Standard z-score with safety for zero std."""
    if std is None or std < 0.0001:
        return 0.0
    return (value - mean) / std


def is_anomalous(stats: WindowStats, baseline: dict, thresholds: dict) -> bool:
    """Is this window anomalous on ANY of the three primary metrics?"""
    er_z = zscore(stats.error_rate, baseline.get("error_rate_mean", 0.05),
                  baseline.get("error_rate_std", 0.02))
    lat_z = zscore(stats.avg_latency_ms, baseline.get("latency_mean", 30),
                   baseline.get("latency_std", 10))
    thr_z = zscore(stats.total, baseline.get("throughput_mean", 100),
                   baseline.get("throughput_std", 20))
    return (er_z > thresholds["error_rate_zscore"] or
            lat_z > thresholds["latency_zscore"] or
            abs(thr_z) > thresholds["throughput_zscore"])


def compute_signals(
    stats: WindowStats,
    history: List[WindowStats],
    downstream_anomalies: Dict[str, bool],
    config: dict,
    baseline: dict,
) -> Signals:
    """Compute the 5 signals for one service in one window."""
    th = config["thresholds"]
    n_persistence = th["min_persistence_windows"]

    # Persistence: of the last N windows, how many were anomalous?
    recent = history[-n_persistence:] if len(history) >= n_persistence else history
    if recent:
        anomalous_count = sum(1 for s in recent if is_anomalous(s, baseline, th))
        persistence = anomalous_count / len(recent)
    else:
        persistence = 0.0

    # Upstream: from config
    upstream = config["services"][stats.service]["upstream_score"]

    # Propagation: fraction of downstreams also anomalous
    deps = config["services"][stats.service]["depends_on"]
    if deps:
        affected = sum(1 for d in deps if downstream_anomalies.get(d, False))
        propagation = affected / len(deps)
    else:
        propagation = 0.0  # leaf service; can't propagate anywhere

    # Magnitude: max z-score, clipped to [0, 1] via sigmoid-ish scaling
    er_z = zscore(stats.error_rate, baseline.get("error_rate_mean", 0.05),
                  baseline.get("error_rate_std", 0.02))
    lat_z = zscore(stats.avg_latency_ms, baseline.get("latency_mean", 30),
                   baseline.get("latency_std", 10))
    thr_z = abs(zscore(stats.total, baseline.get("throughput_mean", 100),
                       baseline.get("throughput_std", 20)))
    max_z = max(er_z, lat_z, thr_z)
    # Scale: z=2.5 -> 0.5, z=5 -> 0.83, z=10 -> 0.95
    magnitude = max_z / (max_z + 5) if max_z > 0 else 0.0

    # Multi-signal: how many metrics are anomalous?
    metrics_anomalous = sum([
        er_z > th["error_rate_zscore"],
        lat_z > th["latency_zscore"],
        abs(thr_z) > th["throughput_zscore"],
    ])
    multi_signal = metrics_anomalous / 3

    return Signals(persistence, upstream, propagation, magnitude, multi_signal)


def score(signals: Signals, weights: dict) -> float:
    """Weighted sum of signals, returns score in [0, 1]."""
    return (
        signals.persistence  * weights["persistence"] +
        signals.upstream     * weights["upstream"] +
        signals.propagation  * weights["propagation"] +
        signals.magnitude    * weights["magnitude"] +
        signals.multi_signal * weights["multi_signal"]
    )


def score_window(
    current_window_stats: Dict[str, WindowStats],
    history_per_service: Dict[str, List[WindowStats]],
    baseline_per_service: Dict[str, dict],
    config: dict,
) -> List[ScoreResult]:
    """Score all services for one time window. Returns list sorted by score desc."""
    th = config["thresholds"]

    # First pass: which services are anomalous right now?
    anomalous = {
        svc: is_anomalous(stats, baseline_per_service.get(svc, {}), th)
        for svc, stats in current_window_stats.items()
    }

    # Second pass: score each service
    results = []
    for svc, stats in current_window_stats.items():
        baseline = baseline_per_service.get(svc, {})
        history = history_per_service.get(svc, [])
        signals = compute_signals(stats, history, anomalous, config, baseline)
        score_value = score(signals, config["weights"])
        results.append(ScoreResult(
            service=svc,
            window_start=stats.window_start,
            score=score_value,
            signals=signals,
            is_root_cause_candidate=anomalous[svc] and score_value > 0.5,
        ))

    # Sort by score descending — highest score is the prime suspect
    results.sort(key=lambda r: r.score, reverse=True)
    return results
