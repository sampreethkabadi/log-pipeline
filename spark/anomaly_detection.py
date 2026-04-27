#!/usr/bin/env python3
"""
Streaming anomaly detection: read Kafka, compute stats, score, write anomalies.

Implements per-batch scoring using forEachBatch since the scoring algorithm
requires Python state (history per service) that's easier to manage outside SQL.
"""

import json
from collections import defaultdict, deque
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, avg, max as fmax,
    sum as fsum, when
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from scoring import WindowStats, score_window, load_config


KAFKA_BROKER = "kafka-broker:9092"
TOPICS = "webserver-logs,auth-logs,db-logs"
HDFS_ANOMALIES_PATH = "hdfs://spark-master:9000/logs/anomalies"
CHECKPOINT_PATH = "hdfs://spark-master:9000/checkpoints/anomalies"

log_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("service", StringType()),
    StructField("level", StringType()),
    StructField("status_code", IntegerType()),
    StructField("latency_ms", IntegerType()),
    StructField("upstream_service", StringType()),
    StructField("message", StringType()),
])

# Module-level state — Spark driver keeps this across batches.
# History per service: rolling window of recent WindowStats.
HISTORY = defaultdict(lambda: deque(maxlen=20))
# Baselines: rolling means/stds per service. Updated as we go.
BASELINES = {}
CONFIG = None


def update_baseline(service, history):
    """Compute simple rolling mean/std from history for baseline reference."""
    if len(history) < 5:
        return  # not enough data yet
    er = [h.error_rate for h in history]
    lat = [h.avg_latency_ms for h in history]
    thr = [h.total for h in history]
    BASELINES[service] = {
        "error_rate_mean": sum(er) / len(er),
        "error_rate_std": (sum((x - sum(er)/len(er))**2 for x in er) / len(er)) ** 0.5,
        "latency_mean": sum(lat) / len(lat),
        "latency_std": (sum((x - sum(lat)/len(lat))**2 for x in lat) / len(lat)) ** 0.5,
        "throughput_mean": sum(thr) / len(thr),
        "throughput_std": (sum((x - sum(thr)/len(thr))**2 for x in thr) / len(thr)) ** 0.5,
    }


WARMUP_BATCHES = 10  # ignore first 10 batches

def process_batch(batch_df, batch_id):
    if batch_id < WARMUP_BATCHES:
        print(f"[warmup] Batch {batch_id} skipped — building baseline")
        # Still update history but don't score or write
        rows = batch_df.collect()
        for row in rows:
            if row["service"]:
                stats = WindowStats(
                    service=row["service"],
                    window_start=str(row["window_start"]),
                    total=row["total"] or 0,
                    errors=row["errors"] or 0,
                    error_rate=float(row["error_rate"] or 0),
                    avg_latency_ms=float(row["avg_latency_ms"] or 0),
                    max_latency_ms=int(row["max_latency_ms"] or 0),
                )
                HISTORY[stats.service].append(stats)
                update_baseline(stats.service, list(HISTORY[stats.service]))
        return

    rows = batch_df.collect()
    if not rows:
        return

    # Group by window_start: each window may have multiple services
    by_window = defaultdict(dict)
    for row in rows:
        win_start = str(row["window_start"])
        stats = WindowStats(
            service=row["service"],
            window_start=win_start,
            total=row["total"] or 0,
            errors=row["errors"] or 0,
            error_rate=float(row["error_rate"] or 0),
            avg_latency_ms=float(row["avg_latency_ms"] or 0),
            max_latency_ms=int(row["max_latency_ms"] or 0),
        )
        by_window[win_start][row["service"]] = stats

    # Score each window
    output = []
    for win_start, services_stats in by_window.items():
        # Update history first (history excludes the window we are scoring)
        for svc, stats in services_stats.items():
            HISTORY[svc].append(stats)
            update_baseline(svc, list(HISTORY[svc]))

        results = score_window(
            current_window_stats=services_stats,
            history_per_service={s: list(HISTORY[s]) for s in services_stats},
            baseline_per_service=BASELINES,
            config=CONFIG,
        )

        # Print to console for live observation
        print(f"\n=== Window {win_start} | Batch {batch_id} ===")
        for r in results:
            marker = "  *ROOT*" if r.is_root_cause_candidate else ""
            print(f"  {r.service:<6} score={r.score:.3f}  "
                  f"pers={r.signals.persistence:.2f} up={r.signals.upstream:.2f} "
                  f"prop={r.signals.propagation:.2f} mag={r.signals.magnitude:.2f} "
                  f"multi={r.signals.multi_signal:.2f}{marker}")

        # Save all scored results, not just root candidates — so dashboard can show ranked list
        for r in results:
            output.append({
                "window_start": win_start,
                "service": r.service,
                "score": r.score,
                "persistence": r.signals.persistence,
                "upstream": r.signals.upstream,
                "propagation": r.signals.propagation,
                "magnitude": r.signals.magnitude,
                "multi_signal": r.signals.multi_signal,
                "is_root_cause_candidate": r.is_root_cause_candidate,
            })

    # Write to HDFS as JSON lines (one file per batch)
    if output:
        out_df = batch_df.sparkSession.createDataFrame(output)
        (out_df.write
            .mode("append")
            .json(HDFS_ANOMALIES_PATH))


def main():
    global CONFIG
    CONFIG = load_config("/home/exouser/pipeline/spark/topology.yaml")

    spark = (SparkSession.builder
            .appName("AnomalyDetection")
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
            .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKER)
           .option("subscribe", TOPICS)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw
              .select(from_json(col("value").cast("string"), log_schema).alias("d"))
              .select("d.*")
              .withColumn("event_time", to_timestamp("timestamp")))

    stats = (parsed
             .withWatermark("event_time", "1 minute")
             .groupBy(
                 window("event_time", "20 seconds", "5 seconds"),
                 "service"
             )
             .agg(
                 count("*").alias("total"),
                 fsum(when(col("level") == "ERROR", 1).otherwise(0)).alias("errors"),
                 avg("latency_ms").alias("avg_latency_ms"),
                 fmax("latency_ms").alias("max_latency_ms"),
             )
             .withColumn("error_rate", col("errors") / col("total"))
             .withColumn("window_start", col("window.start")))

    query = (stats.writeStream
             .outputMode("update")
             .foreachBatch(process_batch)
             .trigger(processingTime="5 seconds")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
