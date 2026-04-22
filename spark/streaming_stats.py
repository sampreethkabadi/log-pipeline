#!/usr/bin/env python3
"""
Spark Streaming job: read Kafka logs, compute per-service windowed stats,
write to HDFS as Parquet.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, avg, max as fmax,
    sum as fsum, when, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)


# Schema matching the log generator output
log_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("service", StringType()),
    StructField("level", StringType()),
    StructField("status_code", IntegerType()),
    StructField("latency_ms", IntegerType()),
    StructField("upstream_service", StringType()),
    StructField("message", StringType()),
])

KAFKA_BROKER = "kafka-broker:9092"
TOPICS = "webserver-logs,auth-logs,db-logs"
HDFS_STATS_PATH = "hdfs://spark-master:9000/logs/stats"
CHECKPOINT_PATH = "hdfs://spark-master:9000/checkpoints/stats"


def main():
    spark = (SparkSession.builder
            .appName("LogStatsStreaming")
            .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
            .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")

    # 1. Read from Kafka
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKER)
           .option("subscribe", TOPICS)
           .option("startingOffsets", "latest")
           .load())

    # 2. Parse JSON
    parsed = (raw
              .select(from_json(col("value").cast("string"), log_schema).alias("d"))
              .select("d.*")
              .withColumn("event_time", to_timestamp("timestamp")))

    # 3. Windowed aggregation: 30-sec window, sliding every 10 sec
    stats = (parsed
             .withWatermark("event_time", "1 minute")
             .groupBy(
                 window("event_time", "30 seconds", "10 seconds"),
                 "service"
             )
             .agg(
                 count("*").alias("total"),
                 fsum(when(col("level") == "ERROR", 1).otherwise(0)).alias("errors"),
                 fsum(when(col("level") == "WARN", 1).otherwise(0)).alias("warns"),
                 avg("latency_ms").alias("avg_latency_ms"),
                 fmax("latency_ms").alias("max_latency_ms"),
                 fsum(when(col("status_code") >= 500, 1).otherwise(0)).alias("status_5xx"),
                 fsum(when((col("status_code") >= 400) & (col("status_code") < 500), 1).otherwise(0)).alias("status_4xx"),
             )
             .withColumn("error_rate", col("errors") / col("total"))
             .withColumn("window_start", col("window.start"))
             .withColumn("window_end", col("window.end"))
             .drop("window"))

    # 4. Write to HDFS as Parquet, append mode
    query = (stats.writeStream
             .outputMode("append")
             .format("parquet")
             .option("path", HDFS_STATS_PATH)
             .trigger(processingTime="10 seconds")
             .start())

    # Also write a brief console output for debugging
    debug = (stats.writeStream
             .outputMode("update")
             .format("console")
             .option("truncate", "false")
             .trigger(processingTime="30 seconds")
             .start())

    query.awaitTermination()


if __name__ == "__main__":
    main()
