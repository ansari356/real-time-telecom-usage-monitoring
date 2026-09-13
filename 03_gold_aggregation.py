# Databricks notebook source
# Databricks notebook source
# =====================================================
# Gold Layer: Business-ready aggregated tables
# Reads Silver (clean data) and produces two Gold tables:
#   1) gold_subscriber_usage - usage summary per subscriber
#   2) gold_tower_load       - load summary per cell tower
# Overwrites each run since it re-reads all of Silver (batch).
# =====================================================

from pyspark.sql.functions import (
    col, count, sum as _sum, max as _max, when
)

# COMMAND ----------

SILVER_TABLE = "telecom_silver"
GOLD_SUBSCRIBER_TABLE = "gold_subscriber_usage"
GOLD_TOWER_TABLE = "gold_tower_load"

silver_df = spark.table(SILVER_TABLE)

# COMMAND ----------

# =====================================================
# Gold 1: Usage summary per subscriber
# =====================================================
subscriber_usage_df = (
    silver_df.groupBy("subscriber_id")
    .agg(
        count(when(col("event_type") == "CALL", True)).alias("total_calls"),
        _sum(when(col("event_type") == "CALL", col("call_duration_sec"))).alias("total_call_duration_sec"),
        count(when(col("event_type") == "DATA", True)).alias("total_data_sessions"),
        _sum(when(col("event_type") == "DATA", col("data_used_mb"))).alias("total_data_mb"),
        count(when(col("event_type") == "SMS", True)).alias("total_sms"),
        count("*").alias("total_events"),
        _max("event_timestamp").alias("last_event_time"),
    )
    .na.fill(0, subset=["total_call_duration_sec", "total_data_mb"])
)

(
    subscriber_usage_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_SUBSCRIBER_TABLE)
)

# COMMAND ----------

# =====================================================
# Gold 2: Load summary per cell tower
# (useful for spotting overloaded towers / hotspots)
# =====================================================
tower_load_df = (
    silver_df.groupBy("cell_tower_id")
    .agg(
        count("*").alias("event_count"),
        _sum(when(col("event_type") == "DATA", col("data_used_mb"))).alias("total_data_mb"),
        _sum(when(col("event_type") == "CALL", col("call_duration_sec"))).alias("total_call_duration_sec"),
    )
    .na.fill(0, subset=["total_data_mb", "total_call_duration_sec"])
    .orderBy(col("event_count").desc())
)

(
    tower_load_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TOWER_TABLE)
)

# COMMAND ----------

print(f"{GOLD_SUBSCRIBER_TABLE}: {spark.table(GOLD_SUBSCRIBER_TABLE).count()} subscribers")
print(f"{GOLD_TOWER_TABLE}: {spark.table(GOLD_TOWER_TABLE).count()} towers")

display(spark.table(GOLD_SUBSCRIBER_TABLE).orderBy(col("total_data_mb").desc()).limit(10))

# COMMAND ----------

display(spark.table(GOLD_TOWER_TABLE))