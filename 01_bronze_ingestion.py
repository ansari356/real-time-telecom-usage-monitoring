# Databricks notebook source
# Databricks notebook source
# =====================================================
# Bronze Layer: Ingest raw events from Kafka (EC2 broker)
# Uses Trigger.AvailableNow -> processes what's available
# then stops. NOT a continuously-running stream, so the
# cluster does not need to stay on 24/7 (cost-friendly).
# =====================================================

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import from_json, col

# COMMAND ----------

# ---- Config ----
KAFKA_BROKER = "13.61.40.234:9092"   # <-- update if your EC2 public IP changes
TOPIC_NAME = "telecom_usage_events"
CHECKPOINT_PATH = "/Volumes/workspace/default/checkpoints/telecom_bronze"
BRONZE_TABLE = "telecom_bronze"

# ---- Schema matching the producer's JSON payload ----
event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("subscriber_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("call_duration_sec", DoubleType()),
    StructField("data_used_mb", DoubleType()),
    StructField("sms_count", IntegerType()),
    StructField("cell_tower_id", StringType()),
    StructField("event_timestamp", StringType()),
])

# COMMAND ----------

# ---- Create the Unity Catalog Volume used for the streaming checkpoint ----
# (DBFS root writes are disabled by default on newer/Free Edition workspaces,
# so checkpoints must live in a Unity Catalog Volume instead.)
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.checkpoints")

# COMMAND ----------

# ---- Read from Kafka (batch-style via AvailableNow trigger) ----
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")  # first run: read everything Kafka has
    .load()
)

# ---- Parse the JSON payload out of Kafka's raw 'value' column ----
parsed_df = (
    raw_df
    .select(
        from_json(col("value").cast("string"), event_schema).alias("data"),
        col("timestamp").alias("kafka_ingest_time"),
        col("partition"),
        col("offset"),
    )
    .select("data.*", "kafka_ingest_time", "partition", "offset")
)

# COMMAND ----------

# ---- Write to Bronze Delta table, then stop (AvailableNow) ----
query = (
    parsed_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)   # <-- this is what makes it "batch", not continuous
    .toTable(BRONZE_TABLE)
)

query.awaitTermination()  # blocks until this batch run finishes, then the cell completes

print(f"Bronze ingestion run complete. Row count now in {BRONZE_TABLE}:")
display(spark.table(BRONZE_TABLE).count())

# COMMAND ----------

display(spark.table("telecom_bronze"))