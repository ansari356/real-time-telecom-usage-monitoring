# Databricks notebook source
# Databricks notebook source
# =====================================================
# Silver Layer: Clean, validate, and deduplicate Bronze data
# Reads the whole Bronze table each run (batch), applies
# validation rules, and overwrites the Silver table with
# only the clean, deduplicated rows.
# =====================================================

from pyspark.sql.functions import col, current_timestamp, to_timestamp

# COMMAND ----------

BRONZE_TABLE = "telecom_bronze"
SILVER_TABLE = "telecom_silver"
REJECTED_TABLE = "telecom_rejected_events"  # keep bad rows for inspection, don't just discard them

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE).withColumn(
    "event_ts_parsed", to_timestamp(col("event_timestamp"))
)

# ---- Validation rules ----
# A row is considered VALID only if all of these hold:
is_valid = (
    col("subscriber_id").isNotNull()
    & (col("event_ts_parsed") <= current_timestamp())  # no future timestamps
    & (
        # negative-value check only applies to the field relevant to that event type
        ((col("event_type") == "CALL") & (col("call_duration_sec") >= 0))
        | ((col("event_type") == "DATA") & (col("data_used_mb") >= 0))
        | ((col("event_type") == "SMS") & (col("sms_count") >= 0))
    )
)

# COMMAND ----------

# ---- Split into valid vs rejected ----
valid_df = bronze_df.filter(is_valid)
rejected_df = bronze_df.filter(~is_valid)

# ---- Deduplicate valid rows by event_id (drop true duplicates / re-sent events) ----
deduped_df = valid_df.dropDuplicates(["event_id"])

# COMMAND ----------

# ---- Write Silver (clean data). Overwrite each run since we re-read all of Bronze. ----
(
    deduped_df
    .drop("event_ts_parsed")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

# ---- Write rejected rows too, so nothing silently disappears ----
(
    rejected_df
    .drop("event_ts_parsed")
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(REJECTED_TABLE)
)

# COMMAND ----------

bronze_count = bronze_df.count()
silver_count = spark.table(SILVER_TABLE).count()
rejected_count = spark.table(REJECTED_TABLE).count()
duplicates_removed = valid_df.count() - silver_count

print(f"Bronze rows read:      {bronze_count}")
print(f"Valid + deduplicated:  {silver_count}")
print(f"Rejected (bad data):   {rejected_count}")
print(f"Duplicates removed:    {duplicates_removed}")

display(spark.table(SILVER_TABLE))