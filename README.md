# Real-Time Telecom Usage Monitoring

![Real-Time Telecom Usage Analytics architecture](./Real%20time%20telecom%20project.png)

**Kafka (self-hosted on AWS EC2) → Databricks (Batch/Trigger.AvailableNow) → Delta Lake (Bronze/Silver/Gold) → SQL Dashboard**

A hands-on data engineering pipeline that simulates real-time telecom usage events
(calls, mobile data, SMS), streams them through a self-managed Apache Kafka broker, and
processes them in Databricks using a Medallion Architecture — with real data validation,
a dead-letter pattern for rejected records, and a cost-conscious batch-style streaming
approach instead of a 24/7 continuous cluster.

📄 **Full detailed write-up:** see [`PROJECT_DOCUMENTATION.md`](./PROJECT_DOCUMENTATION.md)
for the complete architecture rationale, every problem hit during development and how it
was resolved, and full code walkthroughs.

---

## Architecture

```
Python Producer → Kafka (self-hosted on EC2) → Databricks (Trigger.AvailableNow)
      → Bronze Delta table → Silver Delta table (validated + deduplicated)
      → Gold Delta tables (per-subscriber & per-tower aggregates) → SQL Dashboard
```

## Tech Stack

- **Python** + `kafka-python` — synthetic event producer
- **Apache Kafka 4.0.1** (KRaft mode) — self-hosted on an AWS EC2 instance
- **Databricks** (PySpark Structured Streaming, `Trigger.AvailableNow`)
- **Delta Lake** — Bronze / Silver / Gold medallion tables
- **Databricks SQL** — dashboard and reporting

## Repository Structure

```
.
├── kafka_producer.py          # Simulates telecom usage events, publishes to Kafka
├── 01_bronze_ingestion.py     # Databricks notebook: Kafka -> Bronze Delta table
├── 02_silver_cleaning.py      # Databricks notebook: validation + deduplication -> Silver
├── 03_gold_aggregation.py     # Databricks notebook: Silver -> Gold aggregate tables
├── docker-compose.yml         # (Optional) local Kafka setup used during early prototyping
├── telecom_usage_events_sample.jsonl  # Static sample of generated events for offline testing
├── PROJECT_DOCUMENTATION.md   # Full detailed documentation (architecture, decisions, issues, results)
└── README.md
```

## Key Design Decisions

- **Batch-style streaming (`Trigger.AvailableNow`)** instead of a continuously-running
  cluster, to keep compute costs down while still using Spark's built-in, checkpoint-based
  offset tracking for correctness.
- **Self-hosted Kafka on EC2** rather than a managed service, chosen deliberately to get
  hands-on experience with KRaft configuration, listener setup, and cloud networking.
- **Dead-letter table** (`telecom_rejected_events`) for rows that fail validation —
  nothing is silently dropped.

See `PROJECT_DOCUMENTATION.md` for the full reasoning behind each decision, plus a
detailed log of every real problem encountered (Kafka listener misconfiguration, DBFS
restrictions, IP changes on EC2 restart, etc.) and how each was fixed.

## Results (from an actual run)

| Metric | Value |
|---|---|
| Bronze rows ingested | 92,241 |
| Rejected (failed validation) | 4,215 |
| Duplicates removed | 2,539 |
| Silver rows (final, clean) | 85,487 |
| Subscribers with activity | 150 out of 150 (100% coverage) |
| Cell towers with activity | 20 out of 20 |

`92,241 − 4,215 − 2,539 = 85,487` ✅ — confirms the validation/dedup logic behaves
exactly as designed, at meaningful volume (not just a small initial test batch).

## Setup Summary

1. Deploy Kafka on an EC2 instance in KRaft mode (see `PROJECT_DOCUMENTATION.md`, Section 6).
2. Set the `EC2_KAFKA_BROKER` environment variable and run `kafka_producer.py` locally.
3. Import the three notebooks into a Databricks workspace and run them in order:
   `01_bronze_ingestion.py` → `02_silver_cleaning.py` → `03_gold_aggregation.py`.
4. Build a SQL dashboard on top of `gold_subscriber_usage` and `gold_tower_load`.

## Disclaimer

This is a learning/practice project. The Kafka broker uses `PLAINTEXT` (no
authentication/encryption) and its Security Group allows inbound traffic on port 9092
from any IP — **not** a production-appropriate configuration. See the Cost & Security
notes in `PROJECT_DOCUMENTATION.md` (Section 15) for what a production setup would need
instead (SASL/TLS, restricted IP ranges).
