# Real-Time Telecom Usage Monitoring
## Kafka (Self-Hosted on AWS EC2) → Databricks Batch Pipeline → Delta Lake → SQL Dashboard

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Business Context](#2-business-context)
3. [Architecture](#3-architecture)
4. [Technology Stack](#4-technology-stack)
5. [Data Model](#5-data-model)
6. [Infrastructure Setup](#6-infrastructure-setup)
7. [Component 1: Kafka Producer](#7-component-1-kafka-producer)
8. [Component 2: Kafka Broker (Self-Hosted on EC2)](#8-component-2-kafka-broker-self-hosted-on-ec2)
9. [Component 3: Bronze Layer (Raw Ingestion)](#9-component-3-bronze-layer-raw-ingestion)
10. [Component 4: Silver Layer (Cleaning & Validation)](#10-component-4-silver-layer-cleaning--validation)
11. [Component 5: Gold Layer (Business Aggregates)](#11-component-5-gold-layer-business-aggregates)
12. [Component 6: SQL Dashboard](#12-component-6-sql-dashboard)
13. [Key Design Decisions](#13-key-design-decisions)
14. [Problems Encountered & Resolutions](#14-problems-encountered--resolutions)
15. [Cost Management Notes](#15-cost-management-notes)
16. [Results / Validation of Correctness](#16-results--validation-of-correctness)
17. [Possible Future Improvements](#17-possible-future-improvements)
18. [Appendix: Full Source Code](#18-appendix-full-source-code)

---

## 1. Project Overview

This project simulates a **real-time telecom usage monitoring pipeline**. It generates
synthetic subscriber usage events (voice calls, mobile data sessions, and SMS messages),
streams them through **Apache Kafka**, and processes them in **Databricks** using a
**Medallion Architecture** (Bronze → Silver → Gold), ending in a **SQL Dashboard** for
business-facing reporting.

The project was built incrementally, one component at a time, and deliberately includes
realistic data-quality problems (null fields, negative values, future timestamps,
duplicate events) so that the validation logic in the pipeline has genuine issues to catch
— not just clean, idealized data.

**Core idea demonstrated:** an end-to-end data engineering pipeline covering ingestion,
message brokering, batch-style stream processing, data validation, and business reporting
— while also making a deliberate, justified cost-vs-architecture trade-off (explained in
Section 13).

---

## 2. Business Context

**Simulated domain:** Telecom subscriber usage monitoring.

A telecom operator wants visibility into:
- How much data, call time, and SMS volume each subscriber is generating.
- Which cell towers are under the heaviest load.
- Which incoming usage records are invalid or corrupted (so bad data doesn't silently
  pollute downstream billing or analytics systems).

This mirrors a real operational need: telecom operators ingest huge volumes of Call Detail
Records (CDRs) and usage events continuously, and need both **raw data quality control**
and **aggregated, business-ready views** for operations and customer experience teams.

---

## 3. Architecture

```
┌─────────────────────┐
│   Python Producer    │   Generates synthetic usage events (CALL / DATA / SMS)
│   (kafka_producer.py)│   Injects ~5% intentionally invalid events
└──────────┬───────────┘   Injects ~3% intentional duplicate events
           │  publishes JSON messages
           ▼
┌─────────────────────┐
│   Apache Kafka        │   Self-hosted broker on an AWS EC2 instance (KRaft mode,
│   (EC2, port 9092)    │   no Zookeeper). Acts as a durable, decoupling buffer between
└──────────┬───────────┘   the producer and the consumer.
           │  read via Spark's Kafka connector (batch-style)
           ▼
┌─────────────────────┐
│  Databricks           │   Reads from Kafka using Structured Streaming with
│  Bronze Layer         │   Trigger(availableNow=True) — processes everything currently
│  (telecom_bronze)     │   available, then stops. Not a 24/7 continuous stream.
└──────────┬───────────┘
           │  batch job re-reads Bronze each run
           ▼
┌─────────────────────┐
│  Databricks           │   Validates every row (null checks, negative-value checks,
│  Silver Layer         │   future-timestamp checks) and deduplicates by event_id.
│  (telecom_silver +    │   Rejected rows are NOT discarded — they're written to a
│   telecom_rejected_   │   separate "dead letter" table for inspection.
│   events)             │
└──────────┬───────────┘
           │  batch job re-reads Silver each run
           ▼
┌─────────────────────┐
│  Databricks           │   Two aggregate tables:
│  Gold Layer           │   - gold_subscriber_usage (per-subscriber usage summary)
│  (gold_subscriber_    │   - gold_tower_load (per-cell-tower load summary)
│   usage,               │
│   gold_tower_load)    │
└──────────┬───────────┘
           │  queried by
           ▼
┌─────────────────────┐
│  Databricks SQL       │   Bar charts on top subscriber usage and top cell tower load,
│  Dashboard             │   assembled from SQL Editor queries into one dashboard view.
└─────────────────────┘
```

**Why this shape, specifically:**
- Kafka decouples the producer from the consumer: the producer can keep sending events
  regardless of whether Databricks is currently running, and Kafka retains them.
- The Bronze/Silver/Gold split means each layer has a single responsibility: Bronze =
  ingest faithfully, Silver = trust but verify, Gold = summarize for the business.
- Nothing is silently dropped. Invalid rows go to a rejected-events table rather than
  disappearing, which is a real "dead-letter" pattern used in production data platforms.

---

## 4. Technology Stack

| Layer | Technology | Role |
|---|---|---|
| Event generation | Python 3, `kafka-python` library | Simulates telecom usage events |
| Message broker | Apache Kafka 4.0.1 (KRaft mode) | Durable, decoupled event buffer |
| Broker hosting | AWS EC2 (t3.micro, Ubuntu 26.04 LTS) | Self-managed Kafka infrastructure |
| Processing | Databricks (Free Edition), PySpark Structured Streaming | Batch-style ingestion & transformation |
| Storage format | Delta Lake | ACID-compliant table storage for Bronze/Silver/Gold |
| Checkpointing | Unity Catalog Volumes | Required because DBFS root writes are disabled |
| Reporting | Databricks SQL Editor + Dashboards | Business-facing visualization |

---

## 5. Data Model

### 5.1 Raw Event Schema (as produced by the Kafka producer)

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | Unique identifier per event; used for deduplication |
| `subscriber_id` | integer (1–150) | Can be `null` (intentionally, ~5% of the time) |
| `event_type` | string | One of `CALL`, `DATA`, `SMS` |
| `call_duration_sec` | double | Populated only when `event_type = CALL`; can be negative (intentional bad data) |
| `data_used_mb` | double | Populated only when `event_type = DATA`; can be negative (intentional bad data) |
| `sms_count` | integer | Populated only when `event_type = SMS` |
| `cell_tower_id` | string | One of 20 simulated towers (`TOWER_001`–`TOWER_020`) |
| `event_timestamp` | string (ISO-8601) | Can be set to a future date (intentional bad data, e.g. `2030-01-01`) |

### 5.2 Intentional Data Quality Issues (by design)

To make the Silver-layer validation logic meaningful rather than trivial, the producer
deliberately injects:
- **~5% of events** with one of: `subscriber_id = null`, a negative usage value, or a
  future `event_timestamp`.
- **~3% of events** re-sent as an exact duplicate (same `event_id`), simulating
  network retries or producer resends that happen in real systems.

---

## 6. Infrastructure Setup

### 6.1 AWS EC2 Instance

| Setting | Value |
|---|---|
| Instance type | t3.micro (Free Tier eligible) |
| AMI | Ubuntu Server 26.04 LTS |
| Region | eu-north-1 (Stockholm) |
| Security Group | SSH (22) restricted to the developer's IP; custom TCP 9092 open (see cost/security note in Section 13) |
| Elastic IP | **Not allocated** (deliberate choice — see Section 13) |

### 6.2 Kafka Installation (on the EC2 instance)

```bash
# Install Java
sudo apt update
sudo apt install -y openjdk-21-jdk wget

# Download Kafka
wget https://archive.apache.org/dist/kafka/4.0.1/kafka_2.13-4.0.1.tgz
tar -xzf kafka_2.13-4.0.1.tgz
mv kafka_2.13-4.0.1 kafka
cd kafka

# Format the KRaft storage (only needed once)
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c config/server.properties --standalone

# Edit config/server.properties:
#   advertised.listeners=PLAINTEXT://<EC2_PUBLIC_IP>:9092
#   (IMPORTANT: do NOT add the CONTROLLER listener to advertised.listeners —
#    see Section 14, Problem 3)

# Start the broker
bin/kafka-server-start.sh config/server.properties
```

Kafka 4.0.1 runs entirely in **KRaft mode** — ZooKeeper has been fully removed from Kafka
as of this version, so the broker and controller roles run combined in a single process
for this single-node setup.

### 6.3 Topic Creation

```bash
bin/kafka-topics.sh --create \
  --topic telecom_usage_events \
  --bootstrap-server <EC2_PUBLIC_IP>:9092
```

---

## 7. Component 1: Kafka Producer

**File:** `kafka_producer.py`
**Runs on:** the developer's local machine (Windows, via PowerShell)
**Purpose:** continuously generates and publishes synthetic telecom usage events to the
`telecom_usage_events` Kafka topic.

Key design points:
- Connects using plain `PLAINTEXT` (no authentication) since the broker is self-hosted
  without SASL — appropriate for a practice/learning environment, **not** for production.
- Reads the broker address from an environment variable (`EC2_KAFKA_BROKER`) with a
  fallback default constant, so the address can be updated without editing code deeply.
- Deliberately injects bad data and duplicates (see Section 5.2) so the downstream
  validation logic has real problems to solve.

See [Appendix 18.1](#181-kafka_producerpy) for the full code.

---

## 8. Component 2: Kafka Broker (Self-Hosted on EC2)

Rather than using a managed service (e.g. Confluent Cloud), Kafka was deployed manually
on a raw EC2 instance to gain hands-on experience with:
- KRaft cluster metadata formatting (`kafka-storage.sh format`)
- Listener configuration (`listeners` vs `advertised.listeners`)
- The distinction between the **broker listener** (for clients) and the **controller
  listener** (for internal cluster metadata coordination) — a distinction that caused a
  real configuration bug during this project (see Section 14, Problem 3).
- AWS networking basics: Security Groups, public IPs, and the trade-offs of using
  (or not using) an Elastic IP.

---

## 9. Component 3: Bronze Layer (Raw Ingestion)

**Notebook:** `01_bronze_ingestion.py`
**Table produced:** `telecom_bronze`

This notebook reads from the Kafka topic using Spark Structured Streaming, but with
`.trigger(availableNow=True)` instead of the default continuous trigger. This means:
- The job reads everything currently available in Kafka, processes it, writes it to
  Delta, and then **stops** — it does not keep the cluster running indefinitely.
- On subsequent runs, Spark's checkpoint mechanism (stored in a Unity Catalog Volume)
  remembers the last-read offset per partition, so only new messages are picked up —
  no duplication of already-ingested Kafka messages, and no data loss between runs.

This "batch-style streaming" pattern is a deliberate, cost-conscious alternative to
running a 24/7 streaming cluster (explained further in Section 13).

See [Appendix 18.2](#182-01_bronze_ingestionpy) for the full code.

---

## 10. Component 4: Silver Layer (Cleaning & Validation)

**Notebook:** `02_silver_cleaning.py`
**Tables produced:** `telecom_silver` (clean data), `telecom_rejected_events` (rejected data)

This notebook re-reads the entire Bronze table on each run and applies three validation
rules. A row is considered valid **only if all of the following hold**:

1. `subscriber_id` is not null.
2. `event_timestamp` is not in the future (compared to the current system time).
3. The usage value relevant to that event's type is non-negative:
   - `call_duration_sec >= 0` for `CALL` events
   - `data_used_mb >= 0` for `DATA` events
   - `sms_count >= 0` for `SMS` events

Rows that fail validation are **not discarded** — they are written to a separate
`telecom_rejected_events` table. This is a "dead-letter" pattern: nothing disappears
silently, and rejected data remains available for later inspection, debugging, or
reprocessing.

After validation, rows are deduplicated on `event_id` using `dropDuplicates`, which
removes the intentional duplicate events injected by the producer.

See [Appendix 18.3](#183-02_silver_cleaningpy) for the full code.

---

## 11. Component 5: Gold Layer (Business Aggregates)

**Notebook:** `03_gold_aggregation.py`
**Tables produced:** `gold_subscriber_usage`, `gold_tower_load`

Two business-ready aggregate tables are built from Silver:

**`gold_subscriber_usage`** — one row per subscriber, with:
- `total_calls`, `total_call_duration_sec`
- `total_data_sessions`, `total_data_mb`
- `total_sms`
- `total_events`
- `last_event_time`

**`gold_tower_load`** — one row per cell tower, with:
- `event_count` (total events routed through that tower)
- `total_data_mb`, `total_call_duration_sec`

Both tables are fully overwritten on each run (rather than incrementally updated), since
the Silver layer itself is fully rebuilt from Bronze on each run in this batch-oriented
design. This keeps the logic simple and correct at the cost of some recomputation —
an acceptable trade-off given the modest data volume in this project (see Section 17 for
how this could evolve into incremental/merge-based updates at scale).

See [Appendix 18.4](#184-03_gold_aggregationpy) for the full code.

---

## 12. Component 6: SQL Dashboard

Built in Databricks on top of the Bronze, Silver, and Gold tables. The final dashboard
contains **17 widgets**, organized into four sections:

**Overview & Key Metrics**
- Dashboard description text widget
- 6 counters: Total Subscribers (112), Total Data Usage, Total Calls, Total SMS,
  Average Call Duration, Total Events

**Trend Analysis**
- Data Usage Over Time (line chart)
- Call Volume Over Time (line chart)

**Top Consumers**
- Top 10 Subscribers by Data Usage (bar chart)
- Top 10 Subscribers by Call Duration (bar chart)

**Network Performance**
- Network Load by Tower — event counts (bar chart)
- Data Traffic by Tower — MB per tower (bar chart)
- Event Distribution by Type — pie chart (74 DATA, 71 SMS, 65 CALL events observed
  in this run)

**Usage Patterns**
- Usage Patterns by Hour of Day (bar chart)
- Subscriber Usage Details (full detail table)

**Important caveat on the time-based widgets:** "Data Usage Over Time" and "Usage
Patterns by Hour of Day" are visually compelling, but the underlying dataset for this
project spans only a short producer run (a few minutes, ~229 raw events) rather than
real historical traffic over days or weeks. Any apparent "hourly pattern" or "trend" in
these specific charts reflects the random timing of the synthetic producer
(`time.sleep(random.uniform(0.05, 0.3))` between events), not a genuine time-of-day
usage pattern. These widgets are kept because they demonstrate the *capability* to build
time-series reporting once real, longer-running data is available — but they should be
described accurately (as illustrative/simulated) rather than presented as a discovered
business insight.

Example queries used for the core bar charts:

```sql
SELECT subscriber_id, total_data_mb, total_call_duration_sec, total_sms, total_events
FROM gold_subscriber_usage
ORDER BY total_data_mb DESC
LIMIT 15;

SELECT cell_tower_id, event_count, total_data_mb
FROM gold_tower_load
ORDER BY event_count DESC;
```

---

## 13. Key Design Decisions

### 13.1 Batch-style processing (`Trigger.AvailableNow`) instead of continuous streaming

**Decision:** Use Spark Structured Streaming's `availableNow` trigger rather than the
default continuous trigger, and rather than a plain one-off batch read with manual offset
tracking.

**Reasoning:**
- A continuously-running streaming job requires an always-on cluster, which has an
  ongoing cost — not appropriate for a learning/practice project with a limited budget.
- `Trigger.AvailableNow` gives the correctness benefits of the streaming API (automatic,
  exactly-once-per-run offset tracking via checkpoints) while behaving like a batch job:
  it processes what's available, then stops, so the cluster can be started only when
  needed and shut down (or auto-terminated) afterward.
- This is a real, production-recognized cost-optimization pattern, not a workaround —
  it directly demonstrates understanding of the cost/latency trade-offs in streaming
  system design.

### 13.2 Self-hosted Kafka on EC2 instead of a managed service (e.g. Confluent Cloud)

**Reasoning:** The project initially used a local Docker-based Kafka broker, which could
not be reached by Databricks (a cloud service) because `localhost` is not a publicly
routable address. Two alternatives were considered:

| Option | Trade-off |
|---|---|
| Confluent Cloud (managed) | Fastest to set up; authentication and TLS handled automatically; less hands-on infrastructure learning |
| Self-hosted Kafka on EC2 | More setup effort and manual security responsibility; **but** provides direct, hands-on experience with Kafka internals, Linux administration, and cloud networking |

Self-hosted EC2 was chosen deliberately to maximize hands-on learning value for this
practice project.

### 13.3 No Elastic IP allocated

**Reasoning:** An Elastic IP keeps the EC2 instance's public IP address constant across
stop/start cycles, but has a small ongoing cost when not actively associated with a
running instance in some billing scenarios, and adds one more piece of AWS infrastructure
to manage. Since this is a practice project (not a long-running production system), the
instance is generally kept running for the duration of a work session, and the IP address
is simply re-checked and updated in the producer/notebook config on the rare occasions
the instance is stopped and restarted.

### 13.4 Dead-letter table for rejected records

**Reasoning:** Silently dropping invalid rows would hide the actual scope of data quality
issues and make debugging much harder. Writing rejected rows to their own table (with the
same schema as Bronze) keeps the pipeline auditable — anyone can query
`telecom_rejected_events` and see exactly what was rejected and why.

---

## 14. Problems Encountered & Resolutions

This section documents real problems hit during development — kept here deliberately as
part of the documentation, since working through them was a core part of the learning
value of this project.

### Problem 1: Databricks (cloud) cannot reach a locally-hosted Kafka broker

**Symptom:** Kafka worked perfectly when tested locally (producer, and
`kafka-console-consumer` both connected to `localhost:9092` without issue). However,
Databricks clusters run on remote cloud infrastructure and have no network path to a
`localhost` address on a personal machine.

**Resolution:** Migrated the Kafka broker off the local machine entirely. Two paths were
evaluated (Confluent Cloud vs. self-hosted EC2); self-hosted EC2 with a public IP and an
open inbound rule on port 9092 was chosen (see Section 13.2).

### Problem 2: EC2 public IP changes after Stop/Start

**Symptom:** After stopping and restarting the EC2 instance (during a break in the work
session), AWS assigned a new public IPv4 address, breaking the previously-configured
`advertised.listeners` value in Kafka's config, as well as the hardcoded broker addresses
in the producer script and the Databricks notebook.

**Resolution:** This is expected AWS behavior for instances without an Elastic IP
(Section 13.3 explains why an Elastic IP was deliberately not used). The workflow
established was: after every Stop/Start cycle, retrieve the new public IP, update
`advertised.listeners` in `config/server.properties`, and update the broker address in
both `kafka_producer.py` and the Databricks Bronze notebook before restarting Kafka.

### Problem 3: Kafka refused to start / misconfigured `advertised.listeners` with CONTROLLER

**Symptom:** After manually editing `advertised.listeners` to include the public IP, it
was mistakenly also configured with a `CONTROLLER` entry, following an incorrect
assumption that all listeners (from `listeners=`) should also appear in
`advertised.listeners=`.

**Resolution:** In KRaft mode, when a node has the `broker` role,
`advertised.listeners` must **not** include the controller listener. The controller
listener (defined via `controller.listener.names`) is for internal inter-node cluster
metadata coordination only — external clients (producers, consumers, or Databricks) never
communicate with it directly, so it has no reason to be advertised to them. The fix was to
set:
```
advertised.listeners=PLAINTEXT://<EC2_PUBLIC_IP>:9092
```
with no `CONTROLLER://...` entry included.

### Problem 4: `DBFS_DISABLED` error when writing Structured Streaming checkpoints

**Symptom:**
```
[DBFS_DISABLED] Public DBFS root is disabled. Access is denied on path:
/tmp/checkpoints/telecom_bronze
```

**Cause:** Newer Databricks workspaces (including Free Edition) disable direct writes to
the public DBFS root for security reasons. The original checkpoint path
(`/tmp/checkpoints/telecom_bronze`) resolved to a location under that restricted root.

**Resolution:** Created a **Unity Catalog Volume** to hold the checkpoint instead:
```python
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.checkpoints")
```
and updated the checkpoint path to:
```
/Volumes/workspace/default/checkpoints/telecom_bronze
```
Unity Catalog Volumes are the currently-recommended, governed storage mechanism for this
kind of file-based data in Databricks, replacing direct DBFS root access.

### Problem 5: Kafka version mismatch during download

**Symptom:** The originally-planned download URL for Kafka 3.7.0 from
`downloads.apache.org` did not work (older releases are moved to the archive once
superseded).

**Resolution:** Downloaded Kafka 4.0.1 instead, from
`archive.apache.org/dist/kafka/4.0.1/...`. This required re-verifying the configuration
file layout, since Kafka 4.x removed ZooKeeper support entirely and consolidated the
KRaft configuration directly into `config/server.properties` (earlier 3.x versions used a
separate `config/kraft/server.properties` template).

---

## 15. Cost Management Notes

- **EC2 (t3.micro):** Free Tier eligible for new AWS accounts (limited monthly hours for
  12 months). Outside Free Tier, billed hourly while the instance is `running` —
  stopping the instance when not in active use avoids compute charges (storage charges
  for the attached EBS volume still apply while stopped, though minimal for 8 GiB).
- **No Elastic IP:** Avoids the small additional charge associated with an allocated-but-
  unassociated Elastic IP, at the cost of the public IP changing on each Stop/Start
  (see Section 13.3 and Section 14, Problem 2).
- **Databricks Free Edition with `Trigger.AvailableNow`:** Avoids the cost of a
  continuously-running streaming cluster. Compute only runs for the duration of each
  notebook execution.
- **Security Group note:** Port 9092 was opened to `0.0.0.0/0` (all IPs) for simplicity
  during this practice project. **This is not an acceptable configuration for a
  production system** — a production deployment should restrict inbound access to
  specific, known IP ranges (e.g., the data platform's egress IPs) and/or add
  authentication (SASL) and encryption (TLS) to the Kafka listener.

---

## 16. Results / Validation of Correctness

A full end-to-end run (producer left running to generate a larger, more representative
volume of events) produced the following numbers, which were cross-checked for internal
consistency:

| Metric | Value |
|---|---|
| Bronze rows ingested | 92,241 |
| Rows rejected (failed validation) | 4,215 (~4.6%) |
| Duplicate events removed | 2,539 (~2.8%) |
| Silver rows (final, clean) | 85,487 |
| Subscribers with activity (Gold) | 150 out of 150 possible (100% coverage) |
| Cell towers with activity (Gold) | 20 out of 20 |

**Consistency check:** `92,241 (Bronze) − 4,215 (rejected) − 2,539 (duplicates) =
85,487 (Silver)` ✅ — confirms the validation and deduplication logic scales correctly to
a much larger volume, not just the small initial test batch. The observed rejection and
duplication rates (~4.6% and ~2.8%) remain closely aligned with the producer's configured
injection rates (5% and 3% respectively), with the small variance expected from random
sampling.

An earlier, smaller test run (229 raw events) was used during initial development to
verify the pipeline's logic end-to-end before scaling up the producer's runtime to
generate this larger, more representative dataset.

---

## 17. Possible Future Improvements

- **Incremental Gold updates:** Replace the full `overwrite` of Gold tables with an
  incremental `MERGE INTO` approach, so only subscribers/towers with new activity are
  recomputed — more efficient at larger data volumes.
- **Schema evolution handling:** Add explicit handling for Kafka messages that don't match
  the expected schema (currently, malformed JSON would simply produce nulls after
  `from_json`, rather than being flagged).
- **Restrict the Kafka Security Group:** Narrow the inbound rule on port 9092 from
  `0.0.0.0/0` to specific IP ranges, and add SASL/TLS authentication, to make the setup
  closer to a production-appropriate configuration.
- **Automated scheduling:** Use Databricks Jobs to run the three notebooks
  (Bronze → Silver → Gold) on a schedule automatically, rather than manually triggering
  each one.
- **Elastic IP for stability:** If the project moves from "practice" to "always-on demo",
  allocate an Elastic IP to avoid having to update broker addresses after every
  Stop/Start cycle.
- **Historical Silver/Gold instead of full overwrite:** Currently Silver and Gold are
  fully rebuilt from source each run; a production version would likely append/merge
  incrementally and retain history for trend analysis over time.

---

## 18. Appendix: Full Source Code

### 18.1 `kafka_producer.py`

```python
"""
Kafka Producer - Telecom Real-Time Usage Events
-------------------------------------------------
Simulates live subscriber usage events (calls, data, sms) and publishes
them to a self-hosted Kafka broker (running on EC2) as JSON messages.

Requires: pip install kafka-python

Set the broker address as an environment variable before running
(PowerShell example):
    setx EC2_KAFKA_BROKER "13.61.40.234:9092"

Note: setx saves the variable permanently but only takes effect in NEW
terminal windows opened AFTER running it. Close and reopen PowerShell
before running this script. Alternatively, just edit the DEFAULT_BROKER
value below directly.
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------
DEFAULT_BROKER = "13.61.40.234:9092"  # <-- update if your EC2 public IP changes
KAFKA_BROKER = os.environ.get("EC2_KAFKA_BROKER", DEFAULT_BROKER)
TOPIC_NAME = "telecom_usage_events"

# Same subscriber pool used in the batch project (1 to 150)
N_SUBSCRIBERS = 150

# Cell towers to simulate geographic spread
CELL_TOWERS = [f"TOWER_{i:03d}" for i in range(1, 21)]

EVENT_TYPES = ["CALL", "DATA", "SMS"]

# Small chance of intentionally "bad" events, so the Databricks
# validation/dedup step downstream has real problems to catch.
BAD_EVENT_RATE = 0.05
DUPLICATE_RATE = 0.03


def build_event(event_id: str) -> dict:
    subscriber_id = random.randint(1, N_SUBSCRIBERS)
    event_type = random.choice(EVENT_TYPES)

    call_duration_sec = None
    data_used_mb = None
    sms_count = None

    if event_type == "CALL":
        call_duration_sec = round(random.uniform(5, 1800), 1)
    elif event_type == "DATA":
        data_used_mb = round(random.uniform(1, 500), 2)
    else:  # SMS
        sms_count = random.randint(1, 5)

    event = {
        "event_id": event_id,
        "subscriber_id": subscriber_id,
        "event_type": event_type,
        "call_duration_sec": call_duration_sec,
        "data_used_mb": data_used_mb,
        "sms_count": sms_count,
        "cell_tower_id": random.choice(CELL_TOWERS),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Inject occasional bad data on purpose (for the validation step later)
    if random.random() < BAD_EVENT_RATE:
        bad_field = random.choice(["negative_value", "null_subscriber", "future_timestamp"])
        if bad_field == "negative_value":
            if event["data_used_mb"] is not None:
                event["data_used_mb"] = -abs(event["data_used_mb"])
            elif event["call_duration_sec"] is not None:
                event["call_duration_sec"] = -abs(event["call_duration_sec"])
        elif bad_field == "null_subscriber":
            event["subscriber_id"] = None
        elif bad_field == "future_timestamp":
            event["event_timestamp"] = "2030-01-01T00:00:00+00:00"

    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Producing events to topic '{TOPIC_NAME}' on {KAFKA_BROKER} ... Ctrl+C to stop.")

    try:
        while True:
            event_id = str(uuid.uuid4())
            event = build_event(event_id)

            producer.send(TOPIC_NAME, value=event)
            print(event)

            # Occasionally resend the same event to simulate real-world duplicates
            if random.random() < DUPLICATE_RATE:
                producer.send(TOPIC_NAME, value=event)

            time.sleep(random.uniform(0.05, 0.3))  # a few events per second
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
```

### 18.2 `01_bronze_ingestion.py`

```python
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
```

### 18.3 `02_silver_cleaning.py`

```python
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
```

### 18.4 `03_gold_aggregation.py`

```python
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
```

### 18.5 Kafka `server.properties` (key settings only)

```properties
process.roles=broker,controller
node.id=1
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
inter.broker.listener.name=PLAINTEXT
# IMPORTANT: advertised.listeners must NOT include the CONTROLLER entry
# when this node has the broker role (see Section 14, Problem 3).
advertised.listeners=PLAINTEXT://<EC2_PUBLIC_IP>:9092
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
```

---

*Document prepared as part of an incremental, hands-on build of this pipeline. All
numbers in Section 16 come from an actual run of the pipeline on synthetic data, not
projected/estimated figures.*
