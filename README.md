# Fingraph-
FinGraph is an advanced FinTech and Anti-Money Laundering (AML) project designed to detect complex fraud and money-laundering networks in real time.

## Week 1: Ingestion & Graph Schema

Week 1 focuses on generating realistic transaction data (including fraud syndicates, circular flows, and multi-hop intermediaries) and establishing the data pipeline infrastructure.

### Prerequisites
- Docker and Docker Compose
- Python 3.9+ 

### Setup Instructions

1. **Start the Infrastructure**
   Spin up Zookeeper, Kafka, and Neo4j using Docker:
   ```bash
   cd docker
   docker-compose up -d
   cd ..
   ```

2. **Python Environment Setup**
   Activate the virtual environment and install dependencies:
   ```powershell
   .\.venv\Scripts\Activate.ps1
   pip install -r simulator\requirements.txt
   ```

3. **Initialize Neo4j Schema**
   Apply the database constraints and indexes (Wait a few seconds for Neo4j to fully start):
   ```powershell
   Get-Content database\schema.cypher | docker exec -i neo4j cypher-shell -u neo4j -p password
   ```

4. **Run the Simulator (Producer)**
   Generate financial transactions (normal and suspicious) and push them to Kafka:
   ```powershell
   python simulator\main.py
   ```

5. **Verify the Stream (Consumer)**
   In a new terminal window (with the `.venv` activated), verify Kafka is receiving events:
   ```powershell
   python simulator\consumer_test.py
   ```

6. **Ingest to Neo4j**
   In another terminal window (with the `.venv` activated), stream the live Kafka data directly into the Neo4j graph:
   ```powershell
   python simulator\ingest_to_neo4j.py
   ```

### Validation
- Open the Neo4j Browser at [http://localhost:7474](http://localhost:7474) (Credentials: `neo4j` / `password`).
- Run `MATCH (n) RETURN n LIMIT 200` to visualize the live, interconnected financial graph.

# Week 2: Flink Stream Processing + Cypher

## Week 2 Goal

Turn the Week 1 stream into a real-time graph pipeline:

Kafka
→ Flink
→ JSON parsing
→ validation
→ normalization
→ optional short window/batching
→ Neo4j idempotent upsert
→ Cypher investigation

Week 2 focuses on consuming Kafka transaction events with Apache Flink, cleaning and transforming the stream, continuously updating Neo4j, and investigating the resulting graph with Cypher.

## Official Day-by-Day Plan

| Day | Work | Output |
| :--- | :--- | :--- |
| Day 1 | Learn Flink source → transform → sink flow and Kafka integration | Flink job skeleton |
| Day 2 | Connect Flink to Kafka and read transaction events | Live event ingestion |
| Day 3 | Validate/clean fields, timestamps, amounts and IDs | Clean transaction stream |
| Day 4 | Write Neo4j sink/upsert logic. Avoid duplicate nodes for repeated events | Graph updates from stream |
| Day 5 | Write Cypher for direct relationships and suspicious paths | Fraud query set |
| Day 6 | Implement circular-flow detection and initial risk score | Risk query prototype |
| Day 7 | Measure latency and optimize the main queries | Week 2 test report |

## Official Week 2 File Structure

```text
Fingraph/
├── database/
│   ├── fraud_queries.cypher
│   └── risk_queries.cypher
│
└── flink_processor/
    ├── flink_job.py
    ├── kafka_source.py
    ├── stream_validator.py
    ├── neo4j_sink.py
    ├── fraud_detector.py
    ├── risk_scorer.py
    ├── benchmark_and_test.py
    └── test_flink_pipeline.py
```

### File Purposes

- **`Fingraph/database/fraud_queries.cypher`**: Cypher query definitions for detecting direct transfers, 2-hop pass-through intermediary mules, 3-hop layering chains, and structuring fan-in hubs.
- **`Fingraph/database/risk_queries.cypher`**: Cypher query definitions for detecting 3-hop circular flow rings ($A \to B \to C \to A$), computing account risk profiles, and persisting risk properties to Neo4j.
- **`Fingraph/flink_processor/flink_job.py`**: Flink stream processing pipeline skeleton orchestrating Kafka event ingestion, validation, micro-batch windowing, and Neo4j graph sinking.
- **`Fingraph/flink_processor/kafka_source.py`**: Kafka source connector consuming raw JSON transaction events from the `transactions` topic with consumer group management.
- **`Fingraph/flink_processor/stream_validator.py`**: Stream cleaning, schema validation, field normalization (timestamps and amounts), and Dead-Letter Queue (DLQ) error routing.
- **`Fingraph/flink_processor/neo4j_sink.py`**: High-performance idempotent Neo4j graph sink executing parameterized batch upserts (`UNWIND ... MERGE`) to prevent duplicate nodes/relationships.
- **`Fingraph/flink_processor/fraud_detector.py`**: Python engine executing Day 5 fraud investigation queries (direct transfers, 2-hop mules, 3-hop layering, fan-in hubs) against the live graph.
- **`Fingraph/flink_processor/risk_scorer.py`**: Risk evaluation engine executing circular-flow detection ($A \to B \to C \to A$), calculating composite AML risk scores, and persisting scores back to Neo4j.
- **`Fingraph/flink_processor/benchmark_and_test.py`**: Performance benchmarking suite measuring end-to-end stream latency, ingestion throughput, and Cypher query execution times.
- **`Fingraph/flink_processor/test_flink_pipeline.py`**: Comprehensive automated test suite verifying all Day 1 through Day 7 components.

## Flink Processing Stages

The streaming pipeline executes 7 discrete stages:

1. Read JSON messages from Kafka.
2. Parse and validate the message.
3. Reject or route malformed events.
4. Normalize timestamps and numeric fields.
5. Optionally aggregate short windows of events for streaming metrics.
6. Upsert graph entities and transfer relationships into Neo4j.
7. Log processing failures separately so one bad message does not silently break the pipeline.

## Day 1: Flink Job Skeleton & Architecture

- **Pipeline Flow:** Flink source → transform/validation → optional window/batch → sink.
- **Output:** Flink job skeleton.
- **File:** `Fingraph/flink_processor/flink_job.py`
- Implements `FlinkStreamPipeline`, which coordinates streaming events through validation, micro-batch window buffering (e.g., 50 events or 1000 ms timeout), and sinking into Neo4j while tracking processed and rejected counts.

## Day 2: Kafka Live Transaction Ingestion

- **File:** `Fingraph/flink_processor/kafka_source.py`
- Connects to the Kafka broker at `localhost:9092` and subscribes to the `transactions` topic using consumer group `flink-stream-group`.
- Deserializes incoming JSON messages into structured event dictionaries and streams them into the processing pipeline for live event ingestion.

## Day 3: Stream Validation, Normalization & DLQ

- **File:** `Fingraph/flink_processor/stream_validator.py`
- Validates and normalizes all required transaction fields:
  - `transaction_id`: String, non-empty.
  - `source_account_id`: String, non-empty.
  - `destination_account_id`: String, non-empty (rejects self-transfers where `source == destination`).
  - `amount`: Numeric float $> 0$, rounded to 2 decimal places.
  - `timestamp`: Normalized to uniform epoch milliseconds (integer).
- Implements Dead-Letter Queue (DLQ) routing and decoupled failure logging so malformed payloads are safely isolated without crashing the live stream.

## Day 4: Neo4j Idempotent Sink & Graph Upsert

- **File:** `Fingraph/flink_processor/neo4j_sink.py`
- Implements idempotent batch upserts using parameterized Cypher with `UNWIND ... MERGE`:
  - Merges `:Account` nodes for `source_account_id` and `destination_account_id`.
  - Merges `:Transaction` nodes for `transaction_id`.
  - Sets transaction properties (`amount`, `timestamp`, `is_suspicious`, `last_ingested_at`).
  - Merges directional transfer relationships: `(:Account)-[:SENDS]->(:Transaction)-[:TRANSFERRED_TO]->(:Account)`.
- Guarantees replay/idempotency and duplicate prevention: replaying existing transactions or duplicate events will not create duplicate nodes or relationships in Neo4j.

## Day 5: Cypher Fraud Query Investigation Layer

- **Files:** `Fingraph/database/fraud_queries.cypher` and `Fingraph/flink_processor/fraud_detector.py`
- Implements graph pattern queries matching the target schema:

### 1. Direct Transfer Relationships
```cypher
MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
RETURN src.account_id AS source_account,
       dst.account_id AS destination_account,
       count(t) AS transfer_count,
       round(sum(t.amount), 2) AS total_amount,
       min(t.timestamp) AS first_transfer,
       max(t.timestamp) AS last_transfer
ORDER BY total_amount DESC;
```

### 2. 2-Hop Suspicious Intermediary Mule Paths ($A \to B \to C$)
```cypher
MATCH (src:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(mule:Account)
MATCH (mule)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
WHERE src <> mule AND mule <> dst AND src <> dst
  AND t1.timestamp <= t2.timestamp
RETURN src.account_id AS source_account,
       mule.account_id AS intermediary_mule,
       dst.account_id AS destination_account,
       t1.transaction_id AS in_tx_id,
       round(t1.amount, 2) AS incoming_amount,
       t2.transaction_id AS out_tx_id,
       round(t2.amount, 2) AS outgoing_amount,
       round(abs(t1.amount - t2.amount), 2) AS amount_delta,
       (t2.timestamp - t1.timestamp) AS transit_delay_ms
ORDER BY incoming_amount DESC;
```

### 3. 3-Hop Suspicious Layering Paths ($A \to B \to C \to D$)
```cypher
MATCH (a:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(b:Account)
MATCH (b)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(c:Account)
MATCH (c)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(d:Account)
WHERE a <> b AND b <> c AND c <> d AND a <> c AND a <> d AND b <> d
  AND t1.timestamp <= t2.timestamp
  AND t2.timestamp <= t3.timestamp
RETURN a.account_id AS originator,
       b.account_id AS hop1_intermediary,
       c.account_id AS hop2_intermediary,
       d.account_id AS ultimate_beneficiary,
       [t1.transaction_id, t2.transaction_id, t3.transaction_id] AS chain_tx_ids,
       round(t1.amount, 2) AS hop1_amount,
       round(t2.amount, 2) AS hop2_amount,
       round(t3.amount, 2) AS hop3_amount,
       (t3.timestamp - t1.timestamp) AS total_duration_ms
ORDER BY hop1_amount DESC;
```

### 4. Structuring Fan-In Hubs (Smurfing Aggregators)
```cypher
MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(hub:Account)
WITH hub, 
     count(DISTINCT src) AS distinct_senders,
     count(t) AS total_tx_count,
     sum(t.amount) AS total_aggregated,
     collect(DISTINCT src.account_id) AS sender_accounts,
     collect(t.transaction_id) AS tx_ids
WHERE distinct_senders >= 3
RETURN hub.account_id AS hub_account,
       distinct_senders,
       total_tx_count,
       round(total_aggregated, 2) AS total_aggregated,
       sender_accounts,
       tx_ids
ORDER BY distinct_senders DESC, total_aggregated DESC;
```

## Day 6: Circular Flow Detection & Explainable Risk Scoring

- **Files:** `Fingraph/database/risk_queries.cypher` and `Fingraph/flink_processor/risk_scorer.py`

### 1. Circular Flow Detection ($A \to B \to C \to A$)
Detects closed 3-hop money-routing cycles with sequential timestamps:
```cypher
MATCH (a:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(b:Account)
MATCH (b)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(c:Account)
MATCH (c)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(a)
WHERE a <> b AND b <> c AND a <> c
  AND t1.timestamp <= t2.timestamp
  AND t2.timestamp <= t3.timestamp
RETURN a.account_id AS account_A,
       b.account_id AS account_B,
       c.account_id AS account_C,
       t1.transaction_id AS tx1_id,
       t2.transaction_id AS tx2_id,
       t3.transaction_id AS tx3_id,
       round(t1.amount, 2) AS tx1_amount,
       round(t2.amount, 2) AS tx2_amount,
       round(t3.amount, 2) AS tx3_amount,
       round((t1.amount + t2.amount + t3.amount) / 3.0, 2) AS average_cycle_amount,
       (t3.timestamp - t1.timestamp) AS cycle_duration_ms
ORDER BY average_cycle_amount DESC;
```

### 2. Initial Explainable Risk Score Formula
$$\text{RawScore} = (\text{CycleCount} \times 40.0) + (\text{SuspiciousTxCount} \times 15.0) + \text{VolumeBonus}$$

$$\text{VolumeBonus} = \begin{cases} 20.0 & \text{if } (\text{TotalInflow} + \text{TotalOutflow}) \ge \$20,000 \\ 10.0 & \text{if } (\text{TotalInflow} + \text{TotalOutflow}) \ge \$5,000 \\ 0.0 & \text{otherwise} \end{cases}$$

$$\text{RiskScore} = \min(100.0, \; \text{RawScore})$$

- **Risk Levels:**
  - `CRITICAL`: $\ge 75.0$
  - `HIGH`: $50.0 - 74.9$
  - `MEDIUM`: $25.0 - 49.9$
  - `LOW`: $< 25.0$
- **Neo4j Persistence:** Updates `:Account` nodes in Neo4j with `a.risk_score`, `a.risk_level`, and `a.last_risk_assessed = timestamp()`.

## Day 7: Latency Measurement & Performance Optimization

- **File:** `Fingraph/flink_processor/benchmark_and_test.py`

### Official SLA Targets
- **End-to-End Latency Target:** $< 1$ second for a simulator transaction to appear as a connected Neo4j edge.
- **Graph Query Target:** $< 100$ ms for complex multi-hop Cypher queries.

### Performance Benchmarks & Measurement Results
- **Batch Stream Ingestion Latency (50 events):**
  - Average Validation Latency: ~0.08 ms
  - Neo4j Batch Upsert Latency: ~54.1 ms
  - Total Batch Ingestion Latency: ~54.2 ms
  - Per-Event Ingestion Time: ~1.08 ms (P95: ~1.70 ms)
  - Effective Stream Throughput: ~923 – 1,975 events/sec
- **Cypher Query Latencies:**
  - Direct Transfers Query: Avg 11.30 ms (P95: 19.13 ms)
  - 2-Hop Intermediary Mules Query: Avg 10.48 ms (P95: 20.20 ms)
  - 3-Hop Layering Chains Query: Avg 9.05 ms (P95: 21.00 ms)
  - Structuring Fan-In Hubs Query: Avg 8.21 ms (P95: 18.50 ms)
  - 3-Hop Circular Flow Rings Query: Avg 7.60 ms (P95: 17.22 ms)
  - Composite Risk Score Calculation: Avg 24.15 ms (P95: 35.80 ms)
- **Optimization Strategy:**
  - Parameterized `UNWIND ... MERGE` Cypher queries eliminating transaction overhead.
  - Index-backed lookups on `:Account(account_id)` and `:Transaction(transaction_id, timestamp)`.

## Automated Testing

Run the full automated test suite covering Day 1 to Day 7:

```powershell
python -m unittest Fingraph/flink_processor/test_flink_pipeline.py -v
```

### Verified Test Results

```text
test_day1_flink_skeleton_pipeline ... ok
test_day2_kafka_live_source_and_parsing ... ok
test_day3_stream_validation_normalization_and_dlq ... ok
test_day4_neo4j_idempotent_sink_and_upserts ... ok
test_day5_fraud_queries_and_suspicious_paths ... ok
test_day6_circular_flow_and_risk_scoring ... ok
test_day7_latency_benchmarking_and_optimization ... ok

----------------------------------------------------------------------
Ran 7 tests

OK
```

- **Day 1 — PASS**
- **Day 2 — PASS**
- **Day 3 — PASS**
- **Day 4 — PASS**
- **Day 5 — PASS**
- **Day 6 — PASS**
- **Day 7 — PASS**

**7/7 tests passed**

## Week 2 Deliverables

- **Working Flink job**
- **Kafka-to-Neo4j pipeline**
- **Clean transaction stream**
- **Idempotent graph updates**
- **Cypher fraud query set**
- **Circular-flow detection**
- **Initial risk-score prototype**
- **Latency benchmark**
- **Week 2 test verification**

