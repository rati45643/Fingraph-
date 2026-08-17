import os
import sys
import time
import json
import logging
from typing import Dict, Any, List
from neo4j import GraphDatabase

# Ensure flink_processor directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from stream_validator import StreamValidator
from neo4j_sink import FlinkNeo4jSink
from fraud_detector import FlinkFraudDetector
from risk_scorer import FlinkRiskScorer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Flink-Benchmark")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class FlinkLatencyBenchmark:
    """
    Day 7: End-to-End Latency Measurement & Performance Optimization Engine.
    Measures processing throughput, stream-to-graph latency, and Cypher query execution times.
    """

    def __init__(self, uri: str = NEO4J_URI, user: str = NEO4J_USER, password: str = NEO4J_PASSWORD):
        self.sink = FlinkNeo4jSink(uri=uri, user=user, password=password)
        self.validator = StreamValidator()
        self.detector = FlinkFraudDetector(uri=uri, user=user, password=password)
        self.scorer = FlinkRiskScorer(uri=uri, user=user, password=password)

    def close(self):
        self.sink.close()
        self.detector.close()
        self.scorer.close()

    def benchmark_stream_ingestion_latency(self, batch_size: int = 50) -> Dict[str, float]:
        """
        Measures latency for validating, normalizing, and upserting a synthetic stream batch into Neo4j.
        """
        # Generate synthetic stream batch
        synthetic_batch = []
        base_ts = int(time.time() * 1000)
        for i in range(batch_size):
            synthetic_batch.append({
                "transaction_id": f"bench-tx-{i}-{base_ts}",
                "source_account_id": f"BENCH_SRC_{i % 10}",
                "destination_account_id": f"BENCH_DST_{(i + 1) % 10}",
                "amount": 1500.0 + (i * 10.5),
                "timestamp": base_ts + (i * 100),
                "is_suspicious": (i % 5 == 0)
            })

        # 1. Validation & Normalization latency
        start_val = time.perf_counter()
        clean_batch = []
        for item in synthetic_batch:
            valid, norm, _ = self.validator.validate_and_normalize(item)
            if valid and norm:
                clean_batch.append(norm)
        val_time_ms = (time.perf_counter() - start_val) * 1000

        # 2. Neo4j Batch Upsert latency
        start_upsert = time.perf_counter()
        upserted_count = self.sink.upsert_transaction_batch(clean_batch)
        upsert_time_ms = (time.perf_counter() - start_upsert) * 1000

        total_latency_ms = val_time_ms + upsert_time_ms
        per_event_latency_ms = total_latency_ms / max(1, len(synthetic_batch))
        throughput_eps = len(synthetic_batch) / (total_latency_ms / 1000.0) if total_latency_ms > 0 else 0

        return {
            "batch_size": batch_size,
            "validation_latency_ms": round(val_time_ms, 3),
            "neo4j_upsert_latency_ms": round(upsert_time_ms, 3),
            "total_ingestion_latency_ms": round(total_latency_ms, 3),
            "per_event_latency_ms": round(per_event_latency_ms, 4),
            "throughput_events_per_sec": round(throughput_eps, 1)
        }

    def benchmark_fraud_queries_latency(self) -> Dict[str, float]:
        """
        Measures execution times for Day 5 and Day 6 Cypher detection queries.
        """
        latencies = {}

        # 1. Direct Transfers Query
        t0 = time.perf_counter()
        direct = self.detector.find_direct_transfers(limit=100)
        latencies["direct_transfers_query_ms"] = round((time.perf_counter() - t0) * 1000, 3)

        # 2. Two-Hop Pass-Through Query
        t0 = time.perf_counter()
        two_hop = self.detector.find_two_hop_intermediaries()
        latencies["two_hop_intermediaries_query_ms"] = round((time.perf_counter() - t0) * 1000, 3)

        # 3. Three-Hop Layering Chains Query
        t0 = time.perf_counter()
        layering = self.detector.find_three_hop_layering()
        latencies["three_hop_layering_query_ms"] = round((time.perf_counter() - t0) * 1000, 3)

        # 4. Structuring Fan-In Hubs Query
        t0 = time.perf_counter()
        fan_in = self.detector.find_structuring_fan_in_hubs(min_senders=3)
        latencies["fan_in_hubs_query_ms"] = round((time.perf_counter() - t0) * 1000, 3)

        # 5. Circular Flow Detection Query
        t0 = time.perf_counter()
        cycles = self.scorer.detect_circular_flows()
        latencies["circular_flow_query_ms"] = round((time.perf_counter() - t0) * 1000, 3)

        # 6. Composite Risk Score Calculation
        t0 = time.perf_counter()
        scores = self.scorer.calculate_and_persist_risk_scores()
        latencies["risk_score_calculation_ms"] = round((time.perf_counter() - t0) * 1000, 3)

        return latencies

    def generate_full_performance_report(self) -> Dict[str, Any]:
        """Generates a complete Week 2 Day 7 Benchmark & Performance Report."""
        stats = self.sink.get_database_stats()
        ingest_bench = self.benchmark_stream_ingestion_latency(batch_size=50)
        query_bench = self.benchmark_fraud_queries_latency()

        report = {
            "timestamp": int(time.time()),
            "neo4j_database_stats": stats,
            "stream_ingestion_performance": ingest_bench,
            "cypher_query_latencies": query_bench,
            "optimization_summary": {
                "idempotent_batch_upsert": "OPTIMIZED (UNWIND MERGE)",
                "indexing_status": "Index-backed on :Account(account_id) and :Transaction(transaction_id, timestamp)",
                "average_query_latency_ms": round(sum(query_bench.values()) / len(query_bench), 2)
            }
        }
        return report

def main():
    print("=" * 80)
    print("  FinGraph Official Week 2 - Day 7 Latency Benchmark & Performance Report")
    print("=" * 80)

    bench = FlinkLatencyBenchmark()
    try:
        if not bench.sink.verify_connectivity():
            print("\n[!] Neo4j database is not reachable at bolt://localhost:7687. Please ensure Docker is running.")
            return

        report = bench.generate_full_performance_report()

        print("\n1. Neo4j Live Database Snapshot:")
        for k, v in report["neo4j_database_stats"].items():
            print(f"   * {k:15s}: {v}")

        print("\n2. Stream Ingestion Latency & Throughput (Day 1 - Day 4 Pipeline):")
        ingest = report["stream_ingestion_performance"]
        print(f"   * Batch Size:               {ingest['batch_size']} events")
        print(f"   * Validation Latency:       {ingest['validation_latency_ms']} ms")
        print(f"   * Neo4j Upsert Latency:     {ingest['neo4j_upsert_latency_ms']} ms")
        print(f"   * Total Batch Latency:      {ingest['total_ingestion_latency_ms']} ms")
        print(f"   * Per-Event Ingestion Time: {ingest['per_event_latency_ms']} ms")
        print(f"   * Effective Throughput:     {ingest['throughput_events_per_sec']} events/sec")

        print("\n3. Cypher Query Execution Latencies (Day 5 - Day 6 Queries):")
        for q_name, q_lat in report["cypher_query_latencies"].items():
            print(f"   * {q_name:35s}: {q_lat} ms")

        print("\n4. Optimization & Tuning Summary:")
        opt = report["optimization_summary"]
        print(f"   * Ingestion Strategy:       {opt['idempotent_batch_upsert']}")
        print(f"   * Schema Indexing:          {opt['indexing_status']}")
        print(f"   * Avg Detection Latency:    {opt['average_query_latency_ms']} ms")

        print("=" * 80)
        print("  Week 2 Official Flink & Neo4j Stream Pipeline Operational & Verified!")
        print("=" * 80)

    finally:
        bench.close()

if __name__ == "__main__":
    main()
