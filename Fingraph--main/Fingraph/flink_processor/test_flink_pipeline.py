import os
import sys
import unittest
import time

# Ensure flink_processor directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from flink_job import FlinkStreamPipeline
from kafka_source import FlinkKafkaSource
from stream_validator import StreamValidator
from neo4j_sink import FlinkNeo4jSink
from fraud_detector import FlinkFraudDetector
from risk_scorer import FlinkRiskScorer
from benchmark_and_test import FlinkLatencyBenchmark

class TestOfficialFlinkWeek2(unittest.TestCase):
    """
    Automated Test Suite for Official Week 2 Flink Curriculum (Day 1 to Day 7).
    """

    @classmethod
    def setUpClass(cls):
        cls.sink = FlinkNeo4jSink()
        cls.is_db_online = cls.sink.verify_connectivity()

    @classmethod
    def tearDownClass(cls):
        cls.sink.close()

    # ------------------------------------------------------------------------
    # DAY 1: Flink Job Skeleton & Stream Pipeline
    # ------------------------------------------------------------------------
    def test_day1_flink_skeleton_pipeline(self):
        """Day 1: Validates Flink job skeleton execution over a mock streaming batch."""
        pipeline = FlinkStreamPipeline(window_size=10, window_time_ms=500)
        
        mock_events = [
            {
                "transaction_id": f"day1-tx-{i}",
                "source_account_id": f"ACC_SRC_{i}",
                "destination_account_id": f"ACC_DST_{i}",
                "amount": 100.0 * (i + 1),
                "timestamp": int(time.time() * 1000) + i
            }
            for i in range(15)
        ]
        # Append 2 malformed events
        mock_events.append({"transaction_id": "malformed-1", "amount": -50.0}) # Invalid amount & missing accounts
        mock_events.append({"source_account_id": "ACC_A", "destination_account_id": "ACC_A", "amount": 100.0, "transaction_id": "mal-2", "timestamp": 123}) # Self-transfer

        result = pipeline.process_event_stream(iter(mock_events))
        self.assertEqual(result["processed_valid"], 15, "Should process all 15 valid events.")
        self.assertEqual(result["rejected_dlq"], 2, "Should reject both malformed events.")
        self.assertEqual(result["dlq_total"], 2, "DLQ should contain 2 records.")
        pipeline.close()

    # ------------------------------------------------------------------------
    # DAY 2: Kafka Live Source Connector
    # ------------------------------------------------------------------------
    def test_day2_kafka_live_source_and_parsing(self):
        """Day 2: Validates Kafka source connector configuration and event parsing."""
        source = FlinkKafkaSource(topic="transactions", group_id="test-flink-source-group")
        source.connect(timeout_ms=1000)
        
        # Test polling method resilience
        events = list(source.poll_events(max_records=10))
        self.assertIsInstance(events, list)
        source.close()

    # ------------------------------------------------------------------------
    # DAY 3: Stream Validation, Normalization & DLQ Routing
    # ------------------------------------------------------------------------
    def test_day3_stream_validation_normalization_and_dlq(self):
        """Day 3: Validates schema rules, field normalization, and Dead-Letter Queue routing."""
        validator = StreamValidator()

        # Valid transaction
        valid_ev = {
            "transaction_id": "valid-tx-001",
            "source_account_id": "ACC_001",
            "destination_account_id": "ACC_002",
            "amount": 1250.556,
            "timestamp": "1700000000000",
            "is_suspicious": True
        }
        ok, norm, reason = validator.validate_and_normalize(valid_ev)
        self.assertTrue(ok)
        self.assertIsNotNone(norm)
        self.assertEqual(norm["amount"], 1250.56)
        self.assertEqual(norm["timestamp"], 1700000000000)
        self.assertTrue(norm["is_suspicious"])

        # Invalid cases
        invalid_cases = [
            ({}, "Missing mandatory field"),
            ({"transaction_id": "tx1", "source_account_id": "A", "destination_account_id": "A", "amount": 100, "timestamp": 123}, "Self-transfer"),
            ({"transaction_id": "tx2", "source_account_id": "A", "destination_account_id": "B", "amount": -10, "timestamp": 123}, "must be > 0"),
            ("not-a-dict", "not a valid JSON dictionary")
        ]

        for bad_ev, expected_substr in invalid_cases:
            ok, _, err = validator.validate_and_normalize(bad_ev)
            self.assertFalse(ok)
            self.assertIn(expected_substr, err)

        self.assertEqual(len(validator.get_dlq_records()), len(invalid_cases))

    # ------------------------------------------------------------------------
    # DAY 4: Neo4j Idempotent Sink & Upsert Logic
    # ------------------------------------------------------------------------
    def test_day4_neo4j_idempotent_sink_and_upserts(self):
        """Day 4: Validates idempotent batch upsert and duplicate prevention in Neo4j."""
        if not self.is_db_online:
            self.skipTest("Live Neo4j database not reachable.")

        test_tx_id = f"idempotent-test-{int(time.time() * 1000)}"
        test_batch = [{
            "transaction_id": test_tx_id,
            "source_account_id": "ACC_IDEMP_SRC",
            "destination_account_id": "ACC_IDEMP_DST",
            "amount": 4500.00,
            "timestamp": int(time.time() * 1000),
            "is_suspicious": False
        }]

        # Upsert once
        count1 = self.sink.upsert_transaction_batch(test_batch)
        self.assertEqual(count1, 1)

        # Upsert the exact same batch again (replay simulation)
        count2 = self.sink.upsert_transaction_batch(test_batch)
        self.assertEqual(count2, 1)

        # Verify in Neo4j that exactly 1 Transaction node exists (no duplicate)
        with self.sink.driver.session() as session:
            res = session.run(
                "MATCH (t:Transaction {transaction_id: $tx_id}) RETURN count(t) AS c",
                tx_id=test_tx_id
            ).single()
            self.assertEqual(res["c"], 1, "Idempotent upsert must not create duplicate transaction nodes.")

    # ------------------------------------------------------------------------
    # DAY 5: Fraud Query Set & Suspicious Paths
    # ------------------------------------------------------------------------
    def test_day5_fraud_queries_and_suspicious_paths(self):
        """Day 5: Tests direct transfers, 2-hop pass-throughs, 3-hop layering, and fan-in hubs."""
        cypher_file = os.path.join(os.path.dirname(__file__), "..", "database", "fraud_queries.cypher")
        self.assertTrue(os.path.exists(cypher_file), "database/fraud_queries.cypher must exist.")
        
        with open(cypher_file, "r", encoding="utf-8") as f:
            cypher_content = f.read()
        self.assertIn("FindDirectTransfers", cypher_content)
        self.assertIn("FindTwoHopIntermediaryPaths", cypher_content)
        self.assertIn("FindThreeHopLayeringChains", cypher_content)
        self.assertIn("FindStructuringFanInHubs", cypher_content)

        if not self.is_db_online:
            self.skipTest("Live Neo4j database not reachable.")

        detector = FlinkFraudDetector()
        directs = detector.find_direct_transfers(limit=10)
        two_hops = detector.find_two_hop_intermediaries()
        layering = detector.find_three_hop_layering()
        hubs = detector.find_structuring_fan_in_hubs(min_senders=2)

        self.assertIsInstance(directs, list)
        self.assertIsInstance(two_hops, list)
        self.assertIsInstance(layering, list)
        self.assertIsInstance(hubs, list)
        detector.close()

    # ------------------------------------------------------------------------
    # DAY 6: Circular Flow Detection & Initial Risk Scoring
    # ------------------------------------------------------------------------
    def test_day6_circular_flow_and_risk_scoring(self):
        """Day 6: Tests 3-hop circular flow detection and initial risk score calculation."""
        cypher_file = os.path.join(os.path.dirname(__file__), "..", "database", "risk_queries.cypher")
        self.assertTrue(os.path.exists(cypher_file), "database/risk_queries.cypher must exist.")
        
        with open(cypher_file, "r", encoding="utf-8") as f:
            cypher_content = f.read()
        self.assertIn("DetectCircularFlowRings", cypher_content)
        self.assertIn("CalculateAccountInitialRiskScores", cypher_content)
        self.assertIn("UpdateAccountRiskProperties", cypher_content)

        if not self.is_db_online:
            self.skipTest("Live Neo4j database not reachable.")

        scorer = FlinkRiskScorer()
        cycles = scorer.detect_circular_flows()
        self.assertIsInstance(cycles, list)

        scores = scorer.calculate_and_persist_risk_scores()
        self.assertIsInstance(scores, list)
        self.assertGreater(len(scores), 0, "Should calculate risk scores for existing accounts.")

        # Verify that risk_score property is populated on :Account nodes
        with scorer.driver.session() as session:
            res = session.run("MATCH (a:Account) WHERE a.risk_score IS NOT NULL RETURN count(a) AS c").single()
            self.assertGreater(res["c"], 0, "Account nodes must have risk_score persisted.")
        scorer.close()

    # ------------------------------------------------------------------------
    # DAY 7: Latency Benchmarking & Performance Optimization
    # ------------------------------------------------------------------------
    def test_day7_latency_benchmarking_and_optimization(self):
        """Day 7: Measures end-to-end stream ingestion latency, query execution times, and reports."""
        if not self.is_db_online:
            self.skipTest("Live Neo4j database not reachable.")

        bench = FlinkLatencyBenchmark()
        report = bench.generate_full_performance_report()

        self.assertIn("stream_ingestion_performance", report)
        self.assertIn("cypher_query_latencies", report)
        self.assertIn("optimization_summary", report)

        ingest = report["stream_ingestion_performance"]
        self.assertGreater(ingest["throughput_events_per_sec"], 0)
        self.assertLess(ingest["per_event_latency_ms"], 50.0, "Per-event ingestion latency should be under 50ms.")

        latencies = report["cypher_query_latencies"]
        self.assertIn("direct_transfers_query_ms", latencies)
        self.assertIn("circular_flow_query_ms", latencies)
        bench.close()

if __name__ == "__main__":
    unittest.main()
