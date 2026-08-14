import unittest
import os
import sys

# Ensure simulator directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from generator import DataGenerator
from detect_patterns import AMLPatternDetector

class TestAMLDetectionLogic(unittest.TestCase):
    def setUp(self):
        self.generator = DataGenerator(num_people=20, num_banks=2)

    def test_circular_flow_generation_and_rules(self):
        """Validates that circular flow topologies meet the detection query rules."""
        cycles = self.generator.generate_circular_flow()
        self.assertEqual(len(cycles), 3, "Circular flow should generate 3 transactions for a 3-hop ring.")
        
        # Verify loop closure
        a1_src = cycles[0].source_account
        a1_dst = cycles[0].dest_account
        a2_dst = cycles[1].dest_account
        a3_dst = cycles[2].dest_account
        
        self.assertEqual(cycles[1].source_account, a1_dst, "Hop 2 source should match Hop 1 destination.")
        self.assertEqual(cycles[2].source_account, a2_dst, "Hop 3 source should match Hop 2 destination.")
        self.assertEqual(a3_dst, a1_src, "Hop 3 destination should close the cycle back to Hop 1 source.")
        
        # Verify amounts & suspicious flag
        base_amt = cycles[0].amount
        for tx in cycles:
            self.assertTrue(tx.is_suspicious)
            self.assertAlmostEqual(tx.amount, base_amt, delta=0.01)

    def test_smurfing_funnel_generation_and_rules(self):
        """Validates that smurfing/funneling topologies meet the detection query rules."""
        funnel = self.generator.generate_syndicate_funnel()
        self.assertGreaterEqual(len(funnel), 3, "Funnel pattern must have at least 3 source transactions.")
        
        dest_account = funnel[0].dest_account
        distinct_sources = set()
        for tx in funnel:
            self.assertTrue(tx.is_suspicious)
            self.assertEqual(tx.dest_account, dest_account, "All funnel transactions must aggregate into single destination.")
            self.assertGreaterEqual(tx.amount, 5000.0, "Smurfing amount should be >= $5,000.")
            self.assertLess(tx.amount, 10000.0, "Smurfing amount should be under $10,000 reporting threshold.")
            distinct_sources.add(tx.source_account)
            
        self.assertGreaterEqual(len(distinct_sources), 3, "Must have >= 3 distinct source accounts.")

    def test_multihop_layering_generation_and_rules(self):
        """Validates that multi-hop chains meet the detection query rules."""
        chain = self.generator.generate_multi_hop_intermediary()
        self.assertGreaterEqual(len(chain), 2, "Chain must have at least 2 hops.")
        
        # Verify sequential account forwarding
        for i in range(len(chain) - 1):
            curr_tx = chain[i]
            next_tx = chain[i + 1]
            self.assertTrue(curr_tx.is_suspicious)
            self.assertEqual(curr_tx.dest_account, next_tx.source_account, "Intermediary must forward funds to next hop.")
            # Verify skimming/fee reduction rule (should be around 95% of previous hop)
            self.assertLessEqual(next_tx.amount, curr_tx.amount)
            self.assertGreaterEqual(next_tx.amount, 0.80 * curr_tx.amount)

    def test_cypher_file_exists_and_contains_all_queries(self):
        """Verifies that database/detection_queries.cypher contains all required query definitions."""
        cypher_path = os.path.join(os.path.dirname(__file__), "..", "database", "detection_queries.cypher")
        self.assertTrue(os.path.exists(cypher_path), "detection_queries.cypher must exist.")
        
        with open(cypher_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        self.assertIn("DetectCircularFlow3Hop", content)
        self.assertIn("DetectSmurfingFunnel", content)
        self.assertIn("DetectMultiHopLayeringChain", content)
        self.assertIn("DetectTwoHopPassThrough", content)

    def test_live_neo4j_if_available(self):
        """Attempts live detection if Neo4j is running."""
        detector = AMLPatternDetector(connection_timeout=2.0)
        try:
            if detector.verify_connectivity():
                stats = detector.get_database_stats()
                self.assertIsNotNone(stats)
                print(f"\n[Live Test] Successfully connected to Neo4j. Stats: {stats}")
            else:
                print("\n[Live Test] Neo4j is not currently active on localhost:7687 (Docker container stopped).")
        finally:
            detector.close()

if __name__ == "__main__":
    unittest.main()
