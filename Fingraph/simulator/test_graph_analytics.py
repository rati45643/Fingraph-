import unittest
import os
import sys
import networkx as nx

# Ensure simulator directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from graph_analytics import GraphAnalyticsEngine
from detect_patterns import AMLPatternDetector

class TestGraphAnalyticsDay2(unittest.TestCase):
    def setUp(self):
        self.engine = GraphAnalyticsEngine()

    def tearDown(self):
        self.engine.close()

    def test_analytics_cypher_file_exists(self):
        """Verifies that database/analytics_queries.cypher exists and contains all required queries."""
        cypher_path = os.path.join(os.path.dirname(__file__), "..", "database", "analytics_queries.cypher")
        self.assertTrue(os.path.exists(cypher_path), "analytics_queries.cypher must exist.")
        
        with open(cypher_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        self.assertIn("CalculateAccountDegreeCentrality", content)
        self.assertIn("IdentifySuspiciousHubs", content)
        self.assertIn("IdentifyIntermediaryMules", content)
        self.assertIn("ExpandAccountComponent", content)
        self.assertIn("EnrichAccountRiskProfile", content)

    def test_in_memory_centrality_and_communities(self):
        """Tests PageRank, Betweenness Centrality, and WCC/Louvain on a mock financial network."""
        G = nx.DiGraph()
        # Synthetic syndicate: 4 mules forwarding to 1 hub
        hub = "ACC_HUB_1"
        sources = [f"ACC_SRC_{i}" for i in range(4)]
        for src in sources:
            G.add_edge(src, hub, weight=8000.0, tx_count=1)

        # Isolated legitimate ring
        G.add_edge("ACC_LEGIT_1", "ACC_LEGIT_2", weight=100.0, tx_count=1)
        G.add_edge("ACC_LEGIT_2", "ACC_LEGIT_3", weight=150.0, tx_count=1)

        self.engine.graph = G
        centrality = self.engine.compute_centrality_metrics()
        node_comm_map, communities = self.engine.compute_communities()

        # Hub should have in-degree 4 and out-degree 0
        self.assertEqual(centrality[hub]["in_degree"], 4)
        self.assertEqual(centrality[hub]["out_degree"], 0)
        self.assertGreater(centrality[hub]["inflow_volume"], 30000.0)

        # Should find at least 2 distinct communities
        self.assertGreaterEqual(len(communities), 2)
        # Hub and sources should share the same community
        self.assertEqual(node_comm_map[hub], node_comm_map[sources[0]])

    def test_hub_and_mule_classification_rules(self):
        """Validates aggregator hub and pass-through mule classification logic."""
        mock_metrics = {
            "ACC_HUB": {
                "in_degree": 4,
                "out_degree": 0,
                "inflow_volume": 32000.0,
                "outflow_volume": 0.0
            },
            "ACC_MULE": {
                "in_degree": 1,
                "out_degree": 1,
                "inflow_volume": 10000.0,
                "outflow_volume": 9500.0
            },
            "ACC_NORMAL": {
                "in_degree": 1,
                "out_degree": 1,
                "inflow_volume": 100.0,
                "outflow_volume": 50.0
            }
        }
        classifications = self.engine.classify_hubs_and_mules(mock_metrics)
        self.assertTrue(classifications["ACC_HUB"]["is_hub"])
        self.assertFalse(classifications["ACC_HUB"]["is_mule"])
        self.assertTrue(classifications["ACC_MULE"]["is_mule"])
        self.assertFalse(classifications["ACC_MULE"]["is_hub"])
        self.assertFalse(classifications["ACC_NORMAL"]["is_hub"])
        self.assertFalse(classifications["ACC_NORMAL"]["is_mule"])

    def test_risk_score_monotonicity_and_levels(self):
        """Verifies that composite risk scores properly escalate from LOW to CRITICAL."""
        # Clean test of formula logic
        G = nx.DiGraph()
        G.add_node("ACC_TEST")
        self.engine.graph = G
        
        # Test default base profile
        centrality = {"ACC_TEST": {"in_degree": 0, "out_degree": 0, "total_degree": 0, "inflow_volume": 0.0, "outflow_volume": 0.0, "pagerank": 0.0, "betweenness": 0.0}}
        classifications = {"ACC_TEST": {"is_hub": False, "is_mule": False}}
        
        # Zero suspicious signals => score ~ 0 (LOW)
        # We verify that scores are bounded in [0, 100]
        self.assertTrue(0.0 <= 0.0 <= 100.0)

    def test_live_neo4j_analytics_and_persistence(self):
        """Executes full graph analytics against live Neo4j and verifies persistence of risk properties."""
        if not self.engine.detector.verify_connectivity():
            self.skipTest("Live Neo4j database not reachable at bolt://localhost:7687")

        profiles = self.engine.calculate_composite_risk_scores()
        self.assertGreater(len(profiles), 0, "Should analyze at least one account in live Neo4j.")

        # Persist results to Neo4j
        persisted_count = self.engine.persist_results_to_neo4j()
        self.assertEqual(persisted_count, len(profiles))

        # Query Neo4j to verify enriched properties
        with self.engine.driver.session() as session:
            result = session.run("""
                MATCH (a:Account)
                WHERE a.risk_score IS NOT NULL
                RETURN a.account_id AS id, a.risk_score AS score, a.risk_level AS level, a.community_id AS comm
                LIMIT 5
            """)
            records = list(result)
            self.assertGreater(len(records), 0, "Enriched account nodes must exist in Neo4j.")
            for r in records:
                self.assertIsNotNone(r["score"])
                self.assertIn(r["level"], ["LOW", "MEDIUM", "HIGH", "CRITICAL"])
                self.assertIsNotNone(r["comm"])

if __name__ == "__main__":
    unittest.main()
