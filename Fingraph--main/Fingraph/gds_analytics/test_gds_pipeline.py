import os
import sys
import unittest
import logging
from typing import Dict, Any

# Ensure project root is on sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from gds_analytics.gds_runner import FinGraphGDSRunner, DEFAULT_GRAPH_NAME

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinGraph-GDSTest")

class TestFinGraphGDSPipeline(unittest.TestCase):
    """
    Unit & Integration test suite verifying Week 3 Neo4j GDS analytics engine.
    """

    @classmethod
    def setUpClass(cls):
        cls.runner = FinGraphGDSRunner()
        if not cls.runner.verify_connectivity():
            raise ConnectionError("Neo4j database is unreachable at bolt://localhost:7687. Ensure container is running.")

        # Ensure database has seed data to project
        cls._ensure_graph_data()

    @classmethod
    def tearDownClass(cls):
        if cls.runner:
            cls.runner.drop_projection(DEFAULT_GRAPH_NAME)
            cls.runner.close()

    @classmethod
    def _ensure_graph_data(cls):
        """Seeds sample accounts and transactions if the graph is currently empty."""
        with cls.runner._driver.session() as session:
            count_res = session.run("MATCH (a:Account) RETURN count(a) AS c").single()
            if count_res and count_res["c"] > 0:
                logger.info(f"Graph already populated with {count_res['c']} Account nodes.")
                return

            logger.info("Populating test seed graph with Accounts and Transactions...")
            seed_cypher = """
            MERGE (b1:Bank {bank_id: 'BANK_01', name: 'Global Standard Bank'})
            MERGE (b2:Bank {bank_id: 'BANK_02', name: 'Apex Trust'})

            MERGE (p1:Person {person_id: 'P_01', name: 'Alice Walker'})
            MERGE (p2:Person {person_id: 'P_02', name: 'Bob Vance'})
            MERGE (p3:Person {person_id: 'P_03', name: 'Charlie Day'})
            MERGE (p4:Person {person_id: 'P_04', name: 'Diana Prince'})

            MERGE (a1:Account {account_id: 'ACC_01', account_type: 'Checking', risk_score: 85.0, risk_level: 'CRITICAL'})
            MERGE (a2:Account {account_id: 'ACC_02', account_type: 'Savings', risk_score: 60.0, risk_level: 'HIGH'})
            MERGE (a3:Account {account_id: 'ACC_03', account_type: 'Checking', risk_score: 30.0, risk_level: 'MEDIUM'})
            MERGE (a4:Account {account_id: 'ACC_04', account_type: 'Savings', risk_score: 10.0, risk_level: 'LOW'})

            MERGE (p1)-[:OWNS]->(a1)
            MERGE (p2)-[:OWNS]->(a2)
            MERGE (p3)-[:OWNS]->(a3)
            MERGE (p4)-[:OWNS]->(a4)

            MERGE (b1)-[:HOSTS]->(a1)
            MERGE (b1)-[:HOSTS]->(a2)
            MERGE (b2)-[:HOSTS]->(a3)
            MERGE (b2)-[:HOSTS]->(a4)

            // Transactions (A1 -> A2, A2 -> A3, A3 -> A1 cycle + A3 -> A4 transfer)
            MERGE (t1:Transaction {transaction_id: 'TX_SEED_01', amount: 9500.0, timestamp: 1672531200000, is_suspicious: true})
            MERGE (t2:Transaction {transaction_id: 'TX_SEED_02', amount: 9000.0, timestamp: 1672531300000, is_suspicious: true})
            MERGE (t3:Transaction {transaction_id: 'TX_SEED_03', amount: 8500.0, timestamp: 1672531400000, is_suspicious: true})
            MERGE (t4:Transaction {transaction_id: 'TX_SEED_04', amount: 500.0, timestamp: 1672531500000, is_suspicious: false})

            MERGE (a1)-[:SENDS]->(t1)
            MERGE (t1)-[:TRANSFERRED_TO]->(a2)

            MERGE (a2)-[:SENDS]->(t2)
            MERGE (t2)-[:TRANSFERRED_TO]->(a3)

            MERGE (a3)-[:SENDS]->(t3)
            MERGE (t3)-[:TRANSFERRED_TO]->(a1)

            MERGE (a3)-[:SENDS]->(t4)
            MERGE (t4)-[:TRANSFERRED_TO]->(a4)
            """
            session.run(seed_cypher).consume()
            logger.info("Test seed graph created.")

    def test_01_neo4j_connectivity(self):
        """1. Verify Neo4j connectivity"""
        self.assertTrue(self.runner.verify_connectivity(), "Neo4j connection should be active.")

    def test_02_gds_version(self):
        """2. Verify GDS version retrieval"""
        gds_ver = self.runner.get_gds_version()
        self.assertIsNotNone(gds_ver, "GDS version should be returned.")
        self.assertTrue(gds_ver.startswith("2."), f"GDS version should be 2.x, got {gds_ver}")
        logger.info(f"Verified GDS Version: {gds_ver}")

    def test_03_graph_projection_lifecycle(self):
        """3. Verify in-memory graph projection creation & safe drop"""
        test_graph = "finGraph_test_proj"
        # Drop if exists
        self.runner.drop_projection(test_graph)
        self.assertFalse(self.runner.graph_exists(test_graph))

        # Project
        stats = self.runner.project_transfers_graph(test_graph)
        self.assertEqual(stats["graphName"], test_graph)
        self.assertGreater(stats["nodeCount"], 0)
        self.assertGreater(stats["relationshipCount"], 0)
        self.assertTrue(self.runner.graph_exists(test_graph))

        # Drop
        dropped = self.runner.drop_projection(test_graph)
        self.assertTrue(dropped)
        self.assertFalse(self.runner.graph_exists(test_graph))

    def test_04_full_gds_pipeline_execution(self):
        """4. Verify PageRank, WCC, and Louvain execution & node property writeback"""
        # Run projection
        proj_stats = self.runner.project_transfers_graph(DEFAULT_GRAPH_NAME)
        self.assertGreater(proj_stats["nodeCount"], 0)

        # 1. PageRank
        pr_res = self.runner.run_pagerank(DEFAULT_GRAPH_NAME, write_property="pagerank_score")
        self.assertEqual(pr_res["algorithm"], "PageRank")
        self.assertGreater(pr_res["nodePropertiesWritten"], 0)

        # 2. WCC
        wcc_res = self.runner.run_wcc(DEFAULT_GRAPH_NAME, write_property="wcc_component")
        self.assertEqual(wcc_res["algorithm"], "WCC")
        self.assertGreater(wcc_res["componentCount"], 0)
        self.assertGreater(wcc_res["nodePropertiesWritten"], 0)

        # 3. Louvain
        louvain_res = self.runner.run_louvain(DEFAULT_GRAPH_NAME, write_property="louvain_community")
        self.assertEqual(louvain_res["algorithm"], "Louvain")
        self.assertGreater(louvain_res["communityCount"], 0)
        self.assertGreater(louvain_res["nodePropertiesWritten"], 0)

        # Verify persisted properties on Account nodes in Neo4j
        with self.runner._driver.session() as session:
            accounts = session.run("""
                MATCH (a:Account)
                RETURN a.account_id AS id,
                       a.pagerank_score AS pr,
                       a.wcc_component AS wcc,
                       a.louvain_community AS louvain,
                       a.risk_score AS risk_score,
                       a.risk_level AS risk_level
            """).data()

            self.assertGreater(len(accounts), 0, "Accounts must exist in Neo4j.")
            for acc in accounts:
                self.assertIsNotNone(acc["pr"], f"pagerank_score must not be null for {acc['id']}")
                self.assertIsNotNone(acc["wcc"], f"wcc_component must not be null for {acc['id']}")
                self.assertIsNotNone(acc["louvain"], f"louvain_community must not be null for {acc['id']}")
                # Ensure existing risk score & level are preserved
                self.assertIsNotNone(acc["risk_score"], f"risk_score must be preserved for {acc['id']}")
                self.assertIsNotNone(acc["risk_level"], f"risk_level must be preserved for {acc['id']}")

        logger.info(f"Verified GDS properties written for {len(accounts)} accounts.")

    def test_05_summary_retrieval(self):
        """5. Verify summary results query"""
        summary = self.runner.get_gds_results_summary(limit=5)
        self.assertIsInstance(summary, list)
        self.assertGreater(len(summary), 0)
        top = summary[0]
        self.assertIn("account_id", top)
        self.assertIn("pagerank_score", top)
        self.assertIn("wcc_component", top)
        self.assertIn("louvain_community", top)
        self.assertIn("risk_score", top)
        self.assertIn("risk_level", top)

def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFinGraphGDSPipeline)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
