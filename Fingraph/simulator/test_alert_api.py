import unittest
import os
import sys
import json
import time
import threading
import urllib.request
import urllib.error

# Ensure simulator directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from alert_engine import AMLAlertEngine
from api_server import create_server

class TestAlertAndAPIDay3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = AMLAlertEngine()
        cls.test_port = 8899
        cls.server = None
        cls.server_thread = None
        
        # Start API server in background daemon thread on test port
        try:
            cls.server = create_server(host="127.0.0.1", port=cls.test_port)
            cls.server_thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
            cls.server_thread.start()
            time.sleep(0.3)
        except Exception as e:
            print(f"[Warning] Could not start test HTTP server: {e}")

    @classmethod
    def tearDownClass(cls):
        if cls.server:
            cls.server.shutdown()
            cls.server.server_close()
        cls.engine.close()

    def _http_get(self, path: str) -> dict:
        url = f"http://127.0.0.1:{self.test_port}{path}"
        req = urllib.request.Request(url, headers={"User-Agent": "TestClient"})
        with urllib.request.urlopen(req, timeout=5.0) as response:
            self.assertEqual(response.status, 200)
            return json.loads(response.read().decode("utf-8"))

    def _http_post(self, path: str, payload: dict = None) -> dict:
        url = f"http://127.0.0.1:{self.test_port}{path}"
        data = json.dumps(payload or {}).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5.0) as response:
            self.assertEqual(response.status, 200)
            return json.loads(response.read().decode("utf-8"))

    def test_alert_cypher_file_exists(self):
        """Verifies database/alert_queries.cypher exists and contains all required queries."""
        cypher_path = os.path.join(os.path.dirname(__file__), "..", "database", "alert_queries.cypher")
        self.assertTrue(os.path.exists(cypher_path), "alert_queries.cypher must exist.")
        with open(cypher_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("BatchCreateAlerts", content)
        self.assertIn("CreateSuspiciousRing", content)
        self.assertIn("GetActiveAlerts", content)
        self.assertIn("GetAccountSARAuditTrail", content)

    def test_alert_generation_and_schema(self):
        """Tests that alert evaluation produces structured alerts with appropriate severities."""
        if not self.engine.detector.verify_connectivity():
            self.skipTest("Live Neo4j database not reachable.")

        alerts, rings = self.engine.evaluate_and_generate_alerts()
        self.assertGreater(len(alerts), 0, "Alert generation should produce alerts from existing patterns.")
        
        for al in alerts:
            self.assertTrue(al["alert_id"].startswith("ALT-"))
            self.assertIn(al["severity"], ["CRITICAL", "HIGH", "MEDIUM", "LOW"])
            self.assertIn("description", al)
            self.assertIn("account_ids", al)
            self.assertGreater(len(al["account_ids"]), 0)

        for r in rings:
            self.assertTrue(r["ring_id"].startswith("RING-"))
            self.assertIn("ring_type", r)
            self.assertGreater(len(r["member_accounts"]), 0)

    def test_live_alert_persistence_to_neo4j(self):
        """Tests persisting Alert and SuspiciousRing nodes into Neo4j and verifying node presence."""
        if not self.engine.detector.verify_connectivity():
            self.skipTest("Live Neo4j database not reachable.")

        persisted = self.engine.persist_alerts_to_neo4j()
        self.assertGreaterEqual(persisted["alerts_count"], 1)

        # Check Neo4j database
        with self.engine.driver.session() as session:
            res = session.run("MATCH (al:Alert) RETURN count(al) AS alert_count").single()
            self.assertGreater(res["alert_count"], 0, "Alert nodes must exist in Neo4j.")

            res_rings = session.run("MATCH (r:SuspiciousRing) RETURN count(r) AS ring_count").single()
            self.assertGreaterEqual(res_rings["ring_count"], 1, "SuspiciousRing nodes must exist in Neo4j.")

    def test_sar_report_generation(self):
        """Verifies SAR report structure and regulatory fields."""
        if not self.engine.detector.verify_connectivity():
            self.skipTest("Live Neo4j database not reachable.")

        # Find any account in database
        with self.engine.driver.session() as session:
            rec = session.run("MATCH (a:Account) RETURN a.account_id AS id LIMIT 1").single()
            if not rec:
                self.skipTest("No account found in Neo4j.")
            target_acc = rec["id"]

        sar = self.engine.generate_sar_report(target_acc)
        self.assertTrue(sar["sar_id"].startswith("SAR-FINCEN-"))
        self.assertEqual(sar["account_id"], target_acc)
        self.assertIn("filing_date", sar)
        self.assertIn("owner", sar)
        self.assertIn("bank", sar)
        self.assertIn("narrative", sar)
        self.assertIn("transaction_summary", sar)
        self.assertIn("SUSPICIOUS ACTIVITY REPORT", sar["narrative"])

    def test_rest_api_health_endpoint(self):
        """Tests GET /api/health."""
        data = self._http_get("/api/health")
        self.assertIn(data["status"], ["healthy", "degraded"])
        self.assertIn("neo4j_connected", data)

    def test_rest_api_stats_endpoint(self):
        """Tests GET /api/stats."""
        data = self._http_get("/api/stats")
        self.assertIn("total_accounts", data)
        self.assertIn("total_transactions", data)
        self.assertIn("total_alerts", data)
        self.assertIn("critical_alerts", data)
        self.assertIn("alerts_by_severity", data)
        self.assertIn("accounts_by_risk_level", data)

    def test_rest_api_accounts_endpoint(self):
        """Tests GET /api/accounts."""
        data = self._http_get("/api/accounts?limit=10")
        self.assertIn("accounts", data)
        self.assertGreater(len(data["accounts"]), 0)
        self.assertIn("risk_score", data["accounts"][0])

    def test_rest_api_alerts_endpoint(self):
        """Tests GET /api/alerts."""
        data = self._http_get("/api/alerts")
        self.assertIn("alerts", data)

    def test_rest_api_communities_endpoint(self):
        """Tests GET /api/communities."""
        data = self._http_get("/api/communities")
        self.assertIn("communities", data)

    def test_rest_api_sar_endpoint(self):
        """Tests GET /api/sar/{account_id}."""
        with self.engine.driver.session() as session:
            rec = session.run("MATCH (a:Account) RETURN a.account_id AS id LIMIT 1").single()
            target_acc = rec["id"]

        sar = self._http_get(f"/api/sar/{target_acc}")
        self.assertEqual(sar["account_id"], target_acc)
        self.assertTrue(sar["sar_id"].startswith("SAR-FINCEN-"))

    def test_rest_api_trigger_detect_post(self):
        """Tests POST /api/alerts/generate."""
        res = self._http_post("/api/alerts/generate", {})
        self.assertEqual(res["status"], "success")
        self.assertGreater(res["alerts_generated"], 0)

if __name__ == "__main__":
    unittest.main()
