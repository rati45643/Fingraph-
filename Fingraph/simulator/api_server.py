import os
import sys
import json
import time
import logging
from urllib.parse import urlparse, parse_qs
from http.server import HTTPServer, ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional

# Ensure simulator directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from alert_engine import AMLAlertEngine
from graph_analytics import GraphAnalyticsEngine
from detect_patterns import AMLPatternDetector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinGraph-API")

API_PORT = int(os.getenv("PORT", "8000"))
API_HOST = os.getenv("HOST", "0.0.0.0")

class FinGraphAPIHandler(BaseHTTPRequestHandler):
    """
    High-performance Multi-threaded REST API Request Handler using Python standard library.
    Exposes endpoints for AML health, stats, accounts, alerts, communities, and SARs.
    """

    # Shared engines
    alert_engine: Optional[AMLAlertEngine] = None
    analytics_engine: Optional[GraphAnalyticsEngine] = None
    pattern_detector: Optional[AMLPatternDetector] = None

    @classmethod
    def initialize_engines(cls):
        if cls.alert_engine is None:
            cls.alert_engine = AMLAlertEngine()
        if cls.analytics_engine is None:
            cls.analytics_engine = GraphAnalyticsEngine()
        if cls.pattern_detector is None:
            cls.pattern_detector = AMLPatternDetector()

    def _set_headers(self, status_code=200, content_type="application/json"):
        self.send_response(status_code)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(204)

    def _send_json(self, data: Any, status_code=200):
        self._set_headers(status_code, "application/json")
        self.wfile.write(json.dumps(data, indent=2).encode("utf-8"))

    def _send_error(self, message: str, status_code=400):
        self._set_headers(status_code, "application/json")
        self.wfile.write(json.dumps({"error": message, "status": status_code}).encode("utf-8"))

    def do_GET(self):
        self.initialize_engines()
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")
        query_params = parse_qs(parsed_url.query)

        try:
            # 1. Healthcheck
            if path in ["/health", "/api/health"]:
                is_db_connected = False
                try:
                    self.alert_engine.driver.verify_connectivity()
                    is_db_connected = True
                except Exception:
                    is_db_connected = False

                self._send_json({
                    "status": "healthy" if is_db_connected else "degraded",
                    "neo4j_connected": is_db_connected,
                    "service": "FinGraph AML Graph Analytics REST API",
                    "version": "2.3.0",
                    "timestamp": int(time.time())
                })

            # 2. System Stats
            elif path in ["/stats", "/api/stats"]:
                with self.alert_engine.driver.session() as session:
                    # Safe direct Cypher counts for core entities
                    counts_query = """
                    RETURN {
                        persons: count { MATCH (:Person) },
                        banks: count { MATCH (:Bank) },
                        accounts: count { MATCH (:Account) },
                        transactions: count { MATCH (:Transaction) },
                        suspicious_transactions: count { MATCH (t:Transaction) WHERE t.is_suspicious = true },
                        legitimate_transactions: count { MATCH (t:Transaction) WHERE t.is_suspicious IS NULL OR t.is_suspicious = false },
                        alerts: count { MATCH (:Alert) },
                        suspicious_rings: count { MATCH (:SuspiciousRing) },
                        risk_assessments: count { MATCH (:RiskAssessment) }
                    } AS entity_counts
                    """
                    counts_res = session.run(counts_query).single()
                    entity_counts = counts_res["entity_counts"] if counts_res else {}

                    # Alert counts by severity breakdown
                    alert_counts_res = session.run("""
                        MATCH (al:Alert)
                        RETURN al.severity AS severity, count(al) AS count
                    """).data()
                    severity_map = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
                    for r in alert_counts_res:
                        if r["severity"]:
                            severity_map[r["severity"].upper()] = r["count"]

                    # Account risk level breakdown
                    risk_counts_res = session.run("""
                        MATCH (a:Account)
                        WHERE a.risk_level IS NOT NULL
                        RETURN a.risk_level AS level, count(a) AS count
                    """).data()
                    risk_level_map = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0}
                    for r in risk_counts_res:
                        if r["level"]:
                            risk_level_map[r["level"].upper()] = r["count"]

                self._send_json({
                    "total_accounts": entity_counts.get("accounts", 0),
                    "total_transactions": entity_counts.get("transactions", 0),
                    "total_alerts": entity_counts.get("alerts", 0),
                    "critical_alerts": severity_map.get("CRITICAL", 0),
                    "high_alerts": severity_map.get("HIGH", 0),
                    "medium_alerts": severity_map.get("MEDIUM", 0),
                    "low_alerts": severity_map.get("LOW", 0),
                    "suspicious_rings": entity_counts.get("suspicious_rings", 0),
                    "entity_counts": entity_counts,
                    "alerts_by_severity": severity_map,
                    "accounts_by_risk_level": risk_level_map,
                    "timestamp": int(time.time())
                })

            # 3. List Accounts with Filter & Sorting
            elif path == "/api/accounts":
                risk_level = query_params.get("risk_level", [None])[0]
                comm_id = query_params.get("community_id", [None])[0]
                limit = int(query_params.get("limit", [50])[0])

                query = """
                MATCH (a:Account)
                WHERE ($risk_level IS NULL OR a.risk_level = $risk_level)
                  AND ($comm_id IS NULL OR a.community_id = toInteger($comm_id))
                OPTIONAL MATCH (p:Person)-[:OWNS]->(a)
                OPTIONAL MATCH (b:Bank)-[:HOSTS]->(a)
                RETURN a.account_id AS account_id,
                       a.account_type AS account_type,
                       a.risk_score AS risk_score,
                       a.risk_level AS risk_level,
                       a.community_id AS community_id,
                       a.in_degree AS in_degree,
                       a.out_degree AS out_degree,
                       a.pagerank AS pagerank,
                       a.betweenness_centrality AS betweenness_centrality,
                       a.is_hub AS is_hub,
                       a.is_mule AS is_mule,
                       p.name AS owner_name,
                       b.name AS bank_name
                ORDER BY a.risk_score DESC, a.account_id ASC
                LIMIT $limit
                """
                with self.alert_engine.driver.session() as session:
                    records = session.run(query, risk_level=risk_level, comm_id=comm_id, limit=limit).data()
                self._send_json({"count": len(records), "accounts": records})

            # 4. Get Account Detail
            elif path.startswith("/api/accounts/"):
                account_id = path.split("/api/accounts/")[1]
                audit = self.alert_engine.generate_sar_report(account_id)
                if "error" in audit:
                    self._send_error(audit["error"], 404)
                else:
                    self._send_json(audit)

            # 5. List Alerts
            elif path == "/api/alerts":
                severity = query_params.get("severity", [None])[0]
                pattern = query_params.get("pattern_type", [None])[0]
                alerts = self.alert_engine.get_active_alerts(severity_filter=severity)
                if pattern:
                    alerts = [a for a in alerts if a.get("pattern_type", "").upper() == pattern.upper()]
                self._send_json({"count": len(alerts), "alerts": alerts})

            # 6. List Communities / Fraud Syndicates
            elif path == "/api/communities":
                query = """
                MATCH (a:Account)
                WHERE a.community_id IS NOT NULL
                WITH a.community_id AS community_id,
                     collect(a.account_id) AS members,
                     round(avg(coalesce(a.risk_score, 0.0)), 1) AS avg_risk_score,
                     count(a) AS size
                RETURN community_id, size, avg_risk_score, members
                ORDER BY size DESC, avg_risk_score DESC
                """
                with self.alert_engine.driver.session() as session:
                    records = session.run(query).data()
                self._send_json({"count": len(records), "communities": records})

            # 7. Generate SAR Report
            elif path.startswith("/api/sar/"):
                account_id = path.split("/api/sar/")[1]
                sar = self.alert_engine.generate_sar_report(account_id)
                if "error" in sar:
                    self._send_error(sar["error"], 404)
                else:
                    self._send_json(sar)

            else:
                self._send_error(f"Endpoint '{path}' not found.", 404)

        except Exception as e:
            logger.exception(f"Error handling GET {path}: {e}")
            self._send_error(str(e), 500)

    def do_POST(self):
        self.initialize_engines()
        parsed_url = urlparse(self.path)
        path = parsed_url.path.rstrip("/")

        try:
            # 1. Trigger Alert & Risk Generation Pipeline
            if path in ["/api/alerts/generate", "/api/detect"]:
                self.analytics_engine.calculate_composite_risk_scores()
                self.analytics_engine.persist_results_to_neo4j()
                alerts, rings = self.alert_engine.evaluate_and_generate_alerts()
                persisted = self.alert_engine.persist_alerts_to_neo4j()

                self._send_json({
                    "status": "success",
                    "alerts_generated": len(alerts),
                    "rings_generated": len(rings),
                    "persisted": persisted,
                    "timestamp": int(time.time())
                })
            else:
                self._send_error(f"Endpoint '{path}' not found or does not accept POST.", 404)

        except Exception as e:
            logger.exception(f"Error handling POST {path}: {e}")
            self._send_error(str(e), 500)

    def log_message(self, format, *args):
        # Suppress standard access logging to keep console clean
        return

def create_server(host=API_HOST, port=API_PORT) -> ThreadingHTTPServer:
    """Creates a multi-threaded HTTP server."""
    FinGraphAPIHandler.initialize_engines()
    server = ThreadingHTTPServer((host, port), FinGraphAPIHandler)
    return server

def run_server():
    server = create_server()
    print(f"[*] FinGraph REST API server listening on http://{API_HOST}:{API_PORT} (Press Ctrl+C to stop)...", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Shutting down API server...", flush=True)
    finally:
        server.server_close()

if __name__ == "__main__":
    run_server()
