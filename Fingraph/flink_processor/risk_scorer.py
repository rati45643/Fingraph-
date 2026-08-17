import os
import logging
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Flink-RiskScorer")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class FlinkRiskScorer:
    """
    Day 6: Circular-Flow Detection & Initial Risk Score Engine.
    Detects 3-hop round-tripping rings (A -> B -> C -> A) and computes account risk scores.
    """

    def __init__(self, uri: str = NEO4J_URI, user: str = NEO4J_USER, password: str = NEO4J_PASSWORD):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password), connection_timeout=5.0)

    def close(self):
        if self.driver:
            self.driver.close()

    def detect_circular_flows(self) -> List[Dict[str, Any]]:
        """Detects 3-hop closed circular flows (A -> B -> C -> A)."""
        query = """
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
        ORDER BY average_cycle_amount DESC
        """
        with self.driver.session() as session:
            return session.run(query).data()

    def calculate_and_persist_risk_scores(self) -> List[Dict[str, Any]]:
        """
        Calculates composite risk scores for accounts in Neo4j and updates their properties.
        """
        calc_query = """
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[:SENDS]->(t_out:Transaction)-[:TRANSFERRED_TO]->(b:Account)
        OPTIONAL MATCH (src:Account)-[:SENDS]->(t_in:Transaction)-[:TRANSFERRED_TO]->(a)
        WITH a,
             count(DISTINCT t_out) AS out_count,
             count(DISTINCT t_in) AS in_count,
             coalesce(sum(t_out.amount), 0.0) AS total_outflow,
             coalesce(sum(t_in.amount), 0.0) AS total_inflow,
             count(DISTINCT CASE WHEN t_out.is_suspicious = true THEN t_out END) AS sus_out,
             count(DISTINCT CASE WHEN t_in.is_suspicious = true THEN t_in END) AS sus_in

        OPTIONAL MATCH (a)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(n1:Account)
        MATCH (n1)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(n2:Account)
        MATCH (n2)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(a)
        WHERE a <> n1 AND n1 <> n2 AND a <> n2 AND t1.timestamp <= t2.timestamp AND t2.timestamp <= t3.timestamp
        WITH a, out_count, in_count, total_outflow, total_inflow, (sus_out + sus_in) AS total_sus_txs,
             count(DISTINCT t1) AS cycle_count

        WITH a, out_count, in_count, total_outflow, total_inflow, total_sus_txs, cycle_count,
             (cycle_count * 40.0) + (total_sus_txs * 15.0) + 
             (CASE WHEN (total_inflow + total_outflow) >= 20000 THEN 20.0 WHEN (total_inflow + total_outflow) >= 5000 THEN 10.0 ELSE 0.0 END) AS raw_score

        WITH a, out_count, in_count, total_outflow, total_inflow, cycle_count, total_sus_txs,
             CASE WHEN raw_score > 100.0 THEN 100.0 ELSE round(raw_score, 1) END AS risk_score

        WITH a, out_count, in_count, total_outflow, total_inflow, cycle_count, total_sus_txs, risk_score,
             CASE 
               WHEN risk_score >= 75.0 THEN 'CRITICAL'
               WHEN risk_score >= 50.0 THEN 'HIGH'
               WHEN risk_score >= 25.0 THEN 'MEDIUM'
               ELSE 'LOW'
             END AS risk_level

        SET a.risk_score = risk_score,
            a.risk_level = risk_level,
            a.last_risk_assessed = timestamp()

        RETURN a.account_id AS account_id,
               in_count,
               out_count,
               round(total_inflow, 2) AS total_inflow,
               round(total_outflow, 2) AS total_outflow,
               cycle_count,
               total_sus_txs,
               risk_score,
               risk_level
        ORDER BY risk_score DESC
        """
        with self.driver.session() as session:
            records = session.run(calc_query).data()

        logger.info(f"Calculated and persisted risk scores for {len(records)} accounts in Neo4j.")
        return records
