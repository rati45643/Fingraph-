import os
import logging
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Flink-FraudDetector")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class FlinkFraudDetector:
    """
    Day 5: Direct Relationships & Suspicious Path Detector.
    Executes Day 5 Cypher queries against Neo4j to identify 2-hop pass-throughs,
    3-hop layering chains, and structuring fan-in hubs.
    """

    def __init__(self, uri: str = NEO4J_URI, user: str = NEO4J_USER, password: str = NEO4J_PASSWORD):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password), connection_timeout=5.0)

    def close(self):
        if self.driver:
            self.driver.close()

    def find_direct_transfers(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Returns direct transfer summaries between accounts."""
        query = """
        MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
        RETURN src.account_id AS source_account,
               dst.account_id AS destination_account,
               count(t) AS transfer_count,
               round(sum(t.amount), 2) AS total_amount
        ORDER BY total_amount DESC
        LIMIT $limit
        """
        with self.driver.session() as session:
            return session.run(query, limit=limit).data()

    def find_two_hop_intermediaries(self) -> List[Dict[str, Any]]:
        """Finds 2-hop pass-through intermediary mules (A -> B -> C)."""
        query = """
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
        ORDER BY incoming_amount DESC
        """
        with self.driver.session() as session:
            return session.run(query).data()

    def find_three_hop_layering(self) -> List[Dict[str, Any]]:
        """Finds 3-hop layering chains (A -> B -> C -> D)."""
        query = """
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
        ORDER BY hop1_amount DESC
        """
        with self.driver.session() as session:
            return session.run(query).data()

    def find_structuring_fan_in_hubs(self, min_senders: int = 3) -> List[Dict[str, Any]]:
        """Finds structuring fan-in hubs aggregating from multiple accounts."""
        query = """
        MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(hub:Account)
        WITH hub, 
             count(DISTINCT src) AS distinct_senders,
             count(t) AS total_tx_count,
             sum(t.amount) AS total_aggregated,
             collect(DISTINCT src.account_id) AS sender_accounts,
             collect(t.transaction_id) AS tx_ids
        WHERE distinct_senders >= $min_senders
        RETURN hub.account_id AS hub_account,
               distinct_senders,
               total_tx_count,
               round(total_aggregated, 2) AS total_aggregated,
               sender_accounts,
               tx_ids
        ORDER BY distinct_senders DESC, total_aggregated DESC
        """
        with self.driver.session() as session:
            return session.run(query, min_senders=min_senders).data()
