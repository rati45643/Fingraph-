import os
import logging
from typing import List, Dict, Any, Optional
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Flink-Neo4jSink")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class FlinkNeo4jSink:
    """
    Day 4: Idempotent Neo4j Sink & Upsert Engine.
    Performs batched UNWIND MERGE graph updates from the transaction stream.
    Prevents duplicate nodes and relationships for replayed or duplicate events.
    """

    def __init__(self, uri: str = NEO4J_URI, user: str = NEO4J_USER, password: str = NEO4J_PASSWORD):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password), connection_timeout=5.0)

    def close(self):
        if self.driver:
            self.driver.close()

    def verify_connectivity(self) -> bool:
        try:
            self.driver.verify_connectivity()
            return True
        except Exception as e:
            logger.warning(f"Neo4j connectivity check failed: {e}")
            return False

    def upsert_transaction_batch(self, batch: List[Dict[str, Any]]) -> int:
        """
        Idempotently inserts or updates a batch of clean transaction events into Neo4j.
        Returns the number of upserted records.
        """
        if not batch:
            return 0

        upsert_query = """
        UNWIND $batch AS item
        MERGE (src:Account {account_id: item.source_account_id})
        MERGE (dst:Account {account_id: item.destination_account_id})
        MERGE (t:Transaction {transaction_id: item.transaction_id})
        SET t.amount = toFloat(item.amount),
            t.timestamp = toInteger(item.timestamp),
            t.is_suspicious = toBoolean(item.is_suspicious),
            t.last_ingested_at = timestamp()
        MERGE (src)-[:SENDS]->(t)
        MERGE (t)-[:TRANSFERRED_TO]->(dst)
        """
        try:
            with self.driver.session() as session:
                session.run(upsert_query, batch=batch)
            logger.info(f"Idempotently upserted batch of {len(batch)} transactions into Neo4j.")
            return len(batch)
        except Exception as e:
            logger.warning(f"Neo4j batch upsert failed: {e}")
            return 0

    def get_database_stats(self) -> Dict[str, int]:
        """Fetches current node and relationship counts from Neo4j."""
        try:
            with self.driver.session() as session:
                stats = {}
                for label in ["Person", "Bank", "Account", "Transaction"]:
                    res = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()
                    stats[label] = res["c"] if res else 0
                rel_res = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()
                stats["relationships"] = rel_res["c"] if rel_res else 0
                return stats
        except Exception as e:
            logger.warning(f"Could not retrieve Neo4j database stats: {e}")
            return {"Person": 0, "Bank": 0, "Account": 0, "Transaction": 0, "relationships": 0}
