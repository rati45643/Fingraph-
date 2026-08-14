import os
import sys
import logging
from typing import List, Dict, Any
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinGraph-Detector")

# Neo4j Default Config
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class AMLPatternDetector:
    def __init__(self, uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD, connection_timeout=5.0):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = GraphDatabase.driver(
            self.uri, 
            auth=(self.user, self.password),
            connection_timeout=connection_timeout,
            max_connection_lifetime=30
        )

    def close(self):
        if self.driver:
            self.driver.close()

    def verify_connectivity(self) -> bool:
        """Verifies active connection to Neo4j."""
        try:
            self.driver.verify_connectivity()
            return True
        except (ServiceUnavailable, AuthError, Exception) as e:
            logger.warning(f"Neo4j connection check failed at {self.uri}: {e}")
            return False

    def get_database_stats(self) -> Dict[str, int]:
        """Fetches current node and relationship counts from Neo4j."""
        with self.driver.session() as session:
            stats = {}
            labels = ["Person", "Bank", "Account", "Transaction"]
            for label in labels:
                result = session.run(f"MATCH (n:{label}) RETURN count(n) AS count").single()
                stats[label] = result["count"] if result else 0
            
            # Count suspicious vs non-suspicious transactions
            sus_result = session.run(
                "MATCH (t:Transaction) RETURN t.is_suspicious AS is_suspicious, count(t) AS count"
            )
            stats["suspicious_tx"] = 0
            stats["legitimate_tx"] = 0
            for record in sus_result:
                if record["is_suspicious"]:
                    stats["suspicious_tx"] = record["count"]
                else:
                    stats["legitimate_tx"] = record["count"]
            return stats

    def detect_circular_flows(self) -> List[Dict[str, Any]]:
        """
        Detects 3-hop closed circular flows (A -> B -> C -> A).
        """
        query = """
        MATCH (a1:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(a2:Account)
        MATCH (a2)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(a3:Account)
        MATCH (a3)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(a1)
        WHERE a1 <> a2 AND a2 <> a3 AND a1 <> a3
          AND t1.transaction_id <> t2.transaction_id 
          AND t2.transaction_id <> t3.transaction_id
          AND t1.timestamp <= t2.timestamp 
          AND t2.timestamp <= t3.timestamp
          AND abs(t1.amount - t2.amount) <= (0.15 * t1.amount)
          AND abs(t2.amount - t3.amount) <= (0.15 * t2.amount)
        RETURN a1.account_id AS account_A,
               a2.account_id AS account_B,
               a3.account_id AS account_C,
               t1.transaction_id AS tx1_id, t1.amount AS amount_A_to_B,
               t2.transaction_id AS tx2_id, t2.amount AS amount_B_to_C,
               t3.transaction_id AS tx3_id, t3.amount AS amount_C_to_A,
               round(t1.amount, 2) AS cycle_volume,
               (t3.timestamp - t1.timestamp) AS duration_ms
        ORDER BY t1.timestamp DESC
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

    def detect_smurfing_funnels(self) -> List[Dict[str, Any]]:
        """
        Detects syndicate fan-in funnels (many-to-one aggregation just under $10,000).
        """
        query = """
        MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
        WHERE t.amount >= 5000.0 AND t.amount < 10000.0
        WITH dst, 
             count(DISTINCT src) AS distinct_sources,
             count(t) AS transaction_count,
             collect(DISTINCT src.account_id) AS source_accounts,
             collect(t.transaction_id) AS transaction_ids,
             round(sum(t.amount), 2) AS total_deposited,
             min(t.timestamp) AS first_tx_time,
             max(t.timestamp) AS last_tx_time
        WHERE distinct_sources >= 3
        RETURN dst.account_id AS destination_account,
               distinct_sources,
               transaction_count,
               total_deposited,
               source_accounts,
               transaction_ids,
               first_tx_time,
               last_tx_time,
               (last_tx_time - first_tx_time) AS duration_ms
        ORDER BY total_deposited DESC
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

    def detect_multihop_layering(self) -> List[Dict[str, Any]]:
        """
        Detects 4-node / 3-hop layering chains (A -> B -> C -> D) with rapid pass-through.
        """
        query = """
        MATCH (a1:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(a2:Account)
        MATCH (a2)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(a3:Account)
        MATCH (a3)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(a4:Account)
        WHERE a1 <> a2 AND a2 <> a3 AND a3 <> a4 AND a1 <> a4 AND a1 <> a3 AND a2 <> a4
          AND t1.transaction_id <> t2.transaction_id 
          AND t2.transaction_id <> t3.transaction_id
          AND t1.timestamp <= t2.timestamp 
          AND t2.timestamp <= t3.timestamp
          AND t1.amount >= 5000.0
          AND t2.amount >= (0.80 * t1.amount) AND t2.amount <= (1.05 * t1.amount)
          AND t3.amount >= (0.80 * t2.amount) AND t3.amount <= (1.05 * t2.amount)
        RETURN a1.account_id AS originator,
               a2.account_id AS hop1_intermediary,
               a3.account_id AS hop2_intermediary,
               a4.account_id AS ultimate_beneficiary,
               round(t1.amount, 2) AS initial_amount,
               round(t2.amount, 2) AS hop1_amount,
               round(t3.amount, 2) AS final_amount,
               [t1.transaction_id, t2.transaction_id, t3.transaction_id] AS chain_tx_ids,
               (t3.timestamp - t1.timestamp) AS duration_ms
        ORDER BY t1.timestamp DESC
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

    def detect_two_hop_passthrough(self) -> List[Dict[str, Any]]:
        """
        Detects 3-node / 2-hop rapid pass-through intermediary mules (A -> B -> C).
        """
        query = """
        MATCH (src:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(mule:Account)
        MATCH (mule)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
        WHERE src <> mule AND mule <> dst AND src <> dst
          AND t1.transaction_id <> t2.transaction_id
          AND t1.timestamp <= t2.timestamp
          AND t1.amount >= 5000.0
          AND t2.amount >= (0.80 * t1.amount) AND t2.amount <= (1.05 * t1.amount)
        RETURN src.account_id AS source_account,
               mule.account_id AS intermediary_mule,
               dst.account_id AS destination_account,
               round(t1.amount, 2) AS incoming_amount,
               round(t2.amount, 2) AS outgoing_amount,
               t1.transaction_id AS in_tx_id,
               t2.transaction_id AS out_tx_id,
               (t2.timestamp - t1.timestamp) AS delay_ms
        ORDER BY t1.timestamp DESC
        """
        with self.driver.session() as session:
            result = session.run(query)
            return [record.data() for record in result]

def print_separator(char="=", length=80):
    print(char * length, flush=True)

def run_detection_pipeline(detector: AMLPatternDetector):
    stats = detector.get_database_stats()
    
    print("\n--- Current Neo4j Graph Database Snapshot ---", flush=True)
    print(f"  * Total Persons:      {stats.get('Person', 0)}", flush=True)
    print(f"  * Total Banks:        {stats.get('Bank', 0)}", flush=True)
    print(f"  * Total Accounts:     {stats.get('Account', 0)}", flush=True)
    print(f"  * Total Transactions: {stats.get('Transaction', 0)}", flush=True)
    print(f"    - Legitimate:       {stats.get('legitimate_tx', 0)}", flush=True)
    print(f"    - Suspicious (Tag): {stats.get('suspicious_tx', 0)}", flush=True)
    
    # 1. Circular Flow Detection
    print_separator("-")
    print("1. Running Circular Flow (Round-Tripping) Detection...", flush=True)
    cycles = detector.detect_circular_flows()
    print(f"   [+] Detected Circular Rings: {len(cycles)}", flush=True)
    for idx, c in enumerate(cycles[:5], 1):
        print(f"       Ring #{idx}: {c['account_A']} -> {c['account_B']} -> {c['account_C']} -> {c['account_A']}", flush=True)
        print(f"         Amount: ${c['cycle_volume']:,.2f} | Duration: {c['duration_ms']} ms", flush=True)
        print(f"         TXs: [{c['tx1_id'][:8]}..., {c['tx2_id'][:8]}..., {c['tx3_id'][:8]}...]", flush=True)
    if len(cycles) > 5:
        print(f"       ... and {len(cycles) - 5} more cycle(s)", flush=True)

    # 2. Smurfing / Structuring Funnel Detection
    print_separator("-")
    print("2. Running Smurfing / Syndicate Funnel Detection...", flush=True)
    funnels = detector.detect_smurfing_funnels()
    print(f"   [+] Detected Smurfing Funnel Hubs: {len(funnels)}", flush=True)
    for idx, f in enumerate(funnels[:5], 1):
        print(f"       Hub #{idx} Destination: {f['destination_account']}", flush=True)
        print(f"         Sources ({f['distinct_sources']}): {', '.join(f['source_accounts'][:4])}{'...' if len(f['source_accounts']) > 4 else ''}", flush=True)
        print(f"         Total Deposited: ${f['total_deposited']:,.2f} over {f['transaction_count']} transactions ({f['duration_ms']} ms)", flush=True)
    if len(funnels) > 5:
        print(f"       ... and {len(funnels) - 5} more funnel(s)", flush=True)

    # 3. Multi-Hop Layering Chain Detection
    print_separator("-")
    print("3. Running Multi-Hop Layering Chain Detection (3-hop / 4-node)...", flush=True)
    layering = detector.detect_multihop_layering()
    print(f"   [+] Detected Multi-Hop Chains: {len(layering)}", flush=True)
    for idx, l in enumerate(layering[:5], 1):
        print(f"       Chain #{idx}: {l['originator']} -> {l['hop1_intermediary']} -> {l['hop2_intermediary']} -> {l['ultimate_beneficiary']}", flush=True)
        print(f"         Amounts: ${l['initial_amount']:,.2f} -> ${l['hop1_amount']:,.2f} -> ${l['final_amount']:,.2f}", flush=True)
        print(f"         Duration: {l['duration_ms']} ms", flush=True)
    if len(layering) > 5:
        print(f"       ... and {len(layering) - 5} more chain(s)", flush=True)

    # 4. 2-Hop Pass-Through Intermediary Detection
    print_separator("-")
    print("4. Running 2-Hop Pass-Through Intermediary Mule Detection...", flush=True)
    mules = detector.detect_two_hop_passthrough()
    print(f"   [+] Detected 2-Hop Intermediary Mules: {len(mules)}", flush=True)
    for idx, m in enumerate(mules[:5], 1):
        print(f"       Mule #{idx}: {m['source_account']} -> [{m['intermediary_mule']}] -> {m['destination_account']}", flush=True)
        print(f"         In: ${m['incoming_amount']:,.2f} | Out: ${m['outgoing_amount']:,.2f} | Delay: {m['delay_ms']} ms", flush=True)
    if len(mules) > 5:
        print(f"       ... and {len(mules) - 5} more mule flow(s)", flush=True)

    print_separator("=")
    total_detections = len(cycles) + len(funnels) + len(layering) + len(mules)
    print(f"  Summary: Total AML Patterns Identified Across All Topologies: {total_detections}", flush=True)
    print_separator("=")

def main():
    print_separator("=")
    print("  FinGraph AML Pattern Detection Engine (Week 2 - Milestone 1)", flush=True)
    print_separator("=")
    
    detector = AMLPatternDetector()
    try:
        if not detector.verify_connectivity():
            print(f"\n[!] Notice: Neo4j database is currently not reachable at {detector.uri}.", flush=True)
            print("    To start Neo4j: Ensure Docker is running and run 'cd Fingraph/docker && docker-compose up -d'.", flush=True)
            print("    All detection Cypher queries have been written and validated in 'Fingraph/database/detection_queries.cypher'.", flush=True)
            return
        
        run_detection_pipeline(detector)
    except Exception as e:
        logger.error(f"Error during AML detection execution: {e}", exc_info=True)
    finally:
        detector.close()

if __name__ == "__main__":
    main()
