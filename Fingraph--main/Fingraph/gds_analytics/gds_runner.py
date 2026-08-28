import os
import sys
import logging
from typing import Dict, Any, List, Optional
from neo4j import GraphDatabase, Driver

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinGraph-GDS")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

DEFAULT_GRAPH_NAME = "finGraph_transfers"

class FinGraphGDSRunner:
    """
    FinGraph Week 3 - Neo4j Graph Data Science (GDS) Analytics Engine.
    Executes in-memory graph projections and runs PageRank, WCC, and Louvain algorithms
    on the financial transaction network, persisting graph topology metrics to Account nodes.
    """

    def __init__(
        self,
        uri: str = NEO4J_URI,
        user: str = NEO4J_USER,
        password: str = NEO4J_PASSWORD
    ):
        self.uri = uri
        self.user = user
        self.password = password
        self._driver: Optional[Driver] = None
        self._connect()

    def _connect(self):
        try:
            self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password), connection_timeout=5.0)
            self._driver.verify_connectivity()
            logger.info(f"Connected to Neo4j at {self.uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j at {self.uri}: {e}")
            self._driver = None

    def verify_connectivity(self) -> bool:
        if not self._driver:
            return False
        try:
            self._driver.verify_connectivity()
            return True
        except Exception as e:
            logger.warning(f"Connectivity check failed: {e}")
            return False

    def close(self):
        if self._driver:
            self._driver.close()
            self._driver = None

    def get_gds_version(self) -> Optional[str]:
        """Returns the installed Neo4j GDS library version."""
        if not self.verify_connectivity():
            return None
        query = "RETURN gds.version() AS gds_ver"
        try:
            with self._driver.session() as session:
                record = session.run(query).single()
                if record and record["gds_ver"]:
                    return str(record["gds_ver"])
        except Exception as e:
            logger.error(f"Could not retrieve GDS version: {e}")
        return None

    def graph_exists(self, graph_name: str = DEFAULT_GRAPH_NAME) -> bool:
        """Checks if a named graph projection currently exists in GDS memory catalog."""
        if not self.verify_connectivity():
            return False
        query = "CALL gds.graph.exists($graph_name) YIELD exists RETURN exists"
        try:
            with self._driver.session() as session:
                record = session.run(query, graph_name=graph_name).single()
                return bool(record["exists"]) if record else False
        except Exception as e:
            logger.warning(f"Error checking graph existence for '{graph_name}': {e}")
            return False

    def drop_projection(self, graph_name: str = DEFAULT_GRAPH_NAME) -> bool:
        """Drops an in-memory graph projection from GDS catalog if it exists."""
        if not self.verify_connectivity():
            return False
        if not self.graph_exists(graph_name):
            logger.info(f"Graph projection '{graph_name}' does not exist in catalog. Nothing to drop.")
            return True

        query = "CALL gds.graph.drop($graph_name, false) YIELD graphName RETURN graphName"
        try:
            with self._driver.session() as session:
                session.run(query, graph_name=graph_name).consume()
            logger.info(f"Successfully dropped GDS in-memory graph projection '{graph_name}'.")
            return True
        except Exception as e:
            logger.error(f"Failed to drop graph projection '{graph_name}': {e}")
            return False

    def ensure_graph_data(self):
        """Ensures the database contains Account and Transaction nodes; seeds sample data if empty."""
        if not self.verify_connectivity():
            return
        with self._driver.session() as session:
            try:
                count_res = session.run("MATCH (a:Account) RETURN count(a) AS c").single()
                if count_res and count_res["c"] > 0:
                    return
            except Exception:
                pass

            logger.info("Database contains no Account nodes. Seeding sample graph data...")
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
            logger.info("Sample graph data successfully seeded into Neo4j.")

    def project_transfers_graph(self, graph_name: str = DEFAULT_GRAPH_NAME) -> Dict[str, Any]:
        """
        Creates an in-memory direct Account-to-Account GDS graph projection from the existing schema:
        (:Account)-[:SENDS]->(:Transaction)-[:TRANSFERRED_TO]->(:Account)
        with transaction amount as the relationship weight.
        """
        if not self.verify_connectivity():
            raise ConnectionError("Cannot project graph: Not connected to Neo4j.")

        # Ensure database contains node data before projecting
        self.ensure_graph_data()

        # Safely drop projection if it already exists
        if self.graph_exists(graph_name):
            logger.info(f"Graph '{graph_name}' already exists. Dropping prior to recreation.")
            self.drop_projection(graph_name)

        project_query = """
        CALL gds.graph.project.cypher(
            $graph_name,
            'MATCH (a:Account) RETURN id(a) AS id, ["Account"] AS labels',
            'MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
             RETURN id(src) AS source, id(dst) AS target, "TRANSFERRED" AS type, toFloat(t.amount) AS amount'
        )
        YIELD graphName, nodeCount, relationshipCount, projectMillis
        RETURN graphName, nodeCount, relationshipCount, projectMillis
        """
        try:
            with self._driver.session() as session:
                record = session.run(project_query, graph_name=graph_name).single()
                if record:
                    result = {
                        "graphName": record["graphName"],
                        "nodeCount": record["nodeCount"],
                        "relationshipCount": record["relationshipCount"],
                        "projectMillis": record["projectMillis"]
                    }
                    logger.info(
                        f"GDS Projection '{graph_name}' created: {result['nodeCount']} nodes, "
                        f"{result['relationshipCount']} relationships in {result['projectMillis']}ms."
                    )
                    return result
                raise RuntimeError("GDS projection returned empty response.")
        except Exception as e:
            logger.error(f"Failed to create GDS graph projection '{graph_name}': {e}")
            raise

    def run_pagerank(
        self,
        graph_name: str = DEFAULT_GRAPH_NAME,
        write_property: str = "pagerank_score",
        relationship_weight_property: str = "amount",
        max_iterations: int = 20,
        damping_factor: float = 0.85
    ) -> Dict[str, Any]:
        """
        Executes GDS PageRank algorithm and writes the score to the Account nodes.
        Higher scores indicate accounts that receive high volume from influential senders.
        """
        if not self.verify_connectivity():
            raise ConnectionError("Cannot run PageRank: Not connected to Neo4j.")

        query = """
        CALL gds.pageRank.write(
            $graph_name,
            {
                writeProperty: $write_property,
                relationshipWeightProperty: $weight_property,
                maxIterations: $max_iterations,
                dampingFactor: $damping_factor
            }
        )
        YIELD nodePropertiesWritten, ranIterations, computeMillis, writeMillis
        RETURN nodePropertiesWritten, ranIterations, computeMillis, writeMillis
        """
        try:
            with self._driver.session() as session:
                record = session.run(
                    query,
                    graph_name=graph_name,
                    write_property=write_property,
                    weight_property=relationship_weight_property,
                    max_iterations=max_iterations,
                    damping_factor=damping_factor
                ).single()
                if record:
                    result = {
                        "algorithm": "PageRank",
                        "writeProperty": write_property,
                        "nodePropertiesWritten": record["nodePropertiesWritten"],
                        "ranIterations": record["ranIterations"],
                        "computeMillis": record["computeMillis"],
                        "writeMillis": record["writeMillis"]
                    }
                    logger.info(
                        f"PageRank completed: {result['nodePropertiesWritten']} nodes updated in "
                        f"{result['computeMillis'] + result['writeMillis']}ms."
                    )
                    return result
                raise RuntimeError("PageRank execution returned empty result.")
        except Exception as e:
            logger.error(f"Failed to execute PageRank on '{graph_name}': {e}")
            raise

    def run_wcc(
        self,
        graph_name: str = DEFAULT_GRAPH_NAME,
        write_property: str = "wcc_component"
    ) -> Dict[str, Any]:
        """
        Executes Weakly Connected Components (WCC) to partition the network into disconnected clusters.
        Writes the component identifier to Account nodes.
        """
        if not self.verify_connectivity():
            raise ConnectionError("Cannot run WCC: Not connected to Neo4j.")

        query = """
        CALL gds.wcc.write(
            $graph_name,
            {
                writeProperty: $write_property
            }
        )
        YIELD componentCount, nodePropertiesWritten, computeMillis, writeMillis
        RETURN componentCount, nodePropertiesWritten, computeMillis, writeMillis
        """
        try:
            with self._driver.session() as session:
                record = session.run(
                    query,
                    graph_name=graph_name,
                    write_property=write_property
                ).single()
                if record:
                    result = {
                        "algorithm": "WCC",
                        "writeProperty": write_property,
                        "componentCount": record["componentCount"],
                        "nodePropertiesWritten": record["nodePropertiesWritten"],
                        "computeMillis": record["computeMillis"],
                        "writeMillis": record["writeMillis"]
                    }
                    logger.info(
                        f"WCC completed: {result['componentCount']} components detected, "
                        f"{result['nodePropertiesWritten']} nodes updated in {result['computeMillis'] + result['writeMillis']}ms."
                    )
                    return result
                raise RuntimeError("WCC execution returned empty result.")
        except Exception as e:
            logger.error(f"Failed to execute WCC on '{graph_name}': {e}")
            raise

    def run_louvain(
        self,
        graph_name: str = DEFAULT_GRAPH_NAME,
        write_property: str = "louvain_community",
        relationship_weight_property: str = "amount",
        max_levels: int = 10,
        max_iterations: int = 15
    ) -> Dict[str, Any]:
        """
        Executes Louvain Community Detection to uncover densely interconnected transaction syndicates.
        Writes the community identifier to Account nodes.
        """
        if not self.verify_connectivity():
            raise ConnectionError("Cannot run Louvain: Not connected to Neo4j.")

        query = """
        CALL gds.louvain.write(
            $graph_name,
            {
                writeProperty: $write_property,
                relationshipWeightProperty: $weight_property,
                maxLevels: $max_levels,
                maxIterations: $max_iterations
            }
        )
        YIELD communityCount, modularity, nodePropertiesWritten, computeMillis, writeMillis
        RETURN communityCount, modularity, nodePropertiesWritten, computeMillis, writeMillis
        """
        try:
            with self._driver.session() as session:
                record = session.run(
                    query,
                    graph_name=graph_name,
                    write_property=write_property,
                    weight_property=relationship_weight_property,
                    max_levels=max_levels,
                    max_iterations=max_iterations
                ).single()
                if record:
                    result = {
                        "algorithm": "Louvain",
                        "writeProperty": write_property,
                        "communityCount": record["communityCount"],
                        "modularity": record["modularity"],
                        "nodePropertiesWritten": record["nodePropertiesWritten"],
                        "computeMillis": record["computeMillis"],
                        "writeMillis": record["writeMillis"]
                    }
                    logger.info(
                        f"Louvain completed: {result['communityCount']} communities detected (modularity: {result['modularity']:.4f}), "
                        f"{result['nodePropertiesWritten']} nodes updated in {result['computeMillis'] + result['writeMillis']}ms."
                    )
                    return result
                raise RuntimeError("Louvain execution returned empty result.")
        except Exception as e:
            logger.error(f"Failed to execute Louvain on '{graph_name}': {e}")
            raise

    def get_gds_results_summary(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Queries Account nodes to verify that GDS properties were persisted alongside existing Week 2 risk scores.
        """
        if not self.verify_connectivity():
            return []

        query = """
        MATCH (a:Account)
        RETURN a.account_id AS account_id,
               a.account_type AS account_type,
               a.risk_score AS risk_score,
               a.risk_level AS risk_level,
               a.pagerank_score AS pagerank_score,
               a.wcc_component AS wcc_component,
               a.louvain_community AS louvain_community
        ORDER BY a.pagerank_score DESC, a.risk_score DESC
        LIMIT $limit
        """
        try:
            with self._driver.session() as session:
                return session.run(query, limit=limit).data()
        except Exception as e:
            logger.error(f"Failed to fetch GDS results summary: {e}")
            return []

    def run_full_gds_pipeline(
        self,
        graph_name: str = DEFAULT_GRAPH_NAME,
        drop_after: bool = False
    ) -> Dict[str, Any]:
        """
        Executes the complete Week 3 GDS analytics workflow:
        1. Check GDS version & verify connectivity
        2. Project Account -> Account in-memory transfer graph
        3. Run PageRank (written to a.pagerank_score)
        4. Run WCC (written to a.wcc_component)
        5. Run Louvain (written to a.louvain_community)
        6. Return execution summary and result samples
        """
        logger.info("=" * 60)
        logger.info("Starting FinGraph Week 3 GDS Analytics Pipeline")
        logger.info("=" * 60)

        gds_ver = self.get_gds_version()
        if not gds_ver:
            raise RuntimeError("Neo4j GDS plugin is not active or available.")

        # Step 1: Projection
        proj_stats = self.project_transfers_graph(graph_name)

        # Step 2: PageRank
        pagerank_stats = self.run_pagerank(graph_name)

        # Step 3: WCC
        wcc_stats = self.run_wcc(graph_name)

        # Step 4: Louvain
        louvain_stats = self.run_louvain(graph_name)

        # Step 5: Summary
        results_summary = self.get_gds_results_summary(limit=10)

        if drop_after:
            self.drop_projection(graph_name)

        pipeline_report = {
            "gds_version": gds_ver,
            "projection": proj_stats,
            "pagerank": pagerank_stats,
            "wcc": wcc_stats,
            "louvain": louvain_stats,
            "sample_accounts": results_summary
        }

        logger.info("=" * 60)
        logger.info("FinGraph Week 3 GDS Analytics Pipeline Complete")
        logger.info("=" * 60)
        return pipeline_report

def main():
    runner = FinGraphGDSRunner()
    try:
        if not runner.verify_connectivity():
            print("\n[!] Could not connect to Neo4j at bolt://localhost:7687. Ensure Neo4j container is running.")
            sys.exit(1)

        print("\n--- Running FinGraph Week 3 GDS Analytics Pipeline ---")
        report = runner.run_full_gds_pipeline(drop_after=False)

        print(f"\n[+] GDS Version: {report['gds_version']}")
        print(f"[+] Projected Graph: {report['projection']['graphName']} ({report['projection']['nodeCount']} nodes, {report['projection']['relationshipCount']} rels)")
        print(f"[+] PageRank: {report['pagerank']['nodePropertiesWritten']} nodes updated")
        print(f"[+] WCC: {report['wcc']['componentCount']} components, {report['wcc']['nodePropertiesWritten']} nodes updated")
        print(f"[+] Louvain: {report['louvain']['communityCount']} communities (modularity: {report['louvain']['modularity']:.4f})")

        print("\n[+] Top 5 Accounts by PageRank & Risk Score:")
        for idx, acc in enumerate(report["sample_accounts"][:5], 1):
            print(
                f"   {idx}. Account: {acc['account_id']:<15} | "
                f"PageRank: {acc.get('pagerank_score', 0):.4f} | "
                f"Louvain Comm: {acc.get('louvain_community')} | "
                f"WCC Comp: {acc.get('wcc_component')} | "
                f"Risk Score: {acc.get('risk_score', 'N/A')} ({acc.get('risk_level', 'N/A')})"
            )

    finally:
        runner.close()

if __name__ == "__main__":
    main()
