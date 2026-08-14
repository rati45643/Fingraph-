import os
import sys
import logging
from typing import Dict, List, Any, Set, Tuple
import networkx as nx
from neo4j import GraphDatabase

# Ensure simulator directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from detect_patterns import AMLPatternDetector

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinGraph-Analytics")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class GraphAnalyticsEngine:
    """
    Advanced Graph Analytics & AML Risk Scoring Engine.
    Computes Centrality, Community Detection (WCC/Louvain), Hub/Mule Classification,
    and Composite AML Risk Scores with persistence back to Neo4j.
    """

    def __init__(self, uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password), connection_timeout=5.0)
        self.detector = AMLPatternDetector(uri=self.uri, user=self.user, password=self.password)
        self.graph = nx.DiGraph()
        self.account_profiles: Dict[str, Dict[str, Any]] = {}

    def close(self):
        if self.driver:
            self.driver.close()
        if self.detector:
            self.detector.close()

    def build_graph_from_neo4j(self) -> nx.DiGraph:
        """
        Projects all accounts and transactions from Neo4j into a NetworkX directed graph.
        """
        self.graph.clear()
        self.account_profiles.clear()

        query = """
        MATCH (a:Account)
        OPTIONAL MATCH (a)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
        RETURN a.account_id AS source,
               a.account_type AS source_type,
               dst.account_id AS target,
               dst.account_type AS target_type,
               t.transaction_id AS tx_id,
               t.amount AS amount,
               t.timestamp AS timestamp,
               t.is_suspicious AS is_suspicious
        """
        with self.driver.session() as session:
            result = session.run(query)
            for record in result:
                src = record["source"]
                if src:
                    if src not in self.graph:
                        self.graph.add_node(src, account_type=record["source_type"])
                    
                    dst = record["target"]
                    if dst and record["tx_id"]:
                        if dst not in self.graph:
                            self.graph.add_node(dst, account_type=record["target_type"])
                        
                        # Add or update edge
                        weight = float(record["amount"] or 1.0)
                        if self.graph.has_edge(src, dst):
                            self.graph[src][dst]["weight"] += weight
                            self.graph[src][dst]["tx_count"] += 1
                            self.graph[src][dst]["transactions"].append({
                                "tx_id": record["tx_id"],
                                "amount": weight,
                                "timestamp": record["timestamp"],
                                "is_suspicious": record["is_suspicious"]
                            })
                        else:
                            self.graph.add_edge(
                                src, dst,
                                weight=weight,
                                tx_count=1,
                                transactions=[{
                                    "tx_id": record["tx_id"],
                                    "amount": weight,
                                    "timestamp": record["timestamp"],
                                    "is_suspicious": record["is_suspicious"]
                                }]
                            )

        logger.info(f"Graph projected: {self.graph.number_of_nodes()} accounts, {self.graph.number_of_edges()} directed transfer links.")
        return self.graph

    def compute_centrality_metrics(self) -> Dict[str, Dict[str, float]]:
        """
        Computes In-Degree, Out-Degree, PageRank, and Betweenness Centrality.
        """
        if self.graph.number_of_nodes() == 0:
            return {}

        in_degrees = dict(self.graph.in_degree())
        out_degrees = dict(self.graph.out_degree())

        # PageRank (directed with weights)
        try:
            pagerank = nx.pagerank(self.graph, weight="weight", alpha=0.85, max_iter=200)
        except Exception as e:
            logger.warning(f"PageRank fallback to uniform: {e}")
            pagerank = {n: 1.0 / max(1, self.graph.number_of_nodes()) for n in self.graph.nodes()}

        # Betweenness Centrality
        try:
            betweenness = nx.betweenness_centrality(self.graph, weight=None, normalized=True)
        except Exception as e:
            logger.warning(f"Betweenness Centrality fallback: {e}")
            betweenness = {n: 0.0 for n in self.graph.nodes()}

        # Compute Total Inflow & Outflow volumes
        inflow = {n: sum(self.graph[u][n]["weight"] for u in self.graph.predecessors(n)) for n in self.graph.nodes()}
        outflow = {n: sum(self.graph[n][v]["weight"] for v in self.graph.successors(n)) for n in self.graph.nodes()}

        centrality_results = {}
        for node in self.graph.nodes():
            centrality_results[node] = {
                "in_degree": in_degrees.get(node, 0),
                "out_degree": out_degrees.get(node, 0),
                "total_degree": in_degrees.get(node, 0) + out_degrees.get(node, 0),
                "inflow_volume": round(inflow.get(node, 0.0), 2),
                "outflow_volume": round(outflow.get(node, 0.0), 2),
                "pagerank": round(pagerank.get(node, 0.0), 5),
                "betweenness": round(betweenness.get(node, 0.0), 5)
            }

        return centrality_results

    def compute_communities(self) -> Tuple[Dict[str, int], Dict[int, List[str]]]:
        """
        Computes Weakly Connected Components (WCC) and Louvain / Modularity communities.
        """
        if self.graph.number_of_nodes() == 0:
            return {}, {}

        # 1. Weakly Connected Components (WCC)
        wcc_components = list(nx.weakly_connected_components(self.graph))
        node_community_map = {}
        community_members = {}

        # 2. Try Louvain on undirected graph with modularity optimization
        undirected_g = self.graph.to_undirected()
        try:
            communities = list(nx.community.louvain_communities(undirected_g, weight="weight", seed=42))
        except Exception:
            # Fallback to WCC components if Louvain is unavailable
            communities = wcc_components

        for comm_id, member_set in enumerate(communities, start=1):
            community_members[comm_id] = sorted(list(member_set))
            for node in member_set:
                node_community_map[node] = comm_id

        return node_community_map, community_members

    def classify_hubs_and_mules(self, centrality_metrics: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, bool]]:
        """
        Classifies accounts as Aggregator Hubs or Intermediary Money Mules.
        """
        classifications = {}
        for node, metrics in centrality_metrics.items():
            in_deg = metrics["in_degree"]
            out_deg = metrics["out_degree"]
            inflow = metrics["inflow_volume"]
            outflow = metrics["outflow_volume"]

            # Hub: Aggregates from >= 3 distinct sources with high in-degree
            is_hub = in_deg >= 3 and inflow >= 5000.0

            # Mule: Receives and forwards funds with pass-through ratio >= 0.70
            is_mule = False
            if in_deg >= 1 and out_deg >= 1 and inflow > 0:
                pass_through = outflow / inflow
                if 0.70 <= pass_through <= 1.30 and inflow >= 5000.0:
                    is_mule = True

            classifications[node] = {
                "is_hub": is_hub,
                "is_mule": is_mule
            }

        return classifications

    def calculate_composite_risk_scores(self) -> Dict[str, Dict[str, Any]]:
        """
        Integrates topology detections (Cycles, Smurfing, Layering) with graph analytics (Centrality, Communities)
        to compute deterministic composite AML risk scores in [0, 100].
        """
        self.build_graph_from_neo4j()
        centrality = self.compute_centrality_metrics()
        node_comm_map, _ = self.compute_communities()
        classifications = self.classify_hubs_and_mules(centrality)

        # Retrieve pattern counts from detection queries
        cycles = self.detector.detect_circular_flows()
        funnels = self.detector.detect_smurfing_funnels()
        layering_chains = self.detector.detect_multihop_layering()
        mule_paths = self.detector.detect_two_hop_passthrough()

        # Map topology counts per account
        cycle_counts: Dict[str, int] = {n: 0 for n in self.graph.nodes()}
        for c in cycles:
            for acc in [c["account_A"], c["account_B"], c["account_C"]]:
                if acc in cycle_counts:
                    cycle_counts[acc] += 1

        smurf_volume: Dict[str, float] = {n: 0.0 for n in self.graph.nodes()}
        for f in funnels:
            dst = f["destination_account"]
            if dst in smurf_volume:
                smurf_volume[dst] += float(f["total_deposited"])

        layer_depth: Dict[str, int] = {n: 0 for n in self.graph.nodes()}
        for l in layering_chains:
            for acc in [l["originator"], l["hop1_intermediary"], l["hop2_intermediary"], l["ultimate_beneficiary"]]:
                if acc in layer_depth:
                    layer_depth[acc] = max(layer_depth[acc], 3)
        for m in mule_paths:
            mule = m["intermediary_mule"]
            if mule in layer_depth:
                layer_depth[mule] = max(layer_depth[mule], 2)

        # Weights: Cycle (35%), Smurf Volume (30%), Layering Depth (20%), Centrality (15%)
        W_CYCLE = 35.0
        W_SMURF = 30.0
        W_LAYER = 20.0
        W_CENTRALITY = 15.0

        risk_profiles = {}
        for node in self.graph.nodes():
            cent = centrality.get(node, {})
            c_count = cycle_counts.get(node, 0)
            s_vol = smurf_volume.get(node, 0.0)
            l_dpth = layer_depth.get(node, 0)
            is_hub = classifications.get(node, {}).get("is_hub", False)
            is_mule = classifications.get(node, {}).get("is_mule", False)

            # Sub-scores [0.0 to 1.0]
            cycle_subscore = min(1.0, c_count * 0.5)
            smurf_subscore = min(1.0, s_vol / 20000.0)
            layer_subscore = min(1.0, l_dpth / 3.0)
            
            # Centrality subscore (combining betweenness and degree)
            btn = cent.get("betweenness", 0.0)
            deg_norm = min(1.0, cent.get("total_degree", 0) / 10.0)
            centrality_subscore = min(1.0, (btn * 2.0) + (deg_norm * 0.5))

            # Composite Score in [0, 100]
            raw_score = (
                (W_CYCLE * cycle_subscore) +
                (W_SMURF * smurf_subscore) +
                (W_LAYER * layer_subscore) +
                (W_CENTRALITY * centrality_subscore)
            )

            # Boost for confirmed hub or mule classification
            if is_hub and raw_score < 60:
                raw_score += 25.0
            if is_mule and raw_score < 50:
                raw_score += 20.0

            risk_score = round(min(100.0, raw_score), 1)

            # Risk Level
            if risk_score >= 75.0:
                risk_level = "CRITICAL"
            elif risk_score >= 50.0:
                risk_level = "HIGH"
            elif risk_score >= 25.0:
                risk_level = "MEDIUM"
            else:
                risk_level = "LOW"

            risk_profiles[node] = {
                "account_id": node,
                "risk_score": risk_score,
                "risk_level": risk_level,
                "community_id": node_comm_map.get(node, 1),
                "in_degree": cent.get("in_degree", 0),
                "out_degree": cent.get("out_degree", 0),
                "total_degree": cent.get("total_degree", 0),
                "inflow_volume": cent.get("inflow_volume", 0.0),
                "outflow_volume": cent.get("outflow_volume", 0.0),
                "pagerank": cent.get("pagerank", 0.0),
                "betweenness": cent.get("betweenness", 0.0),
                "is_hub": is_hub,
                "is_mule": is_mule,
                "cycle_count": c_count,
                "smurf_volume": s_vol,
                "layer_depth": l_dpth
            }

        self.account_profiles = risk_profiles
        return risk_profiles

    def persist_results_to_neo4j(self) -> int:
        """
        Writes risk scores, centrality metrics, and community assignments back to Neo4j.
        Enriches (:Account) nodes and creates (:RiskAssessment) nodes.
        """
        if not self.account_profiles:
            self.calculate_composite_risk_scores()

        enrichment_query = """
        UNWIND $batch AS item
        MATCH (a:Account {account_id: item.account_id})
        SET a.risk_score = toFloat(item.risk_score),
            a.risk_level = item.risk_level,
            a.community_id = toInteger(item.community_id),
            a.in_degree = toInteger(item.in_degree),
            a.out_degree = toInteger(item.out_degree),
            a.pagerank = toFloat(item.pagerank),
            a.betweenness_centrality = toFloat(item.betweenness),
            a.is_hub = toBoolean(item.is_hub),
            a.is_mule = toBoolean(item.is_mule),
            a.last_assessed_at = timestamp()
        CREATE (ra:RiskAssessment {
            assessment_id: randomUUID(),
            account_id: item.account_id,
            risk_score: toFloat(item.risk_score),
            risk_level: item.risk_level,
            community_id: toInteger(item.community_id),
            timestamp: timestamp()
        })
        MERGE (a)-[:HAS_ASSESSMENT]->(ra)
        """
        batch = list(self.account_profiles.values())
        with self.driver.session() as session:
            session.run(enrichment_query, batch=batch)

        logger.info(f"Successfully enriched {len(batch)} (:Account) nodes and created (:RiskAssessment) entries in Neo4j.")
        return len(batch)

def print_separator(char="=", length=90):
    print(char * length, flush=True)

def main():
    print_separator("=")
    print("  FinGraph AML Graph Analytics & Composite Risk Scoring Engine (Week 2 - Day 2)")
    print_separator("=")

    engine = GraphAnalyticsEngine()
    try:
        if not engine.detector.verify_connectivity():
            print("\n[!] Neo4j database is not reachable at bolt://localhost:7687. Please ensure Docker is running.", flush=True)
            return

        print("\n[*] Projecting financial graph and calculating analytics metrics...", flush=True)
        profiles = engine.calculate_composite_risk_scores()
        _, communities = engine.compute_communities()

        print(f"\n[+] Total Accounts Analyzed: {len(profiles)}")
        print(f"[+] Total Communities / Subgraphs Discovered: {len(communities)}")

        # Print Community Breakdown
        print_separator("-")
        print("1. Community & Fraud Syndicate Clusters:")
        for comm_id, members in sorted(communities.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
            comm_risk = sum(profiles[m]["risk_score"] for m in members) / max(1, len(members))
            print(f"   * Community #{comm_id} ({len(members)} accounts) | Avg Risk Score: {comm_risk:.1f}/100")
            print(f"     Members: {', '.join(members[:5])}{'...' if len(members) > 5 else ''}")

        # Print Top High-Risk Accounts
        print_separator("-")
        print("2. Top High-Risk Accounts (Composite Risk Score Breakdown):")
        sorted_profiles = sorted(profiles.values(), key=lambda x: x["risk_score"], reverse=True)
        for idx, p in enumerate(sorted_profiles[:8], 1):
            flags = []
            if p["is_hub"]: flags.append("AGGREGATOR_HUB")
            if p["is_mule"]: flags.append("MONEY_MULE")
            if p["cycle_count"] > 0: flags.append(f"CYCLE({p['cycle_count']})")
            if p["smurf_volume"] > 0: flags.append(f"SMURF(${p['smurf_volume']:,.0f})")
            flag_str = f" [{', '.join(flags)}]" if flags else ""

            print(f"   #{idx} Account: {p['account_id']} | Risk: {p['risk_score']}/100 ({p['risk_level']}){flag_str}")
            print(f"      Centrality: In-Deg={p['in_degree']}, Out-Deg={p['out_degree']} | PageRank={p['pagerank']:.4f} | Betweenness={p['betweenness']:.4f}")
            print(f"      Volume: In=${p['inflow_volume']:,.2f}, Out=${p['outflow_volume']:,.2f} | Community: #{p['community_id']}")

        # Persist to Neo4j
        print_separator("-")
        print("3. Persisting Analytics & Risk Profiles to Neo4j...")
        enriched_count = engine.persist_results_to_neo4j()
        print(f"   [+] Successfully enriched {enriched_count} accounts with properties: risk_score, risk_level, pagerank, community_id, is_hub, is_mule.")

        print_separator("=")
        print("  Week 2 - Day 2 Graph Analytics Pipeline Completed Successfully!")
        print_separator("=")

    finally:
        engine.close()

if __name__ == "__main__":
    main()
