import os
import sys
import uuid
import time
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from neo4j import GraphDatabase

# Ensure simulator directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from detect_patterns import AMLPatternDetector
from graph_analytics import GraphAnalyticsEngine

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("FinGraph-AlertEngine")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class AMLAlertEngine:
    """
    Real-Time AML Alerting & SAR Generation Engine.
    Evaluates detected topologies & high-risk profiles, creates structured alerts,
    persists Alert and SuspiciousRing nodes in Neo4j, and generates FinCEN-compliant SARs.
    """

    def __init__(self, uri=NEO4J_URI, user=NEO4J_USER, password=NEO4J_PASSWORD):
        self.uri = uri
        self.user = user
        self.password = password
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password), connection_timeout=5.0)
        self.detector = AMLPatternDetector(uri=self.uri, user=self.user, password=self.password)
        self.analytics = GraphAnalyticsEngine(uri=self.uri, user=self.user, password=self.password)
        self.alerts: List[Dict[str, Any]] = []
        self.rings: List[Dict[str, Any]] = []

    def close(self):
        if self.driver:
            self.driver.close()
        if self.detector:
            self.detector.close()
        if self.analytics:
            self.analytics.close()

    def apply_alert_schema(self):
        """Creates indexes and constraints for Alert, SuspiciousRing, and SAR_Report nodes."""
        schema_statements = [
            "CREATE CONSTRAINT alert_id IF NOT EXISTS FOR (al:Alert) REQUIRE al.alert_id IS UNIQUE",
            "CREATE CONSTRAINT ring_id IF NOT EXISTS FOR (r:SuspiciousRing) REQUIRE r.ring_id IS UNIQUE",
            "CREATE CONSTRAINT sar_id IF NOT EXISTS FOR (s:SAR_Report) REQUIRE s.sar_id IS UNIQUE",
            "CREATE INDEX alert_severity IF NOT EXISTS FOR (al:Alert) ON (al.severity)",
            "CREATE INDEX alert_timestamp IF NOT EXISTS FOR (al:Alert) ON (al.timestamp)",
            "CREATE INDEX alert_pattern_type IF NOT EXISTS FOR (al:Alert) ON (al.pattern_type)"
        ]
        with self.driver.session() as session:
            for stmt in schema_statements:
                session.run(stmt)
        logger.info("Alert schema constraints and indexes applied successfully.")

    def evaluate_and_generate_alerts(self) -> Tuple_List_Alerts:
        """
        Runs topology detectors and risk assessment to generate actionable alerts.
        """
        self.alerts.clear()
        self.rings.clear()
        now_ms = int(time.time() * 1000)

        # 1. Circular Flow Alerts
        cycles = self.detector.detect_circular_flows()
        seen_rings = set()
        for c in cycles:
            ring_members = tuple(sorted([c["account_A"], c["account_B"], c["account_C"]]))
            tx_ids = [c["tx1_id"], c["tx2_id"], c["tx3_id"]]
            
            alert = {
                "alert_id": f"ALT-CYC-{uuid.uuid4().hex[:8].upper()}",
                "pattern_type": "CIRCULAR_FLOW",
                "severity": "CRITICAL",
                "risk_score": 90.0,
                "description": f"Closed 3-hop circular flow detected: {c['account_A']} -> {c['account_B']} -> {c['account_C']} -> {c['account_A']} (Volume: ${c['cycle_volume']:,.2f})",
                "timestamp": now_ms,
                "account_ids": list(ring_members),
                "transaction_ids": tx_ids,
                "volume": float(c["cycle_volume"])
            }
            self.alerts.append(alert)

            if ring_members not in seen_rings:
                seen_rings.add(ring_members)
                ring = {
                    "ring_id": f"RING-CYC-{uuid.uuid4().hex[:8].upper()}",
                    "ring_type": "CIRCULAR_ROUND_TRIP",
                    "member_accounts": list(ring_members),
                    "total_volume": float(c["cycle_volume"]),
                    "risk_score": 90.0,
                    "timestamp": now_ms
                }
                self.rings.append(ring)

        # 2. Smurfing / Structuring Funnel Alerts
        funnels = self.detector.detect_smurfing_funnels()
        for f in funnels:
            dst = f["destination_account"]
            sources = f["source_accounts"]
            tx_ids = f["transaction_ids"]
            volume = float(f["total_deposited"])
            
            alert = {
                "alert_id": f"ALT-SMURF-{uuid.uuid4().hex[:8].upper()}",
                "pattern_type": "SMURFING_FUNNEL",
                "severity": "CRITICAL",
                "risk_score": 85.0,
                "description": f"Structuring fan-in hub detected at {dst}: received ${volume:,.2f} from {len(sources)} distinct accounts just below CTR limit.",
                "timestamp": now_ms,
                "account_ids": [dst] + sources,
                "transaction_ids": tx_ids,
                "volume": volume
            }
            self.alerts.append(alert)

            ring = {
                "ring_id": f"RING-SMURF-{uuid.uuid4().hex[:8].upper()}",
                "ring_type": "SMURFING_SYNDICATE",
                "member_accounts": [dst] + sources,
                "total_volume": volume,
                "risk_score": 85.0,
                "timestamp": now_ms
            }
            self.rings.append(ring)

        # 3. Multi-Hop Layering Chains
        layering = self.detector.detect_multihop_layering()
        for l in layering:
            chain_accounts = [l["originator"], l["hop1_intermediary"], l["hop2_intermediary"], l["ultimate_beneficiary"]]
            tx_ids = l["chain_tx_ids"]
            initial_amt = float(l["initial_amount"])

            alert = {
                "alert_id": f"ALT-LAY-{uuid.uuid4().hex[:8].upper()}",
                "pattern_type": "LAYERING_CHAIN",
                "severity": "HIGH",
                "risk_score": 75.0,
                "description": f"Multi-hop layering chain: {l['originator']} -> {l['hop1_intermediary']} -> {l['hop2_intermediary']} -> {l['ultimate_beneficiary']} (Initial: ${initial_amt:,.2f})",
                "timestamp": now_ms,
                "account_ids": chain_accounts,
                "transaction_ids": tx_ids,
                "volume": initial_amt
            }
            self.alerts.append(alert)

        # 4. 2-Hop Pass-Through Mules
        mules = self.detector.detect_two_hop_passthrough()
        for m in mules:
            mule_acc = m["intermediary_mule"]
            tx_ids = [m["in_tx_id"], m["out_tx_id"]]
            in_amt = float(m["incoming_amount"])

            alert = {
                "alert_id": f"ALT-MULE-{uuid.uuid4().hex[:8].upper()}",
                "pattern_type": "PASS_THROUGH_MULE",
                "severity": "HIGH",
                "risk_score": 70.0,
                "description": f"Pass-through intermediary mule {mule_acc} forwarded ${m['outgoing_amount']:,.2f} from {m['source_account']} to {m['destination_account']}.",
                "timestamp": now_ms,
                "account_ids": [mule_acc, m["source_account"], m["destination_account"]],
                "transaction_ids": tx_ids,
                "volume": in_amt
            }
            self.alerts.append(alert)

        # 5. Composite High-Risk Entity Alerts (Risk Score >= 75)
        profiles = self.analytics.calculate_composite_risk_scores()
        for node, prof in profiles.items():
            if prof["risk_score"] >= 75.0:
                alert = {
                    "alert_id": f"ALT-ENT-{uuid.uuid4().hex[:8].upper()}",
                    "pattern_type": "HIGH_RISK_ENTITY",
                    "severity": "CRITICAL",
                    "risk_score": prof["risk_score"],
                    "description": f"High-risk account {node} reached composite risk score {prof['risk_score']}/100 (Level: {prof['risk_level']}). Centrality betweenness={prof['betweenness']:.4f}, PageRank={prof['pagerank']:.4f}.",
                    "timestamp": now_ms,
                    "account_ids": [node],
                    "transaction_ids": [],
                    "volume": prof["inflow_volume"]
                }
                self.alerts.append(alert)

        logger.info(f"Generated {len(self.alerts)} AML alerts and {len(self.rings)} suspicious rings.")
        return self.alerts, self.rings

    def persist_alerts_to_neo4j(self) -> Dict[str, int]:
        """
        Persists generated alerts and suspicious rings into Neo4j.
        """
        self.apply_alert_schema()
        if not self.alerts and not self.rings:
            self.evaluate_and_generate_alerts()

        # Batch insert alerts
        alert_batch_query = """
        UNWIND $alerts AS item
        MERGE (al:Alert {alert_id: item.alert_id})
        SET al.pattern_type = item.pattern_type,
            al.severity = item.severity,
            al.description = item.description,
            al.risk_score = toFloat(item.risk_score),
            al.timestamp = toInteger(item.timestamp),
            al.volume = toFloat(item.volume),
            al.status = 'OPEN'
        WITH al, item
        UNWIND item.account_ids AS acc_id
        MATCH (a:Account {account_id: acc_id})
        MERGE (a)-[:FLAGGED_IN]->(al)
        WITH al, item
        UNWIND item.transaction_ids AS tx_id
        MATCH (t:Transaction {transaction_id: tx_id})
        MERGE (t)-[:TRIGGERED_ALERT]->(al)
        """

        ring_batch_query = """
        UNWIND $rings AS item
        MERGE (r:SuspiciousRing {ring_id: item.ring_id})
        SET r.ring_type = item.ring_type,
            r.member_count = size(item.member_accounts),
            r.total_volume = toFloat(item.total_volume),
            r.risk_score = toFloat(item.risk_score),
            r.timestamp = toInteger(item.timestamp)
        WITH r, item
        UNWIND item.member_accounts AS acc_id
        MATCH (a:Account {account_id: acc_id})
        MERGE (a)-[:MEMBER_OF_RING]->(r)
        """

        with self.driver.session() as session:
            if self.alerts:
                session.run(alert_batch_query, alerts=self.alerts)
            if self.rings:
                session.run(ring_batch_query, rings=self.rings)

        logger.info(f"Persisted {len(self.alerts)} alerts and {len(self.rings)} rings to Neo4j.")
        return {"alerts_count": len(self.alerts), "rings_count": len(self.rings)}

    def get_active_alerts(self, severity_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """Queries all active alerts from Neo4j."""
        query = """
        MATCH (al:Alert)
        OPTIONAL MATCH (a:Account)-[:FLAGGED_IN]->(al)
        OPTIONAL MATCH (t:Transaction)-[:TRIGGERED_ALERT]->(al)
        WITH al, 
             collect(DISTINCT a.account_id) AS accounts,
             collect(DISTINCT t.transaction_id) AS transactions
        RETURN al.alert_id AS alert_id,
               al.pattern_type AS pattern_type,
               al.severity AS severity,
               al.description AS description,
               al.risk_score AS risk_score,
               al.volume AS volume,
               al.timestamp AS timestamp,
               al.status AS status,
               accounts,
               transactions
        ORDER BY al.timestamp DESC, al.risk_score DESC
        """
        with self.driver.session() as session:
            result = session.run(query)
            alerts = [record.data() for record in result]

        if severity_filter:
            alerts = [a for a in alerts if a["severity"].upper() == severity_filter.upper()]

        return alerts

    def generate_sar_report(self, account_id: str) -> Dict[str, Any]:
        """
        Generates a regulatory-grade FinCEN-compliant Suspicious Activity Report (SAR) payload
        and markdown narrative for a specified account.
        """
        query = """
        MATCH (a:Account {account_id: $account_id})
        OPTIONAL MATCH (p:Person)-[:OWNS]->(a)
        OPTIONAL MATCH (b:Bank)-[:HOSTS]->(a)
        OPTIONAL MATCH (src:Account)-[:SENDS]->(t_in:Transaction)-[:TRANSFERRED_TO]->(a)
        OPTIONAL MATCH (a)-[:SENDS]->(t_out:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
        OPTIONAL MATCH (a)-[:FLAGGED_IN]->(al:Alert)
        OPTIONAL MATCH (a)-[:MEMBER_OF_RING]->(r:SuspiciousRing)
        RETURN a.account_id AS account_id,
               a.account_type AS account_type,
               a.risk_score AS risk_score,
               a.risk_level AS risk_level,
               a.community_id AS community_id,
               a.is_hub AS is_hub,
               a.is_mule AS is_mule,
               p.person_id AS owner_id,
               p.name AS owner_name,
               b.bank_id AS bank_id,
               b.name AS bank_name,
               collect(DISTINCT {
                   tx_id: t_in.transaction_id,
                   amount: t_in.amount,
                   timestamp: t_in.timestamp,
                   sender: src.account_id,
                   is_suspicious: t_in.is_suspicious
               }) AS incoming_transactions,
               collect(DISTINCT {
                   tx_id: t_out.transaction_id,
                   amount: t_out.amount,
                   timestamp: t_out.timestamp,
                   receiver: dst.account_id,
                   is_suspicious: t_out.is_suspicious
               }) AS outgoing_transactions,
               collect(DISTINCT {
                   alert_id: al.alert_id,
                   pattern: al.pattern_type,
                   severity: al.severity,
                   description: al.description
               }) AS linked_alerts,
               collect(DISTINCT {
                   ring_id: r.ring_id,
                   ring_type: r.ring_type,
                   total_volume: r.total_volume
               }) AS linked_rings
        """
        with self.driver.session() as session:
            record = session.run(query, account_id=account_id).single()

        if not record or not record["account_id"]:
            return {
                "error": f"Account {account_id} not found in Neo4j.",
                "sar_id": None
            }

        data = record.data()
        in_txs = [t for t in data.get("incoming_transactions", []) if t.get("tx_id")]
        out_txs = [t for t in data.get("outgoing_transactions", []) if t.get("tx_id")]
        alerts = [al for al in data.get("linked_alerts", []) if al.get("alert_id")]
        rings = [r for r in data.get("linked_rings", []) if r.get("ring_id")]

        total_inflow = sum(float(t.get("amount") or 0.0) for t in in_txs)
        total_outflow = sum(float(t.get("amount") or 0.0) for t in out_txs)

        sar_id = f"SAR-FINCEN-{time.strftime('%Y%m%d')}-{uuid.uuid4().hex[:6].upper()}"
        filing_date = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

        # Build Suspicious Activity Narrative
        narrative = f"""
### SUSPICIOUS ACTIVITY REPORT (SAR) - NARRATIVE OF SUSPICIOUS ACTIVITY

**Filing Reference:** {sar_id}
**Filing Date:** {filing_date}
**Subject Account:** {data['account_id']} ({data.get('account_type', 'Standard')})
**Primary Owner:** {data.get('owner_name', 'Unknown')} (ID: {data.get('owner_id', 'N/A')})
**Host Financial Institution:** {data.get('bank_name', 'FinTech Partner Bank')} (ID: {data.get('bank_id', 'N/A')})

---

#### 1. EXECUTIVE SUMMARY & RISK PROFILE
The FinGraph Anti-Money Laundering automated graph intelligence engine flagged subject account `{data['account_id']}` with a Composite Risk Score of **{data.get('risk_score', 0.0)}/100 ({data.get('risk_level', 'UNRATED')})**.
The subject was placed into Community Syndicate Cluster `#{data.get('community_id', 1)}` and triggered **{len(alerts)} automated AML alerts** across circular flow, structuring, and pass-through layering typologies.

#### 2. TRANSACTIONAL OVERVIEW & EXPOSURE
* **Total Inflow Volume:** ${total_inflow:,.2f} across {len(in_txs)} transactions
* **Total Outflow Volume:** ${total_outflow:,.2f} across {len(out_txs)} transactions
* **Net Retention Delta:** ${abs(total_inflow - total_outflow):,.2f}
* **Hub Classification:** {'YES (Aggregator Funnel Hub)' if data.get('is_hub') else 'NO'}
* **Mule Classification:** {'YES (Pass-Through Intermediary Mule)' if data.get('is_mule') else 'NO'}

#### 3. PATTERN DETECTION & SYNDICATE TOPOLOGY
"""
        if rings:
            narrative += f"* **Associated Fraud Syndicates ({len(rings)}):**\n"
            for r in rings:
                narrative += f"  - Ring ID `{r['ring_id']}` ({r['ring_type']}) | Total Ring Exposure: ${r.get('total_volume', 0.0):,.2f}\n"

        if alerts:
            narrative += f"\n* **Triggered AML Alerts ({len(alerts)}):**\n"
            for al in alerts:
                narrative += f"  - [{al['severity']}] `{al['alert_id']}`: {al['pattern']} - {al.get('description', '')}\n"

        narrative += f"""
#### 4. COMPLIANCE & LAW ENFORCEMENT RECOMMENDATIONS
1. **Immediate Account Restrictions:** Impose debit hold on `{data['account_id']}` pending enhanced due diligence (EDD).
2. **Cluster Quarantine:** Review all adjacent accounts in Community Cluster `#{data.get('community_id', 1)}`.
3. **Regulatory Dispatch:** Forward report to FinCEN and relevant financial intelligence units.
"""

        sar_payload = {
            "sar_id": sar_id,
            "filing_date": filing_date,
            "account_id": data["account_id"],
            "account_type": data.get("account_type"),
            "owner": {
                "person_id": data.get("owner_id"),
                "name": data.get("owner_name")
            },
            "bank": {
                "bank_id": data.get("bank_id"),
                "name": data.get("bank_name")
            },
            "risk_assessment": {
                "risk_score": data.get("risk_score"),
                "risk_level": data.get("risk_level"),
                "community_id": data.get("community_id"),
                "is_hub": data.get("is_hub"),
                "is_mule": data.get("is_mule")
            },
            "transaction_summary": {
                "total_inflow": total_inflow,
                "total_outflow": total_outflow,
                "incoming_tx_count": len(in_txs),
                "outgoing_tx_count": len(out_txs),
                "incoming_transactions": in_txs,
                "outgoing_transactions": out_txs
            },
            "alerts": alerts,
            "suspicious_rings": rings,
            "narrative": narrative.strip()
        }

        return sar_payload

# Helper type alias
Tuple_List_Alerts = Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]

def print_separator(char="=", length=90):
    print(char * length, flush=True)

def main():
    print_separator("=")
    print("  FinGraph Real-Time AML Alert & SAR Generation Engine (Week 2 - Day 3)")
    print_separator("=")

    engine = AMLAlertEngine()
    try:
        if not engine.detector.verify_connectivity():
            print("\n[!] Neo4j database is not reachable at bolt://localhost:7687. Please ensure Docker is running.", flush=True)
            return

        print("\n[*] Evaluating AML topology detections and generating alerts...", flush=True)
        alerts, rings = engine.evaluate_and_generate_alerts()

        print(f"[+] Total Alerts Generated: {len(alerts)}")
        print(f"[+] Total Suspicious Rings Identified: {len(rings)}")

        # Print Top Alerts Summary
        print_separator("-")
        print("1. Generated AML Alerts Breakdown:")
        for idx, al in enumerate(alerts[:6], 1):
            print(f"   #{idx} [{al['severity']}] {al['alert_id']} ({al['pattern_type']}) - Risk: {al['risk_score']}/100")
            print(f"      {al['description']}")
            print(f"      Accounts: {', '.join(al['account_ids'])}")

        # Persist to Neo4j
        print_separator("-")
        print("2. Persisting Alerts & Suspicious Rings to Neo4j...")
        persisted = engine.persist_alerts_to_neo4j()
        print(f"   [+] Persisted {persisted['alerts_count']} alerts and {persisted['rings_count']} rings to Neo4j.")

        # Generate sample SAR for highest-risk account
        if alerts and alerts[0]["account_ids"]:
            sample_acc = alerts[0]["account_ids"][0]
            print_separator("-")
            print(f"3. Generating FinCEN-Compliant SAR for High-Risk Subject: {sample_acc}...")
            sar = engine.generate_sar_report(sample_acc)
            print(f"   [+] SAR Filing ID: {sar['sar_id']}")
            print("\n" + sar["narrative"])

        print_separator("=")
        print("  Week 2 - Day 3 Alert & SAR Engine Completed Successfully!")
        print_separator("=")

    finally:
        engine.close()

if __name__ == "__main__":
    main()
