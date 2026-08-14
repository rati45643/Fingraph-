// ============================================================================
// FinGraph AML Graph Analytics Queries - Week 2 Day 2
// Target Schema: (:Account)-[:SENDS]->(:Transaction)-[:TRANSFERRED_TO]->(:Account)
// ============================================================================

// ----------------------------------------------------------------------------
// 1. DEGREE CENTRALITY & FLOW BALANCE CALCULATION
// Description: Computes in-degree, out-degree, total volumes, and pass-through ratios per account.
// ----------------------------------------------------------------------------
// Name: CalculateAccountDegreeCentrality
MATCH (a:Account)
OPTIONAL MATCH (a)-[:SENDS]->(t_out:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
OPTIONAL MATCH (src:Account)-[:SENDS]->(t_in:Transaction)-[:TRANSFERRED_TO]->(a)
WITH a,
     count(DISTINCT t_out) AS out_degree,
     count(DISTINCT t_in) AS in_degree,
     coalesce(sum(t_out.amount), 0.0) AS total_outflow,
     coalesce(sum(t_in.amount), 0.0) AS total_inflow
RETURN a.account_id AS account_id,
       a.account_type AS account_type,
       in_degree,
       out_degree,
       (in_degree + out_degree) AS total_degree,
       round(total_inflow, 2) AS total_inflow,
       round(total_outflow, 2) AS total_outflow,
       round(total_inflow - total_outflow, 2) AS net_flow,
       CASE 
         WHEN total_inflow > 0 THEN round(total_outflow / total_inflow, 3)
         ELSE 0.0 
       END AS pass_through_ratio
ORDER BY total_degree DESC;


// ----------------------------------------------------------------------------
// 2. IDENTIFY SUSPICIOUS AGGREGATOR HUBS (SMURFING RECEIVERS)
// Description: Pinpoints accounts with high in-degree from distinct source senders.
// ----------------------------------------------------------------------------
// Name: IdentifySuspiciousHubs
MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(hub:Account)
WITH hub, 
     count(DISTINCT src) AS distinct_senders,
     count(t) AS deposit_count,
     sum(t.amount) AS total_aggregated,
     min(t.timestamp) AS first_seen,
     max(t.timestamp) AS last_seen
WHERE distinct_senders >= 3
RETURN hub.account_id AS hub_account,
       distinct_senders,
       deposit_count,
       round(total_aggregated, 2) AS total_aggregated,
       (last_seen - first_seen) AS duration_ms
ORDER BY distinct_senders DESC, total_aggregated DESC;


// ----------------------------------------------------------------------------
// 3. IDENTIFY PASS-THROUGH INTERMEDIARY MULES
// Description: Identifies accounts that rapidly receive and forward funds with low net retention.
// ----------------------------------------------------------------------------
// Name: IdentifyIntermediaryMules
MATCH (src:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(mule:Account)
MATCH (mule)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
WHERE src <> mule AND mule <> dst AND src <> dst
  AND t1.timestamp <= t2.timestamp
WITH mule,
     count(DISTINCT src) AS distinct_sources,
     count(DISTINCT dst) AS distinct_destinations,
     count(DISTINCT t1) AS incoming_txs,
     count(DISTINCT t2) AS outgoing_txs,
     sum(t1.amount) AS total_in,
     sum(t2.amount) AS total_out
WHERE distinct_sources >= 1 AND distinct_destinations >= 1
RETURN mule.account_id AS mule_account,
       distinct_sources,
       distinct_destinations,
       incoming_txs,
       outgoing_txs,
       round(total_in, 2) AS total_in,
       round(total_out, 2) AS total_out,
       round(abs(total_in - total_out), 2) AS balance_delta,
       round(total_out / total_in, 3) AS pass_through_rate
ORDER BY total_out DESC;


// ----------------------------------------------------------------------------
// 4. SUBGRAPH & WEAKLY CONNECTED COMPONENT DISCOVERY (APOC)
// Description: Explores connected transaction components for isolated fraud syndicates.
// ----------------------------------------------------------------------------
// Name: ExpandAccountComponent
MATCH (seed:Account)
CALL apoc.path.subgraphNodes(seed, {
    relationshipFilter: "SENDS>|<TRANSFERRED_TO",
    labelFilter: "+Account"
}) YIELD node
WITH seed, collect(DISTINCT node.account_id) AS component_members
WHERE size(component_members) > 1
RETURN seed.account_id AS seed_account,
       size(component_members) AS component_size,
       component_members
ORDER BY component_size DESC;


// ----------------------------------------------------------------------------
// 5. STORE RISK SCORES & ANALYTICS ENRICHMENT
// Description: Enriches (:Account) nodes with computed centrality, community, and risk scores.
// ----------------------------------------------------------------------------
// Name: EnrichAccountRiskProfile
// Parameters: $account_id, $risk_score, $risk_level, $community_id, $in_degree, $out_degree, $pagerank, $betweenness, $is_hub, $is_mule
MATCH (a:Account {account_id: $account_id})
SET a.risk_score = toFloat($risk_score),
    a.risk_level = $risk_level,
    a.community_id = toInteger($community_id),
    a.in_degree = toInteger($in_degree),
    a.out_degree = toInteger($out_degree),
    a.pagerank = toFloat($pagerank),
    a.betweenness_centrality = toFloat($betweenness),
    a.is_hub = toBoolean($is_hub),
    a.is_mule = toBoolean($is_mule),
    a.last_assessed_at = timestamp()
CREATE (ra:RiskAssessment {
    assessment_id: randomUUID(),
    account_id: $account_id,
    risk_score: toFloat($risk_score),
    risk_level: $risk_level,
    community_id: toInteger($community_id),
    timestamp: timestamp()
})
MERGE (a)-[:HAS_ASSESSMENT]->(ra);
