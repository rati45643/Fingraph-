// ============================================================================
// FinGraph Official Week 2 Day 5 - Fraud Query Set
// Target Schema: (:Account)-[:SENDS]->(:Transaction)-[:TRANSFERRED_TO]->(:Account)
// ============================================================================

// ----------------------------------------------------------------------------
// 1. DIRECT RELATIONSHIPS & FLOW SUMMARY
// Description: Returns direct transfer links between accounts with total amount and count.
// ----------------------------------------------------------------------------
// Name: FindDirectTransfers
MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
RETURN src.account_id AS source_account,
       dst.account_id AS destination_account,
       count(t) AS transfer_count,
       round(sum(t.amount), 2) AS total_amount,
       min(t.timestamp) AS first_transfer,
       max(t.timestamp) AS last_transfer
ORDER BY total_amount DESC;


// ----------------------------------------------------------------------------
// 2. MULTI-HOP SUSPICIOUS PASS-THROUGH PATHS (2-HOP / 1-MULE)
// Description: Pinpoints 2-hop paths (A -> B -> C) where B acts as a pass-through intermediary.
// ----------------------------------------------------------------------------
// Name: FindTwoHopIntermediaryPaths
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
ORDER BY incoming_amount DESC;


// ----------------------------------------------------------------------------
// 3. MULTI-HOP LAYERING PATHS (3-HOP / 4-NODES)
// Description: Tracks deep layering chains (A -> B -> C -> D) with time ordering.
// ----------------------------------------------------------------------------
// Name: FindThreeHopLayeringChains
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
ORDER BY hop1_amount DESC;


// ----------------------------------------------------------------------------
// 4. STRUCTURING FAN-IN HUBS (SMURFING RECEIVERS)
// Description: Identifies aggregator accounts receiving deposits from 3+ distinct accounts.
// ----------------------------------------------------------------------------
// Name: FindStructuringFanInHubs
MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(hub:Account)
WITH hub, 
     count(DISTINCT src) AS distinct_senders,
     count(t) AS total_tx_count,
     sum(t.amount) AS total_aggregated,
     collect(DISTINCT src.account_id) AS sender_accounts,
     collect(t.transaction_id) AS tx_ids
WHERE distinct_senders >= 3
RETURN hub.account_id AS hub_account,
       distinct_senders,
       total_tx_count,
       round(total_aggregated, 2) AS total_aggregated,
       sender_accounts,
       tx_ids
ORDER BY distinct_senders DESC, total_aggregated DESC;
