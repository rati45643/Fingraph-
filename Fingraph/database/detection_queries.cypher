// ============================================================================
// FinGraph AML Pattern Detection Queries - Week 2
// Target Schema: (:Account)-[:SENDS]->(:Transaction)-[:TRANSFERRED_TO]->(:Account)
// ============================================================================

// ----------------------------------------------------------------------------
// 1. CIRCULAR FUND FLOW DETECTION (ROUND-TRIPPING)
// Description: Detects closed loops of 3 accounts where money returns to the originator
// Constraints: Timestamps are non-decreasing, and amounts are within a 15% tolerance.
// ----------------------------------------------------------------------------
// Name: DetectCircularFlow3Hop
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
       t1.transaction_id AS tx1_id, t1.amount AS amount_A_to_B, t1.timestamp AS time1,
       t2.transaction_id AS tx2_id, t2.amount AS amount_B_to_C, t2.timestamp AS time2,
       t3.transaction_id AS tx3_id, t3.amount AS amount_C_to_A, t3.timestamp AS time3,
       round(t1.amount, 2) AS cycle_volume,
       (t3.timestamp - t1.timestamp) AS duration_ms
ORDER BY t1.timestamp DESC;


// ----------------------------------------------------------------------------
// 2. SMURFING / STRUCTURING DETECTION (SYNDICATE FAN-IN FUNNEL)
// Description: Identifies destination accounts aggregating deposits from >= 3 distinct 
// source accounts with amounts just below the standard $10,000 reporting threshold ($5,000 - $9,999).
// ----------------------------------------------------------------------------
// Name: DetectSmurfingFunnel
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
ORDER BY total_deposited DESC;


// ----------------------------------------------------------------------------
// 3. MULTI-HOP INTERMEDIARY / LAYERING CHAINS (RAPID PASS-THROUGH)
// Description: Detects multi-step layering chains (A -> B -> C -> D) where intermediary accounts
// receive large funds and forward >= 80% to the next hop within a sequential timeframe.
// ----------------------------------------------------------------------------
// Name: DetectMultiHopLayeringChain
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
ORDER BY t1.timestamp DESC;


// ----------------------------------------------------------------------------
// 4. RAPID 2-HOP PASS-THROUGH INTERMEDIARY (A -> B -> C)
// Description: Detects money-mule intermediary nodes forwarding funds.
// ----------------------------------------------------------------------------
// Name: DetectTwoHopPassThrough
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
ORDER BY t1.timestamp DESC;
