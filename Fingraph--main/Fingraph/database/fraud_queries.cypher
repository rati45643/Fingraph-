cat > database/fraud_queries.cypher <<'EOF'
/*
FinGraph Week 2 - Day 5
Fraud Investigation Queries
*/

/*
FindDirectTransfers
Direct account-to-account transfer summaries.
*/
MATCH (src:Account)-[:SENDS]->(t:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
RETURN src.account_id AS source_account,
       dst.account_id AS destination_account,
       count(t) AS transfer_count,
       round(sum(t.amount), 2) AS total_amount
ORDER BY total_amount DESC
LIMIT 50;


/*
FindTwoHopIntermediaryPaths
Detects pass-through intermediary mule accounts:
A -> B -> C
*/
MATCH (src:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(mule:Account)
MATCH (mule)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(dst:Account)
WHERE src <> mule
  AND mule <> dst
  AND src <> dst
  AND t1.timestamp <= t2.timestamp
RETURN src.account_id AS source_account,
       mule.account_id AS intermediary_mule,
       dst.account_id AS destination_account,
       t1.transaction_id AS in_tx_id,
       round(t1.amount, 2) AS incoming_amount,
       t2.transaction_id AS out_tx_id,
       round(t2.amount, 2) AS outgoing_amount
ORDER BY incoming_amount DESC;


/*
FindThreeHopLayeringChains
Detects:
A -> B -> C -> D
*/
MATCH (a:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(b:Account)
MATCH (b)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(c:Account)
MATCH (c)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(d:Account)
WHERE a <> b
  AND b <> c
  AND c <> d
  AND a <> c
  AND a <> d
  AND b <> d
  AND t1.timestamp <= t2.timestamp
  AND t2.timestamp <= t3.timestamp
RETURN a.account_id AS originator,
       b.account_id AS hop1_intermediary,
       c.account_id AS hop2_intermediary,
       d.account_id AS ultimate_beneficiary,
       [t1.transaction_id, t2.transaction_id, t3.transaction_id] AS chain_tx_ids,
       round(t1.amount, 2) AS hop1_amount,
       round(t2.amount, 2) AS hop2_amount,
       round(t3.amount, 2) AS hop3_amount
ORDER BY hop1_amount DESC;


/*
FindStructuringFanInHubs
Detects accounts receiving transactions from multiple distinct accounts.
*/
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
EOF
