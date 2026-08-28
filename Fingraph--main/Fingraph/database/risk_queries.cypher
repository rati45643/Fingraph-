/*
FinGraph Week 2 - Day 6
Circular Flow Detection & AML Risk Queries
*/

/*
DetectCircularFlowRings
Detects 3-hop circular flows:
A -> B -> C -> A
*/
MATCH (a:Account)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(b:Account)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(c:Account)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(a)
WHERE a <> b
  AND b <> c
  AND a <> c
  AND t1.timestamp <= t2.timestamp
  AND t2.timestamp <= t3.timestamp
RETURN a.account_id AS account_A,
       b.account_id AS account_B,
       c.account_id AS account_C,
       t1.transaction_id AS tx1_id,
       t2.transaction_id AS tx2_id,
       t3.transaction_id AS tx3_id,
       round(t1.amount, 2) AS tx1_amount,
       round(t2.amount, 2) AS tx2_amount,
       round(t3.amount, 2) AS tx3_amount,
       round((t1.amount + t2.amount + t3.amount) / 3.0, 2) AS average_cycle_amount,
       (t3.timestamp - t1.timestamp) AS cycle_duration_ms
ORDER BY average_cycle_amount DESC;


/*
CalculateAccountInitialRiskScores
Calculates initial AML risk scores for Account nodes.

Risk formula:
CycleCount * 40
+ SuspiciousTransactionCount * 15
+ VolumeBonus
*/
MATCH (a:Account)
OPTIONAL MATCH (a)-[:SENDS]->(t_out:Transaction)-[:TRANSFERRED_TO]->(b:Account)
OPTIONAL MATCH (src:Account)-[:SENDS]->(t_in:Transaction)-[:TRANSFERRED_TO]->(a)
WITH a,
     count(DISTINCT t_out) AS out_count,
     count(DISTINCT t_in) AS in_count,
     coalesce(sum(t_out.amount), 0.0) AS total_outflow,
     coalesce(sum(t_in.amount), 0.0) AS total_inflow,
     count(DISTINCT CASE
       WHEN t_out.is_suspicious = true THEN t_out
     END) AS sus_out,
     count(DISTINCT CASE
       WHEN t_in.is_suspicious = true THEN t_in
     END) AS sus_in

OPTIONAL MATCH (a)-[:SENDS]->(t1:Transaction)-[:TRANSFERRED_TO]->(n1:Account)-[:SENDS]->(t2:Transaction)-[:TRANSFERRED_TO]->(n2:Account)-[:SENDS]->(t3:Transaction)-[:TRANSFERRED_TO]->(a)
WHERE a <> n1
  AND n1 <> n2
  AND a <> n2
  AND t1.timestamp <= t2.timestamp
  AND t2.timestamp <= t3.timestamp

WITH a,
     out_count,
     in_count,
     total_outflow,
     total_inflow,
     sus_out + sus_in AS total_sus_txs,
     count(DISTINCT t1) AS cycle_count

WITH a,
     out_count,
     in_count,
     total_outflow,
     total_inflow,
     total_sus_txs,
     cycle_count,
     (cycle_count * 40.0) +
     (total_sus_txs * 15.0) +
     CASE
       WHEN total_inflow + total_outflow >= 20000 THEN 20.0
       WHEN total_inflow + total_outflow >= 5000 THEN 10.0
       ELSE 0.0
     END AS raw_score

WITH a,
     out_count,
     in_count,
     total_outflow,
     total_inflow,
     total_sus_txs,
     cycle_count,
     CASE
       WHEN raw_score > 100.0 THEN 100.0
       ELSE round(raw_score, 1)
     END AS risk_score

RETURN a.account_id AS account_id,
       cycle_count,
       total_sus_txs,
       round(total_inflow, 2) AS total_inflow,
       round(total_outflow, 2) AS total_outflow,
       risk_score;


/*
UpdateAccountRiskProperties
Persists risk score and risk level on Account nodes.
*/
MATCH (a:Account)
WHERE a.risk_score IS NOT NULL
SET a.risk_level =
  CASE
    WHEN a.risk_score >= 75.0 THEN 'CRITICAL'
    WHEN a.risk_score >= 50.0 THEN 'HIGH'
    WHEN a.risk_score >= 25.0 THEN 'MEDIUM'
    ELSE 'LOW'
  END,
  a.last_risk_assessed = timestamp()
RETURN a.account_id AS account_id,
       a.risk_score AS risk_score,
       a.risk_level AS risk_level;
