// ============================================================================
// FinGraph AML Alert & SAR Queries - Week 2 Day 3
// Target Schema: (:Alert), (:SuspiciousRing), (:SAR_Report) & Account/Transaction links
// ============================================================================

// ----------------------------------------------------------------------------
// 1. ALERT SCHEMA CONSTRAINTS & INDEXES
// ----------------------------------------------------------------------------
CREATE CONSTRAINT alert_id IF NOT EXISTS FOR (al:Alert) REQUIRE al.alert_id IS UNIQUE;
CREATE CONSTRAINT ring_id IF NOT EXISTS FOR (r:SuspiciousRing) REQUIRE r.ring_id IS UNIQUE;
CREATE CONSTRAINT sar_id IF NOT EXISTS FOR (s:SAR_Report) REQUIRE s.sar_id IS UNIQUE;

CREATE INDEX alert_severity IF NOT EXISTS FOR (al:Alert) ON (al.severity);
CREATE INDEX alert_timestamp IF NOT EXISTS FOR (al:Alert) ON (al.timestamp);
CREATE INDEX alert_pattern_type IF NOT EXISTS FOR (al:Alert) ON (al.pattern_type);


// ----------------------------------------------------------------------------
// 2. BATCH CREATE ALERTS AND LINK TO ACCOUNTS & TRANSACTIONS
// Description: Upserts :Alert nodes and establishes (:Account)-[:FLAGGED_IN]->(:Alert)
// and (:Transaction)-[:TRIGGERED_ALERT]->(:Alert)
// ----------------------------------------------------------------------------
// Name: BatchCreateAlerts
// Parameters: $alerts = [{alert_id, pattern_type, severity, description, risk_score, timestamp, account_ids, transaction_ids}]
UNWIND $alerts AS item
MERGE (al:Alert {alert_id: item.alert_id})
SET al.pattern_type = item.pattern_type,
    al.severity = item.severity,
    al.description = item.description,
    al.risk_score = toFloat(item.risk_score),
    al.timestamp = toInteger(item.timestamp),
    al.status = 'OPEN'

WITH al, item
UNWIND item.account_ids AS acc_id
MATCH (a:Account {account_id: acc_id})
MERGE (a)-[:FLAGGED_IN]->(al)

WITH al, item
UNWIND item.transaction_ids AS tx_id
MATCH (t:Transaction {transaction_id: tx_id})
MERGE (t)-[:TRIGGERED_ALERT]->(al);


// ----------------------------------------------------------------------------
// 3. CREATE SUSPICIOUS RING NODES AND LINK MEMBERS
// Description: Persists discovered fraud syndicates as :SuspiciousRing nodes.
// ----------------------------------------------------------------------------
// Name: CreateSuspiciousRing
// Parameters: $ring_id, $ring_type, $member_accounts, $total_volume, $risk_score, $timestamp
MERGE (r:SuspiciousRing {ring_id: $ring_id})
SET r.ring_type = $ring_type,
    r.member_count = size($member_accounts),
    r.total_volume = toFloat($total_volume),
    r.risk_score = toFloat($risk_score),
    r.timestamp = toInteger($timestamp)
WITH r
UNWIND $member_accounts AS acc_id
MATCH (a:Account {account_id: acc_id})
MERGE (a)-[:MEMBER_OF_RING]->(r);


// ----------------------------------------------------------------------------
// 4. RETRIEVE ACTIVE ALERTS WITH PARTICIPATING ACCOUNTS & TRANSACTIONS
// Description: Fetches full alert details, severity, linked accounts, and total exposure.
// ----------------------------------------------------------------------------
// Name: GetActiveAlerts
MATCH (al:Alert)
OPTIONAL MATCH (a:Account)-[:FLAGGED_IN]->(al)
OPTIONAL MATCH (t:Transaction)-[:TRIGGERED_ALERT]->(al)
WITH al, 
     collect(DISTINCT a.account_id) AS accounts,
     collect(DISTINCT t.transaction_id) AS transactions,
     round(coalesce(sum(t.amount), 0.0), 2) AS total_exposure
RETURN al.alert_id AS alert_id,
       al.pattern_type AS pattern_type,
       al.severity AS severity,
       al.description AS description,
       al.risk_score AS risk_score,
       al.timestamp AS timestamp,
       al.status AS status,
       accounts,
       transactions,
       total_exposure
ORDER BY al.timestamp DESC;


// ----------------------------------------------------------------------------
// 5. EXTRACT 360-DEGREE AUDIT TRAIL FOR SAR GENERATION
// Description: Extracts complete profile, owner, neighbors, and transaction history for an account.
// ----------------------------------------------------------------------------
// Name: GetAccountSARAuditTrail
// Parameters: $account_id
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
           severity: al.severity
       }) AS linked_alerts,
       collect(DISTINCT {
           ring_id: r.ring_id,
           ring_type: r.ring_type,
           total_volume: r.total_volume
       }) AS linked_rings;
