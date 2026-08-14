// Create Constraints (Ensures uniqueness and implicitly creates indexes)
CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.person_id IS UNIQUE;
CREATE CONSTRAINT bank_id IF NOT EXISTS FOR (b:Bank) REQUIRE b.bank_id IS UNIQUE;
CREATE CONSTRAINT account_id IF NOT EXISTS FOR (a:Account) REQUIRE a.account_id IS UNIQUE;
CREATE CONSTRAINT transaction_id IF NOT EXISTS FOR (t:Transaction) REQUIRE t.transaction_id IS UNIQUE;

// Additional Indexes for query performance (e.g., finding transactions by timestamp)
CREATE INDEX transaction_timestamp IF NOT EXISTS FOR (t:Transaction) ON (t.timestamp);
CREATE INDEX account_type IF NOT EXISTS FOR (a:Account) ON (a.account_type);
