// Clear existing data (use with caution in production)
MATCH (n) DETACH DELETE n;

// Create Banks
CREATE (b1:Bank {bank_id: 'B1', name: 'Global Finance'})
CREATE (b2:Bank {bank_id: 'B2', name: 'Secure Trust Bank'})

// Create People
CREATE (p1:Person {person_id: 'P1', name: 'Alice Smith'})
CREATE (p2:Person {person_id: 'P2', name: 'Bob Jones'})

// Create Accounts and link them to People (OWNS) and Banks (HOSTS)
CREATE (a1:Account {account_id: 'A1', account_type: 'Checking'})
CREATE (a2:Account {account_id: 'A2', account_type: 'Savings'})

WITH p1, p2, b1, b2, a1, a2

// Create Relationships
MERGE (p1)-[:OWNS]->(a1)
MERGE (b1)-[:HOSTS]->(a1)

MERGE (p2)-[:OWNS]->(a2)
MERGE (b2)-[:HOSTS]->(a2)

// Create a Transaction (Node approach)
CREATE (t1:Transaction {transaction_id: 'T1', amount: 500.0, timestamp: 1672531200000})

WITH a1, a2, t1

// Link Accounts via the Transaction
MERGE (a1)-[:SENDS]->(t1)
MERGE (t1)-[:TRANSFERRED_TO]->(a2)

// Return the created sample graph
MATCH (n) RETURN n;
