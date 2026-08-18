import json
import logging
from kafka import KafkaConsumer
from neo4j import GraphDatabase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jIngester:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def ingest_entity(self, entity_data):
        with self.driver.session() as session:
            session.execute_write(self._create_entity_tx, entity_data)

    def ingest_transaction(self, tx_data):
        with self.driver.session() as session:
            session.execute_write(self._create_transaction_tx, tx_data)

    @staticmethod
    def _create_entity_tx(tx, entity_data):
        entity_type = entity_data.get('entity_type')
        if entity_type == 'Person':
            tx.run("MERGE (p:Person {person_id: $person_id}) SET p.name = $name", 
                   person_id=entity_data['person_id'], name=entity_data['name'])
        elif entity_type == 'Bank':
            tx.run("MERGE (b:Bank {bank_id: $bank_id}) SET b.name = $name", 
                   bank_id=entity_data['bank_id'], name=entity_data['name'])
        elif entity_type == 'Account':
            query = """
            MERGE (a:Account {account_id: $account_id}) 
            SET a.account_type = $account_type
            WITH a
            MATCH (p:Person {person_id: $owner_id})
            MATCH (b:Bank {bank_id: $bank_id})
            MERGE (p)-[:OWNS]->(a)
            MERGE (b)-[:HOSTS]->(a)
            """
            tx.run(query, account_id=entity_data['account_id'], account_type=entity_data['account_type'],
                   owner_id=entity_data['owner_id'], bank_id=entity_data['bank_id'])

    @staticmethod
    def _create_transaction_tx(tx, tx_data):
        query = """
        MERGE (t:Transaction {transaction_id: $transaction_id})
        SET t.amount = toFloat($amount), 
            t.timestamp = toInteger($timestamp), 
            t.is_suspicious = toBoolean($is_suspicious),
            t.source_account = $source_account,
            t.dest_account = $dest_account
        WITH t
        MATCH (src:Account {account_id: $source_account})
        MATCH (dst:Account {account_id: $dest_account})
        MERGE (src)-[:SENDS]->(t)
        MERGE (t)-[:TRANSFERRED_TO]->(dst)
        """
        tx.run(query, transaction_id=tx_data['transaction_id'], amount=tx_data['amount'],
               timestamp=tx_data['timestamp'], is_suspicious=tx_data['is_suspicious'],
               source_account=tx_data['source_account'], dest_account=tx_data['dest_account'])


def main():
    logger.info("Starting Neo4j Ingestion Script...")
    
    # Initialize Neo4j Ingester
    ingester = Neo4jIngester("bolt://localhost:7687", "neo4j", "password")

    try:
        # Initialize Kafka Consumer listening to both topics
        consumer = KafkaConsumer(
            'entities', 'transactions',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest', # Start from the beginning to grab missed entities
            enable_auto_commit=True,
            group_id='neo4j-ingest-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info("Connected to Kafka. Waiting for messages on topics 'entities' and 'transactions'...")
        
        for message in consumer:
            topic = message.topic
            data = message.value
            
            if topic == 'entities':
                ingester.ingest_entity(data)
                logger.info(f"Ingested Entity: {data.get('entity_type')} | ID: {data.get('person_id') or data.get('bank_id') or data.get('account_id')}")
            elif topic == 'transactions':
                ingester.ingest_transaction(data)
                logger.info(f"Ingested Transaction: {data.get('transaction_id')} | Amount: {data.get('amount')}")
                
    except Exception as e:
        logger.error(f"Failed to run ingestion: {e}")
    finally:
        ingester.close()
        logger.info("Ingestion shutdown complete.")

if __name__ == "__main__":
    main()
