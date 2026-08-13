import time
import random
import logging
from generator import DataGenerator
from producer import EventProducer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Initializing Data Generator...")
    # Smaller numbers for initial test
    generator = DataGenerator(num_people=20, num_banks=2)
    
    logger.info("Initializing Kafka Producer...")
    producer = EventProducer(bootstrap_servers=['localhost:9092'])
    
    # Optional: Send initial entities to Kafka so Neo4j ingestion can create nodes
    # For now, we focus on transactions, but sending entities is good practice.
    logger.info("Sending static entities (Banks, People, Accounts)...")
    for entity in generator.get_initial_entities():
        data = entity.to_dict()
        key = data.get('bank_id') or data.get('person_id') or data.get('account_id')
        producer.send_event('entities', data, key=key)
        
    time.sleep(1) # Give Kafka a moment
    
    logger.info("Starting transaction stream...")
    try:
        while True:
            # Decide what kind of transaction(s) to generate
            pattern_choice = random.random()
            
            transactions = []
            if pattern_choice < 0.8:
                # 80% chance for an ordinary transaction
                transactions.append(generator.generate_ordinary_transaction())
            elif pattern_choice < 0.9:
                # 10% chance for a funnel (many-to-one)
                logger.info("Generating Syndicate Funnel pattern...")
                transactions.extend(generator.generate_syndicate_funnel())
            elif pattern_choice < 0.95:
                # 5% chance for a multi-hop
                logger.info("Generating Multi-Hop Intermediary pattern...")
                transactions.extend(generator.generate_multi_hop_intermediary())
            else:
                # 5% chance for a circular flow
                logger.info("Generating Circular Flow pattern...")
                transactions.extend(generator.generate_circular_flow())
                
            for tx in transactions:
                # Use transaction_id as Kafka key to ensure ordering if needed
                producer.send_event('transactions', tx.to_dict(), key=tx.transaction_id)
                
            # Sleep for a short interval to simulate live data
            time.sleep(random.uniform(0.1, 1.0))
            
    except KeyboardInterrupt:
        logger.info("Stopping simulator...")
    finally:
        producer.close()
        logger.info("Simulator shutdown complete.")

if __name__ == "__main__":
    main()
