import json
import logging
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting test consumer...")
    
    try:
        consumer = KafkaConsumer(
            'transactions',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='test-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        
        logger.info("Connected to Kafka. Waiting for messages on topic 'transactions'...")
        
        for message in consumer:
            tx = message.value
            # Verify basic structure
            if 'transaction_id' in tx and 'timestamp' in tx:
                logger.info(f"Received valid transaction: {tx['transaction_id']} | Amount: {tx['amount']} | Suspicious: {tx['is_suspicious']}")
            else:
                logger.error(f"Received invalid format: {tx}")
                
    except Exception as e:
        logger.error(f"Failed to consume: {e}")

if __name__ == "__main__":
    main()
