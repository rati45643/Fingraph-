import json
import logging
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EventProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info(f"Connected to Kafka at {bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.producer = None

    def send_event(self, topic: str, event_data: dict, key: str = None):
        if not self.producer:
            logger.warning("Kafka producer not initialized. Printing event instead.")
            print(f"Topic: {topic} | Key: {key} | Data: {event_data}")
            return

        try:
            future = self.producer.send(topic, key=key, value=event_data)
            # Add a callback to log success or failure
            future.add_callback(self._on_send_success)
            future.add_errback(self._on_send_error)
            
            # Flush periodically or let background thread handle it, we'll flush explicitly if needed
        except Exception as e:
            logger.error(f"Error sending message: {e}")

    def _on_send_success(self, record_metadata):
        pass # Optional: logger.debug(f"Message sent to {record_metadata.topic} partition {record_metadata.partition} offset {record_metadata.offset}")

    def _on_send_error(self, exccp):
        logger.error(f"Failed to send message: {exccp}")

    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
