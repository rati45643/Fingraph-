import os
import json
import logging
from typing import Iterator, Dict, Any, Optional
from kafka import KafkaConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Flink-KafkaSource")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

class FlinkKafkaSource:
    """
    Day 2: Kafka Source Connector for Flink Stream Ingestion.
    Subscribes to transactions topic, reads raw JSON records, and emits them to the stream pipeline.
    """

    def __init__(self, topic: str = "transactions", bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, group_id: str = "flink-stream-group"):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.consumer: Optional[KafkaConsumer] = None

    def connect(self, auto_offset_reset: str = "earliest", timeout_ms: int = 2000):
        """Initializes connection to Kafka cluster."""
        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                group_id=self.group_id,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                consumer_timeout_ms=timeout_ms
            )
            logger.info(f"Connected to Kafka topic '{self.topic}' at {self.bootstrap_servers}")
        except Exception as e:
            logger.warning(f"Kafka connection failed: {e}")
            self.consumer = None

    def poll_events(self, max_records: int = 100) -> Iterator[Dict[str, Any]]:
        """Polls a batch of transaction events from Kafka."""
        if not self.consumer:
            self.connect()
        if not self.consumer:
            return

        try:
            records = self.consumer.poll(timeout_ms=1500, max_records=max_records)
            for tp, messages in records.items():
                for msg in messages:
                    if isinstance(msg.value, dict):
                        yield msg.value
        except Exception as e:
            logger.error(f"Error reading from Kafka topic '{self.topic}': {e}")

    def close(self):
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed.")
