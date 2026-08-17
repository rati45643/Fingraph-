import os
import sys
import time
import logging
from typing import List, Dict, Any, Callable, Optional, Iterator

# Ensure flink_processor directory is on python search path
sys.path.insert(0, os.path.dirname(__file__))

from kafka_source import FlinkKafkaSource
from stream_validator import StreamValidator
from neo4j_sink import FlinkNeo4jSink

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Flink-StreamJob")

class FlinkStreamPipeline:
    """
    Day 1: Flink Stream Processing Job Skeleton & Execution Pipeline.
    Encapsulates Source -> Validate/DLQ -> Micro-Batch Window -> Neo4j Sink flow.
    """

    def __init__(
        self,
        source: Optional[FlinkKafkaSource] = None,
        validator: Optional[StreamValidator] = None,
        sink: Optional[FlinkNeo4jSink] = None,
        window_size: int = 50,
        window_time_ms: int = 1000
    ):
        self.source = source or FlinkKafkaSource()
        self.validator = validator or StreamValidator()
        self.sink = sink or FlinkNeo4jSink()
        self.window_size = window_size
        self.window_time_ms = window_time_ms
        self.processed_count = 0
        self.rejected_count = 0
        self.is_running = False

    def process_event_stream(self, stream_iterator: Iterator[Dict[str, Any]]) -> Dict[str, int]:
        """
        Executes the Flink streaming transformation pipeline over an event stream.
        """
        window_batch: List[Dict[str, Any]] = []
        last_flush_time = time.time() * 1000

        for raw_event in stream_iterator:
            is_valid, clean_event, reason = self.validator.validate_and_normalize(raw_event)
            if is_valid and clean_event:
                window_batch.append(clean_event)
                self.processed_count += 1
            else:
                self.rejected_count += 1

            now_ms = time.time() * 1000
            # Trigger micro-batch window flush if size or time threshold met
            if len(window_batch) >= self.window_size or (now_ms - last_flush_time) >= self.window_time_ms:
                if window_batch:
                    self.sink.upsert_transaction_batch(window_batch)
                    window_batch.clear()
                    last_flush_time = now_ms

        # Flush remaining events in window
        if window_batch:
            self.sink.upsert_transaction_batch(window_batch)
            window_batch.clear()

        return {
            "processed_valid": self.processed_count,
            "rejected_dlq": self.rejected_count,
            "dlq_total": len(self.validator.get_dlq_records())
        }

    def run_live_kafka_job(self, max_batches: int = 10):
        """Runs the live Flink stream processor consuming from Kafka."""
        logger.info("Starting live Flink Stream Processing Job...")
        self.is_running = True
        try:
            for batch_idx in range(max_batches):
                events = list(self.source.poll_events(max_records=self.window_size))
                if events:
                    self.process_event_stream(iter(events))
                    logger.info(f"Processed batch #{batch_idx + 1}: {len(events)} events.")
                time.sleep(0.5)
        finally:
            self.close()

    def close(self):
        self.is_running = False
        self.source.close()
        self.sink.close()

if __name__ == "__main__":
    pipeline = FlinkStreamPipeline()
    try:
        pipeline.run_live_kafka_job(max_batches=5)
    except KeyboardInterrupt:
        logger.info("Flink job stopped by user.")
    finally:
        pipeline.close()
