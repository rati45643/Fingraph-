import time
import logging
from typing import Dict, Any, Tuple, Optional, List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Flink-Validator")

class StreamValidator:
    """
    Day 3: Stream Validation, Normalization & Dead-Letter Queue (DLQ) Routing.
    Validates mandatory schema fields, normalizes timestamps/amounts, and discards/routes malformed events.
    """

    MANDATORY_FIELDS = ["transaction_id", "source_account_id", "destination_account_id", "amount", "timestamp"]

    def __init__(self):
        self.dlq: List[Dict[str, Any]] = []

    def validate_and_normalize(self, event: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
        """
        Validates event structure and normalizes fields.
        Returns (is_valid, normalized_event, error_reason).
        """
        if not isinstance(event, dict):
            reason = "Event payload is not a valid JSON dictionary."
            self._route_to_dlq(event, reason)
            return False, None, reason

        # 1. Check mandatory fields
        for field in self.MANDATORY_FIELDS:
            if field not in event or event[field] is None:
                reason = f"Missing mandatory field: '{field}'"
                self._route_to_dlq(event, reason)
                return False, None, reason

        tx_id = str(event["transaction_id"]).strip()
        src = str(event["source_account_id"]).strip()
        dst = str(event["destination_account_id"]).strip()

        if not tx_id or not src or not dst:
            reason = "IDs cannot be empty strings."
            self._route_to_dlq(event, reason)
            return False, None, reason

        if src == dst:
            reason = f"Self-transfer detected (source == destination: '{src}')."
            self._route_to_dlq(event, reason)
            return False, None, reason

        # 2. Validate and normalize amount
        try:
            amount = float(event["amount"])
            if amount <= 0:
                reason = f"Invalid transaction amount: {amount} (must be > 0)."
                self._route_to_dlq(event, reason)
                return False, None, reason
            amount_norm = round(amount, 2)
        except (ValueError, TypeError):
            reason = f"Unparseable numeric amount: '{event.get('amount')}'"
            self._route_to_dlq(event, reason)
            return False, None, reason

        # 3. Validate and normalize timestamp
        try:
            ts = event["timestamp"]
            if isinstance(ts, (int, float)):
                ts_norm = int(ts)
            elif isinstance(ts, str):
                ts_norm = int(float(ts))
            else:
                ts_norm = int(time.time() * 1000)
        except Exception:
            ts_norm = int(time.time() * 1000)

        # Build clean normalized record
        normalized = {
            "transaction_id": tx_id,
            "source_account_id": src,
            "destination_account_id": dst,
            "amount": amount_norm,
            "timestamp": ts_norm,
            "is_suspicious": bool(event.get("is_suspicious", False))
        }

        return True, normalized, None

    def _route_to_dlq(self, raw_event: Any, reason: str):
        """Routes a rejected event to the Dead-Letter Queue."""
        dlq_entry = {
            "raw_event": raw_event,
            "rejection_reason": reason,
            "rejected_at": int(time.time() * 1000)
        }
        self.dlq.append(dlq_entry)
        logger.warning(f"Event rejected and routed to DLQ: {reason}")

    def get_dlq_records(self) -> List[Dict[str, Any]]:
        return list(self.dlq)

    def clear_dlq(self):
        self.dlq.clear()
