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

        # 1. Resolve source and destination account fields with alias support
        src = event.get("source_account_id") if event.get("source_account_id") is not None else event.get("source_account")
        dst = event.get("destination_account_id") if event.get("destination_account_id") is not None else (event.get("dest_account") or event.get("destination_account"))

        # Check mandatory fields
        if "transaction_id" not in event or event["transaction_id"] is None:
            reason = "Missing mandatory field: 'transaction_id'"
            self._route_to_dlq(event, reason)
            return False, None, reason

        if src is None:
            reason = "Missing mandatory field: 'source_account_id'"
            self._route_to_dlq(event, reason)
            return False, None, reason

        if dst is None:
            reason = "Missing mandatory field: 'destination_account_id'"
            self._route_to_dlq(event, reason)
            return False, None, reason

        if "amount" not in event or event["amount"] is None:
            reason = "Missing mandatory field: 'amount'"
            self._route_to_dlq(event, reason)
            return False, None, reason

        if "timestamp" not in event or event["timestamp"] is None:
            reason = "Missing mandatory field: 'timestamp'"
            self._route_to_dlq(event, reason)
            return False, None, reason

        tx_id = str(event["transaction_id"]).strip()
        src_id = str(src).strip()
        dst_id = str(dst).strip()

        if not tx_id or not src_id or not dst_id:
            reason = "IDs cannot be empty strings."
            self._route_to_dlq(event, reason)
            return False, None, reason

        if src_id == dst_id:
            reason = f"Self-transfer detected (source == destination: '{src_id}')."
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
            "source_account_id": src_id,
            "destination_account_id": dst_id,
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
