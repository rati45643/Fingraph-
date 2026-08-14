import uuid
import time
from typing import Dict, Any

class Person:
    def __init__(self, person_id: str, name: str):
        self.person_id = person_id
        self.name = name

    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_type": "Person",
            "person_id": self.person_id,
            "name": self.name
        }

class Bank:
    def __init__(self, bank_id: str, name: str):
        self.bank_id = bank_id
        self.name = name
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_type": "Bank",
            "bank_id": self.bank_id,
            "name": self.name
        }

class Account:
    def __init__(self, account_id: str, account_type: str, owner_id: str, bank_id: str):
        self.account_id = account_id
        self.account_type = account_type
        self.owner_id = owner_id
        self.bank_id = bank_id
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_type": "Account",
            "account_id": self.account_id,
            "account_type": self.account_type,
            "owner_id": self.owner_id,
            "bank_id": self.bank_id
        }

class Transaction:
    def __init__(self, amount: float, source_account: str, dest_account: str, is_suspicious: bool = False):
        self.transaction_id = str(uuid.uuid4())
        self.amount = round(amount, 2)
        # Using current time in milliseconds
        self.timestamp = int(time.time() * 1000)
        self.source_account = source_account
        self.dest_account = dest_account
        self.is_suspicious = is_suspicious
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "entity_type": "Transaction",
            "transaction_id": self.transaction_id,
            "amount": self.amount,
            "timestamp": self.timestamp,
            "source_account": self.source_account,
            "dest_account": self.dest_account,
            "is_suspicious": self.is_suspicious
        }
