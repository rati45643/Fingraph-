import random
import uuid
from faker import Faker
from models import Person, Bank, Account, Transaction

fake = Faker()

class DataGenerator:
    def __init__(self, num_people=50, num_banks=3):
        self.num_people = num_people
        self.num_banks = num_banks
        
        self.people = []
        self.banks = []
        self.accounts = []
        
        self._initialize_static_data()
        
    def _initialize_static_data(self):
        # Create Banks
        for _ in range(self.num_banks):
            bank_id = f"BANK_{uuid.uuid4().hex[:8].upper()}"
            self.banks.append(Bank(bank_id, fake.company()))
            
        # Create People and Accounts
        for _ in range(self.num_people):
            person_id = f"P_{uuid.uuid4().hex[:8].upper()}"
            person = Person(person_id, fake.name())
            self.people.append(person)
            
            # Assign 1 to 3 accounts per person
            num_accounts = random.randint(1, 3)
            for _ in range(num_accounts):
                account_id = f"ACC_{uuid.uuid4().hex[:8].upper()}"
                bank = random.choice(self.banks)
                acc_type = random.choice(["Checking", "Savings"])
                self.accounts.append(Account(account_id, acc_type, person.person_id, bank.bank_id))
                
    def get_initial_entities(self):
        """Yields all static entities to populate the graph initially"""
        for bank in self.banks:
            yield bank
        for person in self.people:
            yield person
        for account in self.accounts:
            yield account
            
    def generate_ordinary_transaction(self):
        """Generates a normal transaction between two random accounts"""
        source = random.choice(self.accounts)
        dest = random.choice(self.accounts)
        while dest.account_id == source.account_id:
            dest = random.choice(self.accounts)
            
        amount = round(random.uniform(10.0, 1000.0), 2)
        return Transaction(amount, source.account_id, dest.account_id, is_suspicious=False)
        
    def generate_syndicate_funnel(self):
        """
        Creates a many-to-one funneling pattern (multiple sources to one destination).
        """
        num_sources = random.randint(3, 7)
        dest_account = random.choice(self.accounts)
        
        source_accounts = random.sample(self.accounts, num_sources)
        if dest_account in source_accounts:
            source_accounts.remove(dest_account)
            
        transactions = []
        for src in source_accounts:
            amount = round(random.uniform(5000.0, 9999.0), 2) # High amounts just under reporting threshold
            transactions.append(Transaction(amount, src.account_id, dest_account.account_id, is_suspicious=True))
            
        return transactions

    def generate_multi_hop_intermediary(self):
        """
        Creates a chain of transactions: A -> B -> C -> D
        """
        chain_length = random.randint(3, 5)
        accounts_chain = random.sample(self.accounts, chain_length)
        
        transactions = []
        base_amount = round(random.uniform(10000.0, 50000.0), 2)
        
        for i in range(chain_length - 1):
            # Amount decreases slightly at each hop (simulating fees/skimming)
            amount = base_amount * (1 - (i * 0.05))
            transactions.append(
                Transaction(amount, accounts_chain[i].account_id, accounts_chain[i+1].account_id, is_suspicious=True)
            )
            
        return transactions

    def generate_circular_flow(self):
        """
        Creates a circular path: A -> B -> C -> A
        """
        accounts_circle = random.sample(self.accounts, 3)
        transactions = []
        base_amount = round(random.uniform(5000.0, 20000.0), 2)
        
        # A -> B
        transactions.append(Transaction(base_amount, accounts_circle[0].account_id, accounts_circle[1].account_id, is_suspicious=True))
        # B -> C
        transactions.append(Transaction(base_amount, accounts_circle[1].account_id, accounts_circle[2].account_id, is_suspicious=True))
        # C -> A
        transactions.append(Transaction(base_amount, accounts_circle[2].account_id, accounts_circle[0].account_id, is_suspicious=True))
        
        return transactions
