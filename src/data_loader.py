import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from src.models import Customer, Card, CardType, Transaction
from src.db_manager import DBManager


class DataLoader:
    def __init__(self, data_dir: Path = Path("../data"), use_db: bool = True):
        self.data_dir = data_dir
        self.use_db = use_db
        self.db_manager = DBManager() if use_db else None

    def load_customers(self) -> Dict[int, Customer]:
        customers = {}

        if self.use_db:
            # Load from database
            self.db_manager.connect()
            query = "SELECT * FROM customers"
            results = self.db_manager.execute_query(query)

            for row in results:
                customer_id = int(row["customer_id"])
                customers[customer_id] = Customer(
                    customer_id=customer_id,
                    name=row["name"],
                    phone_number=row["phone_number"],
                    address=row["address"],
                    email=row["email"],
                    credit_score=int(row["credit_score"]),
                    annual_income=float(row["annual_income"]),
                )
        else:
            # Load from CSV (original implementation)
            with open(self.data_dir / "customers.csv", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    customer_id = int(row["customer_id"])
                    customers[customer_id] = Customer(
                        customer_id=customer_id,
                        name=row["name"],
                        phone_number=row["phone_number"],
                        address=row["address"],
                        email=row["email"],
                        credit_score=int(row["credit_score"]),
                        annual_income=float(row["annual_income"]),
                    )
        return customers

    def load_card_types(self) -> Dict[int, CardType]:
        card_types = {}

        if self.use_db:
            # Load from database
            if (
                not self.db_manager.connection
                or not self.db_manager.connection.is_connected()
            ):
                self.db_manager.connect()

            query = "SELECT * FROM credit_card_types"
            results = self.db_manager.execute_query(query)

            for row in results:
                type_id = int(row["card_type_id"])
                card_types[type_id] = CardType(
                    card_type_id=type_id,
                    name=row["name"],
                    credit_score_min=int(row["credit_score_min"]),
                    credit_score_max=int(row["credit_score_max"]),
                    credit_limit_min=int(row["credit_limit_min"]),
                    credit_limit_max=int(row["credit_limit_max"]),
                    annual_fee=int(row["annual_fee"]),
                    rewards_rate=float(row["rewards_rate"]),
                )
        else:
            # Load from CSV (original implementation)
            with open(self.data_dir / "credit_card_types.csv", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    type_id = int(row["card_type_id"])
                    card_types[type_id] = CardType(
                        card_type_id=type_id,
                        name=row["name"],
                        credit_score_min=int(row["credit_score_min"]),
                        credit_score_max=int(row["credit_score_max"]),
                        credit_limit_min=int(row["credit_limit_min"]),
                        credit_limit_max=int(row["credit_limit_max"]),
                        annual_fee=int(row["annual_fee"]),
                        rewards_rate=float(row["rewards_rate"]),
                    )
        return card_types

    def load_cards(self) -> Dict[int, Card]:
        cards = {}

        if self.use_db:
            # Load from database
            if (
                not self.db_manager.connection
                or not self.db_manager.connection.is_connected()
            ):
                self.db_manager.connect()

            query = "SELECT * FROM cards"
            results = self.db_manager.execute_query(query)

            for row in results:
                card_id = int(row["card_id"])
                # Handle issue_date which may be a datetime object from MySQL
                issue_date = row["issue_date"]
                # No need to convert if already a datetime object
                if not isinstance(issue_date, datetime) and issue_date:
                    issue_date = datetime.fromisoformat(str(issue_date))

                cards[card_id] = Card(
                    card_id=card_id,
                    customer_id=int(row["customer_id"]),
                    card_type_id=int(row["card_type_id"]),
                    card_number=row["card_number"],
                    expiration_date=row["expiration_date"],
                    credit_limit=float(row["credit_limit"]),
                    current_balance=float(row["current_balance"]),
                    issue_date=issue_date,
                )
        else:
            # Load from CSV (original implementation)
            with open(self.data_dir / "cards.csv", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    card_id = int(row["card_id"])
                    cards[card_id] = Card(
                        card_id=card_id,
                        customer_id=int(row["customer_id"]),
                        card_type_id=int(row["card_type_id"]),
                        card_number=row["card_number"],
                        expiration_date=row["expiration_date"],
                        credit_limit=float(row["credit_limit"]),
                        current_balance=float(row["current_balance"]),
                        issue_date=datetime.fromisoformat(row["issue_date"]),
                    )
        return cards

    def load_transactions(self) -> Dict[int, Transaction]:
        """Load transactions from database or CSV"""
        transactions = {}

        if self.use_db:
            # Load from database
            if (
                not self.db_manager.connection
                or not self.db_manager.connection.is_connected()
            ):
                self.db_manager.connect()

            query = "SELECT * FROM transactions"
            results = self.db_manager.execute_query(query)

            for row in results:
                transaction_id = int(row["transaction_id"])
                related_transaction_id = row["related_transaction_id"]
                if related_transaction_id and str(related_transaction_id).strip():
                    related_transaction_id = int(related_transaction_id)
                else:
                    related_transaction_id = None

                # Handle timestamp which may be a datetime object from MySQL
                timestamp = row["timestamp"]
                # No need to convert if already a datetime object
                if not isinstance(timestamp, datetime) and timestamp:
                    timestamp = datetime.fromisoformat(str(timestamp))

                transactions[transaction_id] = Transaction(
                    transaction_id=transaction_id,
                    card_id=int(row["card_id"]),
                    merchant_name=row["merchant_name"],
                    timestamp=timestamp,
                    amount=float(row["amount"]),
                    location=row["location"],
                    transaction_type=row["transaction_type"],
                    related_transaction_id=related_transaction_id,
                )
        else:
            # Load from CSV (original implementation)
            try:
                with open(self.data_dir / "transactions.csv", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        transaction_id = int(row["transaction_id"])
                        related_transaction_id = row["related_transaction_id"]
                        if (
                            related_transaction_id
                            and str(related_transaction_id).strip()
                        ):
                            related_transaction_id = int(related_transaction_id)
                        else:
                            related_transaction_id = None

                        transactions[transaction_id] = Transaction(
                            transaction_id=transaction_id,
                            card_id=int(row["card_id"]),
                            merchant_name=row["merchant_name"],
                            timestamp=datetime.fromisoformat(row["timestamp"]),
                            amount=float(row["amount"]),
                            location=row["location"],
                            transaction_type=row["transaction_type"],
                            related_transaction_id=related_transaction_id,
                        )
            except FileNotFoundError:
                print(
                    f"[DataLoader] Warning: transactions.csv not found in {self.data_dir}"
                )

        return transactions

    def close(self):
        """Close database connection if open"""
        if self.use_db and self.db_manager:
            self.db_manager.disconnect()
