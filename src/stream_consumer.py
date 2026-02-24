"""
Stream Layer (Consumer) of the Lambda Architecture.

Consumes credit-card transactions from Kafka in real-time, validates each
transaction against fraud-detection rules (amount limit, location proximity,
balance cap), and writes the results to results/stream_transactions.csv.
"""

import json
import csv
from pathlib import Path
from datetime import datetime
import sys
from kafka import KafkaConsumer
from collections import defaultdict, Counter

# Add the parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.models import Customer, Card, CardType, Transaction
from src.utils import is_location_close_enough
from src.data_loader import DataLoader


class StreamProcessor:
    """Validates individual transactions and tracks running pending balances."""

    def __init__(self, customers, cards):
        """Initialize with customer/card lookup dicts and set starting balances."""
        self.customers = customers
        self.cards = cards
        # Track running pending balance per card for real-time limit checks
        self.pending_balances = {
            card_id: card.current_balance for card_id, card in self.cards.items()
        }
        self.decline_reason_counts = Counter()

    def process_transaction(self, transaction):
        """Validate a single transaction; return 'pending' or 'declined'."""
        card_id = transaction.get("card_id")
        amount = float(transaction.get("amount", 0.0))
        merchant_location_str = transaction.get("location", "")
        transaction_id = transaction.get("transaction_id", "N/A")

        status = "pending"
        decline_reason = None

        card = self.cards.get(card_id)
        if not card or card.customer_id not in self.customers:
            status = "declined"
            decline_reason = f"Customer or Card {card_id} not found"
            print(
                f"[Consumer] DECLINED: Transaction {transaction_id} - {decline_reason}"
            )
            if decline_reason:
                self.decline_reason_counts[decline_reason] += 1
            return status

        customer = self.customers[card.customer_id]
        credit_limit = card.credit_limit
        current_pending_balance = self.pending_balances.get(
            card_id, card.current_balance
        )

        customer_zip = (
            customer.address.split(",")[-1].strip()[-5:] if customer.address else ""
        )
        merchant_zip = (
            merchant_location_str.split(",")[-1].strip()[-5:]
            if merchant_location_str
            else ""
        )

        if amount > 0:
            if amount >= 0.5 * credit_limit:
                status = "declined"
                decline_reason = "Amount exceeds 50% of credit limit"
            elif not is_location_close_enough(customer_zip, merchant_zip):
                status = "declined"
                decline_reason = "Merchant location too far from customer address"
            elif (current_pending_balance + amount) > credit_limit:
                status = "declined"
                decline_reason = "Transaction would exceed credit limit"

        if status == "pending":
            self.pending_balances[card_id] = current_pending_balance + amount
        elif status == "declined":
            print(
                f"[Consumer] DECLINED: Transaction {transaction_id} - {decline_reason}"
            )
            print(
                f"  Card: {card_id}, Amount: ${amount:.2f}, Customer: {customer.name}"
            )
            if decline_reason:
                self.decline_reason_counts[decline_reason] += 1

        return status


def main():
    """Set up Kafka consumer, process all streamed transactions, and save results."""
    print("[Consumer] Starting transaction stream consumer...")

    data_dir = Path("dataset")
    loader = DataLoader(data_dir, use_db=True)

    customers = loader.load_customers()
    cards = loader.load_cards()
    card_types = loader.load_card_types()
    loader.close()

    processor = StreamProcessor(customers, cards)
    processed_transactions = []

    consumer = KafkaConsumer(
        "transactions_stream",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id="transaction_processor",
        consumer_timeout_ms=5000,
    )

    print("[Consumer] Waiting for transactions...")

    try:
        for msg in consumer:
            transaction = msg.value
            status = processor.process_transaction(transaction)

            processed_tx = transaction.copy()
            processed_tx["status"] = status
            processed_transactions.append(processed_tx)

            if len(processed_transactions) % 100 == 0:
                print(
                    f"[Consumer] Processed {len(processed_transactions)} transactions so far"
                )
    except StopIteration:
        print("[Consumer] No more messages received from Kafka. Finishing up...")

    results_dir = Path("results")
    results_dir.mkdir(exist_ok=True)

    output_file = results_dir / "stream_transactions.csv"
    with open(output_file, "w", newline="") as f:
        if processed_transactions:
            fieldnames = set()
            for tx in processed_transactions:
                fieldnames.update(tx.keys())
            ordered_fieldnames = [
                "transaction_id",
                "card_id",
                "merchant_name",
                "amount",
                "location",
                "timestamp",
                "transaction_type",
                "related_transaction_id",
                "status",
            ]
            final_fieldnames = ordered_fieldnames + sorted(
                list(fieldnames - set(ordered_fieldnames))
            )

            writer = csv.DictWriter(
                f, fieldnames=final_fieldnames, extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(processed_transactions)

    print(f"[Consumer] Completed processing {len(processed_transactions)} transactions")
    print(f"[Consumer] Results saved to '{output_file}'")

    print("\n[Consumer] Summary of Declined Transactions:")
    if processor.decline_reason_counts:
        for reason, count in processor.decline_reason_counts.items():
            print(f"  - {reason}: {count} transactions")
    else:
        print("  - No transactions were declined.")

    print(f"\n[Consumer] Final pending balances:")
    for card_id, balance in processor.pending_balances.items():
        card = cards[card_id]
        print(f"  Card {card_id}: ${balance:.2f} / ${card.credit_limit:.2f} limit")


if __name__ == "__main__":
    main()
