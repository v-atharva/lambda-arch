"""
Batch Layer of the Lambda Architecture.

Reads the stream-processed transactions, approves pending ones, updates
card balances, recalculates customer credit scores based on utilization,
and reduces credit limits when scores drop.
Outputs: batch_transactions.csv, cards_updated.csv, customers_updated.csv
"""

import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import sys

# Add the parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data_loader import DataLoader


class BatchProcessor:
    """Handles all batch-layer processing: approval, scoring, and limit adjustments."""

    def __init__(self, customers, cards):
        """Initialize with copies of customer and card data for safe mutation."""
        self.customers = {cid: cust for cid, cust in customers.items()}
        self.cards = {cid: card for cid, card in cards.items()}
        self.customer_score_changes = {}

    def calculate_credit_score_adjustment(self, usage_percentage: float) -> int:
        """Return a score adjustment (+/-) based on credit utilization brackets."""
        if usage_percentage <= 10:
            return 15
        elif usage_percentage <= 20:
            return 10
        elif usage_percentage <= 30:
            return 5
        elif usage_percentage <= 50:
            return -5
        elif usage_percentage <= 70:
            return -15
        else:
            return -25

    def calculate_new_credit_limit(
        self, old_limit: float, credit_score_change: int
    ) -> float:
        """Reduce the credit limit proportionally when the score drops."""
        if credit_score_change >= 0:
            return old_limit
        if credit_score_change <= -20:
            reduction_factor = 0.85
        elif credit_score_change <= -10:
            reduction_factor = 0.90
        else:
            reduction_factor = 0.95
        new_limit = round(old_limit * reduction_factor, -2)
        return max(new_limit, 100.0)

    def process_transaction_queue(self, stream_file_path):
        """Read stream CSV, approve pending transactions, and update card balances."""
        processed_transactions = []
        try:
            with open(stream_file_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    original_status = row.get("status", "pending")
                    card_id = int(row["card_id"])
                    amount = float(row["amount"])
                    transaction_id = row.get("transaction_id", "N/A")

                    if original_status == "pending":
                        row["status"] = "approved"
                        print(
                            f"[Batch][Log] Transaction {transaction_id}: Status changed from '{original_status}' to 'approved'"
                        )
                        status = "approved"
                    else:
                        status = original_status

                    if status == "approved" and card_id in self.cards:
                        old_balance = self.cards[card_id].current_balance
                        self.cards[card_id].current_balance += amount
                        new_balance = self.cards[card_id].current_balance
                        print(
                            f"[Batch][Log] Card {card_id}: Balance updated from ${old_balance:.2f} to ${new_balance:.2f} (Transaction {transaction_id}, Amount: ${amount:.2f})"
                        )

                    processed_transactions.append(row)
            return processed_transactions
        except Exception as e:
            print(f"[Batch] Error reading stream transactions: {e}")
            return []

    def perform_balance_and_score_adjustments(self):
        """Calculate credit utilization per customer and adjust their scores."""
        customer_cards = defaultdict(list)
        for card in self.cards.values():
            customer_cards[card.customer_id].append(card)

        for customer_id, cards_list in customer_cards.items():
            if customer_id not in self.customers:
                continue

            total_balance = sum(c.current_balance for c in cards_list)
            total_limit = sum(c.credit_limit for c in cards_list)

            usage_percentage = (
                (max(0, total_balance) / total_limit) * 100
                if total_limit > 0
                else (100.0 if total_balance > 0 else 0.0)
            )

            score_adjustment = self.calculate_credit_score_adjustment(usage_percentage)
            self.customer_score_changes[customer_id] = score_adjustment

            old_score = self.customers[customer_id].credit_score
            self.customers[customer_id].credit_score = max(
                300, min(old_score + score_adjustment, 850)
            )
            new_score = self.customers[customer_id].credit_score

            if old_score != new_score:
                print(
                    f"[Batch][Log] Customer {customer_id}: Credit score updated from {old_score} to {new_score} (Adjustment: {score_adjustment})"
                )

    def perform_credit_limit_adjustments(self):
        """Lower card credit limits for customers whose scores dropped."""
        for card_id, card in self.cards.items():
            customer_id = card.customer_id
            if customer_id in self.customer_score_changes:
                score_change = self.customer_score_changes[customer_id]
                old_limit = card.credit_limit
                new_limit = self.calculate_new_credit_limit(old_limit, score_change)
                if old_limit != new_limit:
                    self.cards[card_id].credit_limit = new_limit
                    print(
                        f"[Batch][Log] Card {card_id}: Credit limit updated from ${old_limit:.2f} to ${new_limit:.2f} (Score Change: {score_change})"
                    )


def main():
    """Orchestrate the full batch processing pipeline."""
    print("[Batch] Starting batch processing...")

    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "dataset"
    results_dir = base_dir / "results"
    results_dir.mkdir(exist_ok=True)

    stream_transactions_file = results_dir / "stream_transactions.csv"
    batch_transactions_file = results_dir / "batch_transactions.csv"
    cards_updated_file = results_dir / "cards_updated.csv"
    customers_updated_file = results_dir / "customers_updated.csv"

    print("[Batch] Loading initial data from database...")
    loader = DataLoader(data_dir, use_db=True)
    initial_customers = loader.load_customers()
    initial_cards = loader.load_cards()
    loader.close()

    processor = BatchProcessor(initial_customers, initial_cards)

    print(f"[Batch] Processing transactions from {stream_transactions_file}...")
    processed_batch_transactions = processor.process_transaction_queue(
        stream_transactions_file
    )
    print(f"[Batch] Processed {len(processed_batch_transactions)} transactions.")

    print("[Batch] Calculating credit score adjustments...")
    processor.perform_balance_and_score_adjustments()

    print("[Batch] Updating card credit limits based on score changes...")
    processor.perform_credit_limit_adjustments()

    print("[Batch] Saving updated data...")

    # Save Batch Transactions
    if processed_batch_transactions:
        fieldnames = list(processed_batch_transactions[0].keys())
        try:
            with open(batch_transactions_file, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(processed_batch_transactions)
            print(f"[Batch] Saved batch transactions to {batch_transactions_file}")
        except Exception as e:
            print(f"[Batch] Error writing batch transactions: {e}")
    else:
        print("[Batch] No batch transactions to save.")

    # Save Updated Cards
    card_fieldnames = [
        "card_id",
        "customer_id",
        "card_type_id",
        "card_number",
        "expiration_date",
        "credit_limit",
        "current_balance",
        "issue_date",
    ]
    try:
        with open(cards_updated_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(card_fieldnames)
            for card in processor.cards.values():
                issue_date_str = (
                    card.issue_date.isoformat()
                    if isinstance(card.issue_date, datetime)
                    else card.issue_date
                )
                writer.writerow(
                    [
                        card.card_id,
                        card.customer_id,
                        card.card_type_id,
                        card.card_number,
                        card.expiration_date,
                        f"{card.credit_limit:.2f}",
                        f"{card.current_balance:.2f}",
                        issue_date_str,
                    ]
                )
        print(f"[Batch] Saved updated cards to {cards_updated_file}")
    except Exception as e:
        print(f"[Batch] Error writing updated cards: {e}")

    # Save Updated Customers
    customer_fieldnames = [
        "customer_id",
        "name",
        "phone_number",
        "address",
        "email",
        "credit_score",
        "annual_income",
    ]
    try:
        with open(customers_updated_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(customer_fieldnames)
            for customer in processor.customers.values():
                writer.writerow(
                    [
                        customer.customer_id,
                        customer.name,
                        customer.phone_number,
                        customer.address,
                        customer.email,
                        customer.credit_score,
                        f"{customer.annual_income:.1f}",
                    ]
                )
        print(f"[Batch] Saved updated customers to {customers_updated_file}")
    except Exception as e:
        print(f"[Batch] Error writing updated customers: {e}")

    print("[Batch] Batch processing completed.")


if __name__ == "__main__":
    main()
