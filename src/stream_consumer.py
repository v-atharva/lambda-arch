# src/stream_consumer.py
import json
import csv
from pathlib import Path
from datetime import datetime
import sys
from kafka import KafkaConsumer
from collections import defaultdict, Counter  # Added Counter

# Add the parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.models import Customer, Card, CardType, Transaction
from src.utils import is_location_close_enough
from src.data_loader import DataLoader


def main():
    print("[Consumer] Starting transaction stream consumer...")

    # 1. Load data from database
    data_dir = Path("dataset")  # Keep this for backwards compatibility
    loader = DataLoader(data_dir, use_db=True)  # Set use_db=True to use database

    customers = loader.load_customers()
    cards = loader.load_cards()
    card_types = loader.load_card_types()

    # Close database connection after loading data
    loader.close()

    # 2. Initialize tracking variables
    pending_balances = {
        card_id: card.current_balance for card_id, card in cards.items()
    }
    processed_transactions = []
    decline_reason_counts = Counter()  # Initialize a counter for decline reasons

    # 3. Configure Kafka consumer
    consumer = KafkaConsumer(
        "transactions_stream",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode()),
        group_id="transaction_processor",
        consumer_timeout_ms=5000,  # Add timeout (5000ms = 5 seconds)
    )

    # 4. Process transactions
    print("[Consumer] Waiting for transactions...")
    current_date = None

    try:  # Wrap the loop in a try block
        for msg in consumer:  # This loop will now raise StopIteration on timeout
            transaction = msg.value

            # Convert timestamp string to datetime for processing
            timestamp = datetime.fromisoformat(transaction["timestamp"])
            transaction_date = timestamp.date()

            # --- Start Transaction Processing ---
            card_id = transaction.get("card_id")
            amount = float(transaction.get("amount", 0.0))  # Ensure amount is flo
            merchant_location_str = transaction.get("location", "")
            transaction_id = transaction.get("transaction_id", "N/A")

            status = "pending"  # Default status
            decline_reason = None

            # Basic data checks
            card = cards[card_id]
            if card.customer_id not in customers:
                status = "declined"
                decline_reason = (
                    f"Customer {card.customer_id} for card {card_id} not found"
                )
                print(
                    f"[Consumer] DECLINED: Transaction {transaction_id} - {decline_reason}"
                )
                if decline_reason:
                    decline_reason_counts[decline_reason] += 1
            else:
                customer = customers[card.customer_id]
                credit_limit = card.credit_limit
                # Use the tracked pending balance for checks
                current_pending_balance = pending_balances.get(
                    card_id, card.current_balance
                )
                # Extract zip codes for location check
                customer_zip = (
                    customer.address.split(",")[-1].strip()[-5:]
                    if customer.address
                    else ""
                )
                merchant_zip = (
                    merchant_location_str.split(",")[-1].strip()[-5:]
                    if merchant_location_str
                    else ""
                )
                # Apply validation rules only for purchases (amount > 0)
                if amount > 0:
                    # Rule 1: Amount >= 50% of credit limit
                    if amount >= 0.5 * credit_limit:
                        status = "declined"
                        decline_reason = "Amount exceeds 50% of credit limit"
                    # Rule 2: Location check
                    elif not is_location_close_enough(customer_zip, merchant_zip):
                        status = "declined"
                        decline_reason = (
                            "Merchant location too far from customer address"
                        )
                        # Optional: Add debug prints if needed
                        print(
                            f"[•••DEBUG•••] Location mismatch: Cust={customer_zip}, Merch={merchant_zip}"
                        )
                    # Rule 3: Exceeds credit limit check
                    elif (current_pending_balance + amount) > credit_limit:
                        status = "declined"
                        decline_reason = "Transaction would exceed credit limit"
                # Update pending balance if transaction is not declined
                if status == "pending":
                    pending_balances[card_id] = current_pending_balance + amount
                # Log declined transactions and count reasons
                elif status == "declined":
                    print(
                        f"[Consumer] DECLINED: Transaction {transaction_id} - {decline_reason}"
                    )
                    print(
                        f"  Card: {card_id}, Amount: ${amount:.2f}, Customer: {customer.name}"
                    )
                    if decline_reason:  # Ensure reason exists before counting
                        decline_reason_counts[decline_reason] += 1

            # Store processed transaction with status
            processed_tx = transaction.copy()
            processed_tx["status"] = status
            # Optionally add decline reason to the output CSV
            # processed_tx["decline_reason"] = decline_reason if status == "declined" else ""
            processed_transactions.append(processed_tx)
            # --- End Refactored Transaction Processing ---

            # Check if we've processed all transactions (simple timeout check)
            if len(processed_transactions) % 100 == 0:
                print(
                    f"[Consumer] Processed {len(processed_transactions)} transactions so far"
                )
    except StopIteration:  # Catch the timeout exception
        print("[Consumer] No more messages received from Kafka. Finishing up...")

    # 5. Save processed transactions to CSV (This code is now reachable)
    results_dir = Path("results")  # Changed from ../results to results
    results_dir.mkdir(exist_ok=True)

    output_file = results_dir / "stream_transactions.csv"
    with open(output_file, "w", newline="") as f:
        if processed_transactions:
            # Ensure all potential keys are included in fieldnames
            fieldnames = set()
            for tx in processed_transactions:
                fieldnames.update(tx.keys())
            # Define a preferred order, adding status
            ordered_fieldnames = [
                "transaction_id",
                "card_id",
                "merchant_name",
                "amount",
                "location",
                "timestamp",
                "transaction_type",
                "related_transaction_id",
                "status",  # Added status
            ]
            # Add any extra fields found that weren't in the preferred list
            final_fieldnames = ordered_fieldnames + sorted(
                list(fieldnames - set(ordered_fieldnames))
            )

            writer = csv.DictWriter(
                f, fieldnames=final_fieldnames, extrasaction="ignore"
            )
            writer.writeheader()
            writer.writerows(processed_transactions)

    print(f"[Consumer] Completed processing {len(processed_transactions)} transactions")
    print(f"[Consumer] Results saved to '{output_file}'")  # Use variable

    # --- Added Decline Summary ---
    print("\n[Consumer] Summary of Declined Transactions:")
    if decline_reason_counts:
        for reason, count in decline_reason_counts.items():
            print(f"  - {reason}: {count} transactions")
    else:
        print("  - No transactions were declined.")
    # --- End Decline Summary ---

    print(f"\n[Consumer] Final pending balances:")  # Added newline for spacing

    for card_id, balance in pending_balances.items():
        card = cards[card_id]
        print(f"  Card {card_id}: ${balance:.2f} / ${card.credit_limit:.2f} limit")


if __name__ == "__main__":
    main()
