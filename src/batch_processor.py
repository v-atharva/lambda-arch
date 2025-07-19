import csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import sys

# Add the parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data_loader import DataLoader

# --- Credit Score and Limit Calculation Functions ---


def calculate_credit_score_adjustment(usage_percentage: float) -> int:
    """
    Calculate credit score adjustment based on credit usage percentage.

    Args:
        usage_percentage: Credit usage as a percentage of total available credit (0-100)

    Returns:
        int: Credit score adjustment (positive or negative)
    """
    # Credit utilization best practices suggest keeping usage below 30%
    if usage_percentage <= 10:
        # Excellent utilization: significant score improvement
        return 15
    elif usage_percentage <= 20:
        # Very good utilization
        return 10
    elif usage_percentage <= 30:
        # Good utilization
        return 5
    elif usage_percentage <= 50:
        # Fair utilization: small penalty
        return -5
    elif usage_percentage <= 70:
        # High utilization: moderate penalty
        return -15
    else:
        # Very high utilization: significant penalty
        return -25


def calculate_new_credit_limit(old_limit: float, credit_score_change: int) -> float:
    """
    Calculate new credit limit based on credit score changes.

    Args:
        old_limit: Current credit limit
        credit_score_change: Amount the credit score changed

    Returns:
        float: New credit limit
    """
    # Only reduce limits when scores drop
    if credit_score_change >= 0:
        return old_limit

    # Calculate percentage reduction based on score drop
    if credit_score_change <= -20:
        # Significant drop: reduce by 15%
        reduction_factor = 0.85
    elif credit_score_change <= -10:
        # Moderate drop: reduce by 10%
        reduction_factor = 0.90
    else:
        # Small drop: reduce by 5%
        reduction_factor = 0.95

    # Ensure limit doesn't go below a minimum threshold (e.g., 100 or original min)
    new_limit = round(old_limit * reduction_factor, -2)  # Round to nearest 100
    return max(new_limit, 100.0)  # Example minimum limit


# --- Main Batch Processing Logic ---


def main():
    print("[Batch] Starting batch processing...")

    # Define paths
    base_dir = Path(__file__).resolve().parent.parent
    data_dir = base_dir / "dataset"
    results_dir = base_dir / "results"
    results_dir.mkdir(exist_ok=True)  # Ensure results directory exists

    stream_transactions_file = results_dir / "stream_transactions.csv"
    batch_transactions_file = results_dir / "batch_transactions.csv"
    cards_updated_file = results_dir / "cards_updated.csv"
    customers_updated_file = results_dir / "customers_updated.csv"

    # 1. Load initial data from database
    print("[Batch] Loading initial data from database...")
    loader = DataLoader(data_dir, use_db=True)
    initial_customers = loader.load_customers()
    initial_cards = loader.load_cards()

    # Close database connection after loading data
    loader.close()

    # Create copies to modify
    updated_cards = {cid: card for cid, card in initial_cards.items()}
    updated_customers = {cid: cust for cid, cust in initial_customers.items()}

    # 2. Process Stream Transactions and Update Balances
    print(f"[Batch] Processing transactions from {stream_transactions_file}...")
    processed_batch_transactions = []
    try:
        with open(stream_transactions_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                original_status = row.get("status", "pending")  # Store original status
                card_id = int(row["card_id"])
                amount = float(row["amount"])
                transaction_id = row.get(
                    "transaction_id", "N/A"
                )  # Get transaction ID for logging

                # Approve pending transactions
                if original_status == "pending":
                    row["status"] = "approved"
                    print(
                        f"[Batch][Log] Transaction {transaction_id}: Status changed from '{original_status}' to 'approved'"
                    )
                    status = "approved"  # Update local status variable
                else:
                    status = original_status  # Keep original status if not pending

                # Update balance for approved transactions
                if status == "approved" and card_id in updated_cards:
                    old_balance = updated_cards[card_id].current_balance
                    # Ensure balance update reflects the transaction amount correctly
                    updated_cards[card_id].current_balance += amount
                    new_balance = updated_cards[card_id].current_balance
                    print(
                        f"[Batch][Log] Card {card_id}: Balance updated from ${old_balance:.2f} to ${new_balance:.2f} (Transaction {transaction_id}, Amount: ${amount:.2f})"
                    )

                processed_batch_transactions.append(row)
    except FileNotFoundError:
        print(
            f"[Batch] Error: Stream transactions file not found at {stream_transactions_file}"
        )
        return
    except Exception as e:
        print(f"[Batch] Error reading stream transactions: {e}")
        return

    print(f"[Batch] Processed {len(processed_batch_transactions)} transactions.")

    # 3. Calculate Credit Score Adjustments and Update Customer Scores
    print("[Batch] Calculating credit score adjustments...")
    customer_cards = defaultdict(list)
    for card in updated_cards.values():
        customer_cards[card.customer_id].append(card)

    customer_score_changes = {}
    for customer_id, cards_list in customer_cards.items():
        if customer_id not in updated_customers:
            continue  # Skip if customer data is missing

        total_balance = sum(c.current_balance for c in cards_list)
        total_limit = sum(c.credit_limit for c in cards_list)

        usage_percentage = 0.0
        if total_limit > 0:
            # Ensure balance isn't negative for usage calculation (can happen with refunds)
            usage_percentage = (max(0, total_balance) / total_limit) * 100
        else:
            # Handle case with zero total limit (e.g., only inactive cards)
            # Assign a high usage if balance is positive, otherwise low/zero
            usage_percentage = 100.0 if total_balance > 0 else 0.0

        score_adjustment = calculate_credit_score_adjustment(usage_percentage)
        customer_score_changes[customer_id] = score_adjustment

        # Update customer's credit score
        old_score = updated_customers[customer_id].credit_score
        updated_customers[customer_id].credit_score += score_adjustment
        # Ensure score doesn't go below a minimum (e.g., 300) or above max (e.g., 850)
        updated_customers[customer_id].credit_score = max(
            300, min(updated_customers[customer_id].credit_score, 850)
        )
        new_score = updated_customers[customer_id].credit_score
        if old_score != new_score:  # Log only if score actually changed
            print(
                f"[Batch][Log] Customer {customer_id}: Credit score updated from {old_score} to {new_score} (Adjustment: {score_adjustment})"
            )

    # 4. Update Card Credit Limits
    print("[Batch] Updating card credit limits based on score changes...")
    for card_id, card in updated_cards.items():
        customer_id = card.customer_id
        if customer_id in customer_score_changes:
            score_change = customer_score_changes[customer_id]
            old_limit = card.credit_limit
            new_limit = calculate_new_credit_limit(old_limit, score_change)
            if old_limit != new_limit:  # Log only if limit actually changed
                updated_cards[card_id].credit_limit = new_limit
                print(
                    f"[Batch][Log] Card {card_id}: Credit limit updated from ${old_limit:.2f} to ${new_limit:.2f} (Score Change: {score_change})"
                )

    # 5. Save Results
    print("[Batch] Saving updated data...")

    # Save Batch Transactions
    if processed_batch_transactions:
        # Use the header from the first transaction row
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
            for card in updated_cards.values():
                # Format issue_date back to string if needed, or keep as datetime object if loader handles it
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
            for customer in updated_customers.values():
                writer.writerow(
                    [
                        customer.customer_id,
                        customer.name,
                        customer.phone_number,
                        customer.address,
                        customer.email,
                        customer.credit_score,
                        f"{customer.annual_income:.1f}",  # Format income
                    ]
                )
        print(f"[Batch] Saved updated customers to {customers_updated_file}")
    except Exception as e:
        print(f"[Batch] Error writing updated customers: {e}")

    print("[Batch] Batch processing completed.")


if __name__ == "__main__":
    # Make sure your DataLoader and models are correctly imported
    # Example: from src.data_loader import DataLoader
    # Example: from src.models import Customer, Card
    main()
