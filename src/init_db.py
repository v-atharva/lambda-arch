import csv
from pathlib import Path
import sys

# Add the parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db_manager import DBManager
from datetime import datetime


def create_tables(db_manager):
    """Create database tables if they don't exist."""
    print("[DB Init] Creating database tables if needed...")

    # Create customers table
    db_manager.execute_query(
        """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id INT PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        phone_number VARCHAR(20),
        address VARCHAR(255),
        email VARCHAR(100),
        credit_score INT,
        annual_income DECIMAL(10,2)
    )
    """
    )

    # Create credit_card_types table
    db_manager.execute_query(
        """
    CREATE TABLE IF NOT EXISTS credit_card_types (
        card_type_id INT PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        credit_score_min INT,
        credit_score_max INT,
        credit_limit_min INT,
        credit_limit_max INT,
        annual_fee INT,
        rewards_rate DECIMAL(5,3)
    )
    """
    )

    # Create cards table
    db_manager.execute_query(
        """
    CREATE TABLE IF NOT EXISTS cards (
        card_id INT PRIMARY KEY,
        customer_id INT,
        card_type_id INT,
        card_number VARCHAR(20),
        expiration_date VARCHAR(5),
        credit_limit DECIMAL(10,2),
        current_balance DECIMAL(10,2),
        issue_date DATETIME,
        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
        FOREIGN KEY (card_type_id) REFERENCES credit_card_types(card_type_id)
    )
    """
    )

    # Create transactions table
    db_manager.execute_query(
        """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id INT PRIMARY KEY,
        card_id INT,
        merchant_name VARCHAR(100),
        timestamp DATETIME,
        amount DECIMAL(10,2),
        location VARCHAR(255),
        transaction_type VARCHAR(50),
        related_transaction_id INT,
        FOREIGN KEY (card_id) REFERENCES cards(card_id)
    )
    """
    )

    print("[DB Init] Tables created successfully.")


def load_customers_from_csv(db_manager, data_dir):
    """Load customers from CSV file into database."""
    print("[DB Init] Loading customers...")

    csv_file = data_dir / "customers.csv"
    if not csv_file.exists():
        print(f"[DB Init] Error: {csv_file} not found")
        return

    # Clear existing data
    db_manager.execute_query("DELETE FROM customers")

    # Load data from CSV
    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        insert_query = """
        INSERT INTO customers (customer_id, name, phone_number, address, email, credit_score, annual_income)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        values = []
        for row in reader:
            values.append(
                (
                    int(row["customer_id"]),
                    row["name"],
                    row["phone_number"],
                    row["address"],
                    row["email"],
                    int(row["credit_score"]),
                    float(row["annual_income"]),
                )
            )

        if values:
            rows_affected = db_manager.execute_many(insert_query, values)
            print(f"[DB Init] Loaded {rows_affected} customers.")


def load_card_types_from_csv(db_manager, data_dir):
    """Load card types from CSV file into database."""
    print("[DB Init] Loading card types...")

    csv_file = data_dir / "credit_card_types.csv"
    if not csv_file.exists():
        print(f"[DB Init] Error: {csv_file} not found")
        return

    # Clear existing data
    db_manager.execute_query("DELETE FROM credit_card_types")

    # Load data from CSV
    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        insert_query = """
        INSERT INTO credit_card_types (card_type_id, name, credit_score_min, credit_score_max, 
                                     credit_limit_min, credit_limit_max, annual_fee, rewards_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = []
        for row in reader:
            values.append(
                (
                    int(row["card_type_id"]),
                    row["name"],
                    int(row["credit_score_min"]),
                    int(row["credit_score_max"]),
                    int(row["credit_limit_min"]),
                    int(row["credit_limit_max"]),
                    int(row["annual_fee"]),
                    float(row["rewards_rate"]),
                )
            )

        if values:
            rows_affected = db_manager.execute_many(insert_query, values)
            print(f"[DB Init] Loaded {rows_affected} card types.")


def load_cards_from_csv(db_manager, data_dir):
    """Load cards from CSV file into database."""
    print("[DB Init] Loading cards...")

    csv_file = data_dir / "cards.csv"
    if not csv_file.exists():
        print(f"[DB Init] Error: {csv_file} not found")
        return

    # Clear existing data
    db_manager.execute_query("DELETE FROM cards")

    # Load data from CSV
    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        insert_query = """
        INSERT INTO cards (card_id, customer_id, card_type_id, card_number, 
                         expiration_date, credit_limit, current_balance, issue_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = []
        for row in reader:
            # Parse the ISO format date string to a datetime object for MySQL
            issue_date_str = row["issue_date"]
            issue_date = datetime.fromisoformat(issue_date_str)

            values.append(
                (
                    int(row["card_id"]),
                    int(row["customer_id"]),
                    int(row["card_type_id"]),
                    row["card_number"],
                    row["expiration_date"],
                    float(row["credit_limit"]),
                    float(row["current_balance"]),
                    issue_date,
                )
            )

        if values:
            rows_affected = db_manager.execute_many(insert_query, values)
            print(f"[DB Init] Loaded {rows_affected} cards.")


def load_transactions_from_csv(db_manager, data_dir):
    """Load transactions from CSV file into database."""
    print("[DB Init] Loading transactions...")

    csv_file = data_dir / "transactions.csv"
    if not csv_file.exists():
        print(f"[DB Init] Error: {csv_file} not found")
        return

    # Clear existing data
    db_manager.execute_query("DELETE FROM transactions")

    # Load data from CSV
    with open(csv_file, newline="") as f:
        reader = csv.DictReader(f)
        insert_query = """
        INSERT INTO transactions (transaction_id, card_id, merchant_name, timestamp, 
                                amount, location, transaction_type, related_transaction_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = []
        for row in reader:
            related_id = row["related_transaction_id"]
            if related_id and str(related_id).strip():
                related_id = int(related_id)
            else:
                related_id = None

            # Parse the ISO format date string to a datetime object for MySQL
            timestamp_str = row["timestamp"]
            timestamp = datetime.fromisoformat(timestamp_str)

            values.append(
                (
                    int(row["transaction_id"]),
                    int(row["card_id"]),
                    row["merchant_name"],
                    timestamp,
                    float(row["amount"]),
                    row["location"],
                    row["transaction_type"],
                    related_id,
                )
            )

        if values:
            rows_affected = db_manager.execute_many(insert_query, values)
            print(f"[DB Init] Loaded {rows_affected} transactions.")


def main():
    print("[DB Init] Starting database initialization...")

    # Initialize database manager
    db_manager = DBManager()
    db_manager.connect()

    try:
        # Create tables
        create_tables(db_manager)

        # Load data from CSV files
        data_dir = Path(__file__).resolve().parent.parent / "dataset"
        if not data_dir.exists():
            print(f"[DB Init] Error: Dataset directory {data_dir} not found")
            return

        # Load data in the correct order (to satisfy foreign key constraints)
        load_customers_from_csv(db_manager, data_dir)
        load_card_types_from_csv(db_manager, data_dir)
        load_cards_from_csv(db_manager, data_dir)
        load_transactions_from_csv(db_manager, data_dir)

        print("[DB Init] Database initialization completed successfully.")

    finally:
        # Ensure connection is closed
        db_manager.disconnect()


if __name__ == "__main__":
    main()
