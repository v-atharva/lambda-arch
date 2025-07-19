import json
import csv
from pathlib import Path
from datetime import datetime
from kafka import KafkaProducer
import time
import sys

# Add the parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data_loader import DataLoader


def main():
    print("[Producer] Starting transaction stream producer...")

    # Configure Kafka producer
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode(),
    )

    # Load transactions from database
    data_dir = Path("dataset")
    loader = DataLoader(data_dir, use_db=True)

    transactions_dict = loader.load_transactions()
    transactions = list(transactions_dict.values())

    # Sort transactions by timestamp for correct ordering
    transactions.sort(key=lambda x: x.timestamp.isoformat())

    # Convert dataclass objects to dictionaries for JSON serialization
    serializable_transactions = []
    for tx in transactions:
        # Convert timestamp to string format
        tx_dict = {
            "transaction_id": tx.transaction_id,
            "card_id": tx.card_id,
            "merchant_name": tx.merchant_name,
            "timestamp": tx.timestamp.isoformat(),
            "amount": float(tx.amount),
            "location": tx.location,
            "transaction_type": tx.transaction_type,
            "related_transaction_id": tx.related_transaction_id,
        }
        serializable_transactions.append(tx_dict)

    # Send transactions to Kafka
    print(
        f"[Producer] Sending {len(serializable_transactions)} transactions to Kafka..."
    )
    for tx in serializable_transactions:
        producer.send("transactions_stream", tx)
        producer.flush()
        time.sleep(0.01)  # Small delay to prevent overwhelming the consumer

    print("[Producer] All transactions sent to Kafka")

    # Close database connection
    loader.close()


if __name__ == "__main__":
    main()
