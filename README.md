# Lambda Architecture Banking System

In this repository, I implement a **Lambda Architecture** pipeline for a credit card banking system. The pipeline processes a dataset of 1,014 transactions through two independent layers: a stream (speed) layer and a batch layer.

1. **Load** reference data (customers, cards, card types) into a MySQL database (Serving Layer).
2. **Stream** real-time transactions via Apache Kafka, validating fraud and balance rules instantly (Speed Layer).
3. **Batch** process validated transactions to recalculate customer credit scores and dynamically adjust credit limits (Batch Layer).
4. **Generate** output files tracking approved transactions, declining reasons, and updated customer portfolios.

### Dataset

- `customers.csv` — 30 customers with contact information, income, and base credit scores.
- `cards.csv` — 72 credit cards linked to customers, holding current balance and limits.
- `credit_card_types.csv` — 5 credit card tiers defining minimum score requirements and maximum limits.
- `transactions.csv` — 1,014 transactional records specifying the merchant, location, and amount.

---

## Required Packages

| Package | Purpose |
|---------|---------|
| `kafka-python` | Apache Kafka integration for the producer and consumer |
| `mysql-connector-python` | MySQL database connection and querying |
| `python-dotenv` | Environment variable management from `.env` files |

---

## Setup Instructions (for MAC Users)

### 1. Infrastructure Setup

**MySQL:**
- Ensure you have a local MySQL server running.
- Create a database named `banking_system`.

**Apache Kafka:**
- Ensure Zookeeper and Apache Kafka are installed and running.
  - Via Homebrew: `brew services start zookeeper && brew services start kafka`

### 2. Environment Configuration
Copy `.env.example` to `.env` and fill in your credentials:

```bash
cp .env.example .env 
```

Update the following fields:
- `DB_HOST` — Database host (default: `localhost`)
- `DB_USER` / `DB_PASSWORD` — MySQL credentials
- `DB_NAME` — Database name (default: `banking_system`)
- `KAFKA_BOOTSTRAP_SERVERS` — Kafka connection string (default: `localhost:9092`)

### 3. Running the Pipeline

Follow these steps sequentially to run the full architecture:

#### Database Initialization
Seed the MySQL serving layer with the dataset:
```bash
python3 src/init_db.py
```

#### Speed Layer (Real-time Stream)
In one terminal window, start the Consumer to listen to the Kafka topic:
```bash
python3 src/stream_consumer.py
```

In a second terminal window, start the Producer to stream transactions into Kafka:
```bash
python3 src/stream_producer.py
```

#### Batch Layer (Analytics & Adjustments)
Once the stream finishes processing, run the Batch layer to finalize calculations:
```bash
python3 src/batch_processor.py
```

### Expected Output

The pipeline will:
- Ingest referenced data to MySQL.
- Print live stream metrics, flagging declined transactions and printing a summary of decline reasons.
- Process the batch cycle, printing logs when a customer's credit score changes or a card's limit is proactively lowered.
- Save output CSV files to the `results/` directory:
   - `stream_transactions.csv` — All transactions with real-time `status` (pending/declined).
   - `batch_transactions.csv` — All transactions finalized (`approved`/`declined`) with applied balances.
   - `cards_updated.csv` — Updated card balances and recalculated credit limits.
   - `customers_updated.csv` — Updated customer credit scores based on overall utilization.

---

## Processing Logic

### Overview

Raw data enters the system and is dispatched to both a Speed Layer (for low-latency, real-time fraud validation) and a Batch Layer (for comprehensive, heavy-duty recalculations). The Serving Layer (MySQL) holds the source of truth for reference data.

### Speed Layer (`stream_consumer.py`)

Transactions are pulled from Kafka one by one. The consumer evaluates them against a set of business rules tracking a running `pending_balance`:

1. **Transaction Size Limit** — Declines the transaction if the amount exceeds 50% of the card's total credit limit.
2. **Location Proximity** — Validates the merchant's ZIP code against the customer's home ZIP code using an approximation function. Declines if they are too far apart.
3. **Credit Limit Cap** — Declines the transaction if evaluating the new amount against the running `pending_balance` would exceed the card's maximum limit.

### Batch Layer (`batch_processor.py`)

Takes the output from the stream layer and performs analytical adjustments:

1. **Balance Finalization** — Approves pending transactions and officially increments the card's `current_balance`.
2. **Credit Score Recalculation** — Groups all cards belonging to a customer to calculate a global "utilization percentage". Credit utilization above 30% incurs a penalty, dropping the customer's score by up to 25 points. Utilization below 30% increases the score.
3. **Risk Mitigation (Limit Adjustments)** — If a customer's credit score drops, the batch layer proactively reduces the credit limit on their active cards by 5% to 15% to mitigate risk.

---

```text
root/
├── .env.example              # Environment variable template
├── dataset/
│   ├── customers.csv
│   ├── cards.csv
│   ├── credit_card_types.csv
│   └── transactions.csv
├── results/                  # Generated after execution
├── src/
│   ├── init_db.py            # Database setup
│   ├── data_loader.py        # Data access layer
│   ├── db_manager.py         # MySQL connection manager
│   ├── models.py             # Dataclasses
│   ├── stream_producer.py    # Kafka producer
│   ├── stream_consumer.py    # Kafka consumer (Speed Layer)
│   ├── batch_processor.py    # Batch calculations (Batch Layer)
│   └── utils.py              # Shared logic
└── README.md
```

---
