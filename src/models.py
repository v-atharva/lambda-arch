from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Customer:
    customer_id: int
    name: str
    phone_number: str
    address: str
    email: str
    credit_score: int
    annual_income: float


@dataclass
class Card:
    card_id: int
    customer_id: int
    card_type_id: int
    card_number: str
    expiration_date: str  # "MM/YY" – keep string for now
    credit_limit: float
    current_balance: float
    issue_date: datetime


@dataclass
class CardType:
    card_type_id: int
    name: str
    credit_score_min: int
    credit_score_max: int
    credit_limit_min: int
    credit_limit_max: int
    annual_fee: int
    rewards_rate: float


@dataclass
class Transaction:
    transaction_id: int
    card_id: int
    merchant_name: str
    timestamp: datetime
    amount: float
    location: str
    transaction_type: str
    related_transaction_id: Optional[int]  # blank → None
