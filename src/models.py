"""
Data models for the banking system.

Defines dataclass representations for Customer, Card, CardType, and
Transaction entities used throughout the stream and batch layers.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Customer:
    """Represents a credit card customer with personal and financial info."""

    customer_id: int
    name: str
    phone_number: str
    address: str
    email: str
    credit_score: int
    annual_income: float


@dataclass
class Card:
    """Represents an individual credit card linked to a customer."""

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
    """Defines a credit card tier with score range, limits, fees, and rewards."""

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
    """A single credit card transaction event with type and optional relation."""

    transaction_id: int
    card_id: int
    merchant_name: str
    timestamp: datetime
    amount: float
    location: str
    transaction_type: str
    related_transaction_id: Optional[int]  # blank → None
