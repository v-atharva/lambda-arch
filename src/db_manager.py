import json
import mysql.connector
from pathlib import Path
from typing import Dict, List, Any
import sys


class DBManager:
    """Database manager for handling SQL connections and queries."""

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.config = self._load_config()

    def _load_config(self) -> Dict:
        """Load database configuration from dbconfig.json."""
        config_path = Path(__file__).resolve().parent.parent / "dbconfig.json"
        with open(config_path, "r") as f:
            return json.load(f)

    def connect(self):
        """Establish connection to the database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.config["host"],
                port=self.config["port"],
                database=self.config["database"],
                user=self.config["user"],
                password=self.config["password"],
            )
            self.cursor = self.connection.cursor(dictionary=True)
            print(f"[DB] Connected to database {self.config['database']}")
        except mysql.connector.Error as err:
            print(f"[DB] Error connecting to database: {err}")
            sys.exit(1)

    def disconnect(self):
        """Close the database connection."""
        if self.connection and self.connection.is_connected():
            if self.cursor:
                self.cursor.close()
            self.connection.close()
            print("[DB] Database connection closed")

    def execute_query(self, query: str, params=None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results."""
        if not self.connection or not self.connection.is_connected():
            self.connect()

        try:
            self.cursor.execute(query, params or ())
            if self.cursor.with_rows:
                return self.cursor.fetchall()
            return []
        except mysql.connector.Error as err:
            print(f"[DB] Error executing query: {err}")
            print(f"[DB] Query: {query}")
            return []

    def execute_many(self, query: str, params_list):
        """Execute a SQL query with multiple parameter sets."""
        if not self.connection or not self.connection.is_connected():
            self.connect()

        try:
            self.cursor.executemany(query, params_list)
            self.connection.commit()
            return self.cursor.rowcount
        except mysql.connector.Error as err:
            print(f"[DB] Error executing batch query: {err}")
            self.connection.rollback()
            return 0

    def commit(self):
        """Commit the current transaction."""
        if self.connection and self.connection.is_connected():
            self.connection.commit()
