"""Ingest generated CSV datasets into a SQLite database with validations."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict, List

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = ROOT_DIR / "db" / "ecom.db"

TABLE_SCHEMAS: Dict[str, str] = {
    # Customers master data
    "customers": """
        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            city TEXT,
            signup_date TEXT
        )
    """,
    # Product catalog
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT,
            price REAL NOT NULL
        )
    """,
    # Orders header referencing customers
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL,
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        )
    """,
    # Order line items referencing orders/products
    "order_items": """
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id),
            FOREIGN KEY(product_id) REFERENCES products(id)
        )
    """,
    # Payments referencing orders
    "payments": """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            payment_method TEXT NOT NULL,
            status TEXT NOT NULL,
            payment_date TEXT NOT NULL,
            FOREIGN KEY(order_id) REFERENCES orders(id)
        )
    """,
}

CSV_CONFIG = {
    "customers": {"filename": "customers.csv"},
    "products": {"filename": "products.csv"},
    "orders": {"filename": "orders.csv"},
    "order_items": {"filename": "order_items.csv"},
    "payments": {"filename": "payments.csv"},
}


def get_connection() -> sqlite3.Connection:
    """Create a SQLite connection with FK enforcement."""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def reset_tables(conn: sqlite3.Connection) -> None:
    """Drop existing tables to allow idempotent runs."""
    tables = list(TABLE_SCHEMAS.keys())
    for table in reversed(tables):
        conn.execute(f"DROP TABLE IF EXISTS {table};")


def create_tables(conn: sqlite3.Connection) -> None:
    """Create tables defined in TABLE_SCHEMAS."""
    for schema in TABLE_SCHEMAS.values():
        conn.execute(schema)


def load_csv(table_name: str, conn: sqlite3.Connection) -> pd.DataFrame:
    """Load a CSV into both pandas and SQLite."""
    config = CSV_CONFIG[table_name]
    csv_path = DATA_DIR / config["filename"]
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Loaded {len(df)} rows into '{table_name}'.")
    return df


def validation_reports(
    frames: Dict[str, pd.DataFrame], conn: sqlite3.Connection
) -> None:
    """Run validation checks: missing values, row counts, duplicates."""
    print("\n=== Validation: Missing Values ===")
    for table, df in frames.items():
        missing = df.isna().sum()
        print(f"{table}:")
        print(missing.to_string())

    print("\n=== Validation: Duplicate ID Counts ===")
    for table, df in frames.items():
        if "id" in df.columns:
            duplicates = df["id"].duplicated().sum()
            print(f"{table}: {duplicates} duplicate ids")

    print("\n=== Validation: Row Counts in SQLite ===")
    cursor = conn.cursor()
    for table in frames.keys():
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table}: {count} rows")


def ingest() -> None:
    """Main ingestion orchestrator."""
    DATA_DIR.mkdir(exist_ok=True, parents=True)
    DB_PATH.parent.mkdir(exist_ok=True, parents=True)

    with get_connection() as conn:
        reset_tables(conn)
        create_tables(conn)
        frames: Dict[str, pd.DataFrame] = {}
        for table in CSV_CONFIG.keys():
            frames[table] = load_csv(table, conn)
        validation_reports(frames, conn)
        conn.commit()

    print(f"Ingestion complete. Database stored at {DB_PATH}")


if __name__ == "__main__":
    ingest()

