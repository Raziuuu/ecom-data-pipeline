"""Run analytical SQL queries against the e-commerce SQLite database."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Dict

import pandas as pd
from tabulate import tabulate

ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "db" / "ecom.db"
OUTPUT_DIR = ROOT_DIR / "output"

QueryConfig = Dict[str, str]

QUERIES: Dict[str, QueryConfig] = {
    "top_customers": {
        "description": "Top 10 customers by total spending",
        "sql": """
            -- Aggregate order totals per customer
            SELECT
                c.id AS customer_id,
                c.name,
                c.email,
                SUM(o.total_amount) AS total_spent,
                COUNT(o.id) AS orders_count
            FROM customers c
            JOIN orders o ON c.id = o.customer_id
            GROUP BY c.id, c.name, c.email
            ORDER BY total_spent DESC
            LIMIT 10;
        """,
        "output": OUTPUT_DIR / "top_customers.csv",
    },
    "product_sales": {
        "description": "Most sold products with quantities and revenue",
        "sql": """
            -- Join order_items with products to compute demand
            SELECT
                p.id AS product_id,
                p.name AS product_name,
                p.category,
                SUM(oi.quantity) AS total_quantity,
                ROUND(SUM(oi.quantity * oi.price), 2) AS total_revenue
            FROM order_items oi
            JOIN products p ON p.id = oi.product_id
            GROUP BY p.id, p.name, p.category
            ORDER BY total_quantity DESC
            LIMIT 20;
        """,
        "output": OUTPUT_DIR / "product_sales.csv",
    },
    "city_revenue": {
        "description": "Revenue contribution per customer city",
        "sql": """
            -- Revenue grouped by customer city
            SELECT
                c.city,
                COUNT(DISTINCT o.id) AS orders_count,
                ROUND(SUM(o.total_amount), 2) AS total_revenue
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            GROUP BY c.city
            ORDER BY total_revenue DESC;
        """,
        "output": OUTPUT_DIR / "city_revenue.csv",
    },
    "orders_payments": {
        "description": "Order and payment consolidation",
        "sql": """
            -- Join orders with payments for a consolidated view
            SELECT
                o.id AS order_id,
                o.order_date,
                o.total_amount,
                p.payment_method,
                p.status AS payment_status,
                p.payment_date
            FROM orders o
            LEFT JOIN payments p ON p.order_id = o.id
            ORDER BY o.order_date DESC
            LIMIT 200;
        """,
        "output": OUTPUT_DIR / "orders_payments.csv",
    },
    "monthly_sales": {
        "description": "Month-wise sales summary",
        "sql": """
            -- Monthly aggregation of order totals
            SELECT
                strftime('%Y-%m', o.order_date) AS order_month,
                COUNT(o.id) AS orders_count,
                ROUND(SUM(o.total_amount), 2) AS total_revenue,
                ROUND(AVG(o.total_amount), 2) AS avg_order_value
            FROM orders o
            GROUP BY strftime('%Y-%m', o.order_date)
            ORDER BY order_month;
        """,
        "output": OUTPUT_DIR / "monthly_sales.csv",
    },
}


def run_query(
    name: str, config: QueryConfig, conn: sqlite3.Connection
) -> pd.DataFrame:
    """Execute a SQL query, print it, and persist the result."""
    print(f"\n>>> {name}: {config['description']}")
    df = pd.read_sql_query(config["sql"], conn)
    if df.empty:
        print("No rows returned.")
    else:
        print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
    output_path = config["output"]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Saved {len(df)} rows -> {output_path}")
    return df


def main() -> None:
    """Entrypoint for executing all analytical queries."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        for name, config in QUERIES.items():
            run_query(name, config, conn)


if __name__ == "__main__":
    main()

