"""Generate synthetic e-commerce datasets with referential integrity."""

from __future__ import annotations

import random
from pathlib import Path
from typing import Dict, List

import pandas as pd
from faker import Faker  # type: ignore

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

RNG_SEED = 42
NUM_CUSTOMERS = 300
NUM_PRODUCTS = 120
NUM_ORDERS = 400
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gift_card", "upi"]
PAYMENT_STATUSES = ["completed", "pending", "failed", "refunded"]
CATEGORIES = [
    "Electronics",
    "Home & Kitchen",
    "Books",
    "Fashion",
    "Sports",
    "Beauty",
    "Toys",
]


def configure_randomness() -> Faker:
    """Configure deterministic randomness for Faker and Python's RNG."""
    random.seed(RNG_SEED)
    faker = Faker()
    Faker.seed(RNG_SEED)
    return faker


def generate_customers(faker: Faker) -> List[Dict]:
    """Create synthetic customers."""
    customers = []
    for cid in range(1, NUM_CUSTOMERS + 1):
        customers.append(
            {
                "id": cid,
                "name": faker.name(),
                "email": faker.unique.email(),
                "phone": faker.phone_number(),
                "city": faker.city(),
                "signup_date": faker.date_between(start_date="-2y", end_date="today"),
            }
        )
    return customers


def generate_products(faker: Faker) -> List[Dict]:
    """Create synthetic products."""
    products = []
    for pid in range(1, NUM_PRODUCTS + 1):
        category = random.choice(CATEGORIES)
        price = round(random.uniform(5, 500), 2)
        products.append(
            {
                "id": pid,
                "name": faker.unique.catch_phrase(),
                "category": category,
                "price": price,
            }
        )
    return products


def generate_orders(faker: Faker, customers: List[Dict]) -> List[Dict]:
    """Create orders referencing customers without totals yet."""
    orders = []
    for oid in range(1, NUM_ORDERS + 1):
        customer = random.choice(customers)
        order_date = faker.date_between(start_date="-1y", end_date="today")
        orders.append(
            {
                "id": oid,
                "customer_id": customer["id"],
                "order_date": order_date,
                "total_amount": 0.0,  # placeholder, updated after items
            }
        )
    return orders


def generate_order_items(
    faker: Faker, orders: List[Dict], products: List[Dict]
) -> List[Dict]:
    """Create order items referencing orders and products."""
    order_items: List[Dict] = []
    item_id = 1
    for order in orders:
        num_items = random.randint(1, 5)
        chosen_products = random.sample(products, k=num_items)
        total = 0.0
        for product in chosen_products:
            quantity = random.randint(1, 4)
            price = product["price"]
            line_total = round(price * quantity, 2)
            total += line_total
            order_items.append(
                {
                    "id": item_id,
                    "order_id": order["id"],
                    "product_id": product["id"],
                    "quantity": quantity,
                    "price": price,
                }
            )
            item_id += 1
        order["total_amount"] = round(total, 2)
    return order_items


def generate_payments(faker: Faker, orders: List[Dict]) -> List[Dict]:
    """Create payments referencing orders."""
    payments = []
    for pid, order in enumerate(orders, start=1):
        status = random.choices(
            population=PAYMENT_STATUSES, weights=[0.8, 0.1, 0.05, 0.05], k=1
        )[0]
        method = random.choice(PAYMENT_METHODS)
        payment_date = faker.date_between(
            start_date=order["order_date"], end_date="today"
        )
        payments.append(
            {
                "id": pid,
                "order_id": order["id"],
                "payment_method": method,
                "status": status,
                "payment_date": payment_date,
            }
        )
    return payments


def save_csv(filename: str, records: List[Dict]) -> None:
    """Persist records as CSV."""
    df = pd.DataFrame(records)
    file_path = DATA_DIR / filename
    df.to_csv(file_path, index=False)
    print(f"Wrote {len(df)} rows -> {file_path}")


def main() -> None:
    """Entrypoint for synthetic data generation."""
    DATA_DIR.mkdir(exist_ok=True, parents=True)
    faker = configure_randomness()

    customers = generate_customers(faker)
    products = generate_products(faker)
    orders = generate_orders(faker, customers)
    order_items = generate_order_items(faker, orders, products)
    payments = generate_payments(faker, orders)

    save_csv("customers.csv", customers)
    save_csv("products.csv", products)
    save_csv("orders.csv", orders)
    save_csv("order_items.csv", order_items)
    save_csv("payments.csv", payments)

    print("Synthetic data generation complete.")


if __name__ == "__main__":
    main()

