# Mini E-commerce Data Pipeline

Production-ready synthetic e-commerce pipeline that fabricates realistic CSV datasets, ingests them into SQLite with data quality checks, and publishes multi-table SQL analytics. Designed for repeatable execution from a terminal session inside Cursor.

## Tech Stack
- Python 3.11, pandas, Faker, tabulate
- SQLite (lightweight analytical store)
- CSV for interoperable data exchange

## Project Structure
```
project-root/
├── data/                # synthetic CSVs (generated)
├── db/
│   ├── ecom.db          # SQLite database (generated)
│   ├── ingest.py        # ingestion + validation script
│   └── queries.py       # curated SQL joins + exports
├── output/              # analytical query exports (generated)
├── scripts/
│   └── generate_data.py # Faker-powered data factory
├── README.md
└── requirements.txt
```

## Quickstart
```bash
python -m venv .venv
.venv\Scripts\activate            # Windows PowerShell
pip install -r requirements.txt
```

## 1. Generate Synthetic Data
```bash
python scripts/generate_data.py
```
- Produces five CSVs (`customers`, `products`, `orders`, `order_items`, `payments`) under `data/`.
- Enforces referential integrity by constructing dependent tables from shared in-memory objects.

## 2. Ingest into SQLite with Validation
```bash
python db/ingest.py
```
- Creates `db/ecom.db`, rebuilds the schema, and loads all CSVs.
- Emits:
  - Missing values report per table
  - Duplicate `id` counts
  - Row counts fetched from SQLite to confirm persistence

## 3. Run Analytical SQL Queries
```bash
python db/queries.py
```
- Executes five curated multi-table joins (top customers, product demand, city revenue, order-payment consolidation, monthly trend analysis).
- Prints each result using tabular formatting and writes them to `output/` (`top_customers.csv`, `product_sales.csv`, `city_revenue.csv`, `orders_payments.csv`, `monthly_sales.csv`).

## Example Output
```
+---------------+------------------+----------------------------+---------------+----------------+
|   customer_id | name             | email                      |   total_spent |   orders_count |
|---------------+------------------+----------------------------+---------------+----------------|
|           227 | Christian Waller | shane23@example.net        |      13740.5  |              5 |
|           136 | Ryan Lamb        | williamssteven@example.org |      10445    |              6 |
|            13 | Daniel Baker     | nicole35@example.com       |      10173.3  |              3 |
+---------------+------------------+----------------------------+---------------+----------------+
```
*Excerpt from `output/top_customers.csv` demonstrating deterministic aggregation.*

## GitHub Push Automation
Run these commands from `D:\deligent_assignment1` after verifying the pipeline:
```bash
git init
git add .
git commit -m "feat: add e-commerce data pipeline"
git branch -M main
git remote add origin https://github.com/<your-user>/<repo-name>.git
git push -u origin main
```

## Author
Mahammad Razi • Built with assistance from GPT-5.1 Codex

