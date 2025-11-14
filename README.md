# Mini E-commerce Data Pipeline

Production-ready synthetic e-commerce pipeline that fabricates realistic CSV datasets, ingests them into SQLite with data quality checks, and publishes multi-table SQL analytics. Designed for repeatable execution from a terminal session inside Cursor.


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

1️⃣ Generate Synthetic Data
python scripts/generate_data.py

This script creates five realistic, inter-dependent datasets under /data/:
	•	customers.csv
	•	products.csv
	•	orders.csv
	•	order_items.csv
	•	payments.csv

All tables preserve referential integrity by generating shared IDs in memory before export.

2️⃣ Ingest into SQLite (with Validation)

python db/ingest.py

This step:
	•	Creates db/ecom.db
	•	Rebuilds table schemas (PKs + FKs)
	•	Imports all CSVs
	•	Performs validation:
	•	Missing values per table
	•	Duplicate primary keys
	•	Row count verification

A clean ingestion log is printed to help verify pipeline integrity.



3️⃣ Run Analytical SQL 

python db/queries.py

Executes five curated multi-table joins:
	1.	Top 10 customers by total spend
	2.	Most sold products
	3.	City-wise revenue
	4.	Orders + payments consolidated view
	5.	Monthly sales trend

Each result:
	•	Prints as a formatted table
	•	Saves to /output/ as CSV

🌐 GitHub Push Steps

After verifying the pipeline locally:
git init
git add .
git commit -m "feat: add e-commerce data pipeline"
git branch -M main
git remote add origin https://github.com/Raziuuu/ecom-data-pipeline.git
git push -u origin main


🧩 Technologies Used
	•	Python
	•	Pandas
	•	Faker
	•	SQLite
	•	SQL (Joins, aggregations, grouping)
	•	Cursor IDE (AI-assisted SDLC)



## Author
Mahammad Razi 
Github : https://github.com/Raziuuu
LinkdIn : https://mahammad-razi-6324b5244
