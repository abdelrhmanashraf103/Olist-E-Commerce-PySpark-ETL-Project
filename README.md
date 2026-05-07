# 🛒 Olist E-Commerce — PySpark ETL Project
### Brazilian E-Commerce Public Dataset by Olist (Kaggle)

---

## 📁 Project Structure

```
~/olist_depi_graduation_project/
│
├── src/
│   ├── milestone1_exploration.py     ← M1: Data Exploration & Quality Report
│   ├── milestone2_etl_olist.py       ← M2: ETL Cleaning + Master Table
│   └── milestone3_batch_job.py       ← M3: Production Batch Job + Parquet
│
├── dags/
│   └── olist_etl_dag.py              ← M4: Airflow Daily Scheduler
│
├── data/
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   ├── olist_order_payments_dataset.csv
│   ├── olist_order_reviews_dataset.csv
│   ├── olist_customers_dataset.csv
│   ├── olist_products_dataset.csv
│   ├── olist_sellers_dataset.csv
│   ├── olist_geolocation_dataset.csv
│   └── product_category_name_translation.csv
│
├── output/
│   ├── olist_master_table/            ← Auto-generated after M2
│   └── olist_batch_output/            ← Auto-generated after M3 (partitioned by year/month)
│
├── reports/
│   ├── olist_milestone5_report.html  ← M5: Final Interactive Report
│   └── job_metrics.json              ← Auto-generated after M3
│
├── logs/                             ← Auto-generated
├── checkpoints/                      ← Auto-generated (last_run.json)
├── requirements.txt
└── README.md
```

---

## ⚡ How to Run — Step by Step

### 0️⃣ Install Requirements (once only)
```bash
cd ~/olist_depi_graduation_project
source olist_depi_env/bin/activate
pip install -r requirements.txt
```

---

### 1️⃣ Milestone 1 — Data Exploration
```bash
python src/milestone1_exploration.py
```
✅ Output: Quality report showing row counts, null analysis, and schema for all 9 tables

---

### 2️⃣ Milestone 2 — ETL Pipeline & Master Table
```bash
python src/milestone2_etl_olist.py
```
✅ Output: `output/olist_master_table/` — cleaned and joined Parquet master table

---

### 3️⃣ Milestone 3 — Production Batch Job
```bash
python src/milestone3_batch_job.py
```
✅ Output:
- `output/olist_batch_output/` — partitioned by `purchase_year` / `purchase_month`
- `reports/job_metrics.json` — run duration, record count, status
- `checkpoints/last_run.json` — timestamp for incremental filtering

> ⚠️ **Run Milestone 2 first.** Milestone 3 reads from `output/olist_master_table/`.

---

### 4️⃣ Milestone 4 — Airflow Scheduler
```bash
pip install apache-airflow
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create \
  --username abdo_haroun103 --password haroun1032000 \
  --firstname Abdelrahman --lastname Haroun \
  --role Admin --email haroun103@example.com
cp dags/olist_etl_dag.py ~/airflow/dags/
airflow webserver --port 9000   # Terminal 1
airflow scheduler               # Terminal 2
```
✅ Open: http://localhost:8080  
✅ The full pipeline runs automatically every day at **02:00 AM**

---

### 5️⃣ Milestone 5 — Final Report
```bash
# On Windows (WSL)
explorer.exe reports/olist_milestone5_report.html

# On Linux
xdg-open reports/olist_milestone5_report.html
```

---

## 📊 Dataset Statistics

| Table | Rows |
|---|---|
| orders | 99,441 |
| order_items | 112,650 |
| payments | 103,886 |
| reviews | 99,224 |
| customers | 99,441 |
| products | 32,951 |
| sellers | 3,095 |
| geolocation | 1,000,163 → 19,015 unique zip codes |
| translation | 71 |

---

## 🔧 Data Quality Issues & Fixes

| Issue | Table | Fix Applied |
|---|---|---|
| Null delivery dates | orders | Kept as-is — filtered during delivery analysis |
| Null approval date | orders | `fillna("1970-01-01")` |
| Null product category | products | `fillna("unknown")` |
| Null review comments | reviews | `fillna("")` + `has_comment` boolean flag |
| Portuguese category names | products | Left join with translation CSV |
| Price stored per item | order_items | `GroupBy order_id` + `sum` → `order_total` |
| Multiple payment rows per order | payments | Aggregate → `total_paid` + `main_payment_type` |
| Coordinates outside Brazil | geolocation | Filtered by lat/lng bounding box |
| Duplicate zip codes | geolocation | `GroupBy zip` + averaged coordinates |