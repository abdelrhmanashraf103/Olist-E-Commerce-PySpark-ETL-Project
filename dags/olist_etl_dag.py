"""
=============================================================
MILESTONE 4 — Apache Airflow DAG
Olist Brazilian E-Commerce Dataset
=============================================================
Schedule: every day at 2:00 AM (server time)

Setup:
  pip install apache-airflow
  export AIRFLOW_HOME=~/airflow
  airflow db init
  airflow users create --username abdo_haroun103 --password haroun1032000 \
    --firstname Abdelrahman --lastname Haroun \
    --role Admin --email admin@example.com
  cp dags/olist_etl_dag.py ~/airflow/dags/
  airflow webserver --port 9000   ← Terminal 1
  airflow scheduler               ← Terminal 2
 Then open: http://localhost:9000
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import logging, json, os

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────
BASE_DIR = Variable.get(
    "PROJECT_DIR",
    default_var="/home/new_star_computer_25/pyspark_depi_project/pyspark_depi_env"
)
DATA_DIR    = os.path.join(BASE_DIR, "data")
SRC_DIR     = os.path.join(BASE_DIR, "../src")
METRICS     = os.path.join(BASE_DIR, "reports/job_metrics.json")
LOGS_DIR    = os.path.join(BASE_DIR, "logs")

REQUIRED_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]

DEFAULT_ARGS = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "start_date":        days_ago(1),
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}


# ── Task Functions ────────────────────────────────────────
def check_data_files(**ctx):
    """T1:Ensure all 9 required CSV files are present before starting ETL."""
    missing = []
    for fname in REQUIRED_FILES:
        fpath = os.path.join(DATA_DIR, fname)
        if not os.path.exists(fpath):
            missing.append(fname)
        else:
            size = os.path.getsize(fpath) / 1_000_000
            log.info(f"   {fname} ({size:.1f} MB)")
    if missing:
        raise FileNotFoundError(f" Missing: {missing}")
    log.info(" All 9 files found")


def check_results(**ctx):
    """T5:check the ETL results by reading the metrics JSON file produced by the batch job."""
    if not os.path.exists(METRICS):
        raise FileNotFoundError(" Metrics not found — ETL failed?")
    with open(METRICS) as f:
        m = json.load(f)
    if m.get("status") != "SUCCESS":
        raise RuntimeError(f" ETL failed: {m}")
    log.info(f" ETL OK: {m['total_records']:,} records in {m['duration_seconds']}s")
    ctx["ti"].xcom_push(key="metrics", value=m)


def send_notification(**ctx):
    """T6: إرسال إشعار بنتيجة الـ run."""
    m        = ctx["ti"].xcom_pull(key="metrics")
    run_date = ctx["ds"]
    msg = (
        f" Olist ETL Completed\n"
        f" Date:     {run_date}\n"
        f" Records:  {m['total_records']:,}\n"
        f" Duration: {m['duration_seconds']}s\n"
        f" Output:   {m['output_path']}\n"
    )
    # Uncomment to send Slack:
    # import requests
    # requests.post(Variable.get("SLACK_WEBHOOK"), json={"text": msg})
    log.info(f" {msg}")


# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id="olist_etl_daily",
    description="Daily ETL — Olist Brazilian E-Commerce",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["olist", "pyspark", "etl", "ecommerce"],
) as dag:

    t1 = PythonOperator(
        task_id="check_data_files",
        python_callable=check_data_files,
    )

    t2 = BashOperator(
        task_id="run_exploration",
        bash_command=(
            f"cd {SRC_DIR} && "
            f"source ~/pyspark_depi_project/pyspark_depi_env/bin/activate && "
            f"python milestone1_exploration.py "
            f">> {LOGS_DIR}/m1.log 2>&1"
        ),
    )

    t3 = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=(
            f"cd {SRC_DIR} && "
            f"source ~/pyspark_depi_project/pyspark_depi_env/bin/activate && "
            f"python milestone2_etl_olist.py "
            f">> {LOGS_DIR}/m2.log 2>&1"
        ),
        execution_timeout=timedelta(hours=1),
    )

    t4 = BashOperator(
        task_id="run_batch_job",
        bash_command=(
            f"cd {SRC_DIR} && "
            f"source ~/pyspark_depi_project/pyspark_depi_env/bin/activate && "
            f"python milestone3_batch_job.py "
            f">> {LOGS_DIR}/m3.log 2>&1"
        ),
    )

    t5 = PythonOperator(
        task_id="check_results",
        python_callable=check_results,
    )

    t6 = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
    )

    t7 = BashOperator(
        task_id="cleanup_logs",
        bash_command=f"find {LOGS_DIR} -name '*.log' -mtime +7 -delete || true",
    )

    # T1 → T2 → T3 → T4 → T5 → T6 → T7
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7