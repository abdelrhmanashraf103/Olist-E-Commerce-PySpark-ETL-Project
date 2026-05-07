"""
=============================================================
MILESTONE 3 — Production Batch Job
Olist Brazilian E-Commerce Dataset
=============================================================
Run: python src/milestone3_batch_job.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging, json, os, time
from datetime import datetime

# ── Config ────────────────────────────────────────────────
BASE_DIR    = os.path.expanduser("~/olist_depi_graduation_project")
INPUT_PATH  = os.path.join(BASE_DIR, "output/olist_master_table")  
OUTPUT_PATH = os.path.join(BASE_DIR, "output/olist_batch_output")
METRICS     = os.path.join(BASE_DIR, "reports/job_metrics.json")
CHECKPOINT  = os.path.join(BASE_DIR, "checkpoints/last_run.json")
LOG_FILE    = os.path.join(BASE_DIR, "logs/batch_job.log")

# ── Logging (create logs dir first to avoid FileNotFoundError) ─
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ]
)
log = logging.getLogger("BatchJob")


def get_spark():
    return (SparkSession.builder
            .appName("Olist_BatchJob_v2")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())


def load_checkpoint():
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as f:
            return json.load(f).get("last_run")
    return None


def save_checkpoint():
    os.makedirs(os.path.dirname(CHECKPOINT), exist_ok=True)
    with open(CHECKPOINT, "w") as f:
        json.dump({"last_run": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, f)
    log.info("Checkpoint saved")


def save_metrics(metrics: dict):
    os.makedirs(os.path.dirname(METRICS), exist_ok=True)
    with open(METRICS, "w") as f:
        json.dump(metrics, f, indent=2)
    log.info(f"Metrics saved -> {METRICS}")


def run_job():
    start = time.time()
    log.info("Olist Batch Job starting...")

    # ── Guard: master table must exist ────────────────────
    if not os.path.exists(INPUT_PATH):
        raise FileNotFoundError(
            f"Master table not found at: {INPUT_PATH}\n"
            f"Run milestone2_etl_olist.py first to generate it."
        )

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Load ──────────────────────────────────────────────
    df    = spark.read.parquet(INPUT_PATH)
    total = df.count()
    log.info(f"Loaded {total:,} records from master table")

    # ── Incremental filter ────────────────────────────────
    last_run = load_checkpoint()
    if last_run:
        df = df.filter(F.col("order_purchase_timestamp") > last_run)
        new_count = df.count()
        log.info(f"Incremental mode: {new_count:,} new records since {last_run}")
    else:
        log.info("Full load mode (no checkpoint found)")

    # ── Write partitioned output ──────────────────────────
    os.makedirs(OUTPUT_PATH, exist_ok=True)
    (df.coalesce(4)
       .write
       .mode("overwrite")
       .partitionBy("purchase_year", "purchase_month")
       .parquet(OUTPUT_PATH))
    log.info(f"Partitioned output written -> {OUTPUT_PATH}")

    # ── Save checkpoint + metrics ─────────────────────────
    save_checkpoint()

    output_count = df.count()
    duration     = round(time.time() - start, 2)
    save_metrics({
        "job_run_at":       datetime.now().isoformat(),
        "duration_seconds": duration,
        "total_records":    total,
        "output_records":   output_count,
        "input_path":       INPUT_PATH,
        "output_path":      OUTPUT_PATH,
        "status":           "SUCCESS",
    })

    log.info(f"Done in {duration}s")
    spark.stop()


if __name__ == "__main__":
    run_job()