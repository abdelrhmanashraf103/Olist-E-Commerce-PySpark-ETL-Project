"""
=============================================================
MILESTONE 1 — Data Exploration & Quality Report
Olist Brazilian E-Commerce Dataset
=============================================================
Run: python src/milestone1_exploration.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

#========== Configuration ==========
BASE = os.path.expanduser('~/olist_depi_graduation_project/data')

TABLES ={
    "orders":          "olist_orders_dataset.csv",
    "order_items":     "olist_order_items_dataset.csv",
    "products":        "olist_products_dataset.csv",
    "sellers":         "olist_sellers_dataset.csv",
    "customers":       "olist_customers_dataset.csv",
    "order_reviews":   "olist_order_reviews_dataset.csv",
    "geolocation":     "olist_geolocation_dataset.csv",
    "translation":     "product_category_name_translation.csv",
    "payments":        "olist_order_payments_dataset.csv",
}

# ========= Spark Session Initialization ==========
spark = (SparkSession.builder
         .appName("Milestone 1 - Data Exploration")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", "2g")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ========= Quality Report ==========
print("\n" + "="*60)
print("MILESTONE 1 - DATA EXPLORATION & QUALITY REPORT")
print("="*60 + "\n")    

dfs = {}
for name, fname in TABLES.items():
    path = os.path.join(BASE, fname)
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    dfs[name] = df

    count = df.count()
    print(f"\n {name.upper()})")
    print(f"   - Rows: {count:,} | Cols: {len(df.columns)}")
    print(f"   - Columns: {df.columns}")

    nulls = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()

    bad = {k: v for k, v in nulls.items() if v > 0}
    if bad:
        print("   - Columns with NULLs:")
        for col, cnt in bad.items():
            pct = (cnt / count) * 100
            print(f"     * {col}: {cnt} NULLs ({pct:.2f}%)")
    else:
        print("   - No NULL values found.")

# ========== Extra Exploration (Optional) ==========
print("\n" + "="*60)
print("EXTRA EXPLORATION - KEY INSIGHTS")
print("="*60 + "\n")

print("\n Order Status Distribution:")
dfs["orders"].groupBy("order_status").count()\
    .orderBy("count", ascending=False).show()

print("\n Payment Types Distribution:")
dfs["payments"].groupBy("payment_type").count()\
    .orderBy("count", ascending=False).show()

print("\n Review Scores Distribution:")
dfs["order_reviews"].groupBy("review_score").count()\
    .orderBy("count", ascending=False).show()

print("\n Top 5 Customer States:")
dfs["customers"].groupBy("customer_state").count()\
    .orderBy("count", ascending=False).show(5)

print(" Geolocation States Coverage:")
dfs["geolocation"].groupBy("geolocation_state") \
    .agg(F.count("*").alias("total")) \
    .orderBy("total", ascending=False).show(10)

print("\n Products - Top 5 Categories:")
dfs["products"].groupBy("product_category_name").count()\
    .orderBy("count", ascending=False).show(5)

print("\n Sellers - Top 5 States:")
dfs["sellers"].groupBy("seller_state").count()\
    .orderBy("count", ascending=False).show(5)

spark.stop()
print("\nMilestone 1 exploration completed successfully.\n")