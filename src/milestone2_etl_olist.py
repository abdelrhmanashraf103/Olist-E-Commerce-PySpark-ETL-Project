"""
=============================================================
MILESTONE 2 — PySpark ETL Pipeline
Olist Brazilian E-Commerce Dataset (9 Tables including Geo)
=============================================================
Run: python src/milestone2_etl_olist.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType
import logging, os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("OlistETL")

# ── Config ────────────────────────────────────────────────
BASE   = os.path.expanduser("~/olist_depi_graduation_project/data")
OUTPUT = os.path.expanduser("~/olist_depi_graduation_project/output/olist_master_table")

# ── Spark ─────────────────────────────────────────────────
def get_spark():
    spark = (SparkSession.builder
             .appName("Olist_ETL_v2")
             .master("local[*]")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.driver.memory", "2g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ══════════════════════════════════════════════════════════
# STEP 1 — LOAD ALL TABLES
# ══════════════════════════════════════════════════════════
def load_all(spark):
    log.info(" Loading all 9 Olist CSV files...")
    files = {
        "orders":      "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "payments":    "olist_order_payments_dataset.csv",
        "reviews":     "olist_order_reviews_dataset.csv",
        "customers":   "olist_customers_dataset.csv",
        "products":    "olist_products_dataset.csv",
        "sellers":     "olist_sellers_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        "translation": "product_category_name_translation.csv",
    }
    dfs = {}
    for name, fname in files.items():
        path = os.path.join(BASE, fname)
        dfs[name] = spark.read.csv(path, header=True, inferSchema=True)
        log.info(f"    {name}: {dfs[name].count():,} rows")
    return dfs


# ══════════════════════════════════════════════════════════
# STEP 2 — CLEAN ORDERS
# ══════════════════════════════════════════════════════════
def clean_orders(df):
    log.info(" Cleaning orders...")
    before = df.count()

    # Drop missing critical fields
    df = df.dropna(subset=["order_id", "customer_id", "order_status"])

    # Parse timestamps
    ts_cols = [
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in ts_cols:
        df = df.withColumn(col,
            F.to_timestamp(col, "yyyy-MM-dd HH:mm:ss"))

    # Standardize status
    df = df.withColumn("order_status",
        F.lower(F.trim(F.col("order_status"))))

    # Delivery days (delivered orders only)
    df = df.withColumn("delivery_days",
        F.when(F.col("order_status") == "delivered",
               F.datediff("order_delivered_customer_date",
                          "order_purchase_timestamp"))
         .otherwise(F.lit(None).cast(IntegerType())))

    # Late delivery flag
    df = df.withColumn("is_late",
        F.when(
            (F.col("order_status") == "delivered") &
            (F.col("order_delivered_customer_date") >
             F.col("order_estimated_delivery_date")),
            True).otherwise(False))

    # Date parts
    df = (df.withColumn("purchase_year",  F.year("order_purchase_timestamp"))
            .withColumn("purchase_month", F.month("order_purchase_timestamp"))
            .withColumn("purchase_dow",   F.dayofweek("order_purchase_timestamp")))

    log.info(f"   Orders: {before:,} → {df.count():,}")
    return df


# ══════════════════════════════════════════════════════════
# STEP 3 — CLEAN ORDER ITEMS
# ══════════════════════════════════════════════════════════
def clean_order_items(df):
    log.info(" Cleaning order_items...")

    df = df.withColumn("price",
        F.when(F.col("price").cast(DoubleType()) <= 0, None)
         .otherwise(F.col("price").cast(DoubleType())))

    df = df.withColumn("freight_value",
        F.when(F.col("freight_value").cast(DoubleType()) < 0, 0.0)
         .otherwise(F.col("freight_value").cast(DoubleType())))

    df = df.withColumn("item_total",
        F.round(F.col("price") + F.col("freight_value"), 2))

    df = df.withColumn("shipping_limit_date",
        F.to_timestamp("shipping_limit_date", "yyyy-MM-dd HH:mm:ss"))

    # Aggregate to order level
    df_agg = df.groupBy("order_id").agg(
        F.count("order_item_id").alias("item_count"),
        F.round(F.sum("price"), 2).alias("products_value"),
        F.round(F.sum("freight_value"), 2).alias("freight_total"),
        F.round(F.sum("item_total"), 2).alias("order_total"),
        F.collect_set("product_id").alias("product_ids"),
        F.collect_set("seller_id").alias("seller_ids"),
    )

    log.info(f"   Items aggregated → {df_agg.count():,} orders")
    return df_agg


# ══════════════════════════════════════════════════════════
# STEP 4 — CLEAN PAYMENTS
# ══════════════════════════════════════════════════════════
def clean_payments(df):
    log.info(" Cleaning payments...")

    df = df.withColumn("payment_type",
        F.lower(F.trim(F.col("payment_type"))))

    df = df.withColumn("payment_value",
        F.when(F.col("payment_value") < 0, 0.0)
         .otherwise(F.col("payment_value")))

    # Aggregate per order
    df_agg = df.groupBy("order_id").agg(
        F.round(F.sum("payment_value"), 2).alias("total_paid"),
        F.collect_set("payment_type").alias("payment_types"),
        F.max("payment_installments").alias("max_installments"),
        F.count("*").alias("payment_count"),
    )

    # Main payment type
    main = (df.groupBy("order_id", "payment_type").count()
              .withColumn("rn", F.row_number().over(
                  Window.partitionBy("order_id")
                        .orderBy(F.col("count").desc())))
              .filter(F.col("rn") == 1)
              .select("order_id",
                      F.col("payment_type").alias("main_payment_type")))

    df_agg = df_agg.join(main, "order_id", "left")
    log.info(f"   Payments cleaned → {df_agg.count():,} orders")
    return df_agg


# ══════════════════════════════════════════════════════════
# STEP 5 — CLEAN REVIEWS
# ══════════════════════════════════════════════════════════
def clean_reviews(df):
    log.info(" Cleaning reviews...")

    # Valid scores only
    df = df.withColumn("review_score",
        F.when((F.col("review_score") < 1) |
               (F.col("review_score") > 5), None)
         .otherwise(F.col("review_score").cast(IntegerType())))

    df = df.fillna({
        "review_comment_title":   "",
        "review_comment_message": ""
    })

    df = df.withColumn("has_comment",
        F.when(F.length(F.trim(F.col("review_comment_message"))) > 0,
               True).otherwise(False))

    # One review per order (latest)
    df = (df.withColumn("rn", F.row_number().over(
              Window.partitionBy("order_id")
                    .orderBy(F.col("review_creation_date").desc())))
            .filter(F.col("rn") == 1).drop("rn"))

    # Sentiment
    df = df.withColumn("sentiment",
        F.when(F.col("review_score") >= 4, "positive")
         .when(F.col("review_score") == 3, "neutral")
         .when(F.col("review_score") <= 2, "negative")
         .otherwise("unknown"))

    result = df.select("order_id", "review_score",
                       "has_comment", "sentiment",
                       "review_creation_date")
    log.info(f"   Reviews cleaned → {result.count():,}")
    return result


# ══════════════════════════════════════════════════════════
# STEP 6 — CLEAN PRODUCTS + TRANSLATE
# ══════════════════════════════════════════════════════════
def clean_products(df, translation):
    log.info(" Cleaning products + translating categories...")

    df = df.join(translation, "product_category_name", "left")

    df = df.fillna({
        "product_category_name":         "unknown",
        "product_category_name_english": "unknown",
        "product_name_lenght":            0,
        "product_description_lenght":     0,
        "product_photos_qty":             0,
        "product_weight_g":               0,
        "product_length_cm":              0,
        "product_height_cm":              0,
        "product_width_cm":               0,
    })

    df = df.withColumn("volume_cm3",
        F.col("product_length_cm") *
        F.col("product_height_cm") *
        F.col("product_width_cm"))

    df = df.withColumn("weight_tier",
        F.when(F.col("product_weight_g") <= 500,  "light")
         .when(F.col("product_weight_g") <= 5000, "medium")
         .otherwise("heavy"))

    result = df.select(
        "product_id", "product_category_name_english",
        "product_weight_g", "volume_cm3",
        "weight_tier", "product_photos_qty"
    )
    log.info(f"   Products cleaned → {result.count():,}")
    return result


# ══════════════════════════════════════════════════════════
# STEP 7 — CLEAN GEOLOCATION
# ══════════════════════════════════════════════════════════
def clean_geolocation(df):
    log.info(" Cleaning geolocation...")

    # Cast coordinates
    df = df.withColumn("lat",
        F.col("geolocation_lat").cast(DoubleType()))
    df = df.withColumn("lng",
        F.col("geolocation_lng").cast(DoubleType()))

    # Remove invalid coordinates
    df = df.filter(
        (F.col("lat").between(-33.75, 5.27)) &   # Brazil lat range
        (F.col("lng").between(-73.99, -34.79))    # Brazil lng range
    )

    # Standardize city name
    df = df.withColumn("geolocation_city",
        F.initcap(F.trim(F.col("geolocation_city"))))

    # One row per zip code (average coordinates)
    df = df.groupBy(
        "geolocation_zip_code_prefix",
        "geolocation_state"
    ).agg(
        F.round(F.avg("lat"), 6).alias("geo_lat"),
        F.round(F.avg("lng"), 6).alias("geo_lng"),
        F.first("geolocation_city").alias("geo_city"),
    )

    log.info(f"   Geolocation cleaned → {df.count():,} zip codes")
    return df


# ══════════════════════════════════════════════════════════
# STEP 8 — BUILD MASTER TABLE
# ══════════════════════════════════════════════════════════
def build_master(orders, items_agg, payments_agg,
                 reviews_clean, customers, geo_clean):
    log.info(" Building master analytical table...")

    # Join customers with geolocation
    customers_geo = customers.join(
        geo_clean.select(
            F.col("geolocation_zip_code_prefix").alias("customer_zip_code_prefix"),
            "geo_lat", "geo_lng", "geo_city"
        ),
        "customer_zip_code_prefix", "left"
    )

    # Build master
    master = (orders
        .join(items_agg,     "order_id",    "left")
        .join(payments_agg,  "order_id",    "left")
        .join(reviews_clean, "order_id",    "left")
        .join(customers_geo, "customer_id", "left")
    )

    # Revenue tier
    master = master.withColumn("revenue_tier",
        F.when(F.col("order_total") >= 500, "high")
         .when(F.col("order_total") >= 100, "medium")
         .otherwise("low"))

    # Payment match flag
    master = master.withColumn("payment_match",
        F.round(F.abs(
            F.col("total_paid") - F.col("order_total")
        ), 2) <= 1.0)

    count = master.count()
    log.info(f"    Master table: {count:,} rows × {len(master.columns)} cols")
    return master


# ══════════════════════════════════════════════════════════
# STEP 9 — REPORT
# ══════════════════════════════════════════════════════════
def report(master):
    print("\n" + "="*60)
    print("     ETL REPORT — OLIST MASTER TABLE")
    print("="*60)

    print("\n Orders by Status:")
    master.groupBy("order_status").count() \
        .orderBy("count", ascending=False).show()

    print(" Payment Types:")
    master.groupBy("main_payment_type").count() \
        .orderBy("count", ascending=False).show()

    print(" Avg Review Score by Status:")
    master.groupBy("order_status").agg(
        F.round(F.avg("review_score"), 2).alias("avg_score"),
        F.count("*").alias("orders")
    ).orderBy("avg_score", ascending=False).show()

    print(" Top States by Orders:")
    master.groupBy("customer_state").count() \
        .orderBy("count", ascending=False).show(10)

    print(" Late Delivery Rate:")
    total = master.count()
    late  = master.filter(F.col("is_late") == True).count()
    print(f"   Late: {late:,} / {total:,} = {late/total*100:.1f}%\n")

    print(" Avg Coordinates per State:")
    master.groupBy("customer_state").agg(
        F.round(F.avg("geo_lat"), 4).alias("avg_lat"),
        F.round(F.avg("geo_lng"), 4).alias("avg_lng"),
        F.count("*").alias("orders")
    ).orderBy("orders", ascending=False).show(10)


# ══════════════════════════════════════════════════════════
# STEP 10 — SAVE
# ══════════════════════════════════════════════════════════
def save(df):
    (df.coalesce(4)
       .write.mode("overwrite")
       .partitionBy("purchase_year", "purchase_month")
       .parquet(OUTPUT))
    log.info(f" Saved → {OUTPUT}")


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════
def run():
    spark = get_spark()
    dfs   = load_all(spark)

    orders_clean   = clean_orders(dfs["orders"])
    items_agg      = clean_order_items(dfs["order_items"])
    payments_agg   = clean_payments(dfs["payments"])
    reviews_clean  = clean_reviews(dfs["reviews"])
    products_clean = clean_products(dfs["products"], dfs["translation"])
    geo_clean      = clean_geolocation(dfs["geolocation"])

    master = build_master(
        orders_clean, items_agg, payments_agg,
        reviews_clean, dfs["customers"], geo_clean
    )

    report(master)
    save(master)

    spark.stop()
    log.info(" Milestone 2 Done!")


if __name__ == "__main__":
    run()