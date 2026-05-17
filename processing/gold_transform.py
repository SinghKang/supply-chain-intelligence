# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Gold Transformation Job
# Reads clean Silver Delta data → aggregates →
# creates business KPIs → writes to Gold layer
# ================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum, avg, round,
    when, lit, current_timestamp,
    countDistinct, max, min
)

# ── Spark Session Setup ──────────────────────────
spark = SparkSession.builder \
    .appName("SupplyChain-Gold-Transform") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark Session started!")

# ── Paths ─────────────────────────────────────────
SILVER_PATH = "s3a://silver/shipments/"
GOLD_PATH   = "s3a://gold/"

def read_silver():
    """Read clean Silver Delta table"""
    print("Reading Silver layer...")
    df = spark.read.format("delta").load(SILVER_PATH)
    print(f"Silver records: {df.count()}")
    return df

def carrier_performance(df):
    """
    KPI 1 — Carrier Performance Scorecard
    Which carrier has most delays?
    Which carrier delivers most shipments?

    Why this matters: business can decide
    which carrier to use based on performance
    """
    print("Building carrier performance KPI...")

    df_carrier = df.groupBy("carrier") \
        .agg(
            count("shipment_id").alias("total_shipments"),
            sum(when(col("is_delayed"), 1).otherwise(0))
                .alias("delayed_shipments"),
            sum(when(col("is_delivered"), 1).otherwise(0))
                .alias("delivered_shipments"),
            avg("weight_kg").alias("avg_weight_kg"),
            round(
                sum(when(col("is_delayed"), 1).otherwise(0)) * 100.0
                / count("shipment_id"), 2
            ).alias("delay_rate_pct")
        ) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy(col("delay_rate_pct").desc())

    df_carrier.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}carrier_performance/")

    print(f"✅ Carrier performance written!")
    df_carrier.show()
    return df_carrier

def shipment_status_summary(df):
    """
    KPI 2 — Shipment Status Summary
    How many shipments in each status?

    Why this matters: operations team needs
    to know how many are in transit, delayed etc
    """
    print("Building shipment status summary...")

    df_status = df.groupBy("status") \
        .agg(
            count("shipment_id").alias("total_shipments"),
            countDistinct("order_id").alias("unique_orders")
        ) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy(col("total_shipments").desc())

    df_status.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}shipment_status_summary/")

    print(f"✅ Status summary written!")
    df_status.show()
    return df_status

def route_analysis(df):
    """
    KPI 3 — Route Analysis
    Which origin-destination routes have most delays?

    Why this matters: identify problematic routes
    and optimize logistics accordingly
    """
    print("Building route analysis...")

    df_route = df.groupBy("origin", "destination") \
        .agg(
            count("shipment_id").alias("total_shipments"),
            sum(when(col("is_delayed"), 1).otherwise(0))
                .alias("delayed_shipments"),
            round(
                sum(when(col("is_delayed"), 1).otherwise(0)) * 100.0
                / count("shipment_id"), 2
            ).alias("delay_rate_pct"),
            avg("weight_kg").alias("avg_weight_kg")
        ) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy(col("delay_rate_pct").desc())

    df_route.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}route_analysis/")

    print(f"✅ Route analysis written!")
    df_route.show()
    return df_route

def product_shipment_summary(df):
    """
    KPI 4 — Product Shipment Summary
    Which products are shipped most?
    Which have highest delay rates?

    Why this matters: inventory and
    procurement teams need this data
    """
    print("Building product shipment summary...")

    df_product = df.groupBy("product") \
        .agg(
            count("shipment_id").alias("total_shipments"),
            sum("quantity").alias("total_quantity"),
            sum(when(col("is_delayed"), 1).otherwise(0))
                .alias("delayed_shipments"),
            round(
                sum(when(col("is_delayed"), 1).otherwise(0)) * 100.0
                / count("shipment_id"), 2
            ).alias("delay_rate_pct"),
            avg("weight_kg").alias("avg_weight_kg")
        ) \
        .withColumn("processed_at", current_timestamp()) \
        .orderBy(col("total_shipments").desc())

    df_product.write \
        .format("delta") \
        .mode("overwrite") \
        .save(f"{GOLD_PATH}product_summary/")

    print(f"✅ Product summary written!")
    df_product.show()
    return df_product

def main():
    print("Starting Gold transformation job...\n")

    df_silver = read_silver()

    carrier_performance(df_silver)
    shipment_status_summary(df_silver)
    route_analysis(df_silver)
    product_shipment_summary(df_silver)

    print("\n🎉 Gold layer complete!")
    print("KPIs written:")
    print("  - carrier_performance")
    print("  - shipment_status_summary")
    print("  - route_analysis")
    print("  - product_summary")

if __name__ == "__main__":
    main()
    spark.stop()