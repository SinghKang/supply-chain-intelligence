# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Silver Transformation Job
# Reads raw Bronze JSON → cleans → validates →
# applies business logic → writes to Silver as Delta
# ================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, trim, upper, lower,
    to_timestamp, current_timestamp,
    regexp_replace, lit, coalesce
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    DoubleType, BooleanType
)
from datetime import datetime

# ── Spark Session Setup ──────────────────────────
# Why local[*]: runs Spark locally using all CPU cores
# In production this would point to Databricks cluster
spark = SparkSession.builder \
    .appName("SupplyChain-Silver-Transform") \
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

# Reduce Spark log noise
spark.sparkContext.setLogLevel("ERROR")

print("✅ Spark Session started successfully!")
# Test MinIO connection
try:
    files = spark.sparkContext.wholeTextFiles("s3a://bronze/shipments/")
    print(f"Files found: {files.count()}")
except Exception as e:
    print(f"MinIO connection error: {e}")

# ── Paths ─────────────────────────────────────────
BRONZE_PATH = "s3a://bronze/shipments/2026/05/13/"
SILVER_PATH = "s3a://silver/shipments/"

def read_bronze():
    """
    Read raw JSON files from Bronze layer.
    Why JSON: Bronze stores data exactly as received
    from Kafka — raw JSON format.
    """
    print("Reading Bronze layer...")
    df = spark.read \
    .option("multiline", "true") \
    .json(BRONZE_PATH)
    print(f"Bronze records: {df.count()}")
    df.printSchema()
    return df

def clean_and_validate(df):
    """
    Apply cleaning and validation rules.

    Business rules:
    - Status must be valid
    - Origin and destination must not be null
    - Weight must be positive
    - Add data quality flag
    """
    print("Applying cleaning and validation...")

    valid_statuses = [
        "ORDER_PLACED", "PROCESSING", "SHIPPED",
        "IN_TRANSIT", "OUT_FOR_DELIVERY",
        "DELIVERED", "DELAYED"
    ]

    df_clean = df \
        .withColumn("status",
            upper(trim(col("status")))) \
        .withColumn("carrier",
            trim(col("carrier"))) \
        .withColumn("origin",
            trim(col("origin"))) \
        .withColumn("destination",
            trim(col("destination"))) \
        .withColumn("is_valid_status",
            col("status").isin(valid_statuses)) \
        .withColumn("is_valid_weight",
            col("weight_kg") > 0) \
        .withColumn("has_origin",
            col("origin").isNotNull()) \
        .withColumn("has_destination",
            col("destination").isNotNull()) \
        .withColumn("dq_flag",
            when(
                col("is_valid_status") &
                col("is_valid_weight") &
                col("has_origin") &
                col("has_destination"),
                "PASS"
            ).otherwise("FAIL")) \
        .withColumn("processed_at",
            current_timestamp())

    pass_count = df_clean.filter(col("dq_flag") == "PASS").count()
    fail_count = df_clean.filter(col("dq_flag") == "FAIL").count()

    print(f"✅ DQ PASS: {pass_count}")
    print(f"❌ DQ FAIL: {fail_count}")

    return df_clean

def apply_business_logic(df):
    """
    Apply business logic transformations.

    Business rules:
    - Categorize shipments by weight
    - Flag delayed shipments
    - Calculate if delivery was on time
    """
    print("Applying business logic...")

    df_biz = df \
        .withColumn("weight_category",
            when(col("weight_kg") < 1, "LIGHT")
            .when(col("weight_kg") < 10, "MEDIUM")
            .when(col("weight_kg") < 25, "HEAVY")
            .otherwise("EXTRA_HEAVY")) \
        .withColumn("is_delayed",
            col("status") == "DELAYED") \
        .withColumn("is_delivered",
            col("status") == "DELIVERED")

    return df_biz

def write_silver(df):
    """
    Write clean data to Silver layer as Delta format.

    Why Delta format:
    - ACID transactions — no corrupt data
    - Schema enforcement — bad records rejected
    - Time travel — query previous versions
    - Optimized reads for downstream Gold layer
    """
    print("Writing to Silver layer...")

    # Only write records that passed DQ checks
    df_pass = df.filter(col("dq_flag") == "PASS")

    df_pass.write \
        .format("delta") \
        .mode("overwrite") \
        .save(SILVER_PATH)

    print(f"✅ Silver layer written successfully!")
    print(f"Records written: {df_pass.count()}")

def main():
    print("Starting Silver transformation job...\n")
    df_bronze = read_bronze()
    df_clean  = clean_and_validate(df_bronze)
    df_biz    = apply_business_logic(df_clean)
    write_silver(df_biz)
    print("\n🎉 Silver transformation complete!")

if __name__ == "__main__":
    main()
    spark.stop()