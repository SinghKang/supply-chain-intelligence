# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Data Quality Checks — Silver Layer
# Custom PySpark DQ Framework
# Same concepts as Soda/Great Expectations
# ================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min, max, when, sum
from datetime import datetime

spark = SparkSession.builder \
    .appName("SupplyChain-Quality-Checks") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")\
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Reading Silver layer for quality checks...")
SILVER_PATH = "s3a://silver/shipments/"
df = spark.read.format("delta").load(SILVER_PATH)
total = df.count()
print(f"Total records: {total}\n")

# ── Quality Check Engine ─────────────────────────
results = []

def check(name, passed, actual=None, expected=None):
    status = "✅ PASS" if passed else "❌ FAIL"
    results.append({"name": name, "passed": passed})
    detail = f"(got {actual}, expected {expected})" if actual is not None else ""
    print(f"{status} — {name} {detail}")

print("="*50)
print("RUNNING DATA QUALITY CHECKS")
print("="*50)

# Check 1 — Row count
check("Minimum row count >= 50",
      total >= 50, total, ">= 50")

# Check 2 — Null checks
for col_name in ["carrier", "status", "origin", "destination"]:
    null_count = df.filter(col(col_name).isNull()).count()
    check(f"No nulls in {col_name}",
          null_count == 0, null_count, 0)

# Check 3 — Valid status values
valid_statuses = [
    "ORDER_PLACED", "PROCESSING", "SHIPPED",
    "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "DELAYED"
]
invalid_status_count = df.filter(
    ~col("status").isin(valid_statuses)
).count()
check("All status values are valid",
      invalid_status_count == 0, invalid_status_count, 0)

# Check 4 — Weight positive
min_weight = df.agg(min("weight_kg")).collect()[0][0]
check("All weights are positive",
      min_weight > 0, min_weight, "> 0")

# Check 5 — No duplicate shipment IDs
total_ids    = df.count()
distinct_ids = df.select("shipment_id").distinct().count()
check("No duplicate shipment IDs",
      total_ids == distinct_ids, 
      f"{total_ids - distinct_ids} duplicates", 0)

# Check 6 — DQ flag all PASS
dq_fails = df.filter(col("dq_flag") == "FAIL").count()
check("All DQ flags are PASS",
      dq_fails == 0, dq_fails, 0)

# ── Summary ──────────────────────────────────────
print("="*50)
passed = len([r for r in results if r["passed"]])
failed = len(results) - passed

print(f"\nTotal checks : {len(results)}")
print(f"Passed       : {passed}")
print(f"Failed       : {failed}")

if failed == 0:
    print("\n✅ ALL CHECKS PASSED — data is clean!")
else:
    print(f"\n❌ {failed} CHECKS FAILED — investigate before Gold layer!")

print("="*50)
spark.stop()