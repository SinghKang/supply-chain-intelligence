# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# GenAI Layer — Natural Language Insights
# Connects Gold Delta tables to Llama3 via Ollama
# Answers business questions about supply chain data
# ================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import ollama
import json

# ── Spark Session ────────────────────────────────
spark = SparkSession.builder \
    .appName("SupplyChain-GenAI") \
    .master("local[*]") \
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "admin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Load Gold Data ───────────────────────────────
print("Loading Gold layer data...")

carrier_df = spark.read.format("delta") \
    .load("s3a://gold/carrier_performance/").toPandas()

status_df = spark.read.format("delta") \
    .load("s3a://gold/shipment_status_summary/").toPandas()

route_df = spark.read.format("delta") \
    .load("s3a://gold/route_analysis/").toPandas()

product_df = spark.read.format("delta") \
    .load("s3a://gold/product_summary/").toPandas()

print("✅ Gold data loaded!")

# ── Build Data Context ───────────────────────────
# Why: LLM needs actual data to answer questions
# We convert Gold tables to readable text summary
# and pass it as context to Llama3

def build_context():
    """
    Convert Gold layer data into readable context
    for the LLM to reason about.
    Why text format: LLMs understand natural language
    better than raw JSON or CSV.
    """
    context = """
    You are a supply chain analytics expert. 
    Answer questions based ONLY on the data provided below.
    Be concise, specific, and use numbers from the data.
    
    === CARRIER PERFORMANCE DATA ===
    """
    
    for _, row in carrier_df.iterrows():
        context += f"""
    Carrier: {row['carrier']}
    - Total Shipments: {row['total_shipments']}
    - Delayed: {row['delayed_shipments']}
    - Delivered: {row['delivered_shipments']}
    - Delay Rate: {row['delay_rate_pct']}%
    - Avg Weight: {round(row['avg_weight_kg'], 2)} kg
    """

    context += "\n=== SHIPMENT STATUS SUMMARY ===\n"
    for _, row in status_df.iterrows():
        context += f"Status {row['status']}: {row['total_shipments']} shipments\n"

    context += "\n=== TOP DELAYED ROUTES ===\n"
    for _, row in route_df.head(5).iterrows():
        context += f"{row['origin']} → {row['destination']}: {row['delay_rate_pct']}% delay rate\n"

    context += "\n=== PRODUCT SUMMARY ===\n"
    for _, row in product_df.iterrows():
        context += f"{row['product']}: {row['total_shipments']} shipments, {row['delay_rate_pct']}% delay rate\n"

    return context

# ── Ask Question ─────────────────────────────────
def ask(question, context):
    """
    Send question + data context to Llama3.
    Why include context every time: LLMs have no memory
    of your data — we must provide it each call.
    """
    prompt = f"""
{context}

Based on the supply chain data above, answer this question:
{question}

Be specific with numbers. Keep answer under 5 sentences.
"""
    
    response = ollama.chat(
        model="llama3",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    return response["message"]["content"]

# ── Generate Weekly Insights ─────────────────────
def generate_weekly_insights(context):
    """
    Auto generate a weekly executive summary.
    Why: stakeholders want a summary not raw data.
    This replaces manual PPT creation — exactly
    like your GAP project at MathCo!
    """
    prompt = f"""
{context}

Generate a professional weekly supply chain executive summary with:
1. Overall performance overview
2. Top performing carrier
3. Most problematic carrier and recommendation  
4. Most delayed route and suggested action
5. Top shipped product
6. Key recommendation for next week

Format it as a clean executive report.
Keep it concise and actionable.
"""
    
    response = ollama.chat(
        model="llama3",
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    return response["message"]["content"]

# ── Main ─────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("SUPPLY CHAIN GENAI INSIGHTS ENGINE")
    print("Powered by Llama3 + Gold Layer Data")
    print("="*60 + "\n")

    context = build_context()

    # Predefined business questions
    questions = [
        "Which carrier has the highest delay rate and what is it?",
        "How many shipments are currently in transit?",
        "Which product has the most shipments?",
        "Which route has the highest delay rate?",
        "What percentage of shipments are delivered successfully?"
    ]

    print("📊 ANSWERING BUSINESS QUESTIONS\n")
    for q in questions:
        print(f"❓ {q}")
        answer = ask(q, context)
        print(f"💡 {answer}")
        print("-"*60)

    print("\n📋 GENERATING WEEKLY EXECUTIVE SUMMARY\n")
    summary = generate_weekly_insights(context)
    print(summary)

    # Save summary to file
    with open("genai/weekly_summary.txt", "w") as f:
        f.write("SUPPLY CHAIN WEEKLY INSIGHTS\n")
        f.write("="*60 + "\n\n")
        f.write(summary)

    print("\n✅ Weekly summary saved to genai/weekly_summary.txt")
    print("="*60)

if __name__ == "__main__":
    main()
    spark.stop()