# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Streamlit Dashboard — Production Grade UI
# ================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
import time

# ── Page Config ──────────────────────────────────
st.set_page_config(
    page_title="Supply Chain Intelligence",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ───────────────────────────────────
st.markdown("""
<style>
    /* Main background */
    .stApp {
        background-color: #0f172a;
        color: #e2e8f0;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background-color: #1e293b;
    }
    
    /* KPI Cards */
    .kpi-card {
        background: linear-gradient(135deg, #1e293b, #0f172a);
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
        transition: transform 0.2s;
    }
    
    .kpi-card:hover {
        transform: translateY(-2px);
        border-color: #3b82f6;
    }
    
    .kpi-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #3b82f6;
        margin: 0;
    }
    
    .kpi-label {
        font-size: 0.85rem;
        color: #94a3b8;
        margin-top: 4px;
        text-transform: uppercase;
        letter-spacing: 1px;
    }

    .kpi-delta {
        font-size: 0.8rem;
        color: #10b981;
        margin-top: 4px;
    }

    /* Section headers */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #e2e8f0;
        padding: 8px 0;
        border-bottom: 2px solid #3b82f6;
        margin-bottom: 16px;
    }

    /* Status badges */
    .badge-green {
        background: #064e3b;
        color: #10b981;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }

    .badge-red {
        background: #450a0a;
        color: #ef4444;
        padding: 2px 10px;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }

    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# ── Spark Session ────────────────────────────────
@st.cache_resource
def get_spark():
    """
    Cache Spark session — only created once.
    Why cache_resource: expensive to create,
    reuse across reruns.
    """
    spark = SparkSession.builder \
        .appName("SupplyChain-Dashboard") \
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
    return spark

# ── Data Loading ─────────────────────────────────
@st.cache_data(ttl=300)  # refresh every 5 mins
def load_carrier_data():
    spark = get_spark()
    df = spark.read.format("delta") \
        .load("s3a://gold/carrier_performance/")
    return df.toPandas()

@st.cache_data(ttl=300)
def load_status_data():
    spark = get_spark()
    df = spark.read.format("delta") \
        .load("s3a://gold/shipment_status_summary/")
    return df.toPandas()

@st.cache_data(ttl=300)
def load_route_data():
    spark = get_spark()
    df = spark.read.format("delta") \
        .load("s3a://gold/route_analysis/")
    return df.toPandas()

@st.cache_data(ttl=300)
def load_product_data():
    spark = get_spark()
    df = spark.read.format("delta") \
        .load("s3a://gold/product_summary/")
    return df.toPandas()

# ── Sidebar ──────────────────────────────────────
with st.sidebar:
    st.markdown("### 🚚 Supply Chain Intel")
    st.markdown("---")
    st.markdown("**Pipeline Status**")
    st.markdown('<span class="badge-green">● LIVE</span>', 
                unsafe_allow_html=True)
    st.markdown("")
    st.markdown("**Data Layers**")
    st.markdown("✅ Bronze — Raw Events")
    st.markdown("✅ Silver — Clean Data")
    st.markdown("✅ Gold — KPIs")
    st.markdown("✅ Quality Checks")
    st.markdown("---")
    
    auto_refresh = st.toggle("Auto Refresh (5min)", value=False)
    if st.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()
    
    st.markdown("---")
    st.markdown("**Built with**")
    st.markdown("Kafka • PySpark • Delta Lake")
    st.markdown("Airflow • MinIO • Streamlit")

# ── Header ───────────────────────────────────────
st.markdown("""
<div style='padding: 20px 0 10px 0;'>
    <h1 style='color: #f1f5f9; font-size: 2rem; margin: 0;'>
        🚚 Supply Chain Intelligence Platform
    </h1>
    <p style='color: #64748b; margin: 4px 0 0 0; font-size: 0.9rem;'>
        Real-time insights powered by Kafka • PySpark • Delta Lake • Airflow
    </p>
</div>
""", unsafe_allow_html=True)

st.markdown("---")

# ── Load Data ────────────────────────────────────
with st.spinner("Loading data from Gold layer..."):
    try:
        carrier_df = load_carrier_data()
        status_df  = load_status_data()
        route_df   = load_route_data()
        product_df = load_product_data()
        data_loaded = True
    except Exception as e:
        st.error(f"Error loading data: {e}")
        data_loaded = False

if data_loaded:

    # ── KPI Cards ────────────────────────────────
    st.markdown('<p class="section-header">📊 Key Performance Indicators</p>',
                unsafe_allow_html=True)

    total_shipments = int(carrier_df["total_shipments"].sum())
    total_delayed   = int(carrier_df["delayed_shipments"].sum())
    total_delivered = int(carrier_df["delivered_shipments"].sum())
    avg_delay_rate  = round(carrier_df["delay_rate_pct"].mean(), 1)

    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value">{total_shipments:,}</p>
            <p class="kpi-label">Total Shipments</p>
            <p class="kpi-delta">↑ Updated today</p>
        </div>
        """, unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value" style="color: #10b981">{total_delivered:,}</p>
            <p class="kpi-label">Delivered</p>
            <p class="kpi-delta">✅ Successfully delivered</p>
        </div>
        """, unsafe_allow_html=True)

    with k3:
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value" style="color: #ef4444">{total_delayed:,}</p>
            <p class="kpi-label">Delayed</p>
            <p class="kpi-delta">⚠️ Needs attention</p>
        </div>
        """, unsafe_allow_html=True)

    with k4:
        color = "#ef4444" if avg_delay_rate > 20 else "#f59e0b" if avg_delay_rate > 10 else "#10b981"
        st.markdown(f"""
        <div class="kpi-card">
            <p class="kpi-value" style="color: {color}">{avg_delay_rate}%</p>
            <p class="kpi-label">Avg Delay Rate</p>
            <p class="kpi-delta">Across all carriers</p>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts Row 1 ─────────────────────────────
    st.markdown('<p class="section-header">🚛 Carrier Performance</p>',
                unsafe_allow_html=True)

    c1, c2 = st.columns(2)

    with c1:
        fig = px.bar(
            carrier_df.sort_values("delay_rate_pct", ascending=True),
            x="delay_rate_pct",
            y="carrier",
            orientation="h",
            title="Delay Rate by Carrier (%)",
            color="delay_rate_pct",
            color_continuous_scale=["#10b981", "#f59e0b", "#ef4444"],
            template="plotly_dark"
        )
        fig.update_layout(
            paper_bgcolor="#1e293b",
            plot_bgcolor="#1e293b",
            font_color="#e2e8f0",
            showlegend=False,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.bar(
            carrier_df.sort_values("total_shipments", ascending=False),
            x="carrier",
            y="total_shipments",
            title="Total Shipments by Carrier",
            color="total_shipments",
            color_continuous_scale=["#1e40af", "#3b82f6", "#93c5fd"],
            template="plotly_dark"
        )
        fig.update_layout(
            paper_bgcolor="#1e293b",
            plot_bgcolor="#1e293b",
            font_color="#e2e8f0",
            showlegend=False,
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Charts Row 2 ─────────────────────────────
    st.markdown('<p class="section-header">📦 Shipment Status & Products</p>',
                unsafe_allow_html=True)

    c3, c4 = st.columns(2)

    with c3:
        fig = px.pie(
            status_df,
            values="total_shipments",
            names="status",
            title="Shipment Status Distribution",
            color_discrete_sequence=px.colors.sequential.Blues_r,
            template="plotly_dark",
            hole=0.4
        )
        fig.update_layout(
            paper_bgcolor="#1e293b",
            plot_bgcolor="#1e293b",
            font_color="#e2e8f0"
        )
        st.plotly_chart(fig, use_container_width=True)

    with c4:
        fig = px.bar(
            product_df.sort_values("total_shipments", ascending=False).head(10),
            x="product",
            y="total_shipments",
            title="Top Products by Shipment Volume",
            color="delay_rate_pct",
            color_continuous_scale=["#10b981", "#f59e0b", "#ef4444"],
            template="plotly_dark"
        )
        fig.update_layout(
            paper_bgcolor="#1e293b",
            plot_bgcolor="#1e293b",
            font_color="#e2e8f0",
            coloraxis_showscale=False
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Route Analysis ────────────────────────────
    st.markdown('<p class="section-header">🗺️ Route Analysis</p>',
                unsafe_allow_html=True)

    route_df["route"] = route_df["origin"] + " → " + route_df["destination"]
    fig = px.bar(
        route_df.sort_values("delay_rate_pct", ascending=False).head(10),
        x="route",
        y="delay_rate_pct",
        title="Top 10 Routes by Delay Rate (%)",
        color="delay_rate_pct",
        color_continuous_scale=["#10b981", "#f59e0b", "#ef4444"],
        template="plotly_dark"
    )
    fig.update_layout(
        paper_bgcolor="#1e293b",
        plot_bgcolor="#1e293b",
        font_color="#e2e8f0",
        coloraxis_showscale=False,
        xaxis_tickangle=-45
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── Raw Data Tables ───────────────────────────
    st.markdown('<p class="section-header">📋 Detailed Data</p>',
                unsafe_allow_html=True)

    tab1, tab2, tab3 = st.tabs([
        "🚛 Carrier Performance",
        "📦 Product Summary",
        "🗺️ Route Analysis"
    ])

    with tab1:
        st.dataframe(
            carrier_df[["carrier", "total_shipments",
                        "delivered_shipments", "delayed_shipments",
                        "delay_rate_pct", "avg_weight_kg"]],
            use_container_width=True,
            hide_index=True
        )

    with tab2:
        st.dataframe(
            product_df[["product", "total_shipments",
                        "total_quantity", "delay_rate_pct",
                        "avg_weight_kg"]],
            use_container_width=True,
            hide_index=True
        )

    with tab3:
        st.dataframe(
            route_df[["route", "total_shipments",
                      "delayed_shipments", "delay_rate_pct",
                      "avg_weight_kg"]],
            use_container_width=True,
            hide_index=True
        )

    # ── Footer ────────────────────────────────────
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #475569; font-size: 0.8rem; padding: 10px'>
        Supply Chain Intelligence Platform • Built by Lovepreet Singh •
        Kafka • PySpark • Delta Lake • Airflow • MinIO • Streamlit
    </div>
    """, unsafe_allow_html=True)

    # Auto refresh
    if auto_refresh:
        time.sleep(300)
        st.rerun()