# ================================================
# SUPPLY CHAIN INTELLIGENCE PLATFORM
# Postgres Seed Script — Generates realistic
# master data for suppliers, products, inventory
# and historical orders
# ================================================

import psycopg2
import random
from datetime import datetime, timedelta

# ── Configuration ────────────────────────────────
DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 5432,
    "database": "supplychain_db",
    "user"    : "supplychain",
    "password": "supply123"
}

# ── Realistic Sample Data ─────────────────────────
SUPPLIER_NAMES = [
    "TechSupply Co", "Global Parts Ltd", "FastShip India",
    "Premium Components", "Reliable Vendors", "QuickSource",
    "MegaSupply", "ProParts India", "SwiftVendors", "TopTier Supply",
    "Eastern Traders", "Western Components", "Northern Parts",
    "Southern Supply", "Central Vendors"
]

COUNTRIES = ["India", "China", "USA", "Germany", "Japan", "UK", "France"]
REGIONS   = ["North", "South", "East", "West", "Central"]

PRODUCT_NAMES = [
    "Laptop Pro 15", "Smartphone X12", "Tablet Air",
    "Wireless Headphones", "4K Monitor", "Mechanical Keyboard",
    "Gaming Mouse", "USB Hub", "Webcam HD", "SSD 1TB",
    "RAM 16GB", "Graphics Card", "Motherboard", "Power Supply",
    "Cooling Fan", "LED Strip", "Smart Watch", "Earbuds Pro",
    "Portable Charger", "Cable USB-C"
]

CATEGORIES  = ["Electronics", "Accessories", "Storage", "Computing", "Wearables"]
WAREHOUSES  = ["WH-MUMBAI", "WH-DELHI", "WH-BANGALORE", "WH-CHENNAI", "WH-HYDERABAD"]
CARRIERS    = ["FedEx", "DHL", "Blue Dart", "DTDC", "Delhivery"]
ORDER_STATUSES = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]

def connect():
    return psycopg2.connect(**DB_CONFIG)

def seed_suppliers(cur, count=15):
    """Insert realistic supplier records"""
    print(f"Seeding {count} suppliers...")
    for i in range(count):
        cur.execute("""
            INSERT INTO suppliers 
            (supplier_name, country, region, contact_email, rating, is_active)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            SUPPLIER_NAMES[i],
            random.choice(COUNTRIES),
            random.choice(REGIONS),
            f"contact@{SUPPLIER_NAMES[i].lower().replace(' ', '')}.com",
            round(random.uniform(2.5, 5.0), 2),
            random.choice([True, True, True, False])  # 75% active
        ))
    print(f"✅ {count} suppliers inserted")

def seed_products(cur, count=20):
    """Insert realistic product records"""
    print(f"Seeding {count} products...")
    for i in range(count):
        cur.execute("""
            INSERT INTO products 
            (product_name, category, unit_price, supplier_id)
            VALUES (%s, %s, %s, %s)
        """, (
            PRODUCT_NAMES[i],
            random.choice(CATEGORIES),
            round(random.uniform(500, 150000), 2),
            random.randint(1, 15)  # reference supplier
        ))
    print(f"✅ {count} products inserted")

def seed_inventory(cur, count=50):
    """Insert inventory records across warehouses"""
    print(f"Seeding {count} inventory records...")
    for _ in range(count):
        cur.execute("""
            INSERT INTO inventory 
            (product_id, warehouse_id, quantity, reorder_level)
            VALUES (%s, %s, %s, %s)
        """, (
            random.randint(1, 20),
            random.choice(WAREHOUSES),
            random.randint(0, 1000),
            random.randint(50, 200)
        ))
    print(f"✅ {count} inventory records inserted")

def seed_orders(cur, count=500):
    """Insert 500 historical orders over last 6 months"""
    print(f"Seeding {count} orders...")
    for _ in range(count):
        order_date        = datetime.now() - timedelta(days=random.randint(1, 180))
        expected_delivery = order_date + timedelta(days=random.randint(3, 14))
        status            = random.choice(ORDER_STATUSES)

        actual_delivery = None
        if status == "DELIVERED":
            actual_delivery = (expected_delivery + timedelta(days=random.randint(-2, 5))).date()

        cur.execute("""
            INSERT INTO orders 
            (product_id, supplier_id, quantity, order_status, 
             order_date, expected_delivery, actual_delivery)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            random.randint(1, 20),
            random.randint(1, 15),
            random.randint(1, 100),
            status,
            order_date,
            expected_delivery.date(),
            actual_delivery
        ))
    print(f"✅ {count} orders inserted")

def main():
    print("Starting Postgres seed...")
    conn = connect()
    cur  = conn.cursor()

    try:
        seed_suppliers(cur)
        conn.commit()

        seed_products(cur)
        conn.commit()

        seed_inventory(cur)
        conn.commit()

        seed_orders(cur)
        conn.commit()

        print("\n🎉 All seed data inserted successfully!")
        print("Suppliers : 15")
        print("Products  : 20")
        print("Inventory : 50")
        print("Orders    : 500")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()