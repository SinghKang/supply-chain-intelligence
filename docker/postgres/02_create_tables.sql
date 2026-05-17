-- ================================================
-- SUPPLY CHAIN INTELLIGENCE PLATFORM
-- Source Database Schema
-- ================================================

\c supplychain_db;

-- SUPPLIERS TABLE
CREATE TABLE IF NOT EXISTS suppliers (
    supplier_id     SERIAL PRIMARY KEY,
    supplier_name   VARCHAR(100) NOT NULL,
    country         VARCHAR(50),
    region          VARCHAR(50),
    contact_email   VARCHAR(100),
    rating          DECIMAL(3,2),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PRODUCTS TABLE
CREATE TABLE IF NOT EXISTS products (
    product_id      SERIAL PRIMARY KEY,
    product_name    VARCHAR(100) NOT NULL,
    category        VARCHAR(50),
    unit_price      DECIMAL(10,2),
    supplier_id     INT REFERENCES suppliers(supplier_id),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- INVENTORY TABLE
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id    SERIAL PRIMARY KEY,
    product_id      INT REFERENCES products(product_id),
    warehouse_id    VARCHAR(20),
    quantity        INT NOT NULL,
    reorder_level   INT DEFAULT 100,
    last_updated    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ORDERS TABLE
CREATE TABLE IF NOT EXISTS orders (
    order_id          SERIAL PRIMARY KEY,
    product_id        INT REFERENCES products(product_id),
    supplier_id       INT REFERENCES suppliers(supplier_id),
    quantity          INT NOT NULL,
    order_status      VARCHAR(20) DEFAULT 'PENDING',
    order_date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expected_delivery DATE,
    actual_delivery   DATE
);

-- SHIPMENTS TABLE
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id      SERIAL PRIMARY KEY,
    order_id         INT REFERENCES orders(order_id),
    carrier          VARCHAR(50),
    tracking_number  VARCHAR(100),
    status           VARCHAR(20) DEFAULT 'IN_TRANSIT',
    origin           VARCHAR(100),
    destination      VARCHAR(100),
    shipped_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    delivered_at     TIMESTAMP
);