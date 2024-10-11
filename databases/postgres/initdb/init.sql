CREATE SCHEMA store;

CREATE TABLE store.customer_revenue (
    customer_id VARCHAR(50) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    total_revenue INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE store.product_revenue (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(50),
    feature VARCHAR(50),
    target_audience VARCHAR(50),
    total_revenue INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)