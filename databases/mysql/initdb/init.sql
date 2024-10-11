USE store;

CREATE TABLE customers (
    index_col INT AUTO_INCREMENT UNIQUE,
    customer_id VARCHAR(36) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    age INT,
    gender VARCHAR(10),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE products (
    index_col INT AUTO_INCREMENT UNIQUE,
    product_id VARCHAR(36) PRIMARY KEY,
    product_name VARCHAR(50),
    feature VARCHAR(50),
    target_audience VARCHAR(50),
    price INT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    index_col INT AUTO_INCREMENT UNIQUE,
    order_id VARCHAR(36) PRIMARY KEY,
    customer_id VARCHAR(36),
    product_id VARCHAR(36),
    quantity INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);