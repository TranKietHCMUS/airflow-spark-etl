import random
import time
import uuid
import os
import mysql.connector

database = {
    "host": os.getenv("MYSQL_HOST"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "database": os.getenv("MYSQL_DATABASE")
}

first_names = ["Alice", "Bob", "Charlie", "David", "Eve", "Frank", "Grace", "Hannah", "Isaac", "Jack", "Katherine",
    "Liam", "Mia", "Noah", "Olivia", "Paul", "Quinn", "Ryan", "Sophia", "Tina"]

last_names = ["Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor", "Anderson", 
              "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson"]

genders = ['male', 'female', 'other']

product_names = [
    "X-Pro Max", "Zeta Vision", "Quantum Z1", "NeoSpark", "GigaX S10",
    "Titan Pro", "Velocity Edge", "Aero X5", "Fusion 8", "Pulse Nova"
]

features_list = [
    "5G connectivity", "AI-powered camera", "wireless charging", 
    "120Hz AMOLED display", "fast processor", "48MP triple camera", 
    "long-lasting battery", "face recognition", "in-display fingerprint sensor",
    "foldable design", "durable build", "ultra-fast refresh rate"
]

target_audiences = [
    "tech enthusiasts", "photography lovers", "business professionals", 
    "students", "gamers", "content creators"
]

def generate_customers(num_customers):
    customers = []
    for i in range(num_customers):
        customer = {
            "customer_id": str(uuid.uuid4()),
            "first_name": random.choice(first_names),
            "last_name": random.choice(last_names),
            "age": random.randint(18, 100),
            "gender": random.choice(genders)
        }
        customers.append(customer)
    return customers

def generate_products(num_products):
    products = []
    for i in range(num_products):
        product = {
            "product_id": str(uuid.uuid4()),
            "product_name": random.choice(product_names),
            "feature": random.choice(features_list),
            "target_audience": random.choice(target_audiences),
            "price": random.randint(1, 1000)
        }
        products.append(product)
    return products

def generate_orders(customers, products, num_orders):
    orders = []
    for i in range(num_orders):
        customer = random.choice(customers)
        product = random.choice(products)
        quantity = random.randint(1, 20)
        order = {
            "order_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity
        }
        orders.append(order)
    return orders

def insert_sql(cursor, table_name, data_list):
    if not data_list:
        return
    
    columns = ', '.join(data_list[0].keys())
    
    placeholders = ', '.join(['%s'] * len(data_list[0]))
    
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    values = [tuple(record.values()) for record in data_list]
    
    cursor.executemany(sql, values)
    connection.commit()


try:
    connection = mysql.connector.connect(**database)
    if connection.is_connected():
        print("Connect to MySQL successfully!")
    
    cursor = connection.cursor()
    
    customers = generate_customers(int(os.getenv("NUM_CUSTOMERS")))
    products = generate_products(int(os.getenv("NUM_PRODUCTS")))
    orders = generate_orders(customers, products, int(os.getenv("NUM_ORDERS")))

    insert_sql(cursor, "customers", customers)
    insert_sql(cursor, "products", products)
    insert_sql(cursor, "orders", orders)
    
    while (True):
        new_customers = generate_customers(int(os.getenv("INCREASE_CUSTOMERS")))
        new_products = generate_products(int(os.getenv("INCREASE_PRODUCTS")))

        customers += new_customers
        products += new_products

        new_orders = generate_orders(customers, products, int(os.getenv("INCREASE_ORDERS")))
        orders += new_orders

        insert_sql(cursor, "customers", new_customers)
        insert_sql(cursor, "products", new_products)
        insert_sql(cursor, "orders", new_orders)

        time.sleep(int(os.getenv("TIME_SLEEP")))


except mysql.connector.Error as e:
    print(f"Error when conenct to MySQL: {e}")

finally:
    if connection is not None and connection.is_connected():
        connection.close()
        print("Connect ends!")