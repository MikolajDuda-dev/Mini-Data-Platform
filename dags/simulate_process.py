import time
import random
import psycopg2
import sys

try:
    from faker import Faker
except ImportError:
    print("Brak biblioteki Faker. Zainstaluj ją.")
    sys.exit(1)

DB_PARAMS = {
    "host": "postgres-db",
    "database": "retail_db",
    "user": "myuser",
    "password": "mypassword",
    "port": "5432"
}

fake = Faker()

def create_tables(conn):
    commands = (
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255) NOT NULL,
            product_name VARCHAR(255) NOT NULL,
            quantity INTEGER NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    cur = conn.cursor()
    for command in commands:
        cur.execute(command)
    cur.close()
    conn.commit()

def insert_data(conn):
    cur = conn.cursor()
    customer = fake.name()
    product = random.choice(['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'])
    quantity = random.randint(1, 5)
    price = round(random.uniform(20.0, 1500.0), 2)
    
    sql = """INSERT INTO orders (customer_name, product_name, quantity, price)
             VALUES (%s, %s, %s, %s)"""
    
    cur.execute(sql, (customer, product, quantity, price))
    conn.commit()
    print(f"Dodano: {product} | Klient: {customer}")
    cur.close()

if __name__ == "__main__":
    time.sleep(1) 
    
    conn = None
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_tables(conn)
        print(">>> Tabela orders gotowa.")
        
        
        print(">>> Rozpoczynam generowanie paczki danych (20 rekordów)...")
        for _ in range(20):
            insert_data(conn)
            time.sleep(0.1)
            
        print(">>> Zakończono generowanie danych.")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Błąd: {error}")
        sys.exit(1) 
    finally:
        if conn is not None:
            conn.close()