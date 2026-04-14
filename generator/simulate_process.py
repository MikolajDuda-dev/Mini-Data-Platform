import time
import random
import psycopg2
from faker import Faker
from psycopg2.extras import execute_values

# Konfiguracja połączenia (zgodna z docker-compose)
DB_PARAMS = {
    "host": "localhost",
    "database": "retail_db",
    "user": "myuser",
    "password": "mypassword",
    "port": "5432"
}

fake = Faker()

def create_tables(conn):
    """Tworzy tabelę orders jeśli nie istnieje """
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
    print("Tabele sprawdzone/utworzone.")

def insert_data(conn):
    """Generuje i wstawia syntetyczne dane [cite: 94, 96]"""
    cur = conn.cursor()
    
    # Generowanie losowego zamówienia
    customer = fake.name()
    product = random.choice(['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'])
    quantity = random.randint(1, 5)
    price = round(random.uniform(20.0, 1500.0), 2)
    
    sql = """INSERT INTO orders (customer_name, product_name, quantity, price)
             VALUES (%s, %s, %s, %s)"""
    
    cur.execute(sql, (customer, product, quantity, price))
    conn.commit()
    print(f"Dodano zamówienie: {product} dla {customer}")
    cur.close()

if __name__ == "__main__":
    # Czekamy chwilę na start bazy danych
    time.sleep(2) 
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        create_tables(conn)
        
        # Pętla nieskończona symulująca ciągły biznes
        while True:
            insert_data(conn)
            # Symulacja odstępów czasu między zamówieniami (np. 2 sekundy)
            time.sleep(2)
            
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()