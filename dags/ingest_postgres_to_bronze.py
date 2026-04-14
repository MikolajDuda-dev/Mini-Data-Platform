import pandas as pd
import psycopg2
import sys
from datetime import datetime


DB_PARAMS = {
    "host": "postgres-db", "database": "retail_db", "user": "myuser", "password": "mypassword", "port": "5432"
}
MINIO_OPTS = {
    "key": "minioadmin", "secret": "minioadmin", "client_kwargs": {"endpoint_url": "http://minio:9000"}
}

def ingest_data():
    print(">>> [Ingest] Connecting to Postgres...")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        query = "SELECT * FROM orders"
        df = pd.read_sql(query, conn)
        conn.close()
    except Exception as e:
        print(f"Postgres Error: {e}")
        sys.exit(1)

    if df.empty:
        print(">>> No data in orders table.")
        return

    filename = f"orders_{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"
    path = f"s3://bronze/data/orders/{filename}"
    
    print(f">>> [Ingest] Saving {len(df)} rows to: {path}")
    try:
        df.to_parquet(path, storage_options=MINIO_OPTS, index=False)
        print(">>> Success!")
    except Exception as e:
        print(f"MinIO write error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    ingest_data()