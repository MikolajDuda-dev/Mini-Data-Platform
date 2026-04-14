from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# MinIO Configuration
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT = "http://minio:9000"

def main():
    print(">>> Initializing Spark Session (Bronze -> Silver)...")
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # 1. Loading data from Bronze (Raw Parquet Files)
    print(">>> Reading from Bronze layer...")
    try:
        bronze_df = spark.read.parquet("s3a://bronze/data/orders/*.parquet")
        print(">>> Successfully read Parquet files from Bronze.")
    except Exception as e:
        print(f"!!! Bronze read error. Message: {e}")
        import sys
        sys.exit(1) 

    # 2. Transformations (Data Cleaning & Type Casting)i
    print(">>> Transforming data...")
    final_df = bronze_df.select(
        col("order_id").cast(IntegerType()),
        col("customer_name"),
        col("product_name"),
        col("quantity").cast(IntegerType()),
        col("price").cast(DoubleType()),
        col("order_date").cast(TimestampType())
    )

    print(">>> Sample data after transformation:")
    final_df.show(5, truncate=False)

    # 3. Writing to Silver (Delta Lake)
    print(">>> Writing to Silver layer (Delta format)...")
    final_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .save("s3a://silver/data/orders")
    
    print(">>> SUCCESS! Data saved in Silver Layer.")

if __name__ == "__main__":
    main()