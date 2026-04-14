from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg


MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT = "http://minio:9000"

def main():
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    print(">>> Reading from Silver...")
    silver_df = spark.read.format("delta").load("s3a://silver/data/orders")

    gold_df = silver_df.groupBy("product_name").agg(
        _sum("quantity").alias("total_quantity"),
        avg("price").alias("avg_price"),
        count("order_id").alias("total_orders")
    )

    print(">>> Gold data sample:")
    gold_df.show()

    print(">>> Writing to Gold...")
    gold_df.write.mode("overwrite").parquet("s3a://gold/data/product_stats")
    print(">>> Success!")

if __name__ == "__main__":
    main()