from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


KAFKA_BOOTSTRAP_SERVERS = "broker:29092"
TOPIC = "pg-server.public.orders"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT = "http://minio:9000"

def main():
    print(">>> Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("KafkaToMinioBronze") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(">>> Reading stream from Kafka...")
    # 1. Reading from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 2. Parsing data 
    json_df = df.selectExpr("CAST(value AS STRING) as json_value")

    print(">>> Starting write to MinIO (Delta Lake)...")
    # 3. Writing to MinIO (Bronze Layer - Raw Data)
    query = json_df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://bronze/checkpoints/orders") \
        .option("path", "s3a://bronze/data/orders") \
        .start()

    print(">>> Stream started! Data should flow to MinIO (Bronze Layer)")
    query.awaitTermination()

if __name__ == "__main__":
    main()