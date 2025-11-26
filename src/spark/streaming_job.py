from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import logging

from schemas import payment_schema, enriched_payment_schema
from utils import transform_payments
from sinks import write_raw_events, write_enriched_events, write_to_postgres


# Spark Session
def create_spark_session():
    # Test MinIO connection first
    try:
        spark = (
            SparkSession.builder.appName("PaymentsKafkaSparkStreaming")
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
            .config("spark.hadoop.fs.s3a.access.key", "minio")
            .config("spark.hadoop.fs.s3a.secret.key", "minio123")
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # This allows creating buckets if they don't exist
            .config("spark.hadoop.fs.s3a.bucket.probe", "0")
            .getOrCreate()
        )
        print("✅ Spark session with MinIO configured successfully")
        return spark
    except Exception as e:
        print(f"❌ Spark session creation failed: {e}")
        raise


# Read stream from Kafka
def read_kafka_stream(spark):
    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "payment_stream")
        .option("startingOffsets", "latest")
        .load()
    )
    return df


# Apply Schema
def parse_kafka_value(kafka_df):
    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) as json")
        .select(from_json(col("json"), payment_schema).alias("data"))
        .select("data.*")
    )

    return parsed_df


# Entry point
if __name__ == "__main__":
    spark = create_spark_session()

    kafka_df = read_kafka_stream(spark)
    payments_df = parse_kafka_value(kafka_df)

    print("🚀 Starting streaming... \n")

    # TEST 1: Just console output first
    console_query = (
        payments_df.writeStream.outputMode("append")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    # Wait a bit to see data flowing
    import time

    time.sleep(30)  # Let it run for 30 seconds to see data

    # Then stop and test S3 sinks one by one
    console_query.stop()

    # TEST 2: S3 raw events only
    write_raw_events(payments_df)

    spark.streams.awaitAnyTermination()

    # # 1. Store RAW events to MinIO (Parquet)
    # write_raw_events(payments_df)

    # # 2. Apply business logic (grace period, delays, etc.)
    # enriched_payments_df = transform_payments(payments_df)

    # # 3. Store enriched data
    # write_enriched_events(enriched_payments_df)

    # # 4. Store to PostgreSQL
    # url = "jdbc:postgresql://postgres:5432/payments"
    # properties = {
    #     "user": "admin",
    #     "password": "admin123",
    #     "driver": "org.postgresql.Driver",
    # }

    # write_to_postgres(enriched_payments_df, "enriched_payments", url, properties)

    # spark.streams.awaitAnyTermination()
