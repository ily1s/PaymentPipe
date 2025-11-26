import os
from pyspark.sql import DataFrame


# ---------- MinIO (S3) Parquet Sink ----------
"""
Write a DataFrame to MinIO as Parquet.
- base_path: s3a://bucket/path (e.g. s3a://payments/raw/payments)
- partition_by: list of columns to partition by (e.g. ["year", "month", "day"])
- mode: append/overwrite
"""


# ---------- BRONZE (RAW) -> MinIO ----------
def write_raw_events(df):
    try:
        query = (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", "s3a://lake-bronze/payments")
            .option("checkpointLocation", "s3a://lake-checkpoints-raw")
            .start()
        )
        return query
    except Exception as e:
        print(f"❌ Error starting raw events sink: {e}")
        # Fallback to console
        return df.writeStream.outputMode("append").format("console").start()


# ---------- SILVER (ENRICHED) -> MinIO ----------
def write_enriched_events(df):
    try:
        query = (
            df.writeStream.outputMode("append")
            .format("parquet")
            .option("path", "s3a://lake-silver/payments")
            .option("checkpointLocation", "s3a://lake-checkpoints-enriched")
            .start()
        )
        return query 
    except Exception as e:
        print(f"❌ Error starting enriched events sink: {e}")
        # Fallback to console
        return df.writeStream.outputMode("append").format("console").start()


# ---------- GOLD -> PostgreSQL ----------
# ---------- Postgres JDBC Sink ----------
def write_to_postgres(df: DataFrame, table: str, url: str, properties: dict):
    """
    Write a Spark DataFrame to Postgres using JDBC.
    - table: target table name (schema.table or just table)
    - url: jdbc:postgresql://host:port/dbname
    - properties: dict with "user", "password", "driver"
    """
    df.writeStream.foreachBatch(
        lambda batch_df, batch_id: batch_df.write.jdbc(
            url=url, table=table, mode="append", properties=properties
        )
    ).start()
