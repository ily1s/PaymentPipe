from pyspark.sql.types import *

customer_schema = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)


payment_schema = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("due_date", StringType(), True),
        StructField("paid_at", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("created_at", StringType(), True),
    ]
)

enriched_payment_schema = StructType(
    [
        StructField("payment_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("due_date", StringType(), True),
        StructField("paid_at", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("channel", StringType(), True),
        # Enriched fields
        StructField("delay_days", IntegerType(), True),
        StructField("is_late", IntegerType(), True),
        StructField("amount_usd", DoubleType(), True),
        StructField("avg_delay", DoubleType(), True),
        StructField("custom_due_date", StringType(), True),
        StructField("is_late_post_grace", IntegerType(), True),
    ]
)
