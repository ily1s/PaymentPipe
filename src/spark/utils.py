from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Delay & Late Info
def add_delay_info(payments_df):
    return payments_df.withColumn(
        "delay_days", F.datediff(F.to_date("paid_at"), F.to_date("due_date"))
    ).withColumn("is_late", F.when(F.col("delay_days") > 0, 1).otherwise(0))


# Currency Conversion (to USD)
CURRENCY_RATES = {
    "USD": 1.0,
    "EUR": 1.1,
    "MAD": 0.10,
}


def convert_to_usd(payments_df):
    convert_udf = F.udf(
        lambda currency, amount: round(amount * CURRENCY_RATES.get(currency, 1.0), 2)
    )

    return payments_df.withColumn(
        "amount_usd", convert_udf(F.col("currency"), F.col("amount"))
    )


# Customer Metrics
def compute_customer_metrics(payments_df):
    return (
        payments_df.groupBy("customer_id")
        .agg(
            F.avg("delay_days").alias("avg_delay"),
            F.max("delay_days").alias("max_delay"),
            F.count("payment_id").alias("total_payments"),
            F.sum("is_late").alias("late_count"),
            F.sum("amount_usd").alias("total_paid_usd"),
        )
        .withColumn("late_ratio", F.col("late_count") / F.col("total_payments"))
    )


# Personalized Grace Period
def add_personalized_late_flag(payments_df, customer_metrics_df):
    return (
        payments_df.join(customer_metrics_df, on="customer_id")
        .withColumn(
            "custom_due_date",
            F.date_add(F.to_date("due_date"), F.col("avg_delay").cast("int")),
        )
        .withColumn(
            "is_late_post_grace",
            F.when(F.to_date("paid_at") > F.col("custom_due_date"), 1).otherwise(0),
        )
    )

# ---------- Optional Enhancements ----------
# Cash Flow Metrics
def compute_cashflow_metrics(payments_df):
    daily_cash = payments_df.groupBy(F.to_date("paid_at").alias("payment_day")).agg(
        F.sum("amount_usd").alias("total_paid_usd")
    )

    window_7d = Window.orderBy("payment_day").rowsBetween(-6, 0)

    return daily_cash.withColumn(
        "rolling_7d_avg", F.avg("total_paid_usd").over(window_7d)
    )


# Churn Risk Detection
def detect_churn_risk(df):
    last_payment = df.groupBy("customer_id").agg(
        F.max("paid_at").alias("last_payment_date")
    )

    return last_payment.withColumn(
        "days_since_last_payment",
        F.datediff(F.current_date(), F.to_date("last_payment_date")),
    ).withColumn(
        "is_churn_risk", F.when(F.col("days_since_last_payment") > 30, 1).otherwise(0)
    )


# Behavior Profiling
def add_behavior_flags(df):
    channel_stats = df.groupBy("customer_id", "channel").agg(
        F.count("channel").alias("count")
    )

    channel_window = Window.partitionBy("customer_id").orderBy(F.desc("count"))

    most_used_channel = channel_stats.withColumn(
        "rank", F.row_number().over(channel_window)
    ).filter(F.col("rank") == 1)

    hour_df = df.withColumn("payment_hour", F.hour(F.to_timestamp("paid_at")))

    return (
        df.groupBy("customer_id")
        .agg(F.avg("amount_usd").alias("avg_payment_value"))
        .join(most_used_channel.select("customer_id", "channel"), "customer_id")
        .join(
            hour_df.groupBy("customer_id").agg(
                F.expr("percentile_approx(payment_hour, 0.5)").alias(
                    "most_common_payment_hour"
                )
            ),
            "customer_id",
        )
    )

# Main Transformation Pipeline
def transform_payments(payments_df):
    payments_with_delay = add_delay_info(payments_df)
    payments_converted = convert_to_usd(payments_with_delay)
    customer_metrics = compute_customer_metrics(payments_converted)
    payments_enriched = add_personalized_late_flag(
        payments_converted, customer_metrics
    )
    return payments_enriched