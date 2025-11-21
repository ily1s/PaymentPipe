import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# Core Transformations
def add_delay_info(payments_df):
    payments_df["delay_days"] = (
        payments_df["paid_at"] - payments_df["due_date"]
    ).dt.days
    payments_df["delay_days"] = payments_df["delay_days"].fillna(0).astype(int)

    payments_df["is_late"] = payments_df["delay_days"] > 0

    return payments_df


# Currency Conversion (to USD)
CURRENCY_RATES = {
    "USD": 1.0,
    "EUR": 1.1,
    "MAD": 0.10,
}


def convert_to_usd(payments_df):
    payments_df["amount_usd"] = payments_df.apply(
        lambda row: round(row["amount"] * CURRENCY_RATES.get(row["currency"], 1.0), 2),
        axis=1,
    )
    return payments_df


# Customer Aggregations
def compute_customer_metrics(payments_df):
    grouped = payments_df.groupby("customer_id").agg(
        {
            "delay_days": ["mean", "max"],
            "payment_id": "count",
            "is_late": "sum",
            "amount_usd": "sum",
        }
    )

    grouped.columns = [
        "avg_delay",
        "max_delay",
        "total_payments",
        "late_count",
        "total_paid_usd",
    ]
    grouped["late_ratio"] = grouped["late_count"] / grouped["total_payments"]

    return grouped.reset_index()


# ustom Grace Period & Final Late Flag
# Assume grace = avg_delay per customer
def add_personalized_late_flag(payments_df, customer_metrics_df):
    grace_dict = customer_metrics_df.set_index("customer_id")["avg_delay"].to_dict()

    payments_df["grace_days"] = (
        payments_df["customer_id"].map(grace_dict).fillna(0).round().astype(int)
    )
    payments_df["custom_due_date"] = payments_df["due_date"] + pd.to_timedelta(
        payments_df["grace_days"], unit="D"
    )
    payments_df["is_late_post_grace"] = (
        payments_df["paid_at"] > payments_df["custom_due_date"]
    )

    return payments_df


# Cash Flow Forecasting Fields
def compute_cashflow_metrics(payments_df):
    daily_cash = (
        payments_df.groupby(payments_df["paid_at"].dt.date)["amount_usd"]
        .sum()
        .reset_index()
    )
    daily_cash.columns = ["date", "total_paid_usd"]

    daily_cash["rolling_7d"] = (
        daily_cash["total_paid_usd"].rolling(window=7, min_periods=1).mean()
    )

    return daily_cash


# Churn & Behavior Flags
def identify_churn_risks(payments_df, customers_df, today=datetime.now()):
    last_payment = payments_df.groupby("customer_id")["paid_at"].max().reset_index()
    last_payment["days_since_last_payment"] = (today - last_payment["paid_at"]).dt.days

    churn_df = last_payment.copy()
    churn_df["potential_churn"] = churn_df["days_since_last_payment"] > 45  
    return churn_df


# Payment Habit Profiling
def behavior_profile(payments_df):
    profile = (
        payments_df.groupby("customer_id")
        .agg(
            {
                "amount": "mean",
                "channel": lambda x: x.mode()[0] if not x.mode().empty else "unknown",
                "paid_at": lambda x: x.dt.hour.mode()[0] if not x.mode().empty else -1,
            }
        )
        .rename(
            columns={
                "amount": "avg_amount",
                "channel": "preferred_channel",
                "paid_at": "preferred_hour",
            }
        )
        .reset_index()
    )

    return profile


# Main Transform Pipeline
def transform_pipeline(payments_df, customers_df):
    payments_df = add_delay_info(payments_df)
    payments_df = convert_to_usd(payments_df)

    customer_metrics = compute_customer_metrics(payments_df)
    payments_df = add_personalized_late_flag(payments_df, customer_metrics)

    churn_df = identify_churn_risks(payments_df, customers_df)
    profile_df = behavior_profile(payments_df)
    forecast_df = compute_cashflow_metrics(payments_df)

    return {
        "payments": payments_df,
        "customers": customers_df,
        "metrics": customer_metrics,
        "churn": churn_df,
        "profiles": profile_df,
        "forecast": forecast_df,
    }
