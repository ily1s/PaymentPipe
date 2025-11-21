import pandas as pd


def extract_customers(filepath="data/customers.csv"):
    try:
        customers_df = pd.read_csv(filepath, parse_dates=["created_at"])
        print(f"Loaded {len(customers_df)} customers from csv")
        return customers_df
    except Exception as e:
        print(f"Failed to load customers: {e}")
        return pd.DataFrame()


def extract_payments(filepath="data/payments.csv"):
    try:
        payments_df = pd.read_csv(
            filepath, parse_dates=["due_date", "paid_at", "created_at"]
        )
        print(f"Loaded {len(payments_df)} payments from csv")
        return payments_df
    except Exception as e:
        print(f"Failed to load payments: {e}")
        return pd.DataFrame()
