from extract import extract_customers, extract_payments
from transform import transform_pipeline
from load import load_to_sqlite

def run_etl():
    # Step 1: Extract
    print("Extraction...")
    customers_df = extract_customers()
    payments_df = extract_payments()

    # Step 2: Transform
    print("Transformation...")
    data_dict = transform_pipeline(payments_df, customers_df)

    # Step 3: Load
    print("Loading...")
    load_to_sqlite(data_dict)

    print("ETL pipeline complete!")

if __name__ == "__main__":
    run_etl()
