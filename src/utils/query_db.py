import sqlite3
import pandas as pd

DB_PATH = "data/payments.db"


def run_query(query, db_path=DB_PATH):
    """
    Run a SQL query on the SQLite database and return a pandas DataFrame.
    """
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Query failed: {e}")
        return pd.DataFrame()


def main():
    print("Total revenue per month:")
    q1 = """
    SELECT 
        strftime('%Y-%m', paid_at) AS month,
        SUM(amount_usd) AS total_revenue
    FROM payments
    WHERE status = 'completed'
    GROUP BY month
    ORDER BY month;
    """
    print(run_query(q1))

    print("\n Top 5 customers by total payments:")
    q2 = """
    SELECT 
        c.name,
        m.total_paid_usd,
        m.total_payments,
        m.late_ratio
    FROM customers c
    JOIN metrics m ON c.customer_id = m.customer_id
    ORDER BY m.total_paid_usd DESC
    LIMIT 5;
    """
    print(run_query(q2))

    print("\n Customers flagged as potential churn:")
    q3 = """
    SELECT 
        c.name,
        ch.days_since_last_payment,
        ch.potential_churn
    FROM churn ch
    JOIN customers c ON c.customer_id = ch.customer_id
    WHERE ch.potential_churn = 1
    ORDER BY ch.days_since_last_payment DESC;
    """
    print(run_query(q3))

    print("\n Average delay per customer:")
    q4 = """
    SELECT 
        c.name,
        ROUND(m.avg_delay, 2) AS avg_delay_days,
        m.total_payments,
        m.late_count,
        ROUND(100.0 * m.late_ratio, 1) AS late_percent
    FROM metrics m
    JOIN customers c ON c.customer_id = m.customer_id
    ORDER BY avg_delay_days DESC;
    """
    print(run_query(q4))

    print("\n Payments flagged as late AFTER grace period:")
    q5 = """
    SELECT 
        p.payment_id,
        c.name,
        p.due_date,
        p.paid_at,
        p.delay_days,
        p.grace_days,
        p.custom_due_date,
        p.is_late_post_grace
    FROM payments p
    JOIN customers c ON c.customer_id = p.customer_id
    WHERE p.is_late_post_grace = 1
    ORDER BY p.delay_days DESC
    LIMIT 10;
    """
    print(run_query(q5))

    print("\n % of customers by payment behavior:")
    q6 = """
    SELECT
        SUM(CASE WHEN avg_delay <= 0 THEN 1 ELSE 0 END) AS early_payers,
        SUM(CASE WHEN avg_delay > 0 AND avg_delay <= 5 THEN 1 ELSE 0 END) AS on_time,
        SUM(CASE WHEN avg_delay > 5 THEN 1 ELSE 0 END) AS chronically_late,
        COUNT(*) AS total_customers,
        ROUND(100.0 * SUM(CASE WHEN avg_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_early,
        ROUND(100.0 * SUM(CASE WHEN avg_delay > 0 AND avg_delay <= 5 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_on_time,
        ROUND(100.0 * SUM(CASE WHEN avg_delay > 5 THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_late
    FROM metrics;
    """
    print(run_query(q6))

    print("\n Most delayed payments:")
    q7 = """
    SELECT 
        p.payment_id,
        c.name,
        p.paid_at,
        p.due_date,
        p.delay_days,
        p.grace_days,
        p.status
    FROM payments p
    JOIN customers c ON c.customer_id = p.customer_id
    WHERE p.delay_days > 0
    ORDER BY p.delay_days DESC
    LIMIT 10;
    """
    print(run_query(q7))


if __name__ == "__main__":
    main()
