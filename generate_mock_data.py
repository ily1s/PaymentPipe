import uuid
import csv
import random
from datetime import timedelta
from faker import Faker
from pathlib import Path

faker = Faker()
# faker = Faker('ar_MA')
random.seed(42)
Faker.seed(42)

NUM_CUSTOMERS = 20
NUM_PAYMENTS = 200

CURRENCIES = ["USD", "EUR", "MAD"]
STATUSES = ["completed", "pending", "failed"]
CHANNELS = ["credit_card", "paypal", "bank_transfer", "cash"]


def generate_customers():
    customers = []
    for _ in range(NUM_CUSTOMERS):
        customer_id = str(uuid.uuid4())
        customers.append(
            {
                "customer_id": customer_id,
                "name": faker.name(),
                "email": faker.email(),
                "created_at": faker.date_time_between(start_date="-1y", end_date="now"),
            }
        )
    return customers


def generate_payments(customers):
    payments = []
    for _ in range(NUM_PAYMENTS):
        customer = random.choice(customers)
        payment_id = str(uuid.uuid4())
        due_date = faker.date_time_between(start_date="-60d", end_date="now")
        delay_days = random.choices(
            [0, 2, 5, 10, 15], weights=[0.3, 0.2, 0.2, 0.2, 0.1]
        )[0]
        paid_at = due_date + timedelta(days=delay_days)
        payments.append(
            {
                "payment_id": payment_id,
                "customer_id": customer["customer_id"],
                "due_date": due_date.isoformat(),
                "paid_at": paid_at.isoformat(),
                "amount": round(random.uniform(20.0, 500.0), 2),
                "currency": random.choice(CURRENCIES),
                "status": random.choice(STATUSES),
                "channel": random.choice(CHANNELS),
                "created_at": faker.date_time_between(
                    start_date=due_date - timedelta(days=5), end_date=due_date
                ).isoformat(),
            }
        )
    return payments


def write_csv(data, filepath, fieldnames):
    with open(filepath, mode="w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def main():
    Path("data").mkdir(exist_ok=True)
    customers = generate_customers()
    payments = generate_payments(customers)

    write_csv(customers, "data/customers.csv", fieldnames=customers[0].keys())
    write_csv(payments, "data/payments.csv", fieldnames=payments[0].keys())

    print("Mock data generated: data/customers.csv & data/payments.csv")


if __name__ == "__main__":
    main()
