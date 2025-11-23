import uuid
import json
import random
import time
from datetime import timedelta
from faker import Faker
from kafka import KafkaProducer
from pathlib import Path

faker = Faker()
random.seed(42)
Faker.seed(42)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

CUSTOMERS_FILE = Path("data/base_customers.json")
CUSTOMERS = []
MAX_BASE_CUSTOMERS = 20

CURRENCIES = ["USD", "EUR", "MAD"]
STATUSES = ["completed", "pending", "failed"]
CHANNELS = ["credit_card", "paypal", "bank_transfer", "cash"]


def save_customers():
    CUSTOMERS_FILE.parent.mkdir(exist_ok=True)
    with open(CUSTOMERS_FILE, "w") as f:
        json.dump(CUSTOMERS, f, indent=2)


def create_customer():
    customer = {
        "customer_id": str(uuid.uuid4()),
        "name": faker.name(),
        "email": faker.email(),
        "created_at": faker.date_time_between(
            start_date="-1y", end_date="now"
        ).isoformat(),
    }
    producer.send("customers_stream", customer)
    CUSTOMERS.append(customer)
    save_customers()
    return customer


def create_payment(customer):
    due_date = faker.date_time_between(start_date="-60d", end_date="now")
    delay_days = random.choices([0, 2, 5, 10, 15], weights=[0.3, 0.2, 0.2, 0.2, 0.1])[0]
    paid_at = due_date + timedelta(days=delay_days)

    payment = {
        "payment_id": str(uuid.uuid4()),
        "customer_id": customer["customer_id"],
        "due_date": due_date.isoformat(),
        "paid_at": paid_at.isoformat(),
        "amount": round(random.uniform(20, 500), 2),
        "currency": random.choice(CURRENCIES),
        "status": random.choice(STATUSES),
        "channel": random.choice(CHANNELS),
        "created_at": faker.date_time_between(
            start_date=due_date - timedelta(days=5), end_date=due_date
        ).isoformat(),
    }

    producer.send("payments_stream", payment)


# ---- INIT BASE CUSTOMERS ----
if CUSTOMERS_FILE.exists():
    CUSTOMERS = json.load(open(CUSTOMERS_FILE))
    print("Loaded existing customers:", len(CUSTOMERS))
else:
    for _ in range(MAX_BASE_CUSTOMERS):
        create_customer()
    print("Base customers created")


# ---- STREAM LOOP ----
while True:
    # Occasionally add new customer (10% chance)
    if random.random() < 0.1:
        create_customer()
        print("New customer added")

    customer = random.choice(CUSTOMERS)
    create_payment(customer)
    print("Payment sent")

    time.sleep(1)
