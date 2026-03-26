import json
import time
import uuid
import random
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "transactions"

payment_methods = [
    "credit_card",
    "debit_card",
    "paypal",
    "apple_pay",
    "google_pay"
]

transaction_status = [
    "success",
    "failed",
    "refund"
]

currencies = ["USD"]

while True:

    amount = round(random.uniform(10, 500), 2)

    event = {
        "event_type": "payment_transaction",
        "transaction_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "user_id": fake.uuid4(),
        "payment_method": random.choice(payment_methods),
        "transaction_status": random.choice(transaction_status),
        "amount": amount,
        "currency": random.choice(currencies),
        "transaction_timestamp": datetime.utcnow().isoformat()
    }

    producer.send(topic, event)

    print("Transaction Event Sent:", event)

    time.sleep(7)