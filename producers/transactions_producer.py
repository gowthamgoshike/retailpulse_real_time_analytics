import json
import time
import uuid
import random
from kafka import KafkaProducer
from faker import Faker
from datetime import datetime
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import KAFKA_BROKER, TRANSACTIONS_TOPIC, TRANSACTIONS_INTERVAL

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

payment_methods = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
transaction_status = ["success", "failed", "refund"]
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

    producer.send(TRANSACTIONS_TOPIC, event)
    print("Transaction Event Sent:", event)
    time.sleep(TRANSACTIONS_INTERVAL)