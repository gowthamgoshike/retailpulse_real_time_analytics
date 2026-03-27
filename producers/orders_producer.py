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

from config.settings import KAFKA_BROKER, ORDERS_TOPIC, ORDERS_INTERVAL

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = [
    {"id": "P100", "name": "Wireless Mouse", "category": "Electronics", "price": 25.99},
    {"id": "P101", "name": "Bluetooth Headphones", "category": "Electronics", "price": 89.99},
    {"id": "P102", "name": "Running Shoes", "category": "Footwear", "price": 120.00},
    {"id": "P103", "name": "Smart Watch", "category": "Electronics", "price": 199.99},
    {"id": "P104", "name": "Laptop Backpack", "category": "Accessories", "price": 49.99}
]

while True:
    product = random.choice(products)
    quantity = random.randint(1,3)

    order_event = {
        "event_type": "order_created",
        "order_id": str(uuid.uuid4()),
        "user_id": fake.uuid4(),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "price": product["price"],
        "total_amount": quantity * product["price"],
        "order_status": "confirmed",
        "order_timestamp": datetime.utcnow().isoformat()
    }

    producer.send(ORDERS_TOPIC, order_event)
    print("Order Event Sent:", order_event)
    time.sleep(ORDERS_INTERVAL)