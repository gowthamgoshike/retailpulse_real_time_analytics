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

topic = "user_activity"

activities = [
    "homepage_visit",
    "product_view",
    "add_to_cart",
    "checkout",
    "search"
]

products = [
    {"id": "P100", "name": "Wireless Mouse"},
    {"id": "P101", "name": "Bluetooth Headphones"},
    {"id": "P102", "name": "Running Shoes"},
    {"id": "P103", "name": "Smart Watch"},
    {"id": "P104", "name": "Laptop Backpack"}
]

devices = ["mobile", "desktop", "tablet"]
traffic_sources = ["google_ads", "facebook_ads", "organic", "email_campaign"]

while True:

    product = random.choice(products)
    activity = random.choice(activities)

    event = {
        "event_type": "user_activity",
        "user_id": fake.uuid4(),
        "session_id": str(uuid.uuid4()),
        "activity_type": activity,
        "product_id": product["id"],
        "product_name": product["name"],
        "page_url": f"/product/{product['id']}",
        "device_type": random.choice(devices),
        "traffic_source": random.choice(traffic_sources),
        "activity_timestamp": datetime.utcnow().isoformat()
    }

    producer.send(topic, event)

    print("User Activity Event:", event)

    time.sleep(1)