import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Kafka broker
KAFKA_BROKER = os.getenv("KAFKA_BROKER")

# Kafka topics
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC")
USER_ACTIVITY_TOPIC = os.getenv("USER_ACTIVITY_TOPIC")
TRANSACTIONS_TOPIC = os.getenv("TRANSACTIONS_TOPIC")

# Producer intervals
ORDERS_INTERVAL = int(os.getenv("ORDERS_INTERVAL", 2))
USER_ACTIVITY_INTERVAL = int(os.getenv("USER_ACTIVITY_INTERVAL", 1))
TRANSACTIONS_INTERVAL = int(os.getenv("TRANSACTIONS_INTERVAL", 3))