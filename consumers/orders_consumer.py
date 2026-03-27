import json
from kafka import KafkaConsumer
from config.settings import KAFKA_BROKER, ORDERS_TOPIC

consumer = KafkaConsumer(
    ORDERS_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Starting Orders Consumer...")
for message in consumer:
    print("Order Event:", message.value)