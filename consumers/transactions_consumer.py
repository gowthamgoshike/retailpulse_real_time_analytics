import json
from kafka import KafkaConsumer
from config.settings import KAFKA_BROKER, TRANSACTIONS_TOPIC

consumer = KafkaConsumer(
    TRANSACTIONS_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Starting Transactions Consumer...")
for message in consumer:
    print("Transaction Event:", message.value)