import json
from kafka import KafkaConsumer
from config.settings import KAFKA_BROKER, USER_ACTIVITY_TOPIC

consumer = KafkaConsumer(
    USER_ACTIVITY_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Starting User Activity Consumer...")
for message in consumer:
    print("User Activity Event:", message.value)