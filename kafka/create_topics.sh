#!/bin/bash

echo "Creating Kafka topics..."

docker exec kafka kafka-topics \
--create \
--topic orders \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

docker exec kafka kafka-topics \
--create \
--topic user_activity \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

docker exec kafka kafka-topics \
--create \
--topic transactions \
--bootstrap-server localhost:9092 \
--partitions 3 \
--replication-factor 1

echo "Kafka topics created successfully!"