#!/bin/bash
set -e

# Define the Kafka broker address
BROKER="broker:9092"

# Create the Kafka topic
echo "Creating Kafka topic..."
# create the topic with replication factor 1 and a single partition
kafka-topics --create --topic my_topic --bootstrap-server "$BROKER" --partitions 1 --replication-factor 1
