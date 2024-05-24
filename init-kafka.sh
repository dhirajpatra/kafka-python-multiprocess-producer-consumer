#!/bin/bash
set -e

# Define the Kafka data directory
DATA_DIR="/var/lib/kafka/data"

# Check if the data directory exists
if [ ! -d "$DATA_DIR" ]; then
  echo "Data directory $DATA_DIR does not exist. Creating it..."
  mkdir -p "$DATA_DIR"
fi

# Ensure the directory is writable by the default user
# echo "Setting permissions on $DATA_DIR..."
# chown -R root:root "$DATA_DIR"
# chmod -R 755 "$DATA_DIR"

# Execute the original command
exec "$@"
