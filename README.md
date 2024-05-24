
# Kafka Producer and Consumer with Python multiprocessing and Docker

## Overview

This application demonstrates a Kafka setup with a producer and consumer using Python multiprocessing, Docker, and Docker Compose. The producer sends messages to a Kafka topic, and the consumer reads messages from the same topic. The configuration settings for Kafka are managed using a properties file.

Kafka server can handle multiple consumer processes simultaneously. Kafka is designed to support multiple consumers reading from the same topic concurrently. Though consumer group now have single consumer instances here. You can configure the kafka and the sysetm/server to make them too multiprocessing.

## Features

- **Producer**: Sends messages to a Kafka topic multiprocess way.
- **Consumer**: Reads messages from a Kafka topic multiprocess way.
- **Configuration**: Managed using a `config.properties` file.

## Prerequisites

- Docker and Docker Compose installed on your machine

## Installation

1. **Clone the repository:**

   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. **Create a `config.properties` file:**

   ```ini
   [kafka_client]
   bootstrap.servers=broker:9092
   security.protocol=PLAINTEXT

   [consumer]
   auto.offset.reset=earliest
   group.id=pizza_shop
   enable.auto.commit=true
   max.poll.interval.ms=3000000
   ```

3. **Create a `requirements.txt` file:**

   ```text
   confluent-kafka
   configparser
   ```

## Running the Application

1. **Build and start the services:**

   ```bash
   docker-compose up --build
   ```

2. **Check the logs:**

   To see the producer sending messages and the consumer receiving them:

   ```bash
   docker-compose logs -f
   ```

3. **Stopping the services:**

   ```bash
   docker-compose down
   ```

## Application Details

### Producer (`producer.py`)

The producer reads configuration from `config.properties` and sends messages to the Kafka topic.

### Consumer (`consumer.py`)

The consumer reads configuration from `config.properties` and subscribes to the Kafka topic to consume messages.

## Docker Setup

- **Dockerfile_producer**: Builds the Docker image for the producer.
- **Dockerfile_consumer**: Builds the Docker image for the consumer.
- **docker-compose.yml**: Manages multi-container Docker applications, including Kafka, Zookeeper, producer, and consumer.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
```

This `README.md` provides a clear overview of what the application does, how to install it, and how to run it. Adjust the repository URL and directory names as needed.

You can get more tutorial from https://dhirajpatra.blogspot.com
