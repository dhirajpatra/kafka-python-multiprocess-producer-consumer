# consumer.py
from confluent_kafka import Consumer, KafkaError
import configparser
import time


# Initializing consumer object
def initialize_consumer(kafka_config, consumer_config):
    max_retries = 10
    retry_delay = 3  # seconds
    current_retry = 0

    while current_retry < max_retries:
        try:
            consumer = Consumer({**kafka_config, **consumer_config})
            return consumer
        except Exception as e:
            print(
                f"Failed to initialize Kafka consumer: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            current_retry += 1

    # If max retries reached, raise an exception
    raise Exception(
        "Failed to initialize Kafka consumer after maximum retries")


# Callback function for consumer assignment
def print_assignment(consumer, partitions):
    print('Assignment:', partitions)


# Worker function for kafka consumer
def kafka_consumer_worker(topic, kafka_config, consumer_config):
    consumer = initialize_consumer(kafka_config, consumer_config)
    consumer.subscribe([topic], on_assign=print_assignment)
    print("Subscribed to topic:", topic)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # Process the message
            print('Received message: {}'.format(msg.value().decode('utf-8')))
    except Exception as e:
        print(f"Unexpected error in consumer: {e}")
    finally:
        consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    # Delay to ensure Kafka broker is up
    time.sleep(10)

    config = configparser.ConfigParser()
    config.read('config.properties')
    kafka_config = dict(config['kafka_client'])
    consumer_config = dict(config['consumer'])

    topic = "my_topic"

    try:
        # Start Kafka consumer worker
        kafka_consumer_worker(topic, kafka_config, consumer_config)
    except KeyboardInterrupt:
        print("Consumer interrupted by user")
    except Exception as e:
        print(f"Error starting consumer: {e}")
