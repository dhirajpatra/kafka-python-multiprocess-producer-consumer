# producer.py
from confluent_kafka import Producer
import multiprocessing
import configparser
import time


# Function to initialize Kafka producer with retry mechanism
def initialize_producer(config):
    max_retries = 10
    retry_delay = 3  # seconds
    current_retry = 0

    while current_retry < max_retries:
        try:
            producer = Producer(config)
            return producer
        except Exception as e:
            print(
                f"Failed to initialize Kafka producer: {e}. Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            current_retry += 1

    # If max retries reached, raise an exception
    raise Exception(
        "Failed to initialize Kafka producer after maximum retries")


# Worker function for kafka producer
def kafka_producer_worker(args):
    topic, message, config = args

    # Initialize Kafka producer with retry mechanism
    producer = initialize_producer(config)

    try:
        # Delivery report handler for produced messages
        def delivery_report(err, msg):
            if err is not None:
                print(f'Delivery failed: {err}')
            else:
                print(
                    f'Message delivered to {msg.topic()} [{msg.partition()}]')

        # Produce the message
        producer.produce(topic, value=message, callback=delivery_report)
        producer.poll(0)

        # Wait for the message to be delivered
        producer.flush()
    finally:
        # Ensure producer resources are cleaned up
        producer.flush()  # Flush any pending messages
        producer.poll(0)  # Poll to handle delivery callbacks


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('config.properties')
    kafka_config = dict(config['kafka_client'])

    topic = "my_topic"

    # contine produce messages 
    while True:
        # sleep for 5 seconds
        time.sleep(5)

        messages = [f"Message {i} {time.strftime('%Y-%m-%d %H:%M:%S')}" for i in range(4)]

        # Create a pool of processes
        pool = multiprocessing.Pool()

        # Prepare arguments for the worker function
        args_list = [(topic, message, kafka_config) for message in messages]

        try:
            # Use map to distribute work among processes
            pool.map(kafka_producer_worker, args_list)
        finally:
            # Close the pool to release resources
            pool.close()
            pool.join()
