import threading
import configparser
import time
from confluent_kafka import Consumer as KafkaConsumer, KafkaError


class Consumer(threading.Thread):
    def __init__(self, kafka_config, consumer_config, topic):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.kafka_config = kafka_config
        self.consumer_config = consumer_config
        self.topic = topic

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer({**self.kafka_config, **self.consumer_config})
        consumer.subscribe([self.topic])
        print(f"Kafka consumer started")
        try:
            while not self.stop_event.is_set():
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
                print('Received message: {}'.format(
                    msg.value().decode('utf-8')))
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

    consumer = Consumer(kafka_config, consumer_config, topic)

    try:
        consumer.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        consumer.stop()
        consumer.join()
        print("Consumer interrupted by user")
    except Exception as e:
        print(f"Error starting consumer: {e}")
