import os
import json
from confluent_kafka import Consumer, KafkaError

# Load non-sensitive configuration from config file
with open('config.json') as config_file:
    conf = json.load(config_file)

# Override with sensitive configuration from environment variables
conf['bootstrap.servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS', conf['bootstrap.servers'])
conf['sasl.mechanisms'] = os.getenv('KAFKA_SASL_MECHANISMS', conf['sasl.mechanisms'])
conf['security.protocol'] = os.getenv('KAFKA_SECURITY_PROTOCOL', conf['security.protocol'])
conf['sasl.username'] = os.getenv('KAFKA_SASL_USERNAME', conf['sasl.username'])
conf['sasl.password'] = os.getenv('KAFKA_SASL_PASSWORD', conf['sasl.password'])
conf['group.id'] = 'my-consumer-group2'
conf['auto.offset.reset'] = 'earliest'

# Create Consumer instance
consumer = Consumer(conf)

# Subscribe to topic
consumer.subscribe(['iot_data_topic'])

# Function to handle consumed messages
def consume_messages():
    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for new messages with a timeout of 1 second
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    print(f'Reached end of partition for {msg.topic()}/{msg.partition()}')
                    break
                elif msg.error():
                    print(f'Error occurred: {msg.error().str()}')
                continue

            # Print the message value
            print(f'Received message: {msg.value().decode("utf-8")}')

    except KeyboardInterrupt:
        pass
    finally:
        # Close the consumer to commit final offsets
        consumer.close()

if __name__ == '__main__':
    consume_messages()