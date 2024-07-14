import os
import json
from confluent_kafka import Producer

# Load non-sensitive configuration from config file
with open('config.json') as config_file:
    conf = json.load(config_file)

# Override with sensitive configuration from environment variables
conf['bootstrap.servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS', conf['bootstrap.servers'])
conf['sasl.mechanisms'] = os.getenv('KAFKA_SASL_MECHANISMS', conf['sasl.mechanisms'])
conf['security.protocol'] = os.getenv('KAFKA_SECURITY_PROTOCOL', conf['security.protocol'])
conf['sasl.username'] = os.getenv('KAFKA_SASL_USERNAME', conf['sasl.username'])
conf['sasl.password'] = os.getenv('KAFKA_SASL_PASSWORD', conf['sasl.password'])

# Create Producer instance
producer = Producer(conf)

# Callback when message is delivered or failed
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Function to read JSON files and produce messages
def produce_messages_from_files(data_dir):
    for filename in os.listdir(data_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(data_dir, filename)
            with open(filepath) as file:
                try:
                    messages = json.load(file)
                    if not isinstance(messages, list):
                        messages = [messages]
                    for message in messages:
                        producer.produce(
                            topic='iot_data_topic',
                            key=message['stationId'],
                            value=json.dumps(message),
                            callback=delivery_report
                        )
                        producer.poll(0)
                except json.JSONDecodeError as e:
                    print(f"Failed to decode JSON from {filename}: {e}")

if __name__ == '__main__':
    if len(os.sys.argv) != 2:
        print("Usage: python kafka_producer.py <subfolder_name>")
        os.sys.exit(1)
    
    subfolder_name = os.sys.argv[1]
    data_directory = os.path.join(os.path.dirname(__file__), 'data', subfolder_name)
    
    if not os.path.exists(data_directory):
        print(f"Subfolder {subfolder_name} does not exist in the data directory.")
        os.sys.exit(1)
    
    produce_messages_from_files(data_directory)
    # Wait for any outstanding messages to be delivered
    producer.flush()