from kafka import KafkaConsumer
import requests
import json

# Kafka configuration
kafka_topic = 'kafka-topic'
kafka_bootstrap_servers = ['localhost:9092']

# Splunk HEC configuration
splunk_hec_url = 'http://localhost:8088/services/collector'
splunk_token = 'ea66c526-354a-470c-b842-0b63772b1d0a'

# Create Kafka consumer
consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_bootstrap_servers)

# Function to send data to Splunk
def send_to_splunk(data):
    headers = {
        'Authorization': f'Splunk {splunk_token}',
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(splunk_hec_url, headers=headers, data=json.dumps(data))
        response.raise_for_status()  # This will raise an error for HTTP status codes 4xx/5xx
        print(f"Sent to Splunk: {response.status_code}, {response.text}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")

# Consume messages from Kafka and send to Splunk
try:
    for message in consumer:
        event = {'event': message.value.decode('utf-8')}
        send_to_splunk(event)
except Exception as e:
    print(f"Error while consuming messages: {e}")
