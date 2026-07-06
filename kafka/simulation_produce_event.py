import json
import time
import sys
import os
import io
import struct
import requests
import fastavro
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

IS_DOCKER = os.path.exists('/.dockerenv')

KAFKA_BROKERS = ['kafka:29092'] if IS_DOCKER else ['localhost:9092']
FILE_PATH = 'data/events.jsonl' if IS_DOCKER else r'C:\Data Streaming\data\events.jsonl'
SCHEMA_REGISTRY_URL = 'http://schema-registry:8081' if IS_DOCKER else 'http://localhost:8089'

TOPIC_NAME = 'olist-events'
TARGET_EPS = 1500
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1

AVRO_SCHEMA = {
    "type": "record",
    "name": "OlistEvent",
    "namespace": "olist",
    "fields": [
        {"name": "event_id", "type": ["null", "string"], "default": None},
        {"name": "event_type", "type": ["null", "string"], "default": None},
        {"name": "timestamp", "type": ["null", "string"], "default": None},
        {"name": "user_id", "type": ["null", "string"], "default": None},
        {"name": "session_id", "type": ["null", "string"], "default": None},
        {"name": "order_id", "type": ["null", "string"], "default": None},
        {"name": "order_status", "type": ["null", "string"], "default": None},
        {"name": "payload_items", "type": ["null", "string"], "default": None},
        {"name": "payload_payments", "type": ["null", "string"], "default": None},
        {"name": "payload_review", "type": ["null", "string"], "default": None},
        {"name": "metadata_device", "type": ["null", "string"], "default": None},
        {"name": "metadata_os", "type": ["null", "string"], "default": None},
        {"name": "customer_id", "type": ["null", "string"], "default": None},
        {"name": "customer_zip_code", "type": ["null", "string"], "default": None},
        {"name": "customer_city", "type": ["null", "string"], "default": None},
        {"name": "customer_state", "type": ["null", "string"], "default": None},
        {"name": "customer_latitude", "type": ["null", "string"], "default": None},
        {"name": "customer_longitude", "type": ["null", "string"], "default": None},
        {"name": "estimated_delivery_date", "type": ["null", "string"], "default": None},
        {"name": "actual_delivery_date", "type": ["null", "string"], "default": None}
    ]
}


def ensure_topic_exists():
    """Checks if the topic exists, and creates it if missing."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id='admin_init'
        )
        if TOPIC_NAME not in admin_client.list_topics():
            new_topic = NewTopic(
                name=TOPIC_NAME,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
            admin_client.create_topics(new_topics=[new_topic])
            print(f"Topic '{TOPIC_NAME}' created successfully.")
        else:
            print(f"Topic '{TOPIC_NAME}' already exists.")
        admin_client.close()
    except TopicAlreadyExistsError:
        pass
    except Exception as e:
        print(f"Failed to check or create topic: {e}")
        sys.exit(1)


def register_schema():
    """Register the Avro schema to Schema Registry and return the schema ID."""
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC_NAME}-value/versions"
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schema": json.dumps(AVRO_SCHEMA)}
    
    for attempt in range(15):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=5)
            if response.status_code == 200:
                schema_id = response.json()["id"]
                print(f"Registered schema successfully. Schema ID: {schema_id}")
                return schema_id
            else:
                print(f"Failed to register schema: {response.text}")
        except Exception as e:
            print(f"Waiting for Schema Registry... ({e})")
        time.sleep(3)
    print("Could not connect to Schema Registry. Exiting.")
    sys.exit(1)


def serialize_avro(event, parsed_schema, schema_id):
    """Serialize Python dict to Confluent Avro binary format (magic byte + schema_id + raw avro)."""
    record = {}
    for field in AVRO_SCHEMA["fields"]:
        name = field["name"]
        val = event.get(name)
        if isinstance(val, (dict, list)):
            record[name] = json.dumps(val)
        else:
            record[name] = str(val) if val is not None else None

    fo = io.BytesIO()
    fo.write(struct.pack(">bI", 0, schema_id))
    # Write fastavro serialized payload
    fastavro.schemaless_writer(fo, parsed_schema, record)
    return fo.getvalue()


if __name__ == "__main__":
    ensure_topic_exists()
    schema_id = register_schema()
    parsed_avro_schema = fastavro.parse_schema(AVRO_SCHEMA)

    def serialize_avro_wrapper(event):
        return serialize_avro(event, parsed_avro_schema, schema_id)

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=serialize_avro_wrapper,
            acks=1,
            compression_type='gzip',
            batch_size=16384,
            linger_ms=10
        )
    except Exception as e:
        print(f"Failed to initialize Kafka Producer: {e}")
        sys.exit(1)

    print(f"Starting real-time streaming from {FILE_PATH} at {TARGET_EPS} eps...")
    
    start_time = time.time()
    sent_count = 0

    with open(FILE_PATH, 'r', encoding='utf-8') as file:
        for line in file:
            if not line.strip():
                continue

            try:
                event = json.loads(line.strip())
                event['timestamp'] = datetime.now(timezone.utc).isoformat()

                producer.send(TOPIC_NAME, value=event)
                sent_count += 1

                # Precise rate-limiting calculation
                elapsed = time.time() - start_time
                expected_time = sent_count / TARGET_EPS
                if elapsed < expected_time:
                    time.sleep(expected_time - elapsed)

                if sent_count % (TARGET_EPS * 5) == 0:
                    print(f"Sent {sent_count} events. Current speed: {sent_count / (time.time() - start_time):.2f} eps")

            except KeyboardInterrupt:
                print("\nStopping producer...")
                break
            except Exception as e:
                print(f"Error processing line {sent_count}: {e}")

    producer.flush()
    producer.close()
    print(f"Finished. Sent total of {sent_count} events in {time.time() - start_time:.2f} seconds.")