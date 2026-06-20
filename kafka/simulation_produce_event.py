import json
import time
import sys
import os
from datetime import datetime, timezone
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError

IS_DOCKER = os.path.exists('/.dockerenv')

KAFKA_BROKERS = ['kafka:29092'] if IS_DOCKER else ['localhost:9092']
FILE_PATH = 'data/events.jsonl' if IS_DOCKER else r'C:\Data Streaming\data\events.jsonl'

TOPIC_NAME = 'olist-events'
TARGET_EPS = 50
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1


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


def serialize_json(data):
    """Encodes JSON dict to bytes."""
    return json.dumps(data, ensure_ascii=False).encode('utf-8')


if __name__ == "__main__":
    ensure_topic_exists()

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=serialize_json,
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