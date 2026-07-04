import sys
import urllib.request
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

KAFKA_BROKERS = "kafka:29092"
TOPIC_NAME = "olist-events"
BRONZE_TABLE = "olist.bronze.raw_events"
CHECKPOINT_LOCATION = "s3a://olist/checkpoints/raw_events"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"


def fetch_latest_schema(schema_registry_url, topic):
    """Fetches the latest Avro schema string from Schema Registry using standard urllib."""
    url = f"{schema_registry_url}/subjects/{topic}-value/versions/latest"
    # We should retry as Schema Registry might start slightly after Kafka and Spark
    for attempt in range(15):
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode("utf-8"))
                    print(f"Fetched latest schema from registry (ID: {data['id']})")
                    return data["schema"]
        except Exception as e:
            print(f"Schema Registry not ready yet. Retrying in 3s...({e})")
            time.sleep(3)
    print("Could not fetch schema from Schema Registry. Exiting.")
    sys.exit(1)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("Kafka_To_Bronze_Ingestion").getOrCreate()

    # Fetch schema dynamically
    schema_json_str = fetch_latest_schema(SCHEMA_REGISTRY_URL, TOPIC_NAME)

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .select(
            from_avro(
                expr("substring(value, 6)"),
                schema_json_str
            ).alias("data")
        )
        .select("data.*")
    )

    query = (
        parsed_stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("path", BRONZE_TABLE)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()