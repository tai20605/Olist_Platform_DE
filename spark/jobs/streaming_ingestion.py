import os
import sys
import urllib.request
import json
import time
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.avro.functions import from_avro

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("KafkaToBronzeIngestion")

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka:29092")
TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "olist-events")
BRONZE_TABLE = os.environ.get("BRONZE_TABLE", "olist.bronze.raw_events")
CHECKPOINT_LOCATION = os.environ.get("CHECKPOINT_LOCATION", "s3a://olist/checkpoints/raw_events")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")


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
                    logger.info(f"Fetched latest schema from registry (ID: {data['id']})")
                    return data["schema"]
        except Exception as e:
            logger.warning(f"Schema Registry not ready yet (attempt {attempt+1}/15). Retrying in 3s... ({e})")
            time.sleep(3)
    logger.error("Could not fetch schema from Schema Registry. Exiting.")
    sys.exit(1)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("Kafka_To_Bronze_Ingestion")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    # Fetch schema dynamically
    schema_json_str = fetch_latest_schema(SCHEMA_REGISTRY_URL, TOPIC_NAME)

    logger.info("Initializing Spark Kafka structured streaming reader...")
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "latest")  # Only read new messages when no checkpoint exists
        .option("failOnDataLoss", "false")    # Tolerate offset gaps after Kafka retention cleanup
        .load()
    )

    logger.info("Parsing Kafka records using Avro schema from Schema Registry...")
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

    logger.info(f"Writing streaming records to Iceberg table: {BRONZE_TABLE}")
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