import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

KAFKA_BROKERS = "kafka:29092"
TOPIC_NAME = "olist-events"
BRONZE_TABLE = "olist.bronze.raw_events"
CHECKPOINT_LOCATION = "s3a://olist/checkpoints/raw_events"

RAW_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("payload_items", StringType(), True),
    StructField("payload_payments", StringType(), True),
    StructField("payload_review", StringType(), True),
    StructField("metadata_device", StringType(), True),
    StructField("metadata_os", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_zip_code", StringType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_state", StringType(), True),
    StructField("customer_latitude", StringType(), True),
    StructField("customer_longitude", StringType(), True),
    StructField("estimated_delivery_date", StringType(), True),
    StructField("actual_delivery_date", StringType(), True)
])

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Kafka_To_Bronze_Ingestion").getOrCreate()

    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), RAW_EVENT_SCHEMA).alias("data"))
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