from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Initialize_Olist_Iceberg_Infrastructure").getOrCreate()
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.bronze")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS olist.gold")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS olist.bronze.raw_events (
            event_id STRING, event_type STRING, timestamp STRING, user_id STRING,
            session_id STRING, order_id STRING, order_status STRING, payload_items STRING,
            payload_payments STRING, payload_review STRING, metadata_device STRING, metadata_os STRING,
            customer_id STRING, customer_zip_code STRING, customer_city STRING, customer_state STRING,
            customer_latitude STRING, customer_longitude STRING, estimated_delivery_date STRING, actual_delivery_date STRING
        ) 
        USING iceberg
        PARTITIONED BY (event_type)
    """)
    
    print("Infrastructure initialization completed successfully.")
    spark.stop()