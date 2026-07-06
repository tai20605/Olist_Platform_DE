import os
import sys
import logging
from pyspark.sql import SparkSession

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ExportGoldToPostgres")

# Fetch connection parameters from environment variables
db_host = os.environ.get("WAREHOUSE_PG_HOST", "postgres-warehouse")
db_port = os.environ.get("WAREHOUSE_PG_PORT", "5432")
db_name = os.environ.get("WAREHOUSE_PG_DB", "olist_warehouse")
db_user = os.environ.get("WAREHOUSE_PG_USER", "postgres")
db_password = os.environ.get("WAREHOUSE_PG_PASSWORD", "postgres_password")

JDBC_URL = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
JDBC_PROPS = {
    "user":     db_user,
    "password": db_password,
    "driver":   "org.postgresql.Driver",
}

GOLD_TABLES = [
    "dim_categories",
    "dim_customers",
    "dim_geography",
    "dim_order_attributes",
    "dim_products",
    "dim_reviews",
    "dim_sellers",
    "fct_orders",
]


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Export_Gold_To_Postgres_Warehouse") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.sql.iceberg.vectorization.enabled", "false") \
        .config("spark.sql.parquet.enableVectorizedReader", "false") \
        .getOrCreate()

    logger.info("============================================================")
    logger.info("  Exporting Gold layer -> Postgres dw schema")
    logger.info("============================================================")

    failed = []
    for table in GOLD_TABLES:
        source = f"olist.gold.{table}"
        target = f"dw.{table}"
        try:
            df = spark.read.table(source)
            row_count = df.count()
            df.write.jdbc(url=JDBC_URL, table=target, mode="overwrite", properties=JDBC_PROPS)
            logger.info(f"  OK   {source} -> {target}  ({row_count:,} rows)")
        except Exception as e:
            logger.error(f"  FAIL {table}: {e}", exc_info=True)
            failed.append(table)

    spark.stop()

    if failed:
        logger.error(f"Failed tables: {failed}")
        sys.exit(1)

    logger.info("All Gold tables exported successfully.")
