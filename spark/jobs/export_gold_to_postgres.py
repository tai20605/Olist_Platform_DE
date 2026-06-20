import sys
from pyspark.sql import SparkSession

JDBC_URL = "jdbc:postgresql://postgres-warehouse:5432/olist_warehouse"
JDBC_PROPS = {
    "user":     "postgres",
    "password": "postgres_password",
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
        .getOrCreate()

    print("=" * 60)
    print("  Exporting Gold layer → Postgres dw schema")
    print("=" * 60)

    failed = []
    for table in GOLD_TABLES:
        source = f"olist.gold.{table}"
        target = f"dw.{table}"
        try:
            df = spark.read.table(source)
            row_count = df.count()
            df.write.jdbc(url=JDBC_URL, table=target, mode="overwrite", properties=JDBC_PROPS)
            print(f"  OK  {source} -> {target}  ({row_count:,} rows)")
        except Exception as e:
            print(f"  FAIL  {table}: {e}")
            failed.append(table)

    spark.stop()

    if failed:
        print(f"\nFailed tables: {failed}")
        sys.exit(1)

    print("\nAll Gold tables exported successfully.")
