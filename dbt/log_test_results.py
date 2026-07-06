import os
import sys
import json
import logging
import psycopg2
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("dbt_log_test_results")

def parse_run_results(run_id, file_path="target/run_results.json"):
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return []

    with open(file_path, "r", encoding="utf-8") as f:
        run_results = json.load(f)

    parsed_results = []
    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        parts = unique_id.split(".")
        test_name = "unknown"
        model_name = "unknown"
        
        if len(parts) >= 3:
            full_test_name = parts[2]
            # Match model names in Olist project
            models = [
                'stg_customers', 'stg_order_items', 'stg_order_payments', 
                'stg_order_reviews', 'stg_order_status_history',
                'dim_categories', 'dim_customers', 'dim_geography', 
                'dim_order_attributes', 'dim_products', 'dim_reviews', 
                'dim_sellers', 'fct_orders'
            ]
            for m in models:
                if m in full_test_name:
                    model_name = m
                    break
            
            # Determine test type
            if full_test_name.startswith("unique_"):
                test_name = "unique"
            elif full_test_name.startswith("not_null_"):
                test_name = "not_null"
            elif "relationships_" in full_test_name:
                test_name = "relationships"
            elif "accepted_values_" in full_test_name:
                test_name = "accepted_values"
            else:
                test_name = full_test_name
                
        status = result.get("status", "error")
        failures = result.get("failures", 0)
        message = result.get("message", "")
        
        # Deduce layer from model name
        if model_name.startswith("stg_"):
            layer = "silver"
        elif model_name.startswith("dim_") or model_name.startswith("fct_"):
            layer = "gold"
        else:
            layer = "unknown"

        parsed_results.append((
            run_id,
            layer,
            model_name,
            test_name,
            status,
            failures or 0,
            message[:2000] if message else None
        ))
        
    return parsed_results

def insert_to_db(records):
    if not records:
        logger.info("No test results to log.")
        return

    db_host = os.environ.get("WAREHOUSE_PG_HOST", "postgres-warehouse")
    db_port = os.environ.get("WAREHOUSE_PG_PORT", "5432")
    db_name = os.environ.get("WAREHOUSE_PG_DB", "olist_warehouse")
    db_user = os.environ.get("WAREHOUSE_PG_USER", "postgres")
    db_password = os.environ.get("WAREHOUSE_PG_PASSWORD", "postgres_password")

    sql = """
        INSERT INTO monitoring.dq_gate_log 
        (run_id, layer, model_name, test_name, status, failure_count, error_detail, tested_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
    """
    
    try:
        conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name,
            user=db_user, password=db_password
        )
        cur = conn.cursor()
        cur.executemany(sql, records)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Successfully logged {len(records)} test results to PostgreSQL database.")
    except Exception as e:
        logger.error(f"Failed to log test results to database: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("Usage: python log_test_results.py <run_id>")
        sys.exit(1)
        
    run_id = sys.argv[1]
    logger.info(f"Parsing run results for run_id: {run_id}")
    records = parse_run_results(run_id)
    insert_to_db(records)
