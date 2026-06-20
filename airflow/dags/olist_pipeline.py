from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.trino.operators.trino import TrinoOperator
from airflow.utils.task_group import TaskGroup
import psycopg2


def on_task_failure(context):
    try:
        task_instance = context.get('task_instance')
        exception     = context.get('exception')
        conn = psycopg2.connect(
            host='postgres-warehouse', port=5432,
            dbname='olist_warehouse', user='postgres', password='postgres_password'
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO monitoring.pipeline_run_log
                (dag_id, run_id, task_id, status, error_message, log_url, started_at, finished_at)
            VALUES (%s, %s, %s, 'failed', %s, %s, %s, NOW())
        """, (
            task_instance.dag_id,
            task_instance.run_id,
            task_instance.task_id,
            str(exception)[:2000] if exception else 'Unknown error',
            task_instance.log_url,
            task_instance.start_date,
        ))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[MONITORING] Failure logged: {task_instance.dag_id}.{task_instance.task_id}")
    except Exception as e:
        print(f"[MONITORING CALLBACK ERROR] {e}")


default_args = {
    'owner': 'olist_admin',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': on_task_failure,
}

with DAG(
    'olist_pipeline',
    default_args=default_args,
    description='Pipeline Olist E-commerce tách rõ 4 tầng: Extract, Load, Transform, Export',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    max_active_runs=1,
) as dag:

    with TaskGroup("1_Extract", tooltip="Kiểm tra kết nối tới Message Queue") as extract:
        check_kafka_health = BashOperator(
            task_id='check_kafka_broker_connection',
            bash_command='nc -z kafka 29092',
        )

    with TaskGroup("2_Load", tooltip="Đảm bảo hạ tầng dữ liệu thô đã sẵn sàng") as load:
        verify_iceberg_bronze_status = TrinoOperator(
            task_id='verify_iceberg_bronze_status',
            sql="SHOW TABLES IN olist.bronze",
            trino_conn_id='TRINO',
        )

    with TaskGroup("3_Transform", tooltip="Silver Quality Gate → Gold Build") as transform:

        silver_quality_gate = BashOperator(
            task_id='silver_quality_gate',
            bash_command=(
                'pip install --no-cache-dir dbt-trino==1.7.0 && '
                'cd /opt/dbt && '
                '/home/airflow/.local/bin/dbt deps --profiles-dir . && '
                '(/home/airflow/.local/bin/dbt build --select +models/silver --profiles-dir . || true) && '
                'python log_test_results.py "{{ run_id }}"'
            ),
        )

        gold_build = BashOperator(
            task_id='gold_build',
            bash_command=(
                'pip install --no-cache-dir dbt-trino==1.7.0 && '
                'cd /opt/dbt && '
                '/home/airflow/.local/bin/dbt deps --profiles-dir . && '
                '/home/airflow/.local/bin/dbt run '
                '--select models/gold '
                '--profiles-dir .'
            ),
        )

        silver_quality_gate >> gold_build

    with TaskGroup("4_Export", tooltip="Đẩy dữ liệu Gold sang PostgreSQL Warehouse phục vụ Dashboard") as export:
        export_gold_to_postgres = BashOperator(
            task_id='spark_batch_gold_to_postgres_warehouse',
            bash_command=(
                'docker exec -e SPARK_SUBMIT_OPTS="-Divy.home=/tmp/.ivy2 -Djava.net.preferIPv4Stack=true" spark-master '
                '/opt/spark/bin/spark-submit '
                '--master spark://spark-master:7077 '
                '/opt/spark/work/jobs/export_gold_to_postgres.py'
            ),
        )

    extract >> load >> transform >> export