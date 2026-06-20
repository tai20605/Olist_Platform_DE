
from datetime import datetime, timedelta, timezone
import json
import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.providers.trino.hooks.trino import TrinoHook

# Ngưỡng cảnh báo
STALE_MIN    = 60    
CRITICAL_MIN = 120   
ANOMALY_PCT  = 30   

# Helpers
def pg():
    """Trả về psycopg2 connection từ Airflow Connection (không hardcode password)."""
    c = BaseHook.get_connection('POSTGRES_WAREHOUSE')
    return psycopg2.connect(host=c.host, port=c.port or 5432,
                            dbname=c.schema, user=c.login, password=c.password)


def trino(sql):
    """Chạy query Trino, trả về list of tuples."""
    return TrinoHook(trino_conn_id='TRINO').get_records(sql)


def now():
    """Thời điểm hiện tại (UTC, không timezone) để INSERT vào PostgreSQL."""
    return datetime.now(tz=timezone.utc).replace(tzinfo=None)


def insert(table, rows, cols):
    """Ghi nhiều rows vào bảng monitoring.{table}."""
    if not rows:
        return
    sql = f"INSERT INTO monitoring.{table} ({','.join(cols)}) VALUES ({','.join(['%s']*len(cols))})"
    conn = pg()
    try:
        cur = conn.cursor()
        cur.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()


# Task 1: Kiểm tra freshness

def check_freshness(**ctx):
    run_id  = ctx['dag_run'].run_id
    t       = now()
    results = {}
    rows    = []

    checks = [
        ('raw_events', "SELECT MAX(from_iso8601_timestamp(timestamp)) FROM olist.bronze.raw_events"),
    ]

    for table, sql in checks:
        try:
            max_time = trino(sql)[0][0]
            if not max_time:
                mins, status = None, 'critical'
            else:
                mins = round((t - max_time.replace(tzinfo=None)).total_seconds() / 60, 2)
                status = 'fresh' if mins <= STALE_MIN else ('stale' if mins <= CRITICAL_MIN else 'critical')
            print(f"[FRESHNESS] {table}: {status} ({mins} min)")
        except Exception as e:
            print(f"[FRESHNESS ERROR] {table}: {e}")
            max_time, mins, status = None, None, 'critical'

        results[table] = {'status': status, 'freshness_min': mins}
        rows.append((run_id, table, max_time, mins, status, STALE_MIN, t))

    ctx['ti'].xcom_push(key='freshness', value=results)
    insert('freshness_log', rows,
           ['run_id', 'table_name', 'max_event_time', 'freshness_min', 'status', 'threshold_min', 'checked_at'])


# Task 2: Kiểm tra volume

def check_volume(**ctx):
    """Đếm records Silver & Gold, phát hiện đột biến > 30% so với TB 7 ngày."""
    run_id      = ctx['dag_run'].run_id
    t           = now()
    snapshot_ts = t.replace(minute=(t.minute // 15) * 15, second=0, microsecond=0)

    tables = [
        ('silver', 'stg_customers',            'SELECT COUNT(*) FROM olist.silver.stg_customers'),
        ('silver', 'stg_order_items',         'SELECT COUNT(*) FROM olist.silver.stg_order_items'),
        ('silver', 'stg_order_payments',       'SELECT COUNT(*) FROM olist.silver.stg_order_payments'),
        ('silver', 'stg_order_reviews',        'SELECT COUNT(*) FROM olist.silver.stg_order_reviews'),
        ('silver', 'stg_order_status_history', 'SELECT COUNT(*) FROM olist.silver.stg_order_status_history'),
        ('gold',   'dim_categories',           'SELECT COUNT(*) FROM olist.gold.dim_categories'),
        ('gold',   'dim_customers',            'SELECT COUNT(*) FROM olist.gold.dim_customers'),
        ('gold',   'dim_geography',            'SELECT COUNT(*) FROM olist.gold.dim_geography'),
        ('gold',   'dim_order_attributes',     'SELECT COUNT(*) FROM olist.gold.dim_order_attributes'),
        ('gold',   'dim_products',             'SELECT COUNT(*) FROM olist.gold.dim_products'),
        ('gold',   'dim_reviews',              'SELECT COUNT(*) FROM olist.gold.dim_reviews'),
        ('gold',   'dim_sellers',              'SELECT COUNT(*) FROM olist.gold.dim_sellers'),
        ('gold',   'fct_orders',               'SELECT COUNT(*) FROM olist.gold.fct_orders'),
    ]

    # Lấy TB 7 ngày cho tất cả bảng trong 1 query
    names = [t[1] for t in tables]
    conn  = pg()
    cur   = conn.cursor()
    cur.execute("SELECT table_name, AVG(record_count) FROM monitoring.volume_snapshot_log "
                "WHERE table_name = ANY(%s) AND snapshot_hour >= NOW() - INTERVAL '7 days' "
                "GROUP BY table_name", (names,))
    avg7d = {r[0]: float(r[1]) for r in cur.fetchall() if r[1]}
    conn.close()

    rows      = []
    anomalies = []

    for layer, name, sql in tables:
        try:
            count = int(trino(sql)[0][0])
            avg   = avg7d.get(name)
            pct   = round((count - avg) / avg * 100, 2) if avg else None
            is_an = abs(pct) > ANOMALY_PCT if pct is not None else False

            if is_an:
                anomalies.append({'table': name, 'current': count, 'avg_7d': avg, 'pct_change': pct})
                print(f"[ANOMALY] {name}: {count} records ({pct:+.1f}%)")
            else:
                print(f"[OK] {name}: {count} records")

            rows.append((run_id, layer, name, count, snapshot_ts, pct, is_an, t))
        except Exception as e:
            print(f"[VOLUME ERROR] {name}: {e}")
            anomalies.append({'table': name, 'current': 0, 'error': str(e)})
            rows.append((run_id, layer, name, 0, snapshot_ts, None, True, t))

    ctx['ti'].xcom_push(key='anomalies', value=anomalies)
    insert('volume_snapshot_log', rows,
           ['run_id', 'layer', 'table_name', 'record_count',
            'snapshot_hour', 'pct_change_vs_avg', 'is_anomaly', 'created_at'])


# Task 3: Tổng hợp kết quả

def health_summary(**ctx):
    run_id    = ctx['dag_run'].run_id
    ti        = ctx['ti']
    t         = now()
    freshness = ti.xcom_pull(task_ids='check_data_freshness', key='freshness') or {}
    anomalies = ti.xcom_pull(task_ids='check_volume_snapshots', key='anomalies') or []

    if any(v['status'] == 'critical' for v in freshness.values()):
        status = 'CRITICAL'
    elif any(v['status'] == 'stale' for v in freshness.values()) or anomalies:
        status = 'WARNING'
    else:
        status = 'HEALTHY'

    detail = json.dumps({'freshness': freshness, 'anomalies': anomalies}, ensure_ascii=False)

    conn = pg()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO monitoring.pipeline_run_log "
            "(dag_id, run_id, task_id, status, error_message, started_at, finished_at, created_at) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
            ('olist_monitoring', run_id, 'health_summary', status,
             detail if status != 'HEALTHY' else None, t, now(), t)
        )
        conn.commit()
    finally:
        conn.close()

    print(f"\n{'='*50}\n  STATUS: {status}\n{'='*50}")


with DAG(
    dag_id='olist_monitoring',
    default_args={
        'owner': 'olist_monitoring',
        'start_date': datetime(2026, 1, 1, tzinfo=timezone.utc),
        'retries': 0,
    },
    schedule_interval='*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    tags=['monitoring'],
) as dag:

    t1 = PythonOperator(task_id='check_data_freshness',  python_callable=check_freshness)
    t2 = PythonOperator(task_id='check_volume_snapshots', python_callable=check_volume)
    t3 = PythonOperator(task_id='log_pipeline_health',    python_callable=health_summary)

    [t1, t2] >> t3
