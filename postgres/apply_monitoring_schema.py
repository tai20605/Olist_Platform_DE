
import psycopg2

CONN_PARAMS = {
    'host':     'localhost',
    'port':     5402,        
    'dbname':   'olist_warehouse',
    'user':     'postgres',
    'password': 'postgres_password',
}

SQL = """
-- ============================================================
-- SCHEMA: monitoring (Data Observability)
-- ============================================================
CREATE SCHEMA IF NOT EXISTS monitoring;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_run_log (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(255) NOT NULL,
    run_id          VARCHAR(255),
    task_id         VARCHAR(255),
    status          VARCHAR(50) NOT NULL,
    error_message   TEXT,
    log_url         TEXT,
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP,
    duration_sec    NUMERIC(10, 2),
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monitoring.dq_gate_log (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255),
    layer           VARCHAR(20) NOT NULL,
    model_name      VARCHAR(255) NOT NULL,
    test_name       VARCHAR(255) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    failure_count   INTEGER DEFAULT 0,
    error_detail    TEXT,
    tested_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monitoring.volume_snapshot_log (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255),
    layer           VARCHAR(20) NOT NULL,
    table_name      VARCHAR(255) NOT NULL,
    record_count    BIGINT NOT NULL,
    snapshot_hour   TIMESTAMP NOT NULL,
    pct_change_vs_avg NUMERIC(8, 2),
    is_anomaly      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS monitoring.freshness_log (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255),
    table_name      VARCHAR(255) NOT NULL,
    max_event_time  TIMESTAMP,
    freshness_min   NUMERIC(10, 2),
    status          VARCHAR(20) NOT NULL,
    threshold_min   INTEGER NOT NULL,
    checked_at      TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_created   ON monitoring.pipeline_run_log   (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_gate_tested         ON monitoring.dq_gate_log        (tested_at DESC);
CREATE INDEX IF NOT EXISTS idx_volume_snapshot_hour   ON monitoring.volume_snapshot_log (snapshot_hour DESC);
CREATE INDEX IF NOT EXISTS idx_freshness_checked      ON monitoring.freshness_log       (checked_at DESC);
"""

if __name__ == '__main__':
    print("Connecting to postgres-warehouse (localhost:5402)...")
    conn = psycopg2.connect(**CONN_PARAMS)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(SQL)
    print(" Monitoring schema created successfully!")
    print(" Tables: pipeline_run_log, dq_gate_log, volume_snapshot_log, freshness_log")
    cur.close()
    conn.close()
