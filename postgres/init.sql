
CREATE SCHEMA IF NOT EXISTS monitoring;
CREATE SCHEMA IF NOT EXISTS dw;

CREATE TABLE IF NOT EXISTS monitoring.pipeline_run_log (
    id              SERIAL PRIMARY KEY,
    dag_id          VARCHAR(255) NOT NULL,
    run_id          VARCHAR(255),
    task_id         VARCHAR(255),
    status          VARCHAR(50) NOT NULL,       
    error_message   TEXT,
    log_url         TEXT,
    started_at      TIMESTAMP NOT NULL,
    finished_at     TIMESTAMP,
    duration_sec    NUMERIC(10, 2),
    created_at      TIMESTAMP DEFAULT NOW()
);

-- ------------------------------------------------------------
-- Bảng 2: DATA QUALITY GATE LOG
-- Ghi kết quả từng lần Silver Quality Gate chạy
-- ------------------------------------------------------------
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

-- ------------------------------------------------------------
-- Bảng 3: VOLUME SNAPSHOT LOG
-- Ghi số record mỗi bảng mỗi lần pipeline chạy
-- Dùng để phát hiện đột biến (anomaly detection)
-- ------------------------------------------------------------
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

-- ------------------------------------------------------------
-- Bảng 4: FRESHNESS LOG
-- Ghi độ tươi của dữ liệu (data latency)
-- ------------------------------------------------------------
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

-- Indexes để tăng tốc Grafana query
CREATE INDEX IF NOT EXISTS idx_pipeline_run_created   ON monitoring.pipeline_run_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_gate_tested         ON monitoring.dq_gate_log      (tested_at DESC);
CREATE INDEX IF NOT EXISTS idx_volume_snapshot_hour   ON monitoring.volume_snapshot_log (snapshot_hour DESC);
CREATE INDEX IF NOT EXISTS idx_freshness_checked      ON monitoring.freshness_log    (checked_at DESC);