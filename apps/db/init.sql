CREATE TABLE IF NOT EXISTS query_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    query_type VARCHAR(10),
    zone_id VARCHAR(10),
    cache_key VARCHAR(255),
    source VARCHAR(20),
    latency_ms FLOAT
);

CREATE TABLE IF NOT EXISTS kafka_query_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    message_id VARCHAR(255),
    query_type VARCHAR(10),
    zone_id VARCHAR(10),
    cache_key VARCHAR(255),
    consumer_id VARCHAR(50),
    retry_count INT DEFAULT 0,
    source VARCHAR(20),
    latency_ms FLOAT,
    status VARCHAR(20),
    error_reason TEXT
);

CREATE TABLE IF NOT EXISTS dlq_log (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    message_id VARCHAR(255) UNIQUE,
    payload JSONB,
    total_retries INT,
    reason TEXT
);

CREATE TABLE IF NOT EXISTS backlog_samples (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    topic VARCHAR(100),
    partition INT,
    consumer_group VARCHAR(100),
    lag BIGINT
);

CREATE TABLE IF NOT EXISTS consumer_stats (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    consumer_id VARCHAR(50),
    messages_processed INT DEFAULT 0,
    messages_failed INT DEFAULT 0,
    avg_latency_ms FLOAT
);

CREATE TABLE IF NOT EXISTS service_events (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    service VARCHAR(50),
    event VARCHAR(20)
);
