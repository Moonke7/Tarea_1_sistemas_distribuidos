CREATE TABLE IF NOT EXISTS query_metrics (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    query_type VARCHAR(10),
    zone_id VARCHAR(10),
    cache_key VARCHAR(255),
    source VARCHAR(20),
    latency_ms FLOAT
);
