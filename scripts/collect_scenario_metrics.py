import sys
import json
import subprocess

def run_cmd(cmd):
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.strip()

def get_pg_json(query):
    cmd = f'docker compose exec -T metrics psql -U sistemas_d -d metrics_db -qtA -c "SELECT row_to_json(t) FROM ({query}) t;"'
    res = run_cmd(cmd)
    return json.loads(res) if res else {}



def main():
    if len(sys.argv) < 4:
        print("Uso: collect_scenario_metrics.py <file> <consumers> <dist>")
        sys.exit(1)
        
    out_file, consumers, dist = sys.argv[1:4]
    
    kafka_throughput = get_pg_json('''
        SELECT 
            COUNT(*) as total_consultas,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as exitosas,
            ROUND(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp)))::numeric, 2) as tiempo_total_segundos,
            ROUND((SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / GREATEST(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))), 1))::numeric, 2) as throughput_qps
        FROM kafka_query_metrics
    ''')
    
    kafka_latency = get_pg_json('''
        SELECT 
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) as latencia_p50_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as latencia_p95_ms
        FROM kafka_query_metrics
        WHERE status = 'success'
    ''')
    
    kafka_retry_rate = get_pg_json('''
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) as con_reintentos,
            ROUND((SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)), 2) as retry_rate_porcentaje
        FROM kafka_query_metrics
    ''')
    
    kafka_recovery_rate = get_pg_json('''
        WITH retry_stats AS (
            SELECT 
                SUM(CASE WHEN retry_count > 0 THEN 1 ELSE 0 END) as total_reintentos,
                SUM(CASE WHEN retry_count > 0 AND status = 'success' THEN 1 ELSE 0 END) as recuperados
            FROM kafka_query_metrics
        )
        SELECT 
            total_reintentos,
            recuperados,
            ROUND((recuperados * 100.0 / NULLIF(total_reintentos, 0)), 2) as recovery_rate_porcentaje
        FROM retry_stats
    ''')
    
    kafka_dlq_rate = get_pg_json('''
        WITH dlq_stats AS (
            SELECT 
                (SELECT COUNT(*) FROM kafka_query_metrics) as total_mensajes,
                (SELECT COUNT(*) FROM dlq_log) as total_dlq
        )
        SELECT 
            total_mensajes,
            total_dlq,
            ROUND((total_dlq * 100.0 / NULLIF(total_mensajes, 0)), 2) as dlq_rate_porcentaje
        FROM dlq_stats
    ''')
    
    kafka_backlog_size = get_pg_json('''
        WITH total_lag_per_ts AS (
            SELECT timestamp, SUM(lag) as total_lag
            FROM backlog_samples
            GROUP BY timestamp
        )
        SELECT
            MAX(total_lag) as peak_lag,
            MAX(timestamp) as last_sample_ts,
            COUNT(*) as total_samples
        FROM total_lag_per_ts
    ''')

    kafka_recovery_time = get_pg_json('''
        WITH restore_event AS (
            SELECT MAX(timestamp) as restore_ts
            FROM service_events
            WHERE service = 'responses' AND event = 'up'
        ),
        lag_after_restore AS (
            SELECT timestamp, SUM(lag) as total_lag
            FROM backlog_samples
            WHERE timestamp > (SELECT restore_ts FROM restore_event)
            GROUP BY timestamp
        ),
        drained_at AS (
            SELECT MIN(timestamp) as drained_ts
            FROM lag_after_restore
            WHERE total_lag <= 5
        )
        SELECT
            (SELECT restore_ts FROM restore_event) as restore_timestamp,
            (SELECT drained_ts FROM drained_at) as drained_timestamp,
            CASE
                WHEN (SELECT restore_ts FROM restore_event) IS NOT NULL
                 AND (SELECT drained_ts FROM drained_at) IS NOT NULL
                THEN ROUND(EXTRACT(EPOCH FROM (
                    (SELECT drained_ts FROM drained_at) - (SELECT restore_ts FROM restore_event)
                ))::numeric, 2)
                ELSE NULL
            END as recovery_time_seconds
    ''')
    
    scenario_data = {
        "escenario": {
            "consumidores": int(consumers),
            "distribucion": dist,
        },
        "resultados": {
            "throughput": kafka_throughput,
            "latencia": kafka_latency,
            "retry_rate": kafka_retry_rate,
            "recovery_rate": kafka_recovery_rate,
            "dlq_rate": kafka_dlq_rate,
            "backlog_size": kafka_backlog_size,
            "recovery_time": kafka_recovery_time
        }
    }
    
    with open(out_file, 'w') as f:
        json.dump(scenario_data, f, indent=2)

if __name__ == "__main__":
    main()
