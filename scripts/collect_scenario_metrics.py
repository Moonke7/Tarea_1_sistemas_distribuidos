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

def get_pg_json_agg(query):
    cmd = f'docker compose exec -T metrics psql -U sistemas_d -d metrics_db -qtA -c "SELECT coalesce(json_agg(row_to_json(t)), \'[]\') FROM ({query}) t;"'
    res = run_cmd(cmd)
    return json.loads(res) if res else []

def main():
    if len(sys.argv) < 4:
        print("Uso: collect_scenario_metrics.py <file> <consumers> <dist>")
        sys.exit(1)
        
    out_file, consumers, dist = sys.argv[1:4]
    
    hit_rate = get_pg_json('''
        SELECT 
            SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) as hits,
            SUM(CASE WHEN source = 'responses' THEN 1 ELSE 0 END) as misses,
            COUNT(*) as total,
            ROUND((SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)), 2) as hit_rate_porcentaje
        FROM query_metrics
    ''')
    
    throughput = get_pg_json('''
        SELECT 
            COUNT(*) as total_consultas,
            ROUND(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp)))::numeric, 2) as tiempo_total_segundos,
            ROUND((COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))), 1))::numeric, 2) as throughput_qps
        FROM query_metrics
    ''')
    
    latency = get_pg_json('''
        SELECT 
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) as latencia_p50_ms,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as latencia_p95_ms
        FROM query_metrics
    ''')
    
    uptime = run_cmd("docker compose exec -T redis redis-cli info server | grep uptime_in_seconds | tr -d '\r' | cut -d: -f2")
    evicted = run_cmd("docker compose exec -T redis redis-cli info stats | grep evicted_keys | tr -d '\r' | cut -d: -f2")
    expired = run_cmd("docker compose exec -T redis redis-cli info stats | grep expired_keys | tr -d '\r' | cut -d: -f2")
    
    evictions_per_min = 0.0
    if uptime and evicted and int(uptime) > 0:
        evictions_per_min = float(evicted) / (float(uptime) / 60)
        
    efficiency = get_pg_json('''
        WITH stats AS (
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) as hits,
                SUM(CASE WHEN source = 'responses' THEN 1 ELSE 0 END) as misses,
                COALESCE(AVG(CASE WHEN source = 'cache' THEN latency_ms END), 0) as t_cache,
                COALESCE(AVG(CASE WHEN source = 'responses' THEN latency_ms END), 0) as t_db
            FROM query_metrics
        )
        SELECT 
            ROUND(((hits * t_cache - misses * t_db) / NULLIF(total, 0))::numeric, 2) as cache_efficiency
        FROM stats
    ''')
    
    zones = get_pg_json_agg('''
        SELECT 
            zone_id as zona,
            SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) as hits,
            COUNT(*) as total,
            ROUND((SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)), 2) as hit_rate_porcentaje
        FROM query_metrics
        WHERE zone_id IS NOT NULL
        GROUP BY zone_id
        ORDER BY zona
    ''')
    
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
    
    kafka_backlog = get_pg_json('''
        SELECT 
            topic,
            partition,
            consumer_group,
            lag,
            timestamp
        FROM backlog_samples
        ORDER BY timestamp DESC
        LIMIT 1
    ''')
    
    scenario_data = {
        "escenario": {
            "consumidores": int(consumers),
            "distribucion": dist,
        },
        "resultados_tarea1": {
            "hit_rate": hit_rate,
            "throughput": throughput,
            "latencia": latency,
            "eviction": {
                "evicted_keys": int(evicted) if evicted else 0,
                "expired_keys": int(expired) if expired else 0,
                "evictions_per_min": round(evictions_per_min, 2)
            },
            "efficiency": efficiency,
            "zonas": zones
        },
        "resultados_tarea2": {
            "throughput": kafka_throughput,
            "latencia": kafka_latency,
            "retry_rate": kafka_retry_rate,
            "recovery_rate": kafka_recovery_rate,
            "dlq_rate": kafka_dlq_rate,
            "backlog": kafka_backlog
        }
    }
    
    with open(out_file, 'w') as f:
        json.dump(scenario_data, f, indent=2)

if __name__ == "__main__":
    main()
