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
    if len(sys.argv) != 5:
        print("Uso: collect_scenario_metrics.py <file> <mem> <dist> <pol>")
        sys.exit(1)
        
    out_file, mem, dist, pol = sys.argv[1:5]
    
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
    
    scenario_data = {
        "escenario": {
            "memoria": mem,
            "distribucion": dist,
            "politica": pol
        },
        "resultados": {
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
        }
    }
    
    with open(out_file, 'w') as f:
        json.dump(scenario_data, f, indent=2)

if __name__ == "__main__":
    main()
