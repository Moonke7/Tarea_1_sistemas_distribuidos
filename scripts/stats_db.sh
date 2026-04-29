#!/bin/bash

cd "$(dirname "$0")/.." || exit

echo "📊 --- ESTADÍSTICAS DEL SISTEMA DE DISTRIBUCIÓN --- 📊"

echo -e "\n1️⃣  Hit Rate (Hits / (Hits + Misses)):"
docker compose exec -T metrics psql -U sistemas_d -d metrics_db -c "
SELECT 
    SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) as hits,
    SUM(CASE WHEN source = 'responses' THEN 1 ELSE 0 END) as misses,
    COUNT(*) as total,
    ROUND((SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)), 2) as hit_rate_porcentaje
FROM query_metrics;
"

echo -e "\n2️⃣  Throughput (Consultas exitosas / segundo):"
docker compose exec -T metrics psql -U sistemas_d -d metrics_db -c "
SELECT 
    COUNT(*) as total_consultas,
    ROUND(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp)))::numeric, 2) as tiempo_total_segundos,
    ROUND((COUNT(*) / GREATEST(EXTRACT(EPOCH FROM (MAX(timestamp) - MIN(timestamp))), 1))::numeric, 2) as throughput_qps
FROM query_metrics;
"

echo -e "\n3️⃣  Latencia p50 / p95 (Percentiles de tiempo de respuesta):"
docker compose exec -T metrics psql -U sistemas_d -d metrics_db -c "
SELECT 
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms) as latencia_p50_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as latencia_p95_ms
FROM query_metrics;
"

echo -e "\n4️⃣  Eviction Rate (Evictions / minuto en Redis):"
# Obtenemos uptime en segundos y evicted keys directamente desde redis
UPTIME=$(docker compose exec -T redis redis-cli info server | grep uptime_in_seconds | tr -d '\r' | cut -d: -f2)
EVICTED=$(docker compose exec -T redis redis-cli info stats | grep evicted_keys | tr -d '\r' | cut -d: -f2)
EXPIRED=$(docker compose exec -T redis redis-cli info stats | grep expired_keys | tr -d '\r' | cut -d: -f2)

echo "  Total Evicted Keys (memoria): $EVICTED"
echo "  Total Expired Keys (TTL): $EXPIRED"

if [ -n "$UPTIME" ] && [ -n "$EVICTED" ] && [ "$UPTIME" -gt 0 ]; then
    EVICTIONS_PER_MIN=$(awk "BEGIN {print ($EVICTED / ($UPTIME / 60))}")
    echo "  Total Evicted Keys: $EVICTED"
    echo "  Uptime (minutos): $(awk "BEGIN {print $UPTIME / 60}")"
    echo "  Eviction Rate: $EVICTIONS_PER_MIN evictions/minuto"
else
    echo "  No se pudo calcular el Eviction Rate. Verifica que Redis esté corriendo."
fi

echo -e "\n5️⃣  Cache Efficiency ((hits*t_cache - misses*t_db) / total):"
docker compose exec -T metrics psql -U sistemas_d -d metrics_db -c "
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
    hits, 
    misses, 
    ROUND(t_cache::numeric, 2) as t_cache_ms, 
    ROUND(t_db::numeric, 2) as t_db_ms,
    ROUND(((hits * t_cache - misses * t_db) / NULLIF(total, 0))::numeric, 2) as cache_efficiency
FROM stats;
"

echo -e "\n6️⃣  Tasa de hits por zona"
docker compose exec -T metrics psql -U sistemas_d -d metrics_db -c "
SELECT 
    zone_id as zona,
    SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) as hits,
    COUNT(*) as total,
    ROUND((SUM(CASE WHEN source = 'cache' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0)), 2) as hit_rate_porcentaje
FROM query_metrics
WHERE zone_id IS NOT NULL
GROUP BY zone_id
ORDER BY zona;
"

echo -e "\n7️⃣  Uso de Memoria en Caché (Redis):"
USED_MEM=$(docker compose exec -T redis redis-cli info memory | grep '^used_memory_human:' | tr -d '\r' | cut -d: -f2)
MAX_MEM=$(docker compose exec -T redis redis-cli info memory | grep '^maxmemory_human:' | tr -d '\r' | cut -d: -f2)
POLITICA=$(docker compose exec -T redis redis-cli info memory | grep '^maxmemory_policy:' | tr -d '\r' | cut -d: -f2)

echo "  Memoria usada  : $USED_MEM"
echo "  Límite máximo  : $MAX_MEM"
echo "  Política actual: $POLITICA"
echo ""