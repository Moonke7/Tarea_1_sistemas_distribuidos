#!/bin/bash

cd "$(dirname "$0")/.." || exit

mkdir -p resulta2

DISTRIBUTION="${DISTRIBUTION:-ZIPF}"
SCENARIO_FILTER="$1"

cleanup() {
    echo "[SCENARIO] Limpiando contenedores..."
    docker compose down -v
}

# Permite Ctrl+C: mata procesos hijos y limpia Docker
trap 'echo ""; echo "[SCENARIO] Interrumpido. Limpiando..."; kill $MONITOR_PID $CONSUMER_LOGS_PID 2>/dev/null || true; cleanup; exit 1' INT TERM

run_scenario() {
    local name="$1"
    local scale="$2"
    local description="$3"

    if [ -n "$SCENARIO_FILTER" ] && [ "$SCENARIO_FILTER" != "$name" ]; then
        return
    fi

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Escenario: $description ($scale consumidores)"
    echo "═══════════════════════════════════════════════════"

    cleanup

    echo "[SCENARIO] Levantando servicios con $scale consumidor(es)..."
    export DISTRIBUTION=$DISTRIBUTION
    export FAILURE_RATE=0.3
    export KAFKA_PARTITIONS=$scale
    docker compose up --build -d --scale consumer="$scale"

    echo "[SCENARIO] Iniciando monitor de backlog..."
    python3 scripts/monitor_backlog.py &
    MONITOR_PID=$!

    echo "[SCENARIO] Mostrando logs del consumidor (tráfico corriendo en segundo plano)..."
    docker compose logs -f consumer &
    CONSUMER_LOGS_PID=$!

    # Espera a que trafic termine de publicar mensajes
    docker compose wait trafic 2>/dev/null
    echo "[SCENARIO] Trafic terminó. Esperando que los consumers drenen Kafka..."

    # Espera a que todos los consumers terminen de procesar
    # (sondea cada 3s hasta que no quede ningún consumer corriendo)
    while docker compose ps --status running consumer 2>/dev/null | grep -q "consumer"; do
        sleep 3
    done
    echo "[SCENARIO] Todos los consumers terminaron. Recolectando métricas..."

    kill $CONSUMER_LOGS_PID 2>/dev/null || true
    kill $MONITOR_PID 2>/dev/null || true

    local outfile="resulta2/${name}.json"
    echo "[SCENARIO] Recolectando métricas → $outfile"
    python3 scripts/collect_scenario_metrics.py "$outfile" "$scale" "$(echo "$DISTRIBUTION" | tr '[:upper:]' '[:lower:]')"

    echo "[SCENARIO] Escenario '$name' completado."
    echo ""
}


# ─── Ejecución ─────────────────────────────────

# Escenario 1: 1 consumidor
run_scenario "kafka-1-consumer" "1" "Kafka"

# Escenario 2: 5 consumidores
run_scenario "kafka-5-consumers" "5" "Kafka"

# Escenario 3: 10 consumidores
run_scenario "kafka-10-consumers" "10" "Kafka"


echo ""
echo "═══════════════════════════════════════════════════"
echo "  Escenarios completados."
echo "  Resultados en resulta2/"
echo "═══════════════════════════════════════════════════"
