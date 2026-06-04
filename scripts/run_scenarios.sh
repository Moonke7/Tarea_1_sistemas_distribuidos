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
trap 'echo ""; echo "[SCENARIO] Interrumpido. Limpiando..."; kill $CONSUMER_LOGS_PID 2>/dev/null || true; cleanup; exit 1' INT TERM

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
    docker compose up -d monitor

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
    docker compose stop monitor

    local outfile="resulta2/${name}.json"
    echo "[SCENARIO] Recolectando métricas → $outfile"
    python3 scripts/collect_scenario_metrics.py "$outfile" "$scale" "$(echo "$DISTRIBUTION" | tr '[:upper:]' '[:lower:]')"

    echo "[SCENARIO] Escenario '$name' completado."
    echo ""
}

run_failure_scenario() {
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
    docker compose up -d monitor

    echo "[SCENARIO] Mostrando logs del consumidor (tráfico corriendo en segundo plano)..."
    docker compose logs -f consumer &
    CONSUMER_LOGS_PID=$!

    echo "[SCENARIO] Esperando 25s antes de simular caída de responses..."
    sleep 25

    echo "[SCENARIO] Deteniendo responses (simulando falla)..."
    docker compose stop responses
    docker compose exec -T metrics psql -U sistemas_d -d metrics_db -c "INSERT INTO service_events (service, event) VALUES ('responses', 'down');"

    echo "[SCENARIO] Esperando 15s con responses caído..."
    sleep 15

    echo "[SCENARIO] Reiniciando responses (recuperación)..."
    docker compose start responses

    # Espera a que trafic termine de publicar mensajes
    docker compose wait trafic 2>/dev/null
    echo "[SCENARIO] Trafic terminó. Esperando que los consumers drenen Kafka..."

    # Espera a que todos los consumers terminen de procesar
    while docker compose ps --status running consumer 2>/dev/null | grep -q "consumer"; do
        sleep 3
    done
    echo "[SCENARIO] Todos los consumers terminaron. Recolectando métricas..."

    kill $CONSUMER_LOGS_PID 2>/dev/null || true
    docker compose stop monitor

    local outfile="resulta2/${name}.json"
    echo "[SCENARIO] Recolectando métricas → $outfile"
    python3 scripts/collect_scenario_metrics.py "$outfile" "$scale" "$(echo "$DISTRIBUTION" | tr '[:upper:]' '[:lower:]')"

    echo "[SCENARIO] Escenario '$name' completado."
    echo ""
}

run_no_retry_scenario() {
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

    echo "[SCENARIO] Levantando servicios con $scale consumidor(es) (SIN REINTENTOS)..."
    export DISTRIBUTION=$DISTRIBUTION
    export FAILURE_RATE=0.3
    export KAFKA_PARTITIONS=$scale
    export ENABLE_RETRIES=false
    docker compose up --build -d --scale consumer="$scale"

    echo "[SCENARIO] Iniciando monitor de backlog..."
    docker compose up -d monitor

    echo "[SCENARIO] Mostrando logs del consumidor..."
    docker compose logs -f consumer &
    CONSUMER_LOGS_PID=$!

    docker compose wait trafic 2>/dev/null
    echo "[SCENARIO] Trafic terminó. Esperando que los consumers drenen Kafka..."

    while docker compose ps --status running consumer 2>/dev/null | grep -q "consumer"; do
        sleep 3
    done
    echo "[SCENARIO] Todos los consumers terminaron. Recolectando métricas..."

    kill $CONSUMER_LOGS_PID 2>/dev/null || true
    docker compose stop monitor

    local outfile="resulta2/${name}.json"
    echo "[SCENARIO] Recolectando métricas → $outfile"
    python3 scripts/collect_scenario_metrics.py "$outfile" "$scale" "$(echo "$DISTRIBUTION" | tr '[:upper:]' '[:lower:]')"

    echo "[SCENARIO] Escenario '$name' completado."
    echo ""
}


run_spike_scenario() {
    local name="$1"
    local scale="$2"
    local description="$3"

    # Exportar variables para el spike
    export SPIKE_MODE=true
    export SPIKE_START_DELAY=8
    export SPIKE_DURATION=10
    export SPIKE_SLEEP=0

    run_scenario "${name}" "${scale}" "${description}"

    unset SPIKE_MODE SPIKE_START_DELAY SPIKE_DURATION SPIKE_SLEEP
}

# ─── Ejecución ─────────────────────────────────

# Escenario 10: Sin reintentos con 1 consumidor
#run_no_retry_scenario "kafka-no-retry-1-consumer" "1" "Sin Reintentos + 1 Consumer"

# Escenario 11: Sin reintentos con 5 consumidores
#run_no_retry_scenario "kafka-no-retry-5-consumers" "5" "Sin Reintentos + 5 Consumers"

# Escenario 12: Sin reintentos con 10 consumidores
#run_no_retry_scenario "kafka-no-retry-10-consumers" "10" "Sin Reintentos + 10 Consumers"


# Escenario 4: Recuperación ante falla con 1 consumidor
#run_failure_scenario "kafka-failure-1-consumer" "1" "Falla + 1 Consumer"

# Escenario 5: Recuperación ante falla con 5 consumidores
#run_failure_scenario "kafka-failure-5-consumers" "5" "Falla + 5 Consumers"

# Escenario 6: Recuperación ante falla con 10 consumidores
#run_failure_scenario "kafka-failure-10-consumers" "10" "Falla + 10 Consumers"

# Escenario 1: 1 consumidor
run_scenario "kafka-1-consumer" "1" "Kafka"

# Escenario 2: 5 consumidores
run_scenario "kafka-5-consumers" "5" "Kafka"

# Escenario 3: 10 consumidores
run_scenario "kafka-10-consumers" "10" "Kafka"

# Escenario 7: Spike de tráfico con 1 consumidor
#run_spike_scenario "kafka-spike-1-consumer" "1" "Spike de Tráfico + 1 Consumer"

# Escenario 8: Spike de tráfico con 5 consumidores
#run_spike_scenario "kafka-spike-5-consumers" "5" "Spike de Tráfico + 5 Consumers"

# Escenario 9: Spike de tráfico con 10 consumidores
#run_spike_scenario "kafka-spike-10-consumers" "10" "Spike de Tráfico + 10 Consumers"


echo ""
echo "═══════════════════════════════════════════════════"
echo "  Escenarios completados."
echo "  Resultados en resulta2/"
echo "═══════════════════════════════════════════════════"
