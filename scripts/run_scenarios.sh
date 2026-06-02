#!/bin/bash
# run_scenarios.sh — Escenarios T2 (Apache Kafka)
# Reemplaza los escenarios de T1 por los de T2.
#
# Escenarios:
#   1. Kafka + 1 Consumer
#   2. Kafka + 3 Consumers
#   3. Falla Temporal (automática: detiene responses 45s)
#   4. Falla Temporal (manual: pausar responses desde Docker Desktop)
#   5. Spike de Tráfico
#
# Uso:
#   ./scripts/run_scenarios.sh              # corre todos
#   ./scripts/run_scenarios.sh kafka-1       # corre solo uno
#

cd "$(dirname "$0")/.." || exit

mkdir -p resulta2

DISTRIBUTION="${DISTRIBUTION:-ZIPF}"
SCENARIO_FILTER="$1"

cleanup() {
    echo "[SCENARIO] Deteniendo monitor_backlog si está corriendo..."
    kill %1 2>/dev/null || true
    echo "[SCENARIO] Limpiando contenedores..."
    docker compose down -v
}

run_scenario() {
    local name="$1"
    local scale="$2"
    local extra_env="$3"
    local description="$4"

    if [ -n "$SCENARIO_FILTER" ] && [ "$SCENARIO_FILTER" != "$name" ]; then
        return
    fi

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Escenario: $description"
    echo "═══════════════════════════════════════════════════"

    cleanup

    echo "[SCENARIO] Levantando servicios (consumer=$scale)..."
    export DISTRIBUTION=$DISTRIBUTION
    export $extra_env 2>/dev/null || true
    docker compose up --build -d --scale consumer="$scale"

    echo "[SCENARIO] Iniciando monitor de backlog..."
    python3 scripts/monitor_backlog.py &
    MONITOR_PID=$!

    echo "[SCENARIO] Ejecutando tráfico (logs en vivo)..."
    docker compose logs -f trafic

    echo "[SCENARIO] Tráfico terminado. Deteniendo monitor..."
    kill $MONITOR_PID 2>/dev/null || true

    local outfile="resulta2/${name}.json"
    echo "[SCENARIO] Recolectando métricas → $outfile"
    python3 scripts/collect_scenario_metrics.py \
        "$outfile" \
        "kafka" \
        "$scale" \
        "${DISTRIBUTION,,}" \
        "$(echo $extra_env | sed 's/.*=//')"

    echo "[SCENARIO] Escenario '$name' completado."
    echo ""
}

run_scenario_manual_failure() {
    local name="$1"
    local scale="$2"

    if [ -n "$SCENARIO_FILTER" ] && [ "$SCENARIO_FILTER" != "$name" ]; then
        return
    fi

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Escenario: Falla Temporal (manual)"
    echo "═══════════════════════════════════════════════════"

    cleanup

    echo "[SCENARIO] Levantando servicios (consumer=$scale)..."
    export DISTRIBUTION=$DISTRIBUTION
    docker compose up --build -d --scale consumer="$scale"

    echo "[SCENARIO] Iniciando monitor de backlog..."
    python3 scripts/monitor_backlog.py &
    MONITOR_PID=$!

    echo ""
    echo "┌─────────────────────────────────────────────────────────┐"
    echo "│ INSTRUCCIÓN: Detén el contenedor 'responses' desde     │"
    echo "│ Docker Desktop (docker stop) y presiona ENTER para      │"
    echo "│ iniciar el tráfico.                                     │"
    echo "└─────────────────────────────────────────────────────────┘"
    read -r

    echo "[SCENARIO] Ejecutando tráfico (logs en vivo)..."
    docker compose logs -f trafic &

    echo ""
    echo "┌─────────────────────────────────────────────────────────┐"
    echo "│ INSTRUCCIÓN: Si aún no lo has hecho, levanta 'responses'│"
    echo "│ desde Docker Desktop ahora. Espera a que el tráfico     │"
    echo "│ termine automáticamente.                                │"
    echo "└─────────────────────────────────────────────────────────┘"
    wait

    echo "[SCENARIO] Tráfico terminado. Deteniendo monitor..."
    kill $MONITOR_PID 2>/dev/null || true

    local outfile="resulta2/${name}.json"
    echo "[SCENARIO] Recolectando métricas → $outfile"
    python3 scripts/collect_scenario_metrics.py \
        "$outfile" \
        "kafka" \
        "$scale" \
        "${DISTRIBUTION,,}" \
        "manual-failure"

    echo "[SCENARIO] Escenario '$name' completado."
    echo ""
}

run_scenario_auto_failure() {
    local name="$1"
    local scale="$2"
    local failure_duration="${3:-45}"

    if [ -n "$SCENARIO_FILTER" ] && [ "$SCENARIO_FILTER" != "$name" ]; then
        return
    fi

    echo ""
    echo "═══════════════════════════════════════════════════"
    echo "  Escenario: Falla Temporal (automática)"
    echo "  Caída de responses por ${failure_duration}s"
    echo "═══════════════════════════════════════════════════"

    cleanup

    echo "[SCENARIO] Levantando servicios (consumer=$scale)..."
    export DISTRIBUTION=$DISTRIBUTION
    docker compose up --build -d --scale consumer="$scale"

    echo "[SCENARIO] Iniciando monitor de backlog..."
    python3 scripts/monitor_backlog.py &
    MONITOR_PID=$!

    echo "[SCENARIO] Iniciando tráfico en background..."
    docker compose logs -f trafic &
    TRAFIC_PID=$!

    sleep 5

    echo "[SCENARIO] Deteniendo responses por ${failure_duration}s..."
    docker compose stop responses

    sleep "$failure_duration"

    echo "[SCENARIO] Levantando responses nuevamente..."
    docker compose start responses

    wait $TRAFIC_PID 2>/dev/null || true

    echo "[SCENARIO] Tráfico terminado. Deteniendo monitor..."
    kill $MONITOR_PID 2>/dev/null || true

    local outfile="resulta2/${name}.json"
    echo "[SCENARIO] Recolectando métricas → $outfile"
    python3 scripts/collect_scenario_metrics.py \
        "$outfile" \
        "kafka" \
        "$scale" \
        "${DISTRIBUTION,,}" \
        "auto-failure-${failure_duration}s"

    echo "[SCENARIO] Escenario '$name' completado."
    echo ""
}


# ─── Ejecución de escenarios ─────────────────────────────────

# Escenario 1: Kafka + 1 Consumer
run_scenario "kafka-1-consumer" "1" "" "Kafka + 1 Consumer"

# Escenario 2: Kafka + 3 Consumers
run_scenario "kafka-3-consumers" "3" "" "Kafka + 3 Consumers"

# Escenario 3: Falla Temporal automática
run_scenario_auto_failure "falla-temporal-auto" "1" "45"

# Escenario 4: Falla Temporal manual
run_scenario_manual_failure "falla-temporal-manual" "1"

# Escenario 5: Spike de Tráfico (1 consumer, muchos workers)
run_scenario "spike-trafico" "1" "MAX_WORKERS=50" "Spike de Tráfico (MAX_WORKERS=50)"


echo ""
echo "═══════════════════════════════════════════════════"
echo "  Todos los escenarios T2 completados."
echo "  Resultados en resulta2/"
echo "═══════════════════════════════════════════════════"
