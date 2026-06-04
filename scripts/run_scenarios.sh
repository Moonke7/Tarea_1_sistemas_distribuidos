#!/bin/bash
cd "$(dirname "$0")/.." || exit

mkdir -p resulta2

MEMORIES=("50mb")
DISTRIBUTIONS=("ZIPF")
POLICIES=("allkeys-lfu")

for mem in "${MEMORIES[@]}"; do
    for dist in "${DISTRIBUTIONS[@]}"; do
        for pol in "${POLICIES[@]}"; do
            echo "▶️  Corriendo escenario: Memoria=$mem, Dist=$dist, Politica=$pol"
            
            export REDIS_MAXMEMORY=$mem
            export DISTRIBUTION=$dist
            export REDIS_POLICY=$pol
            
            # salida de los archivos de metricas
            POL_SHORT=${pol/allkeys-/}
            DIST_LOWER=$(echo "$dist" | tr '[:upper:]' '[:lower:]')
            OUTPUT_FILE="resulta2/${mem}_${POL_SHORT}_${DIST_LOWER}.json"
            
            echo "Deteniendo y limpiando contenedores..."
            docker compose down -v
            
            echo "Iniciando servicios..."
            docker compose up --build -d
            
            echo "Ejecutando tráfico y esperando a que termine (mostrando logs en vivo)..."
            docker compose logs -f trafic
            
            echo "Recolectando métricas para este escenario..."
            python3 scripts/collect_scenario_metrics.py "$OUTPUT_FILE" "$mem" "$dist" "$pol"
            
        done
    done
done

echo "✅ Resultados guardados en la carpeta resulta2/"

# ─── Escenario de falla ─────────────────────────────────
echo ""
echo "▶️  Corriendo escenario de falla: responses se apaga durante ejecución"

export REDIS_MAXMEMORY=50mb
export DISTRIBUTION=ZIPF
export REDIS_POLICY=allkeys-lfu
export REQUEST_TIMEOUT=2

OUTPUT_FILE="resulta2/failure_test.json"

echo "Deteniendo y limpiando contenedores..."
docker compose down -v

echo "Iniciando servicios..."
docker compose up --build -d

echo "Esperando 5s con tráfico normal..."
sleep 5

echo "🔥 Apagando responses..."
docker compose stop responses

echo "Esperando 10s con responses caído..."
sleep 10

echo "✅ Levantando responses..."
docker compose start responses

echo "Esperando a que termine el tráfico..."
docker compose wait trafic

echo "Recolectando métricas para escenario de falla..."
python3 scripts/collect_scenario_metrics.py "$OUTPUT_FILE" "50mb" "ZIPF" "allkeys-lfu" "failure"

echo "✅ Escenario de falla completado"
