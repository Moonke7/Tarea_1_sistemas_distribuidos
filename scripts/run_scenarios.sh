#!/bin/bash
cd "$(dirname "$0")/.." || exit

mkdir -p resulta2

MEMORIES=("50mb")
DISTRIBUTIONS=("ZIPF" "UNIFORME")
POLICIES=("allkeys-lfu" "allkeys-random")

for mem in "${MEMORIES[@]}"; do
    for dist in "${DISTRIBUTIONS[@]}"; do
        for pol in "${POLICIES[@]}"; do
            echo "======================================================"
            echo "▶️  Corriendo escenario: Memoria=$mem, Dist=$dist, Politica=$pol"
            echo "======================================================"
            
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

echo "======================================================"
echo "✅ Todos los escenarios completados."
echo "✅ Resultados guardados en la carpeta results/"
echo "======================================================"
