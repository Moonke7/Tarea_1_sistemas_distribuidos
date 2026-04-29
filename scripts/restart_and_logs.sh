#!/bin/bash
cd "$(dirname "$0")/.." || exit

echo "Deteniendo contenedores y limpiando volúmenes (BD y caché reset)..."
docker compose down -v

echo "Reconstruyendo e iniciando servicios en background..."
docker compose up --build -d

echo "Conectando a los logs..."
docker compose logs -f
