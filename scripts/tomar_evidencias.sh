#!/usr/bin/env bash
# archivo: scripts/tomar_evidencias.sh
# Copia los reportes que generan los jobs (limpieza, sentimiento, tasas, balanceo) a docs/evidencias/
# para armar las tablas del informe.
set -euo pipefail
export MSYS_NO_PATHCONV=1
PROYECTO="${COMPOSE_PROJECT_NAME:-lifemap}"
mkdir -p docs/evidencias
docker compose -p "$PROYECTO" cp spark-master:/data/reportes/. docs/evidencias/
echo "Reportes copiados a docs/evidencias/:"
ls -R docs/evidencias
