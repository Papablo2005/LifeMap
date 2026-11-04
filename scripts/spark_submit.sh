#!/usr/bin/env bash
# archivo: scripts/spark_submit.sh
# Ejecuta un job de Spark dentro del contenedor spark-master (el driver corre ahí; los ejecutores, en spark-worker).
# Uso:   bash scripts/spark_submit.sh sentimiento | mapreduce_tasas | balanceo
# Variables opcionales (se pasan al job):  REPS  FRAC_MUESTRA  MIN_DIAS_NEGOCIO  N_FEATURES  SEMILLA  ...
set -euo pipefail
export MSYS_NO_PATHCONV=1            # Git Bash (Windows): no convertir /rutas en rutas de Windows
JOB="${1:?Indique el job: sentimiento | mapreduce_tasas | balanceo}"
PROYECTO="${COMPOSE_PROJECT_NAME:-lifemap}"

ENVS=()
for v in REPS FRAC_MUESTRA MIN_DIAS_NEGOCIO N_FEATURES SEMILLA GEOHASH_PRECISION SHUFFLE_PARTS; do
  if [ -n "${!v:-}" ]; then ENVS+=(-e "$v=${!v}"); fi
done

docker compose -p "$PROYECTO" exec -T "${ENVS[@]}" spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.host=spark-master \
  --driver-memory "${DRIVER_MEM:-1500m}" --executor-memory "${EXECUTOR_MEM:-1500m}" \
  "/opt/jobs/${JOB}.py"
