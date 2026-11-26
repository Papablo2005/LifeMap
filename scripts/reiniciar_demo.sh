#!/usr/bin/env bash
# archivo: scripts/reiniciar_demo.sh
# Deja el streaming "en T" para repetir la demo desde cero (ensayos y sustentación):
#   borra lo que produjo el streaming (reviews_stream, alertas, ventanas del streaming), vacía el topic,
#   borra los checkpoints y reconstruye el estado de los negocios en T (sin reentrenar el modelo).
# NO toca los datos de la Entrega 1 ni los modelos. Uso:  bash scripts/reiniciar_demo.sh
set -euo pipefail
export MSYS_NO_PATHCONV=1
P="${COMPOSE_PROJECT_NAME:-lifemap}"
DC="docker compose -p $P"

echo "1/5 Deteniendo el streaming..."
$DC stop streaming

echo "2/5 Borrando resultados del streaming en MongoDB..."
$DC exec -T mongo sh -c 'mongosh --quiet -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin lifemap --eval "
  db.reviews_stream.drop(); db.alerts.drop();
  printjson(db.ventanas_geohash.deleteMany({fuente: \"streaming\"}));
  db.meta.deleteOne({_id: \"streaming\"});"'

echo "3/5 Vaciando el topic reviews (se borra y se vuelve a crear con 3 particiones)..."
$DC exec -T kafka bash -c 'export KAFKA_HEAP_OPTS=-Xmx128m; T=/opt/kafka/bin/kafka-topics.sh;
  $T --bootstrap-server localhost:9092 --delete --topic reviews || true; sleep 5;
  $T --bootstrap-server localhost:9092 --create --if-not-exists --topic reviews --partitions 3 --replication-factor 1'

echo "4/5 Borrando checkpoints y reconstruyendo el estado de los negocios en T..."
$DC run --rm --no-deps --entrypoint bash streaming -c 'rm -rf /checkpoints/*'
SOLO_ESTADO=1 COMPOSE_PROJECT_NAME="$P" bash scripts/spark_submit.sh riesgo

echo "5/5 Arrancando el streaming..."
$DC up -d streaming
echo "Listo. Publique con:  docker compose run --rm productor"
