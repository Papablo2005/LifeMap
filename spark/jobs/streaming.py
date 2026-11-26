# archivo: spark/jobs/streaming.py
"""Entrega 2 · Spark Structured Streaming: Kafka -> sentimiento -> MongoDB, ventanas por geohash y alertas.

Lee el topic `reviews` y corre DOS consultas continuas sobre las mismas reseñas:

  1) "resenas" (foreachBatch, cada 5 s):
     - clasifica cada reseña con el modelo de sentimiento guardado (se CARGA, nunca se reentrena) y la
       escribe en reviews_stream con predicción, probabilidad, marca de procesamiento y versión del modelo;
     - ALERTAS: para los negocios que recibieron reseñas en el micro-lote, actualiza su estado (common/riesgo.py,
       la misma función del entrenamiento), recalcula sus variables, los puntúa con el modelo de riesgo y
       guarda en `alerts` (con geometría, probabilidad y fecha) los que superan el umbral;
     - registra los offsets procesados por partición (para el retraso del consumidor en /health).

  2) "ventanas": ventanas de 30 días por celda de geohash sobre el TIEMPO DEL EVENTO (campo date), con
     watermark de 7 días. Cuenta reseñas y % negativo, guarda cada ventana con la geometría de su celda en
     ventanas_geohash y la marca como hotspot si su % negativo sube frente a la ventana anterior (common/hotspot.py).

Arranque: espera a que existan los modelos (los entrena la etapa batch); mientras tanto el contenedor sigue
vivo y sano (latido en /tmp/latido) e informa "esperando_modelos" en MongoDB.
"""
import os
import threading
import time
from datetime import datetime, timedelta

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from common import config, versiones
from common.competencia import contar_competidores
from common.geohash import punto_geojson
from common.hotspot import documento_ventana, evaluar_hotspot, sin_zona
from common.riesgo import a_datetime, agregar, calcular_variables, estado_vacio

SENT = os.path.join(config.MODELS_DIR, "sentimiento_latest")
RIESGO = os.path.join(config.MODELS_DIR, "riesgo_latest")
CHECKPOINTS = os.environ.get("CHECKPOINT_DIR", "/checkpoints")
TRIGGER = os.environ.get("TRIGGER", "5 seconds")
MAX_POR_LOTE = os.environ.get("MAX_OFFSETS_POR_LOTE", "2000")
NOMBRES = ["negativo", "neutral", "positivo"]
LATIDO = "/tmp/latido"

ESQUEMA = StructType([StructField(c, t) for c, t in [
    ("review_id", StringType()), ("business_id", StringType()), ("stars", DoubleType()),
    ("text", StringType()), ("date", StringType()), ("geohash", StringType()),
    ("categoria_principal", StringType()), ("name", StringType()), ("lat", DoubleType()),
    ("lon", DoubleType()), ("publicado_en", StringType())]])

_db = None


def db():
    global _db
    if _db is None:
        from pymongo import MongoClient
        _db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    return _db


def meta(cambios):
    db()["meta"].update_one({"_id": "streaming"}, cambios, upsert=True)


def latir():
    """Latido para el healthcheck del contenedor y para /health (estado del streaming)."""
    while True:
        with open(LATIDO, "w") as f:
            f.write(datetime.utcnow().isoformat())
        try:
            meta({"$set": {"latido": datetime.utcnow()}})
        except Exception as e:                    # Mongo caído: el latido local sigue
            print(f"[streaming] no se pudo registrar el latido: {e}", flush=True)
        time.sleep(10)


def esperar_modelos():
    while not (versiones.leer(SENT) and versiones.leer(RIESGO)):
        meta({"$set": {"estado": "esperando_modelos", "detalle":
                       "Entrene primero: spark_submit.sh sentimiento y spark_submit.sh riesgo"}})
        print("[streaming] esperando modelos en /models (sentimiento_latest y riesgo_latest)...", flush=True)
        time.sleep(30)


# --------------------------------------------------------------------------- consulta 1: reseñas y alertas
def procesar_lote(df, lote_id, modelo_riesgo, info_sent, info_riesgo, spark):
    filas = df.select("review_id", "business_id", "stars", "text", "event_time", "geohash",
                      "categoria_principal", "name", "lat", "lon", "publicado_en", "partition", "offset",
                      "prediction", vector_to_array("probability").alias("p")).collect()
    if not filas:
        return
    from pymongo import ReplaceOne
    ahora = datetime.utcnow()
    docs, offsets = [], {}
    for r in filas:
        p = [float(x) for x in r["p"]]
        clase = int(r["prediction"])
        doc = {"_id": r["review_id"], "review_id": r["review_id"], "business_id": r["business_id"],
               "stars": r["stars"], "text": r["text"], "date": sin_zona(r["event_time"]), "geohash": r["geohash"],
               "categoria_principal": r["categoria_principal"], "prediccion": NOMBRES[clase], "clase_id": clase,
               "probabilidad": p[clase], "probabilidades": dict(zip(NOMBRES, p)),
               "procesado_en": ahora, "publicado_en": a_datetime(r["publicado_en"]),
               "modelo_version": info_sent["version"], "kafka": {"particion": r["partition"], "offset": r["offset"]}}
        if r["lon"] is not None and r["lat"] is not None:
            doc["location"] = {"type": "Point", "coordinates": [r["lon"], r["lat"]]}
        docs.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
        k = str(r["partition"])
        offsets[k] = max(offsets.get(k, -1), int(r["offset"]) + 1)
    db()["reviews_stream"].bulk_write(docs, ordered=False)

    n_alertas = actualizar_riesgo(filas, modelo_riesgo, info_riesgo, spark)
    meta({"$set": {"estado": "activo", "ultimo_lote": ahora, "ultimo_lote_id": int(lote_id),
                   "reloj_evento": sin_zona(max(r["event_time"] for r in filas)),
                   **{f"offsets.{k}": v for k, v in offsets.items()}},
          "$inc": {"resenas_procesadas": len(filas), "lotes": 1}})
    print(f"[streaming] lote {lote_id}: {len(filas)} reseñas clasificadas, {n_alertas} alertas activas nuevas/"
          f"actualizadas", flush=True)


def actualizar_riesgo(filas, modelo_riesgo, info_riesgo, spark):
    """Actualiza el estado de los negocios del lote, los puntúa y escribe alertas. Devuelve # alertas."""
    from pymongo import ReplaceOne, UpdateOne
    umbral = float(info_riesgo["umbral"])
    ref = sin_zona(max(r["event_time"] for r in filas))        # "ahora" en el reloj del evento
    por_mes = {}
    for r in filas:
        p = [float(x) for x in r["p"]]
        fecha = sin_zona(r["event_time"])
        k = (r["business_id"], fecha.strftime("%Y-%m"))
        a = por_mes.setdefault(k, {"n": 0, "neg": 0, "sent": 0.0, "estrellas": 0.0, "min": fecha, "max": fecha})
        a["n"] += 1
        a["neg"] += int(int(r["prediction"]) == 0)
        a["sent"] += p[2] - p[0]
        a["estrellas"] += float(r["stars"] or 0)
        a["min"], a["max"] = min(a["min"], fecha), max(a["max"], fecha)
    ids = sorted({bid for bid, _ in por_mes})
    estados = {e["_id"]: e for e in db()["estado_negocio"].find({"_id": {"$in": ids}})}
    negocios = {n["_id"]: n for n in db()["businesses"].find(
        {"_id": {"$in": ids}}, {"name": 1, "location": 1, "categoria_principal": 1, "geohash": 1})}
    ultimo = {r["business_id"]: r for r in filas}
    for bid in ids:
        if bid not in estados:                                  # negocio nuevo (primera reseña después de T)
            estados[bid] = estado_vacio(bid, contar_competidores(db(), negocios.get(bid), ref,
                                                                 config.RADIO_COMPETENCIA_M) if bid in negocios else 0)
    for (bid, mes), a in por_mes.items():
        agregar(estados[bid], mes, a["n"], a["neg"], a["sent"], a["estrellas"], a["min"], a["max"])

    variables = {bid: calcular_variables(estados[bid], ref) for bid in ids}
    df = spark.createDataFrame([{"business_id": bid, **v} for bid, v in variables.items()])
    probs = {r["business_id"]: float(r["p1"]) for r in modelo_riesgo.transform(df).select(
        "business_id", vector_to_array("probability")[1].alias("p1")).collect()}

    ahora = datetime.utcnow()
    ops_estado, ops_neg, ops_alerta, n_alertas = [], [], [], 0
    for bid in ids:
        e, prob = estados[bid], probs[bid]
        e.update({"variables": variables[bid], "probabilidad": prob, "fecha_ref": ref, "fuente": "streaming"})
        ops_estado.append(ReplaceOne({"_id": bid}, e, upsert=True))
        alerta = prob >= umbral
        ops_neg.append(UpdateOne({"_id": bid}, {"$set": {"riesgo": {
            "probabilidad": prob, "alerta": alerta, "umbral": umbral, "fecha_ref": ref, "fuente": "streaming",
            "modelo_version": info_riesgo["version"]}}}))
        neg, r = negocios.get(bid, {}), ultimo[bid]
        if alerta:
            n_alertas += 1
            if "location" in neg:
                geom = neg["location"]
            elif r["lon"] is not None and r["lat"] is not None:
                geom = {"type": "Point", "coordinates": [r["lon"], r["lat"]]}
            else:
                geom = punto_geojson(r["geohash"])
            ops_alerta.append(UpdateOne({"_id": bid}, {
                "$set": {"business_id": bid, "name": neg.get("name") or r["name"],
                         "categoria_principal": neg.get("categoria_principal") or r["categoria_principal"],
                         "geohash": neg.get("geohash") or r["geohash"], "location": geom, "probabilidad": prob,
                         "umbral": umbral, "fecha_evento": ref, "procesado_en": ahora, "activa": True,
                         "variables": variables[bid], "modelo_version": info_riesgo["version"]},
                "$setOnInsert": {"primera_alerta": ref}, "$inc": {"veces": 1}}, upsert=True))
        else:
            ops_alerta.append(UpdateOne({"_id": bid, "activa": True}, {"$set": {
                "activa": False, "probabilidad": prob, "desactivada_en": ref, "procesado_en": ahora}}))
    db()["estado_negocio"].bulk_write(ops_estado, ordered=False)
    if ops_neg:
        db()["businesses"].bulk_write(ops_neg, ordered=False)
    if ops_alerta:
        db()["alerts"].bulk_write(ops_alerta, ordered=False)
    return n_alertas


# --------------------------------------------------------------------------- consulta 2: ventanas
def procesar_ventanas(df, lote_id):
    filas = df.collect()
    if not filas:
        return
    from pymongo import ReplaceOne
    col = db()["ventanas_geohash"]
    ops, hot = [], 0
    for r in filas:
        inicio, fin = sin_zona(r["window"]["start"]), sin_zona(r["window"]["end"])
        doc = documento_ventana(r["geohash"], inicio, fin, r["n"], r["negativas"], "streaming")
        anterior = col.find_one({"_id": f"{r['geohash']}|{(inicio - timedelta(days=config.VENTANA_DIAS)):%Y-%m-%d}"})
        evaluar_hotspot(doc, anterior, config.UMBRAL_HOTSPOT, config.MIN_RESENAS_VENTANA)
        doc["actualizado_en"] = datetime.utcnow()
        hot += int(doc["hotspot"])
        ops.append(ReplaceOne({"_id": doc["_id"]}, doc, upsert=True))
    col.bulk_write(ops, ordered=False)
    print(f"[streaming] ventanas lote {lote_id}: {len(ops)} ventanas actualizadas, {hot} hotspot", flush=True)


def main():
    threading.Thread(target=latir, daemon=True).start()
    esperar_modelos()
    spark = (SparkSession.builder.appName("lifemap-streaming")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTS", "8"))
             .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # ---- modelos: se CARGAN (no se reentrena nada en el flujo); se registra qué versión quedó cargada
    modelo_sent, modelo_riesgo = PipelineModel.load(SENT), PipelineModel.load(RIESGO)
    info_sent, info_riesgo = versiones.leer(SENT), versiones.leer(RIESGO)
    meta({"$set": {"estado": "iniciando", "modelo_version": info_sent["version"],
                   "modelo_huella": versiones.huella(SENT), "modelo_fecha_t": info_sent["fecha_t"],
                   "riesgo_version": info_riesgo["version"], "riesgo_huella": versiones.huella(RIESGO),
                   "riesgo_umbral": info_riesgo["umbral"], "topic": config.KAFKA_TOPIC, "inicio": datetime.utcnow()}})
    print(f"[streaming] modelos cargados: sentimiento v{info_sent['version']} (T={info_sent['fecha_t']}), "
          f"riesgo v{info_riesgo['version']} (umbral {info_riesgo['umbral']})", flush=True)

    db()["reviews_stream"].create_index("business_id")
    db()["reviews_stream"].create_index("date")
    db()["alerts"].create_index([("location", "2dsphere")])
    db()["ventanas_geohash"].create_index([("centro", "2dsphere")])
    db()["ventanas_geohash"].create_index([("inicio", 1)])

    crudo = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP)
             .option("subscribe", config.KAFKA_TOPIC)
             .option("startingOffsets", "earliest")
             .option("maxOffsetsPerTrigger", MAX_POR_LOTE)
             .option("failOnDataLoss", "false").load())
    mensajes = (crudo.select("partition", "offset", F.from_json(F.col("value").cast("string"), ESQUEMA).alias("m"))
                .select("partition", "offset", "m.*")
                .withColumn("event_time", F.to_timestamp("date"))         # TIEMPO DEL EVENTO, no de ingesta
                .where(F.col("event_time").isNotNull() & F.col("text").isNotNull()))
    clasificadas = modelo_sent.transform(mensajes)

    q1 = (clasificadas.writeStream.queryName("resenas")
          .foreachBatch(lambda df, i: procesar_lote(df, i, modelo_riesgo, info_sent, info_riesgo, spark))
          .option("checkpointLocation", os.path.join(CHECKPOINTS, "resenas"))
          .trigger(processingTime=TRIGGER).start())

    ventanas = (clasificadas.withWatermark("event_time", f"{config.WATERMARK_DIAS} days")
                .groupBy(F.window("event_time", f"{config.VENTANA_DIAS} days"), "geohash")
                .agg(F.count("*").alias("n"), F.sum((F.col("prediction") == 0).cast("int")).alias("negativas")))
    q2 = (ventanas.writeStream.queryName("ventanas").outputMode("update")
          .foreachBatch(procesar_ventanas)
          .option("checkpointLocation", os.path.join(CHECKPOINTS, "ventanas"))
          .trigger(processingTime=TRIGGER).start())

    meta({"$set": {"estado": "activo", "consultas": [q1.name, q2.name]}})
    print("[streaming] consultas activas: resenas (clasificación + alertas) y ventanas (geohash, watermark)", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
