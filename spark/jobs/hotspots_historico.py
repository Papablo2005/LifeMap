# archivo: spark/jobs/hotspots_historico.py
"""Entrega 2 · Ventanas históricas por geohash (hasta T) y justificación del umbral de hotspot.

Con las reseñas hasta T, clasificadas con el MISMO modelo que usa el streaming, calcula ventanas fijas
de 30 días por celda de geohash (las mismas fronteras que en el streaming: Spark alinea las ventanas
desde 1970-01-01 UTC). Con eso:
  1. guarda esas ventanas en ventanas_geohash (fuente "historico"): así la primera ventana del streaming
     ya tiene "ventana anterior" contra la cual compararse;
  2. mide cuánto sube normalmente el % negativo de una ventana a la siguiente (en celdas con al menos
     MIN_RESENAS_VENTANA reseñas en ambas) y propone UMBRAL_HOTSPOT = percentil 90 de esas subidas:
     un hotspot es una subida que solo ocurre en ~10 % de los casos.

Uso:  bash scripts/spark_submit.sh hotspots_historico
"""
import json
import os

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import config
from common.hotspot import documento_ventana, evaluar_hotspot

MODELO = os.path.join(config.MODELS_DIR, "sentimiento_latest")

spark = (SparkSession.builder.appName("lifemap-hotspots-historico")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def percentil(valores, p):
    if not valores:
        return None
    v = sorted(valores)
    k = (len(v) - 1) * p
    i = int(k)
    return v[i] if i + 1 >= len(v) else v[i] + (v[i + 1] - v[i]) * (k - i)


def ventanas_de(pred):
    """Ventanas fijas de VENTANA_DIAS por celda: reseñas y negativas (predicción del modelo = 0)."""
    return (pred.groupBy(F.window("date", f"{config.VENTANA_DIAS} days"), "geohash")
            .agg(F.count("*").alias("n"), F.sum((F.col("prediction") == 0).cast("int")).alias("negativas"))
            .select("geohash", F.col("window.start").alias("inicio"), F.col("window.end").alias("fin"),
                    "n", "negativas"))


def main():
    t = config.fecha_t()
    resenas = (spark.read.format("mongodb").option("connection.uri", config.MONGO_URI)
               .option("database", config.MONGO_DB).option("collection", "reviews").load()
               .where(F.col("date") <= F.lit(config.FECHA_T).cast("timestamp"))
               .select("review_id", "business_id", "text", "date", "geohash"))
    ventanas = ventanas_de(PipelineModel.load(MODELO).transform(resenas)).collect()

    docs = {}
    for r in ventanas:
        d = documento_ventana(r["geohash"], r["inicio"], r["fin"], r["n"], r["negativas"], "historico")
        docs[d["_id"]] = d
    from datetime import timedelta
    subidas = []
    for d in docs.values():
        ant = docs.get(f"{d['geohash']}|{(d['inicio'] - timedelta(days=config.VENTANA_DIAS)):%Y-%m-%d}")
        evaluar_hotspot(d, ant, config.UMBRAL_HOTSPOT, config.MIN_RESENAS_VENTANA)
        if ant and d["n"] >= config.MIN_RESENAS_VENTANA and ant["n"] >= config.MIN_RESENAS_VENTANA:
            subidas.append(d["delta"])

    from pymongo import ASCENDING, GEOSPHERE, MongoClient, ReplaceOne
    col = MongoClient(config.MONGO_URI)[config.MONGO_DB]["ventanas_geohash"]
    col.delete_many({"fuente": "historico"})
    ops = [ReplaceOne({"_id": k}, v, upsert=True) for k, v in docs.items()]
    for i in range(0, len(ops), 1000):
        col.bulk_write(ops[i:i + 1000], ordered=False)
    col.create_index([("centro", GEOSPHERE)])
    col.create_index([("geohash", ASCENDING), ("inicio", ASCENDING)])
    col.create_index([("inicio", ASCENDING)])

    pcts = {f"p{int(p * 100)}": percentil(subidas, p) for p in (0.5, 0.75, 0.9, 0.95)}
    sugerido = round(pcts["p90"], 2) if pcts["p90"] is not None else None
    hot = sum(1 for d in docs.values() if d["hotspot"])
    print(f"[hotspots] T={t:%Y-%m-%d}  ventanas históricas={len(docs):,}  "
          f"pares comparables (n >= {config.MIN_RESENAS_VENTANA} en ambas)={len(subidas):,}")
    print("[hotspots] cambio del % negativo entre ventanas consecutivas: " +
          "  ".join(f"{k}={v:+.3f}" for k, v in pcts.items() if v is not None))
    print(f"[hotspots] UMBRAL_HOTSPOT sugerido (percentil 90) = {sugerido}   configurado = {config.UMBRAL_HOTSPOT}")
    print(f"[hotspots] con el umbral configurado, {hot} ventanas históricas serían hotspot "
          f"({hot / max(1, len(subidas)):.1%} de los pares comparables)")
    os.makedirs(config.REPORT_DIR, exist_ok=True)
    with open(os.path.join(config.REPORT_DIR, "hotspots.json"), "w", encoding="utf-8") as f:
        json.dump({"fecha_t": config.FECHA_T, "ventana_dias": config.VENTANA_DIAS,
                   "min_resenas_ventana": config.MIN_RESENAS_VENTANA, "ventanas": len(docs),
                   "pares_comparables": len(subidas), "percentiles_subida": pcts,
                   "umbral_sugerido_p90": sugerido, "umbral_configurado": config.UMBRAL_HOTSPOT,
                   "hotspots_historicos": hot}, f, indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
