# archivo: spark/jobs/riesgo.py
"""Entrega 2 · Modelo de riesgo de cierre (patrón de KafkaMed aplicado a negocios).

1. Variables por negocio con datos HASTA T (nada posterior): sentimiento medio, tendencia del % negativo
   en los últimos 6 meses, estrellas medias de sus reseñas, número de reseñas, edad, competidores de la
   misma categoría a 500 m ($geoNear) y reseñas de los últimos 6 meses. Las calcula common/riesgo.py,
   la MISMA función que usa el streaming.
2. Etiqueta: 1 si está cerrado y su última reseña cae entre T y T+12 meses; 0 si sigue con actividad
   después o sigue abierto; los negocios sin actividad después de T se excluyen.
3. Partición sin fuga: cada negocio aparece UNA sola vez (una fila) y las variables miran solo antes de T
   mientras la etiqueta mira solo después. Validación cruzada estratificada de 5 pliegues por negocio:
   cada negocio se evalúa con un modelo que no lo vio.
4. Regresión logística y Random Forest con pesos de clase; métricas AUC-ROC, AUC-PR (la línea base de
   AUC-PR es el % de positivos) y recall en el umbral elegido (el más alto con recall >= RECALL_OBJETIVO:
   en alerta temprana es peor no avisar un cierre que dar una falsa alarma).
5. Guarda el mejor (por AUC-PR) versionado en /models/riesgo_latest y el ESTADO de cada negocio en T
   (colección estado_negocio), que el streaming va actualizando con cada reseña nueva.

Uso:  bash scripts/spark_submit.sh riesgo
      SOLO_ESTADO=1 bash scripts/spark_submit.sh riesgo    (reconstruye el estado en T sin reentrenar;
                                                             lo usa scripts/reiniciar_demo.sh)
"""
import json
import os
import random
import time

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import config, versiones
from common.competencia import contar_competidores
from common.riesgo import VARIABLES, agregar, calcular_variables, estado_vacio, etiqueta

SENT = os.path.join(config.MODELS_DIR, "sentimiento_latest")
RIESGO = os.path.join(config.MODELS_DIR, "riesgo_latest")
PLIEGUES = int(os.environ.get("PLIEGUES", "5"))
SEMILLA = int(os.environ.get("SEMILLA", "42"))
SOLO_ESTADO = os.environ.get("SOLO_ESTADO", "0") == "1"

spark = (SparkSession.builder.appName("lifemap-riesgo")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def agregar_por_mes(resenas, modelo_sent):
    """Clasifica reseñas (business_id, text, stars, date) y las agrega por negocio y mes."""
    pred = modelo_sent.transform(resenas).withColumn("p", vector_to_array("probability"))
    return (pred.groupBy("business_id", F.date_format("date", "yyyy-MM").alias("mes"))
            .agg(F.count("*").alias("n"), F.sum((F.col("prediction") == 0).cast("int")).alias("neg"),
                 F.sum(F.col("p")[2] - F.col("p")[0]).alias("suma_sent"), F.sum("stars").alias("suma_estrellas"),
                 F.min("date").alias("primera"), F.max("date").alias("ultima")).collect())


def agregados_hasta_t():
    """Reseñas <= T (nada posterior) clasificadas con el modelo de sentimiento, agregadas por negocio y mes."""
    resenas = (spark.read.format("mongodb").option("connection.uri", config.MONGO_URI)
               .option("database", config.MONGO_DB).option("collection", "reviews").load()
               .where(F.col("date") <= F.lit(config.FECHA_T).cast("timestamp"))
               .select("business_id", "text", "stars", "date"))
    return agregar_por_mes(resenas, PipelineModel.load(SENT))


def construir_estados(db, t):
    negocios = {n["_id"]: n for n in db["businesses"].find(
        {}, {"name": 1, "location": 1, "categoria_principal": 1, "vida": 1, "geohash": 1})}
    estados = {}
    for r in agregados_hasta_t():
        e = estados.get(r["business_id"])
        if e is None:
            e = estados[r["business_id"]] = estado_vacio(r["business_id"])
        agregar(e, r["mes"], r["n"], r["neg"], r["suma_sent"], r["suma_estrellas"], r["primera"], r["ultima"])
    for bid, e in estados.items():
        e["competidores_500m"] = contar_competidores(db, negocios.get(bid), t, config.RADIO_COMPETENCIA_M)
    return negocios, estados


def pliegues_estratificados(filas):
    """Asigna a cada negocio un pliegue 0..PLIEGUES-1 manteniendo la proporción de positivos."""
    rnd = random.Random(SEMILLA)
    for clase in (0, 1):
        grupo = [f for f in filas if f["label"] == clase]
        rnd.shuffle(grupo)
        for i, f in enumerate(grupo):
            f["pliegue"] = i % PLIEGUES


def candidatos(n_pos, n_neg):
    peso_pos = n_neg / max(1, n_pos)                  # pesos de clase: compensan el desbalance
    base = [VectorAssembler(inputCols=VARIABLES, outputCol="crudas"),
            StandardScaler(inputCol="crudas", outputCol="features", withMean=True, withStd=True)]
    return peso_pos, {
        "RegresionLogistica": Pipeline(stages=base + [LogisticRegression(
            featuresCol="features", labelCol="label", weightCol="peso", maxIter=100, regParam=0.01)]),
        "RandomForest": Pipeline(stages=base + [RandomForestClassifier(
            featuresCol="features", labelCol="label", weightCol="peso", numTrees=200, maxDepth=5,
            minInstancesPerNode=5, seed=SEMILLA)]),
    }


def metricas(pares, umbral):
    tp = sum(1 for p, y in pares if p >= umbral and y == 1)
    fp = sum(1 for p, y in pares if p >= umbral and y == 0)
    fn = sum(1 for p, y in pares if p < umbral and y == 1)
    tn = sum(1 for p, y in pares if p < umbral and y == 0)
    prec = tp / (tp + fp) if tp + fp else 0.0
    rec = tp / (tp + fn) if tp + fn else 0.0
    return {"umbral": round(umbral, 4), "precision": round(prec, 4), "recall": round(rec, 4),
            "f1": round(2 * prec * rec / (prec + rec), 4) if prec + rec else 0.0,
            "matriz": {"tp": tp, "fp": fp, "fn": fn, "tn": tn},
            "alertas_pct": round((tp + fp) / len(pares), 4)}


def elegir_umbral(pares):
    """El umbral MÁS ALTO que todavía detecta al menos RECALL_OBJETIVO de los cierres."""
    for u in sorted({round(p, 4) for p, _ in pares}, reverse=True):
        if metricas(pares, u)["recall"] >= config.RECALL_OBJETIVO:
            return u
    return 0.0


def entrenar(df, filas):
    n_pos = sum(f["label"] for f in filas)
    peso_pos, modelos = candidatos(n_pos, len(filas) - n_pos)
    df = df.withColumn("peso", F.when(F.col("label") == 1, F.lit(peso_pos)).otherwise(F.lit(1.0))).cache()
    ev_roc = BinaryClassificationEvaluator(rawPredictionCol="probability", metricName="areaUnderROC")
    ev_pr = BinaryClassificationEvaluator(rawPredictionCol="probability", metricName="areaUnderPR")
    resultados, oof_por_modelo = {}, {}
    for nombre, pipe in modelos.items():
        oof = None
        for k in range(PLIEGUES):
            pred = pipe.fit(df.where(F.col("pliegue") != k)).transform(df.where(F.col("pliegue") == k))
            oof = pred if oof is None else oof.unionByName(pred)
        oof = oof.cache()
        pares = [(float(r["p1"]), int(r["label"])) for r in
                 oof.select(vector_to_array("probability")[1].alias("p1"), "label").collect()]
        umbral = elegir_umbral(pares)
        resultados[nombre] = {"auc_roc": round(ev_roc.evaluate(oof), 4), "auc_pr": round(ev_pr.evaluate(oof), 4),
                              "en_umbral_elegido": metricas(pares, umbral), "en_umbral_0_5": metricas(pares, 0.5)}
        oof_por_modelo[nombre] = pares
        r = resultados[nombre]
        print(f"[riesgo] {nombre:<20} AUC-ROC={r['auc_roc']:.3f}  AUC-PR={r['auc_pr']:.3f} "
              f"(línea base {n_pos / len(filas):.3f})  umbral={umbral:.3f}  "
              f"recall={r['en_umbral_elegido']['recall']:.3f}  precision={r['en_umbral_elegido']['precision']:.3f}")
    mejor = max(resultados, key=lambda k: resultados[k]["auc_pr"])
    final = modelos[mejor].fit(df)
    ultimo = final.stages[-1]
    if hasattr(ultimo, "coefficients"):
        importancia = {v: round(float(c), 4) for v, c in zip(VARIABLES, ultimo.coefficients)}
    else:
        importancia = {v: round(float(c), 4) for v, c in zip(VARIABLES, ultimo.featureImportances)}
    return mejor, final, resultados, importancia, peso_pos


def puntuar(modelo, estados, t):
    filas = [{"business_id": bid, **calcular_variables(e, t)} for bid, e in estados.items()]
    df = spark.createDataFrame(filas)
    return {r["business_id"]: float(r["p1"]) for r in modelo.transform(df).select(
        "business_id", vector_to_array("probability")[1].alias("p1")).collect()}, filas


def main():
    from pymongo import MongoClient, ReplaceOne, UpdateOne
    t0 = time.perf_counter()
    t = config.fecha_t()
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    negocios, estados = construir_estados(db, t)
    print(f"[riesgo] T={t:%Y-%m-%d}  negocios con reseñas hasta T={len(estados):,}")

    if SOLO_ESTADO:
        modelo, info = PipelineModel.load(RIESGO), versiones.leer(RIESGO)
        umbral = info["umbral"]
    else:
        filas = []
        for bid, e in estados.items():
            v = negocios[bid]["vida"]
            y = etiqueta(v["evento"] == 1, v["ultima_resena"], t)
            if y is not None:
                filas.append({"business_id": bid, "label": float(y), **calcular_variables(e, t)})
        n_pos = int(sum(f["label"] for f in filas))
        excluidos = len(estados) - len(filas)
        print(f"[riesgo] población={len(filas):,}  positivos={n_pos:,} ({n_pos / len(filas):.1%})  "
              f"negativos={len(filas) - n_pos:,}  excluidos sin actividad después de T={excluidos:,}")
        pliegues_estratificados(filas)
        df = spark.createDataFrame(filas)
        mejor, modelo, resultados, importancia, peso_pos = entrenar(df, filas)
        umbral = resultados[mejor]["en_umbral_elegido"]["umbral"]
        version = config.RIESGO_VERSION
        for ruta in (os.path.join(config.MODELS_DIR, f"riesgo_v{version}"), RIESGO):
            modelo.write().overwrite().save(ruta)
            versiones.escribir(ruta, version, config.FECHA_T, modelo=mejor, umbral=umbral, variables=VARIABLES,
                               auc_pr=resultados[mejor]["auc_pr"], auc_roc=resultados[mejor]["auc_roc"])
        info = versiones.leer(RIESGO)
        reporte = {"fecha_t": config.FECHA_T, "poblacion": len(filas), "positivos": n_pos,
                   "negativos": len(filas) - n_pos, "excluidos_sin_actividad": excluidos,
                   "prevalencia": round(n_pos / len(filas), 4), "pliegues": PLIEGUES, "peso_positivos": peso_pos,
                   "recall_objetivo": config.RECALL_OBJETIVO, "variables": VARIABLES,
                   "resultados": resultados, "mejor_modelo": mejor, "importancia": importancia,
                   "segundos": round(time.perf_counter() - t0, 1)}
        os.makedirs(config.REPORT_DIR, exist_ok=True)
        with open(os.path.join(config.REPORT_DIR, "riesgo.json"), "w", encoding="utf-8") as f:
            json.dump(reporte, f, indent=2, ensure_ascii=False)
        print(f"[riesgo] mejor por AUC-PR: {mejor}  umbral de alerta={umbral}  importancia={importancia}")

    probs, filas_t = puntuar(modelo, estados, t)
    variables = {f["business_id"]: {k: f[k] for k in VARIABLES} for f in filas_t}
    db["estado_negocio"].drop()
    ops = [ReplaceOne({"_id": bid}, {**e, "variables": variables[bid], "probabilidad": probs[bid],
                                     "fecha_ref": t, "fuente": "batch_T"}, upsert=True)
           for bid, e in estados.items()]
    for i in range(0, len(ops), 1000):
        db["estado_negocio"].bulk_write(ops[i:i + 1000], ordered=False)
    ops = [UpdateOne({"_id": bid}, {"$set": {"riesgo": {
        "probabilidad": p, "alerta": p >= umbral, "umbral": umbral, "fecha_ref": t, "fuente": "batch_T",
        "modelo_version": info["version"]}}}) for bid, p in probs.items()]
    for i in range(0, len(ops), 1000):
        db["businesses"].bulk_write(ops[i:i + 1000], ordered=False)
    print(f"[riesgo] estado en T guardado para {len(ops):,} negocios (estado_negocio y businesses.riesgo); "
          f"sobre el umbral en T: {sum(p >= umbral for p in probs.values()):,}  ({time.perf_counter() - t0:.0f} s)")
    spark.stop()


if __name__ == "__main__":
    main()
