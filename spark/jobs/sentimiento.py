# archivo: spark/jobs/sentimiento.py
#!/usr/bin/env python3
"""Entrega 1 · Sentimiento en Spark (MLlib).

Pipeline (diseño de SentimentStream): limpieza -> tokenización -> stopwords (inglés)
-> HashingTF -> IDF -> clasificador. Se entrenan Naive Bayes y regresión logística
(más una variante con pesos de clase para el desbalance) con partición
entrenamiento/prueba POR NEGOCIO (un negocio nunca queda en los dos lados).
Reporta precision, recall y F1 por clase + matriz de confusión, guarda el mejor
modelo (por macro-F1) para la Entrega 2 y deja el sentimiento de cada negocio en MongoDB.

Fuente de datos: SENT_FUENTE=mongo (conector, por defecto) o parquet (pruebas).
"""
import json
import os
import time
from datetime import datetime

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.feature import HashingTF, IDF, SQLTransformer, StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import config
from common.etiquetas import NOMBRES
from common.metricas import metricas_desde_matriz

FUENTE = os.environ.get("SENT_FUENTE", "mongo")
PARQUET = os.environ.get("SENT_PARQUET", os.path.join(config.CLEAN_DIR, "resenas"))
N_FEATURES = int(os.environ.get("N_FEATURES", "32768"))
SEMILLA = int(os.environ.get("SEMILLA", "42"))
FRAC_PRUEBA = float(os.environ.get("FRAC_PRUEBA", "0.2"))
FRAC_MUESTRA = float(os.environ.get("FRAC_MUESTRA", "1.0"))       # <1 para pruebas rápidas
ESCRIBIR_MONGO = os.environ.get("ESCRIBIR_MONGO", "1") == "1"
MODELOS = config.MODELS_DIR
REPORTE = os.path.join(config.REPORT_DIR, "sentimiento.json")

spark = (SparkSession.builder.appName("lifemap-sentimiento")
         .config("spark.sql.shuffle.partitions", os.environ.get("SHUFFLE_PARTS", "16"))
         .config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def leer_resenas():
    if FUENTE == "parquet":
        df = spark.read.parquet(PARQUET)
    else:
        df = (spark.read.format("mongodb").option("connection.uri", config.MONGO_URI)
              .option("database", config.MONGO_DB).option("collection", "reviews").load())
    df = df.select("business_id", "text", F.col("label").cast("double").alias("label"))
    return df.sample(False, FRAC_MUESTRA, SEMILLA) if FRAC_MUESTRA < 1.0 else df


# Limpieza DENTRO del pipeline: así /predict aplica exactamente la misma limpieza.
LIMPIEZA = (r"SELECT *, regexp_replace(regexp_replace(lower(text), 'https?://\\S+', ' '), "
            r"'[^a-z\\s]', ' ') AS limpio FROM __THIS__")


def pipeline_preprocesamiento():
    return Pipeline(stages=[
        SQLTransformer(statement=LIMPIEZA),
        Tokenizer(inputCol="limpio", outputCol="palabras"),
        StopWordsRemover(inputCol="palabras", outputCol="filtradas"),      # stopwords en inglés por defecto
        HashingTF(inputCol="filtradas", outputCol="tf", numFeatures=N_FEATURES),
        IDF(inputCol="tf", outputCol="features", minDocFreq=2),
    ])


def evaluar(pred):
    filas = pred.groupBy("label", "prediction").count().collect()
    matriz = [[0] * 3 for _ in range(3)]
    for r in filas:
        matriz[int(r["label"])][int(r["prediction"])] = int(r["count"])
    return metricas_desde_matriz(matriz)


def imprimir(nombre, m):
    print(f"\n=== {nombre} ===  accuracy={m['accuracy']:.4f}  macro-F1={m['macro_f1']:.4f}  "
          f"weighted-F1={m['weighted_f1']:.4f}")
    print(f"{'clase':<10}{'precision':>10}{'recall':>9}{'f1':>8}{'soporte':>10}")
    for c, v in m["por_clase"].items():
        print(f"{c:<10}{v['precision']:>10.3f}{v['recall']:>9.3f}{v['f1']:>8.3f}{v['soporte']:>10,}")
    print("matriz de confusión (filas = real, columnas = predicho):")
    for nombre_c, fila in zip(NOMBRES, m["matriz_confusion"]):
        print(f"  {nombre_c:<10}{fila}")


def main():
    t0 = time.perf_counter()
    df = leer_resenas().cache()
    total = df.count()
    dist = {NOMBRES[int(r["label"])]: int(r["count"]) for r in df.groupBy("label").count().collect()}
    print(f"[sent] reseñas={total:,}  distribución={dist}")

    # ---- partición POR NEGOCIO (no por reseña): evita que un negocio quede en los dos lados
    ids = df.select("business_id").distinct()
    ids_tr, ids_te = ids.randomSplit([1 - FRAC_PRUEBA, FRAC_PRUEBA], seed=SEMILLA)
    ids_tr, ids_te = ids_tr.cache(), ids_te.cache()
    solapados = ids_tr.intersect(ids_te).count()
    assert solapados == 0, "Un negocio quedó en entrenamiento y prueba"
    train = df.join(ids_tr, "business_id").cache()
    test = df.join(ids_te, "business_id").cache()
    n_tr, n_te = train.count(), test.count()
    print(f"[sent] negocios train={ids_tr.count():,} test={ids_te.count():,} (en común: {solapados})  "
          f"reseñas train={n_tr:,} test={n_te:,} ({n_te / (n_tr + n_te):.1%} en prueba)")

    # ---- preprocesamiento ajustado SOLO con entrenamiento (el IDF no ve la prueba)
    prep = pipeline_preprocesamiento().fit(train)
    tr_f, te_f = prep.transform(train).cache(), prep.transform(test).cache()

    # pesos de clase = total / (3 * conteo de la clase): compensan el desbalance
    cuentas = {int(r["label"]): int(r["count"]) for r in tr_f.groupBy("label").count().collect()}
    pesos = {c: n_tr / (3 * n) for c, n in cuentas.items()}
    mapa = F.create_map(*[x for c, w in pesos.items() for x in (F.lit(float(c)), F.lit(float(w)))])
    tr_w = tr_f.withColumn("peso", mapa[F.col("label")])

    modelos = {
        "NaiveBayes": NaiveBayes(smoothing=1.0, modelType="multinomial").fit(tr_f),
        "RegresionLogistica": LogisticRegression(family="multinomial", maxIter=30, regParam=0.01).fit(tr_f),
        "RegresionLogistica_pesos": LogisticRegression(family="multinomial", maxIter=30, regParam=0.01,
                                                       weightCol="peso").fit(tr_w),
    }
    resultados = {}
    for nombre, m in modelos.items():
        resultados[nombre] = evaluar(m.transform(te_f))
        imprimir(nombre, resultados[nombre])

    # ---- guardar el mejor modelo (macro-F1) para la Entrega 2: se carga SIN reentrenar
    mejor = max(resultados, key=lambda k: resultados[k]["macro_f1"])
    final = PipelineModel(stages=prep.stages + [modelos[mejor]])
    marca = datetime.now().strftime("%Y%m%d_%H%M%S")
    for ruta in (os.path.join(MODELOS, f"sentimiento_{marca}"), os.path.join(MODELOS, "sentimiento_latest")):
        final.write().overwrite().save(ruta)
    print(f"\n[sent] mejor modelo por macro-F1: {mejor}  -> guardado en {MODELOS}/sentimiento_latest "
          f"(copia versionada sentimiento_{marca})")

    # ---- sentimiento de cada negocio (todas sus reseñas) -> documento del negocio en Mongo
    pred = modelos[mejor].transform(prep.transform(df))
    agregados = (pred.groupBy("business_id").agg(
        F.count("*").alias("n"),
        F.sum((F.col("prediction") == 0).cast("int")).alias("neg_pred"),
        F.sum((F.col("label") == 0).cast("int")).alias("neg_real"),
        F.sum(F.col("prediction") - 1).alias("score")).collect())
    if ESCRIBIR_MONGO:
        from pymongo import MongoClient, UpdateOne
        col = MongoClient(config.MONGO_URI)[config.MONGO_DB]["businesses"]
        ops = [UpdateOne({"_id": r["business_id"]}, {"$set": {"sentimiento": {
            "pct_negativo": r["neg_pred"] / r["n"], "pct_negativo_real": r["neg_real"] / r["n"],
            "media": r["score"] / r["n"],               # -1 = todo negativo ... +1 = todo positivo
            "n_clasificadas": int(r["n"]), "modelo": mejor}}}) for r in agregados]
        for i in range(0, len(ops), 1000):
            col.bulk_write(ops[i:i + 1000], ordered=False)
        print(f"[sent] sentimiento actualizado en {len(ops):,} negocios (colección businesses)")

    os.makedirs(config.REPORT_DIR, exist_ok=True)
    with open(REPORTE, "w", encoding="utf-8") as f:
        json.dump({"resenas": total, "distribucion": dist, "n_features": N_FEATURES, "semilla": SEMILLA,
                   "negocios_en_comun_train_test": solapados,
                   "resenas_train": n_tr, "resenas_test": n_te, "pesos_de_clase": pesos,
                   "mejor_modelo": mejor, "resultados": resultados,
                   "segundos": round(time.perf_counter() - t0, 1)}, f, indent=2, ensure_ascii=False)
    print(f"[sent] reporte en {REPORTE}  ({time.perf_counter() - t0:.0f} s)")
    spark.stop()


if __name__ == "__main__":
    main()
