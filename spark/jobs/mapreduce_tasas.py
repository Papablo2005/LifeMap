# archivo: spark/jobs/mapreduce_tasas.py
#!/usr/bin/env python3
"""Entrega 1 · MapReduce con RDD y tasa de cierre anualizada.

  MAP:    cada negocio emite (clave, (cierre, dias_negocio))
  REDUCE: reduceByKey suma componente a componente
  clave = categoría principal  |  celda de geohash

El resultado se VALIDA contra un groupBy de DataFrames, se aplica un MÍNIMO de
días-negocio (para no sacar conclusiones de grupos pequeños) y se guarda en
colecciones nuevas de MongoDB, junto con el porcentaje de reseñas negativas.

  tasa de cierre (%) = cierres / (días-negocio / 365) * 100
"""
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import config, geohash
from common.tasas import muestra_suficiente, tasa_cierre_pct

FUENTE = os.environ.get("TASAS_FUENTE", "mongo")           # mongo | parquet (pruebas)
ESCRIBIR_MONGO = os.environ.get("ESCRIBIR_MONGO", "1") == "1"
MIN_DIAS = config.MIN_DIAS_NEGOCIO

spark = (SparkSession.builder.appName("lifemap-mapreduce-tasas")
         .config("spark.sql.shuffle.partitions", "16").config("spark.ui.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def leer_mongo(coleccion):
    return (spark.read.format("mongodb").option("connection.uri", config.MONGO_URI)
            .option("database", config.MONGO_DB).option("collection", coleccion).load())


def cargar():
    if FUENTE == "parquet":
        neg = spark.read.parquet(os.path.join(config.CLEAN_DIR, "negocios"))
        res = spark.read.parquet(os.path.join(config.CLEAN_DIR, "resenas"))
    else:
        neg = leer_mongo("businesses").select(
            "business_id", "categoria_principal", "geohash",
            F.col("vida.evento").alias("evento"), F.col("vida.dias_negocio").alias("dias_negocio"))
        res = leer_mongo("reviews")
    neg = neg.select("business_id", "categoria_principal", "geohash",
                     F.col("evento").cast("int"), F.col("dias_negocio").cast("long"))
    return neg, res.select("geohash", "categoria_principal", F.col("label").cast("int").alias("label"))


def mapreduce(negocios, clave):
    """MAP + REDUCE con RDD. Devuelve ({clave: (cierres, dias)}, {clave: n_negocios})."""
    mapeado = negocios.rdd.map(lambda r: (r[clave] or "Sin categoría", (int(r["evento"]), int(r["dias_negocio"]))))
    reducido = mapeado.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return dict(reducido.collect()), dict(mapeado.countByKey())


def con_groupby(negocios, clave):
    """La MISMA agregación con DataFrames, para validar el resultado del RDD."""
    filas = (negocios.withColumn(clave, F.coalesce(F.col(clave), F.lit("Sin categoría")))
             .groupBy(clave).agg(F.sum("evento").alias("c"), F.sum("dias_negocio").alias("d")).collect())
    return {r[clave]: (int(r["c"]), int(r["d"])) for r in filas}


def pct_negativo(resenas, clave):
    filas = (resenas.groupBy(clave).agg(F.count("*").alias("n"),
                                         F.sum((F.col("label") == 0).cast("int")).alias("neg")).collect())
    return {r[clave]: (int(r["n"]), int(r["neg"])) for r in filas}


def construir(rdd_res, n_neg, neg_res, clave_nombre, con_geometria):
    docs = []
    for k, (cierres, dias) in sorted(rdd_res.items()):
        n_res, neg = neg_res.get(k, (0, 0))
        d = {"_id": k, clave_nombre: k, "negocios": n_neg[k], "cierres": cierres, "dias_negocio": dias,
             "tasa_cierre_pct": tasa_cierre_pct(cierres, dias),
             "muestra_suficiente": muestra_suficiente(dias, MIN_DIAS), "min_dias_negocio": MIN_DIAS,
             "resenas": n_res, "pct_negativo": (neg / n_res) if n_res else None}
        if con_geometria:
            d["location"] = geohash.punto_geojson(k)         # centro de la celda
            d["celda"] = geohash.poligono_geojson(k)          # polígono de la celda
        docs.append(d)
    return docs


def main():
    t0 = time.perf_counter()
    negocios, resenas = cargar()
    negocios = negocios.cache()
    print(f"[tasas] negocios={negocios.count():,}")
    salida, validacion = {}, {}
    for clave, nombre, geo, coleccion in (("categoria_principal", "categoria", False, "closure_by_category"),
                                          ("geohash", "geohash", True, "closure_by_geohash")):
        rdd_res, n_neg = mapreduce(negocios, clave)
        df_res = con_groupby(negocios, clave)
        coincide = rdd_res == df_res
        validacion[clave] = {"claves": len(rdd_res), "coincide_con_groupby": coincide,
                             "cierres_rdd": sum(v[0] for v in rdd_res.values()),
                             "dias_rdd": sum(v[1] for v in rdd_res.values()),
                             "cierres_df": sum(v[0] for v in df_res.values()),
                             "dias_df": sum(v[1] for v in df_res.values())}
        print(f"[tasas] {clave}: RDD == groupBy en {len(rdd_res)} claves -> {coincide}  "
              f"(cierres {validacion[clave]['cierres_rdd']}/{validacion[clave]['cierres_df']}, "
              f"días-negocio {validacion[clave]['dias_rdd']:,}/{validacion[clave]['dias_df']:,})")
        salida[coleccion] = construir(rdd_res, n_neg, pct_negativo(resenas, clave), nombre, geo)

    cierres = sum(d["cierres"] for d in salida["closure_by_category"])
    dias = sum(d["dias_negocio"] for d in salida["closure_by_category"])
    tasa_global = tasa_cierre_pct(cierres, dias)
    sugerido = round(365 / (tasa_global / 100)) if tasa_global else None
    print(f"\n[tasas] GLOBAL: cierres={cierres:,}  días-negocio={dias:,}  tasa anualizada={tasa_global:.2f} %")
    print(f"[tasas] sugerencia para justificar el mínimo: ~1 cierre esperado por grupo = "
          f"365/(tasa/100) = {sugerido:,} días-negocio  (usado: MIN_DIAS_NEGOCIO={MIN_DIAS})")
    for coleccion, docs in salida.items():
        suf = sum(1 for d in docs if d["muestra_suficiente"])
        print(f"\n== {coleccion}: {len(docs)} grupos, {suf} con muestra suficiente (>= {MIN_DIAS} días-negocio) ==")
        for d in sorted(docs, key=lambda x: -x["dias_negocio"])[:12]:
            t = "—" if d["tasa_cierre_pct"] is None else f"{d['tasa_cierre_pct']:.2f}"
            pn = "—" if d["pct_negativo"] is None else f"{d['pct_negativo'] * 100:.1f}"
            print(f"  {str(d['_id']):<24} negocios={d['negocios']:>5} cierres={d['cierres']:>4} "
                  f"días={d['dias_negocio']:>9,} tasa={t:>7} %  neg={pn:>5} %  "
                  f"{'ok' if d['muestra_suficiente'] else 'MUESTRA INSUFICIENTE'}")

    os.makedirs(config.REPORT_DIR, exist_ok=True)
    with open(os.path.join(config.REPORT_DIR, "tasas.json"), "w", encoding="utf-8") as f:
        json.dump({"min_dias_negocio": MIN_DIAS, "tasa_global_pct": tasa_global, "minimo_sugerido": sugerido,
                   "validacion": validacion, **salida}, f, indent=2, ensure_ascii=False, default=str)
    if ESCRIBIR_MONGO:
        from pymongo import MongoClient
        db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
        for coleccion, docs in salida.items():
            db[coleccion].drop()
            db[coleccion].insert_many(docs)
        db["closure_by_geohash"].create_index([("location", "2dsphere")])
        db["meta"].replace_one({"_id": "mapreduce"}, {"_id": "mapreduce", "validacion": validacion,
                               "tasa_global_pct": tasa_global, "min_dias_negocio": MIN_DIAS}, upsert=True)
        print("\n[tasas] guardado en MongoDB: closure_by_category, closure_by_geohash")
    print(f"[tasas] listo en {time.perf_counter() - t0:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main()
