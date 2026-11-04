# archivo: spark/jobs/balanceo.py
#!/usr/bin/env python3
"""Entrega 1 · Balanceo de carga.

(1) TIEMPOS: el mismo trabajo MapReduce con 2, 8, 32 y 200 particiones, con 1 corrida
    de calentamiento descartada y REPS repeticiones por configuración (media, mediana,
    desviación estándar, mínimo y máximo).
(2) SESGO: filas por partición con spark_partition_id() al repartir POR CLAVE
    (categoría / geohash) frente a un reparto SIN clave.

El trabajo medido parte de las RESEÑAS (para que haya volumen de datos que repartir):
  reseñas -> vida por negocio (min fecha, max fecha, conteo) con reduceByKey
          -> (categoría, (cierre, días_negocio)) con reduceByKey.
"""
import json
import os
import statistics as st
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common import config

REPS = int(os.environ.get("REPS", "5"))
PARTICIONES = [2, 8, 32, 200]
N_SESGO = 8
SALIDA = os.path.join(config.REPORT_DIR, "balanceo")

spark = (SparkSession.builder.appName("lifemap-balanceo").config("spark.ui.enabled", "false").getOrCreate())
sc = spark.sparkContext
sc.setLogLevel("WARN")

negocios = spark.read.parquet(os.path.join(config.CLEAN_DIR, "negocios")).select(
    "business_id", "categoria_principal", "evento").collect()
info = sc.broadcast({r["business_id"]: (r["categoria_principal"], int(r["evento"])) for r in negocios})
base = spark.read.parquet(os.path.join(config.CLEAN_DIR, "resenas")).select(
    "business_id", "date", "categoria_principal", "geohash").cache()
n_filas = base.count()


def trabajo(df):
    """El trabajo MapReduce que se mide. Devuelve {categoría: (cierres, días_negocio)}."""
    vida = (df.rdd.map(lambda r: (r["business_id"], (r["date"].toordinal(), r["date"].toordinal(), 1)))
            .reduceByKey(lambda a, b: (min(a[0], b[0]), max(a[1], b[1]), a[2] + b[2])))
    por_cat = (vida.map(lambda kv: (info.value[kv[0]][0], (info.value[kv[0]][1], kv[1][1] - kv[1][0] + 1)))
               .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])))
    return dict(por_cat.collect())


# ---------------------------------------------------------------- (1) tiempos
referencia = trabajo(base)                      # calentamiento: JVM, caché de archivos... NO se mide
print(f"[balanceo] filas={n_filas:,}  categorías={len(referencia)}  "
      f"cierres={sum(v[0] for v in referencia.values())}  núcleos (defaultParallelism)={sc.defaultParallelism}")
filas = []
for p in PARTICIONES:
    tiempos = []
    for _ in range(REPS):
        t0 = time.perf_counter()
        resultado = trabajo(base.repartition(p))
        tiempos.append(time.perf_counter() - t0)
        assert resultado == referencia, "El resultado cambió al variar las particiones"
    filas.append({"particiones": p, **{f"rep{i + 1}_s": round(t, 2) for i, t in enumerate(tiempos)},
                  "media_s": round(st.mean(tiempos), 2), "mediana_s": round(st.median(tiempos), 2),
                  "desv_s": round(st.stdev(tiempos), 2) if len(tiempos) > 1 else 0.0,
                  "min_s": round(min(tiempos), 2), "max_s": round(max(tiempos), 2)})
ejecutores = sc._jsc.sc().getExecutorMemoryStatus().size() - 1       # después de correr: ya registrados
print(f"\n=== Tiempos ({REPS} repeticiones + 1 calentamiento descartado; ejecutores={ejecutores}) ===")
print(f"{'part.':>6}" + "".join(f"{'rep' + str(i + 1):>8}" for i in range(REPS)) +
      f"{'media':>9}{'mediana':>9}{'desv':>8}")
for f in filas:
    print(f"{f['particiones']:>6}" + "".join(f"{f[f'rep{i + 1}_s']:>8.2f}" for i in range(REPS)) +
          f"{f['media_s']:>9.2f}{f['mediana_s']:>9.2f}{f['desv_s']:>8.2f}")


# ------------------------------------------------------------------- (2) sesgo
def reparto(df_rep):
    d = (df_rep.withColumn("part", F.spark_partition_id()).groupBy("part")
         .agg(F.count("*").alias("filas"), F.countDistinct("categoria_principal").alias("categorias"))
         .orderBy("part").collect())
    filas_p = [int(r["filas"]) for r in d]
    media = sum(filas_p) / len(filas_p)
    sd = (sum((x - media) ** 2 for x in filas_p) / len(filas_p)) ** 0.5
    return {"particiones_con_datos": len(filas_p), "filas_por_particion": filas_p,
            "categorias_por_particion": [int(r["categorias"]) for r in d],
            "min": min(filas_p), "max": max(filas_p), "media": round(media, 1),
            "max_sobre_media": round(max(filas_p) / media, 2), "coef_variacion": round(sd / media, 2)}


sesgo = {"repartition(8, categoria_principal)  [con clave]": reparto(base.repartition(N_SESGO, "categoria_principal")),
         "repartition(8, geohash)               [con clave]": reparto(base.repartition(N_SESGO, "geohash")),
         "repartition(8)                        [sin clave]": reparto(base.repartition(N_SESGO))}
print(f"\n=== Sesgo: filas por partición ({N_SESGO} particiones) ===")
for nombre, r in sesgo.items():
    print(f"{nombre}\n   filas={r['filas_por_particion']}\n   min={r['min']:,} max={r['max']:,} media={r['media']:,} "
          f"max/media={r['max_sobre_media']} coef.variación={r['coef_variacion']}")

top = (base.groupBy("categoria_principal").count().orderBy(F.desc("count")).collect())
total = sum(int(r["count"]) for r in top)
acum, concentracion = 0.0, []
for r in top[:8]:
    acum += int(r["count"]) / total * 100
    concentracion.append({"categoria": r["categoria_principal"], "filas": int(r["count"]),
                          "pct": round(int(r["count"]) / total * 100, 1), "pct_acum": round(acum, 1)})
print("\nCategorías que concentran las filas (explican el sesgo por clave):")
for c in concentracion:
    print(f"  {c['categoria']:<22}{c['filas']:>9,}  {c['pct']:>5}%  acum {c['pct_acum']}%")

os.makedirs(SALIDA, exist_ok=True)
with open(os.path.join(SALIDA, "balanceo.json"), "w", encoding="utf-8") as f:
    json.dump({"reps": REPS, "filas": n_filas, "nucleos": sc.defaultParallelism, "ejecutores": ejecutores,
               "tiempos": filas, "sesgo": sesgo, "concentracion": concentracion}, f, indent=2, ensure_ascii=False)
with open(os.path.join(SALIDA, "tiempos_particiones.csv"), "w", encoding="utf-8") as f:
    cols = list(filas[0].keys())
    f.write(",".join(cols) + "\n")
    for fila in filas:
        f.write(",".join(str(fila[c]) for c in cols) + "\n")
print(f"\n[balanceo] guardado en {SALIDA}")
spark.stop()
