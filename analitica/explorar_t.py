# archivo: analitica/explorar_t.py
"""Ayuda a elegir la fecha de corte T y el inicio de observación (Entrega 2). Solo lee MongoDB.

Para cada T candidata muestra:
  - reseñas hasta T (entrenamiento) y después de T (las que publicará el productor) y cuánto dura la demo;
  - población del modelo de riesgo: negocios con actividad antes y después de T, positivos (cerrados con
    última reseña entre T y T+12 meses), negativos y excluidos (sin actividad después de T).
Regla de recomendación: la T MÁS TEMPRANA con al menos MIN_POSITIVOS positivos (más datos para el
streaming y menos dependencia de un solo año). También imprime reseñas por año para justificar
INICIO_OBSERVACION (cuándo Yelp ya cubría bien el área).

Uso:  docker compose run --rm ingest python /app/analitica/explorar_t.py
"""
import os
import sys
from datetime import datetime

from pymongo import MongoClient

sys.path.insert(0, "/app")
from common import config  # noqa: E402
from common.riesgo import etiqueta, sumar_meses  # noqa: E402

MIN_POSITIVOS = int(os.environ.get("MIN_POSITIVOS", "50"))


def main():
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    meta = db["meta"].find_one({"_id": "dataset"}) or {}
    negocios = list(db["businesses"].find({}, {"vida": 1}))
    corte = meta.get("fecha_corte") or max(n["vida"]["ultima_resena"] for n in negocios)
    print(f"Negocios: {len(negocios):,}   fecha de corte del dataset: {corte:%Y-%m-%d}")

    por_anio = {r["_id"]: r["n"] for r in db["reviews"].aggregate(
        [{"$group": {"_id": {"$year": "$date"}, "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}])}
    print("\nReseñas por año (para justificar INICIO_OBSERVACION):")
    for anio, n in por_anio.items():
        print(f"  {anio}: {n:>7,} {'#' * max(1, n // 500)}")

    candidatas = []
    for anio in range(2016, corte.year):
        for mes in (1, 7):
            t = datetime(anio, mes, 1)
            if sumar_meses(t, 12) <= corte:
                candidatas.append(t)

    print(f"\n{'T':<12}{'reseñas<=T':>12}{'reseñas>T':>11}{'demo(min)':>10}{'población':>11}"
          f"{'positivos':>10}{'negativos':>10}{'excluidos':>10}{'% pos':>7}")
    recomendada = None
    for t in candidatas:
        antes = db["reviews"].count_documents({"date": {"$lte": t}})
        despues = db["reviews"].count_documents({"date": {"$gt": t}})
        meses_stream = (corte - t).days / 30.4
        demo_min = (corte - t).days / config.VELOCIDAD_DIAS_POR_SEG / 60
        pos = neg = exc = 0
        for n in negocios:
            v = n["vida"]
            if v["primera_resena"] > t:          # aún no existía en T: no tiene variables
                continue
            e = etiqueta(v["evento"] == 1, v["ultima_resena"], t)
            if e is None:
                exc += 1
            elif e == 1:
                pos += 1
            else:
                neg += 1
        pobl = pos + neg
        print(f"{t:%Y-%m-%d}  {antes:>12,}{despues:>11,}{demo_min:>10.1f}{pobl:>11,}{pos:>10,}{neg:>10,}"
              f"{exc:>10,}{(pos / pobl if pobl else 0):>7.1%}   ({meses_stream:.0f} meses de streaming)")
        if recomendada is None and pos >= MIN_POSITIVOS:
            recomendada = t
    if recomendada:
        print(f"\nRECOMENDADA: FECHA_T={recomendada:%Y-%m-%d}  (la más temprana con >= {MIN_POSITIVOS} positivos)")
    else:
        print(f"\nNinguna T tiene {MIN_POSITIVOS} positivos: use la de más positivos y explíquelo en el informe.")
    print(f"Configurada ahora en config.env: FECHA_T={config.FECHA_T}")


if __name__ == "__main__":
    main()
