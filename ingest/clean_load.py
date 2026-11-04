# archivo: ingest/clean_load.py
"""Paso 2: ingesta y limpieza distribuida con Dask + vida de los negocios + carga a MongoDB.

Flujo:
  business.json / review.json --(Dask, lectura PARTICIONADA)--> limpieza por partición
  --> Parquet limpio (/data/clean)  --> vida de cada negocio --> MongoDB (por lotes)

Deja un reporte con los conteos antes/después de CADA regla
(/data/reportes/limpieza.json) para el informe.
"""
import glob
import json
import os
import shutil
import time
from pathlib import Path

import dask
import dask.dataframe as dd
import pandas as pd
from dask.distributed import Client

from carga_mongo import (cargar_resenas_particion, crear_indices, docs_negocios,
                         insertar_lotes)
from common import config
from limpieza import (REGLAS_NEGOCIO, REGLAS_RESENA, construir_vida, limpiar_negocios_particion,
                      limpiar_resenas_particion, quitar_duplicados, reporte_reglas)

BLOCK_NEGOCIOS = os.environ.get("BLOCKSIZE_NEGOCIOS", "32MB")
BLOCK_RESENAS = os.environ.get("BLOCKSIZE_RESENAS", "64MB")
MAX_PARTICIONES = os.environ.get("MAX_PARTITIONS", "").strip()       # vacío = todo; para pruebas, p. ej. 2
CARGAR_MONGO = os.environ.get("CARGAR_MONGO", "1") == "1"
LOTE = int(os.environ.get("BATCH_SIZE", "5000"))
CLEAN = Path(config.CLEAN_DIR)
REPORTE = Path(config.REPORT_DIR) / "limpieza.json"
FMT_PARQUET = dict(coerce_timestamps="ms", allow_truncated_timestamps=True)   # Spark no lee nanosegundos


def buscar(patron):
    rutas = sorted(glob.glob(os.path.join(config.RAW_DIR, "**", patron), recursive=True))
    if not rutas:
        raise SystemExit(f"No encuentro {patron} en {config.RAW_DIR}: corra primero download.py")
    return rutas[0]


def leer_json_lineas(ruta, blocksize):
    """Lectura PARTICIONADA: cada bloque de ~blocksize lo procesa un worker."""
    ddf = dd.read_json(ruta, lines=True, blocksize=blocksize, dtype=False, convert_dates=False)
    partes = ddf.to_delayed()
    return partes[:int(MAX_PARTICIONES)] if MAX_PARTICIONES else partes


def _suma(dicts):
    out = {}
    for d in dicts:
        for k, v in d.items():
            out[k] = out.get(k, 0) + v
    return out


# ---------------------------------------------------------------- funciones que corren en los workers
def tarea_negocios(pdf, estados, bbox, familia, precision):
    return limpiar_negocios_particion(pdf, estados, bbox, familia, precision)


def tarea_resenas(pdf, ids_validos, indice, destino):
    limpias, leidos, descartes, fecha_max = limpiar_resenas_particion(pdf, ids_validos)
    if len(limpias):
        limpias.to_parquet(os.path.join(destino, f"part-{indice:05d}.parquet"), index=False, **FMT_PARQUET)
    return {"leidos": leidos, "conservadas": len(limpias), "descartes": descartes, "fecha_max": fecha_max}


# ------------------------------------------------------------------------------------- principal
def main():
    t0 = time.perf_counter()
    cliente = Client(os.environ["DASK_SCHEDULER"]) if os.environ.get("DASK_SCHEDULER") else Client(
        n_workers=2, threads_per_worker=1)
    print(f"[clean] Dask: {cliente.dashboard_link}")
    reporte = {"area": {"estados": config.AREA_STATES, "bbox": config.AREA_BBOX, "familia": config.FAMILIA,
                        "geohash_precision": config.GEOHASH_PRECISION}}

    # ---------- 1) NEGOCIOS (lectura particionada + reglas por partición)
    ruta_b = buscar("*business*.json")
    partes_b = leer_json_lineas(ruta_b, BLOCK_NEGOCIOS)
    res = dask.compute(*[dask.delayed(tarea_negocios)(p, config.AREA_STATES, config.bbox(), config.FAMILIA,
                                                       config.GEOHASH_PRECISION) for p in partes_b])
    leidos_b = sum(r[1] for r in res)
    descartes_b = _suma([r[2] for r in res])
    negocios = pd.concat([r[0] for r in res], ignore_index=True)
    negocios, n_dup = quitar_duplicados(negocios, "business_id")
    descartes_b[REGLAS_NEGOCIO[6]] = n_dup
    print(f"[clean] negocios: leídos={leidos_b:,}  del área y familia={len(negocios):,}")

    # ---------- 2) RESEÑAS (lectura particionada; unión reseña-negocio = filtrar por business_id)
    ruta_r = buscar("*review*.json")
    tmp = CLEAN / "_resenas_tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True, exist_ok=True)
    ids = set(negocios["business_id"])
    partes_r = leer_json_lineas(ruta_r, BLOCK_RESENAS)
    cuentas = dask.compute(*[dask.delayed(tarea_resenas)(p, ids, i, str(tmp)) for i, p in enumerate(partes_r)])
    leidos_r = sum(c["leidos"] for c in cuentas)
    descartes_r = _suma([c["descartes"] for c in cuentas])
    conservadas = sum(c["conservadas"] for c in cuentas)
    fechas = [pd.Timestamp(c["fecha_max"]) for c in cuentas if c["fecha_max"]]
    fecha_corte = max(fechas)          # última fecha de reseña presente en el dataset
    print(f"[clean] reseñas: leídas={leidos_r:,}  tras reglas por partición={conservadas:,}")

    # ---------- 3) duplicados globales de review_id + unión con los datos del negocio
    resenas = dd.read_parquet(str(tmp))
    mapa = negocios[["business_id", "geohash", "categoria_principal"]]
    resenas = resenas.drop_duplicates(subset=["review_id"]).merge(mapa, on="business_id", how="inner")
    destino = CLEAN / "resenas"
    shutil.rmtree(destino, ignore_errors=True)
    resenas.to_parquet(str(destino), write_index=False, **FMT_PARQUET)
    shutil.rmtree(tmp, ignore_errors=True)
    resenas = dd.read_parquet(str(destino))
    n_final_r = len(resenas)
    descartes_r[REGLAS_RESENA[4]] = conservadas - n_final_r
    print(f"[clean] reseñas finales={n_final_r:,}")

    # ---------- 4) VIDA de cada negocio: primera y última reseña, número de reseñas
    agg = resenas.groupby("business_id").agg({"date": ["min", "max"], "review_id": "count"}).compute()
    agg.columns = ["primera_resena", "ultima_resena", "n_resenas"]
    agg = agg.reset_index()
    vida = construir_vida(negocios, agg)
    descartes_b[REGLAS_NEGOCIO[7]] = len(negocios) - len(vida)
    vida["primera_resena"] = vida["primera_resena"].astype("datetime64[ms]")
    vida["ultima_resena"] = vida["ultima_resena"].astype("datetime64[ms]")
    dir_neg = CLEAN / "negocios"
    shutil.rmtree(dir_neg, ignore_errors=True)
    dir_neg.mkdir(parents=True)
    vida.to_parquet(dir_neg / "negocios.parquet", index=False, **FMT_PARQUET)
    n_cerrados = int(vida["evento"].sum())
    print(f"[clean] negocios con vida={len(vida):,}  cierres observados={n_cerrados:,}  "
          f"censurados={len(vida) - n_cerrados:,}  fecha de corte={fecha_corte.date()}")

    # ---------- 5) carga por lotes a MongoDB
    mongo_info = {}
    if CARGAR_MONGO:
        from pymongo import MongoClient
        mongo = MongoClient(config.MONGO_URI)
        db = mongo[config.MONGO_DB]
        db["businesses"].drop()
        db["reviews"].drop()
        n_neg = insertar_lotes(db["businesses"], docs_negocios(vida), LOTE)
        n_res = int(resenas.map_partitions(cargar_resenas_particion, config.MONGO_URI, config.MONGO_DB, LOTE,
                                           meta={"insertados": "int64"}).compute()["insertados"].sum())
        crear_indices(db)          # el 2dsphere falla si hay geometrías inválidas
        db["meta"].replace_one({"_id": "dataset"}, {
            "_id": "dataset", "fecha_corte": fecha_corte.to_pydatetime(), "area": reporte["area"],
            "negocios": n_neg, "resenas": n_res, "cierres": n_cerrados}, upsert=True)
        mongo_info = {"negocios_insertados": n_neg, "resenas_insertadas": n_res}
        print(f"[clean] MongoDB: negocios={n_neg:,}  reseñas={n_res:,}")

    reporte["negocios"] = {"leidos": leidos_b, "conservados": len(vida),
                           "reglas": reporte_reglas(leidos_b, REGLAS_NEGOCIO, descartes_b)}
    reporte["resenas"] = {"leidas": leidos_r, "conservadas": n_final_r,
                          "reglas": reporte_reglas(leidos_r, REGLAS_RESENA, descartes_r)}
    reporte["vida"] = {"fecha_corte": str(fecha_corte.date()), "cierres": n_cerrados,
                       "censurados": len(vida) - n_cerrados, "dias_negocio_total": int(vida["dias_negocio"].sum())}
    reporte["mongo"] = mongo_info
    reporte["segundos"] = round(time.perf_counter() - t0, 1)
    REPORTE.parent.mkdir(parents=True, exist_ok=True)
    REPORTE.write_text(json.dumps(reporte, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print("[clean] Reporte en", REPORTE)
    for sec in ("negocios", "resenas"):
        print(f"\n--- reglas de {sec} ---")
        for r in reporte[sec]["reglas"]:
            print(f"  {r['regla']:<58} antes={r['antes']:>10,} descartados={r['descartados']:>9,} "
                  f"despues={r['despues']:>10,}")
    cliente.close()


if __name__ == "__main__":
    main()
