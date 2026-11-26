# archivo: analitica/supervivencia.py
"""Entrega 2 · Curvas de supervivencia de Kaplan-Meier por categoría principal y por zona.

Cómo se tratan los datos (igual que los discos del ejercicio de MapReduce):
  - Origen del tiempo: la primera reseña (aproximación de la apertura).
  - Evento: is_open = 0 (cierre observado) en la fecha de la última reseña.
  - Censura: is_open = 1. Solo se sabe que seguía abierto en la fecha de corte del dataset, así que su
    tiempo llega HASTA la fecha de corte y se marca como censurado (no es un cierre ni se elimina).
  - Entrada tardía: Yelp no cubría bien el área antes de INICIO_OBSERVACION. Un negocio cuya primera
    reseña es anterior a esa fecha entra al grupo en riesgo con la edad que tenía en INICIO_OBSERVACION
    (argumento `entry` de lifelines). Los que ya habían cerrado antes no se observaron y se excluyen.
  - Grupos pequeños: solo se reporta mediana y se comparan grupos con al menos MIN_NEGOCIOS_KM negocios
    y MIN_EVENTOS_KM cierres; los demás se guardan marcados como "suficiente": false.
  - Zonas: polígonos de config/zonas.geojson; los negocios de cada zona se consultan con $geoWithin.

Salida: colecciones `survival` y `zonas` en MongoDB, reportes/supervivencia.json y gráficos PNG.
Uso:  docker compose run --rm ingest python /app/analitica/supervivencia.py
"""
import json
import math
import os
import sys
from datetime import datetime

sys.path.insert(0, "/app")
from common import config  # noqa: E402

ANIO = 365.25
REJILLA = [round(x * 0.25, 2) for x in range(0, 81)]          # 0 a 20 años, cada trimestre
ZONAS = os.environ.get("ZONAS_GEOJSON", os.path.join(os.path.dirname(__file__), "..", "config", "zonas.geojson"))
SALIDA = os.path.join(config.REPORT_DIR, "supervivencia")


def preparar(negocios, corte, inicio_obs):
    """Convierte documentos de businesses en filas (id, duracion, evento, entrada) en años.

    Devuelve (filas, excluidos_antes_de_observacion)."""
    filas, excluidos = [], 0
    for n in negocios:
        v = n["vida"]
        cerrado = v["evento"] == 1
        fin = v["ultima_resena"] if cerrado else corte          # censurado: vivo hasta la fecha de corte
        if fin < inicio_obs:                                    # salió antes de empezar a observar
            excluidos += 1
            continue
        duracion = max((fin - v["primera_resena"]).days + 1, 1) / ANIO
        entrada = max(0, (inicio_obs - v["primera_resena"]).days) / ANIO
        if duracion <= entrada:
            excluidos += 1
            continue
        filas.append({"id": n["_id"], "duracion": duracion, "evento": int(cerrado), "entrada": entrada,
                      "categoria": n.get("categoria_principal")})
    return filas, excluidos


def en_riesgo(filas, t):
    """Negocios en riesgo en el tiempo t: ya entraron (entrada <= t) y siguen (duracion >= t)."""
    return sum(1 for f in filas if f["entrada"] <= t <= f["duracion"])


def ajustar(filas, etiqueta, con_entrada=True):
    from lifelines import KaplanMeierFitter
    km = KaplanMeierFitter()
    km.fit([f["duracion"] for f in filas], [f["evento"] for f in filas],
           entry=[f["entrada"] for f in filas] if con_entrada else None, label=etiqueta)
    return km


def resumen_grupo(tipo, grupo, nombre, filas):
    n, eventos = len(filas), sum(f["evento"] for f in filas)
    suficiente = n >= config.MIN_NEGOCIOS_KM and eventos >= config.MIN_EVENTOS_KM
    doc = {"_id": f"{tipo}:{grupo}", "tipo": tipo, "grupo": grupo, "nombre": nombre, "negocios": n,
           "cierres": eventos, "censurados": n - eventos, "suficiente": suficiente,
           "mediana_anios": None, "curva": []}
    if n < 2 or eventos == 0:
        doc["nota"] = "sin cierres suficientes para estimar la curva"
        return doc, None
    km = ajustar(filas, nombre)
    s = km.survival_function_at_times(REJILLA).values
    ci = km.confidence_interval_survival_function_
    ci = ci.reindex(ci.index.union(REJILLA)).ffill().bfill().loc[REJILLA].values
    maxt = max(f["duracion"] for f in filas)
    doc["curva"] = [{"t_anios": t, "supervivencia": round(float(s[i]), 4),
                     "ic_inferior": round(float(ci[i][0]), 4), "ic_superior": round(float(ci[i][1]), 4),
                     "en_riesgo": en_riesgo(filas, t)} for i, t in enumerate(REJILLA) if t <= maxt]
    med = km.median_survival_time_
    if suficiente and med is not None and math.isfinite(med):
        doc["mediana_anios"] = round(float(med), 2)
    elif suficiente:
        doc["nota"] = "la curva no baja de 0.5: la mediana de vida no se alcanza en el periodo observado"
    else:
        doc["nota"] = (f"grupo pequeño (< {config.MIN_NEGOCIOS_KM} negocios o < {config.MIN_EVENTOS_KM} "
                       "cierres): no se sacan conclusiones")
    for anios in (1, 3, 5, 10):
        doc[f"supervivencia_{anios}a"] = round(float(km.survival_function_at_times([anios]).values[0]), 4)
    return doc, km


def graficar(kms, titulo, archivo, filas_por_grupo=None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from lifelines.plotting import add_at_risk_counts
    if not kms:
        return
    fig, ax = plt.subplots(figsize=(9, 6))
    for km in kms:
        km.plot_survival_function(ax=ax, ci_show=len(kms) <= 3)
    ax.set_title(titulo)
    ax.set_xlabel("años desde la primera reseña")
    ax.set_ylabel("probabilidad de seguir abierto")
    ax.set_xlim(0, 15)
    ax.set_ylim(0, 1.02)
    ax.grid(alpha=0.3)
    if len(kms) <= 4:
        add_at_risk_counts(*kms, ax=ax, rows_to_show=["At risk"], xticks=[0, 3, 6, 9, 12, 15])
    fig.tight_layout()
    fig.savefig(archivo, dpi=130)
    plt.close(fig)


def main():
    from pymongo import ASCENDING, MongoClient
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    meta = db["meta"].find_one({"_id": "dataset"}) or {}
    negocios = list(db["businesses"].find({}, {"vida": 1, "categoria_principal": 1, "location": 1}))
    corte = meta.get("fecha_corte") or max(n["vida"]["ultima_resena"] for n in negocios)
    inicio = datetime.strptime(config.INICIO_OBSERVACION, "%Y-%m-%d")
    filas, excluidos = preparar(negocios, corte, inicio)
    por_id = {f["id"]: f for f in filas}
    print(f"[km] negocios={len(negocios):,}  usados={len(filas):,}  excluidos (salieron antes de "
          f"{inicio:%Y-%m-%d})={excluidos:,}  con entrada tardía={sum(f['entrada'] > 0 for f in filas):,}")

    docs, informe = [], {"fecha_corte": f"{corte:%Y-%m-%d}", "inicio_observacion": config.INICIO_OBSERVACION,
                         "usados": len(filas), "excluidos": excluidos, "grupos": []}
    os.makedirs(SALIDA, exist_ok=True)

    # ---- global: con y sin entrada tardía (muestra el efecto de tratarla)
    g, km_g = resumen_grupo("global", "todos", "Todos los negocios Nightlife", filas)
    km_ingenuo = ajustar(filas, "Sin tratar entrada tardía", con_entrada=False)
    med_ing = km_ingenuo.median_survival_time_
    g["mediana_sin_entrada_tardia"] = round(float(med_ing), 2) if math.isfinite(med_ing) else None
    docs.append(g)
    graficar([km_g, km_ingenuo], "Global: con y sin entrada tardía", os.path.join(SALIDA, "km_global.png"))

    # ---- por categoría principal
    kms_cat = []
    categorias = sorted({f["categoria"] for f in filas if f["categoria"]})
    for cat in categorias:
        d, km = resumen_grupo("categoria", cat, cat, [f for f in filas if f["categoria"] == cat])
        docs.append(d)
        if km is not None and d["suficiente"]:
            kms_cat.append((d["negocios"], km))
    kms_cat = [km for _, km in sorted(kms_cat, key=lambda x: -x[0])[:6]]
    graficar(kms_cat, "Supervivencia por categoría (grupos suficientes)", os.path.join(SALIDA, "km_categorias.png"))

    # ---- por zona: polígonos consultados con $geoWithin
    with open(ZONAS, encoding="utf-8") as f:
        zonas = json.load(f)["features"]
    db["zonas"].drop()
    kms_zona = []
    for z in zonas:
        zid, nombre, geom = z["properties"]["id"], z["properties"]["nombre"], z["geometry"]
        db["zonas"].insert_one({"_id": zid, "nombre": nombre, "geometria": geom})
        ids = [d["_id"] for d in db["businesses"].find({"location": {"$geoWithin": {"$geometry": geom}}}, {"_id": 1})]
        d, km = resumen_grupo("zona", zid, nombre, [por_id[i] for i in ids if i in por_id])
        d["geometria"] = geom
        docs.append(d)
        if km is not None and d["suficiente"]:
            kms_zona.append(km)
    graficar(kms_zona, "Supervivencia por zona (grupos suficientes)", os.path.join(SALIDA, "km_zonas.png"))

    db["survival"].drop()
    db["survival"].insert_many(docs)
    db["survival"].create_index([("tipo", ASCENDING), ("grupo", ASCENDING)])

    print(f"\n{'tipo':<10}{'grupo':<38}{'negocios':>9}{'cierres':>8}{'suf.':>6}{'mediana(a)':>11}{'S(5a)':>8}"
          f"{'en riesgo t=0/5/10':>20}")
    for d in docs:
        er = {c["t_anios"]: c["en_riesgo"] for c in d["curva"]}
        print(f"{d['tipo']:<10}{d['nombre'][:37]:<38}{d['negocios']:>9}{d['cierres']:>8}"
              f"{'sí' if d['suficiente'] else 'no':>6}{str(d['mediana_anios']):>11}"
              f"{str(d.get('supervivencia_5a', '-')):>8}"
              f"{str(er.get(0.0, '-')) + '/' + str(er.get(5.0, '-')) + '/' + str(er.get(10.0, '-')):>20}")
        informe["grupos"].append({k: v for k, v in d.items() if k not in ("curva", "geometria", "km")})
    print(f"\nMediana global con entrada tardía: {g['mediana_anios']} años; "
          f"sin tratarla: {g['mediana_sin_entrada_tardia']} años")
    with open(os.path.join(config.REPORT_DIR, "supervivencia.json"), "w", encoding="utf-8") as f:
        json.dump(informe, f, indent=2, ensure_ascii=False)
    print(f"[km] curvas en MongoDB (survival), gráficos en {SALIDA}")


if __name__ == "__main__":
    main()
