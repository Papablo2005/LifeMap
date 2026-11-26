# archivo: api/app.py
"""API Flask de LifeMap (Entregas 1 y 2).

  GET  /health                                  estado de cada servicio + hash del commit
  GET  /businesses/near?lat&lon&radio[&categoria&limite]   $near
  POST /businesses/within[?categoria&limite]    (cuerpo: GeoJSON Polygon)   $geoWithin
  GET  /zones/summary?lat&lon&radio[&categoria] agregación con $geoNear: sentimiento medio y tasa de cierre
  GET  /stats/closure?por=categoria|geohash&min_dias=N[&limite]   tasas de cierre (MapReduce)
  POST /predict                                 {"text": "..."} -> sentimiento
  --- Entrega 2 ---
  GET  /live/hotspots?lat&lon&radio[&ventana=ultima|YYYY-MM-DD&solo_hotspots&limite]   ventanas por geohash
  GET  /live/evolucion[?lat&lon&radio]          % negativo por ventana de 30 días (para el dashboard)
  GET  /alerts[?lat&lon&radio&activas&limite]   alertas de riesgo por radio ($geoWithin $centerSphere)
  POST /alerts[?activas&limite]                 (cuerpo: GeoJSON Polygon) alertas dentro del polígono
  GET  /survival                                todas las curvas (formato plano, para Power BI)
  GET  /survival/<categoria>[?tipo=categoria|zona|global]   curva de Kaplan-Meier de un grupo
  GET  /risk/near?lat&lon&radio[&categoria&limite]          riesgo de cierre de los negocios cercanos ($geoNear)
"""
import json
import os
from datetime import date, datetime

from flask import Flask, Response, request
from pymongo import DESCENDING, MongoClient
from pymongo.errors import PyMongoError

import health
import predictor
from common import config
from common.tasas import tasa_cierre_pct
from validators import (ValidationError, parse_bool, parse_categoria, parse_fecha_o_ultima, parse_limite,
                        parse_min_dias, parse_por, parse_poligono, parse_punto, parse_punto_opcional, parse_radio,
                        parse_texto, parse_tipo_grupo)

RADIO_TIERRA_M = 6378100.0

MONGO_URI = os.environ.get("MONGO_URI", config.MONGO_URI)
DB = os.environ.get("MONGO_DB", config.MONGO_DB)
COLECCIONES_TASAS = {"categoria": "closure_by_category", "geohash": "closure_by_geohash"}
PROYECCION_NEGOCIO = {"_id": 0, "business_id": 1, "name": 1, "city": 1, "stars": 1, "review_count": 1,
                      "is_open": 1, "categories": 1, "categoria_principal": 1, "geohash": 1,
                      "location": 1, "vida": 1, "sentimiento": 1}

app = Flask(__name__)
_cliente = None


def db():
    """Crea la conexión la primera vez que se necesita y la reutiliza."""
    global _cliente
    if _cliente is None:
        _cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    return _cliente[DB]


def _a_json(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    return str(o)


def respuesta(obj, status=200):
    return Response(json.dumps(obj, ensure_ascii=False, default=_a_json), status=status,
                    mimetype="application/json")


@app.errorhandler(ValidationError)
def error_validacion(e):
    return respuesta({"error": str(e)}, 400)


@app.errorhandler(PyMongoError)
def error_mongo(e):
    return respuesta({"error": "Base de datos no disponible", "detalle": str(e)[:200]}, 503)


@app.errorhandler(predictor.ModeloNoDisponible)
def error_modelo(e):
    return respuesta({"error": "Modelo de sentimiento no disponible", "detalle": str(e)}, 503)


# ------------------------------------------------------------------ /health
@app.get("/health")
def estado():
    informe, status = health.verificar(lambda: db().command("ping"),
                                       lambda: {"meta": db()["meta"].find_one({"_id": "streaming"})})
    return respuesta(informe, status)


# ------------------------------------------------------- /businesses/near ($near)
@app.get("/businesses/near")
def cerca():
    punto = parse_punto(request.args)
    radio = parse_radio(request.args)
    limite = parse_limite(request.args)
    categoria = parse_categoria(request.args)
    filtro = {"location": {"$near": {"$geometry": punto, "$maxDistance": radio}}}      # radio en metros
    if categoria:
        filtro["categories"] = categoria
    docs = list(db()["businesses"].find(filtro, PROYECCION_NEGOCIO).limit(limite))      # ya vienen por distancia
    return respuesta({"total": len(docs), "radio_m": radio, "categoria": categoria, "resultados": docs})


# ---------------------------------------------------- /businesses/within ($geoWithin)
@app.post("/businesses/within")
def dentro():
    poligono = parse_poligono(request.get_json(silent=True))
    limite = parse_limite(request.args)
    categoria = parse_categoria(request.args)
    filtro = {"location": {"$geoWithin": {"$geometry": poligono}}}
    if categoria:
        filtro["categories"] = categoria
    col = db()["businesses"]
    docs = list(col.find(filtro, PROYECCION_NEGOCIO).limit(limite))
    return respuesta({"total_en_poligono": col.count_documents(filtro), "devueltos": len(docs),
                      "categoria": categoria, "resultados": docs})


# ------------------------------------------------------- /zones/summary ($geoNear)
@app.get("/zones/summary")
def resumen_zona():
    """Sentimiento medio y tasa de cierre alrededor de un punto.

    $geoNear DEBE ser la primera etapa del pipeline. La tasa se calcula con las SUMAS de la zona
    (cierres y días-negocio), no promediando tasas de cada negocio.
    El sentimiento medio va ponderado por número de reseñas: -1 = todo negativo ... +1 = todo positivo.
    """
    punto = parse_punto(request.args)
    radio = parse_radio(request.args)
    categoria = parse_categoria(request.args)
    geo = {"near": punto, "distanceField": "distancia_m", "maxDistance": radio, "spherical": True,
           "key": "location"}
    if categoria:
        geo["query"] = {"categories": categoria}
    pipeline = [
        {"$geoNear": geo},
        {"$group": {
            "_id": None,
            "negocios": {"$sum": 1},
            "cierres": {"$sum": "$vida.evento"},
            "dias_negocio": {"$sum": "$vida.dias_negocio"},
            "resenas": {"$sum": "$sentimiento.n_clasificadas"},
            "suma_sentimiento": {"$sum": {"$multiply": ["$sentimiento.media", "$sentimiento.n_clasificadas"]}},
            "suma_negativas": {"$sum": {"$multiply": ["$sentimiento.pct_negativo", "$sentimiento.n_clasificadas"]}},
            "distancia_promedio_m": {"$avg": "$distancia_m"},
        }},
    ]
    r = next(iter(db()["businesses"].aggregate(pipeline)), None)
    if r is None:
        return respuesta({"centro": punto, "radio_m": radio, "categoria": categoria, "negocios": 0,
                          "mensaje": "No hay negocios en ese radio"})
    n_res = r["resenas"] or 0
    return respuesta({
        "centro": punto, "radio_m": radio, "categoria": categoria,
        "negocios": r["negocios"], "resenas": n_res,
        "sentimiento_medio": (r["suma_sentimiento"] / n_res) if n_res else None,
        "pct_negativo": (r["suma_negativas"] / n_res) if n_res else None,
        "cierres": r["cierres"], "dias_negocio": r["dias_negocio"],
        "tasa_cierre_pct": tasa_cierre_pct(r["cierres"], r["dias_negocio"]),
        "distancia_promedio_m": r["distancia_promedio_m"],
    })


# ---------------------------------------------------------------- /stats/closure
@app.get("/stats/closure")
def tasas_cierre():
    """Tasas de cierre por categoría o por celda de geohash (salida del MapReduce).

    `min_dias` es el mínimo de días-negocio: los grupos con menos exposición NO se devuelven
    (con tan pocos datos una tasa no es confiable) y se informa cuántos se dejaron fuera.
    """
    por = parse_por(request.args)
    min_dias = parse_min_dias(request.args, config.MIN_DIAS_NEGOCIO)
    limite = parse_limite(request.args)
    col = db()[COLECCIONES_TASAS[por]]
    filtro = {"dias_negocio": {"$gte": min_dias}}
    docs = list(col.find(filtro, {"celda": 0}).sort("tasa_cierre_pct", DESCENDING).limit(limite))
    return respuesta({"por": por, "min_dias": min_dias, "total": len(docs),
                      "grupos_excluidos_por_muestra": col.count_documents({"dias_negocio": {"$lt": min_dias}}),
                      "resultados": docs})


# -------------------------------------------------------------------- /predict
@app.post("/predict")
def predecir():
    texto = parse_texto(request.get_json(silent=True))
    return respuesta({"texto": texto[:200], **predictor.predecir(texto)})


# ================================================================== ENTREGA 2
def _circulo(punto, radio):
    """Filtro $geoWithin con un círculo (radio en metros -> radianes)."""
    return {"$geoWithin": {"$centerSphere": [punto["coordinates"], radio / RADIO_TIERRA_M]}}


def _plano_ventana(d):
    lon, lat = d["centro"]["coordinates"]
    return {"geohash": d["geohash"], "inicio": d["inicio"], "fin": d["fin"], "fuente": d.get("fuente"),
            "resenas": d["n"], "negativas": d["negativas"], "pct_negativo": d["pct_negativo"],
            "pct_negativo_anterior": d.get("pct_negativo_anterior"), "delta": d.get("delta"),
            "hotspot": bool(d.get("hotspot")), "lat": lat, "lon": lon, "celda": d.get("celda")}


# ------------------------------------------------------------------ /live/hotspots
@app.get("/live/hotspots")
def hotspots():
    """Ventanas de 30 días por celda de geohash alrededor de un punto. ventana=ultima (por defecto) toma la
    ventana más reciente que tenga datos en ese radio; solo_hotspots=1 deja solo las celdas en alerta."""
    punto = parse_punto(request.args)
    radio = parse_radio(request.args, maximo=100_000)       # toda el área metropolitana cabe en ~60 km
    ventana = parse_fecha_o_ultima(request.args)
    solo = parse_bool(request.args, "solo_hotspots")
    limite = parse_limite(request.args, default=500)
    col = db()["ventanas_geohash"]
    filtro = {"centro": _circulo(punto, radio)}
    if ventana is None:
        ultima = next(iter(col.find(filtro, {"inicio": 1}).sort("inicio", DESCENDING).limit(1)), None)
        if ultima is None:
            return respuesta({"centro": punto, "radio_m": radio, "ventana_inicio": None, "total": 0,
                              "hotspots": 0, "resultados": [], "mensaje": "Aún no hay ventanas en ese radio"})
        ventana = ultima["inicio"]
    filtro["inicio"] = ventana
    if solo:
        filtro["hotspot"] = True
    docs = [_plano_ventana(d) for d in col.find(filtro).sort("pct_negativo", DESCENDING).limit(limite)]
    return respuesta({"centro": punto, "radio_m": radio, "ventana_inicio": ventana, "total": len(docs),
                      "hotspots": sum(d["hotspot"] for d in docs), "resultados": docs})


@app.get("/live/evolucion")
def evolucion():
    """% de reseñas negativas por ventana de 30 días (suma de las celdas del radio, o de toda el área)."""
    punto, radio = parse_punto_opcional(request.args)
    filtro = {"centro": _circulo(punto, radio)} if punto else {}
    pipeline = [{"$match": filtro},
                {"$group": {"_id": "$inicio", "resenas": {"$sum": "$n"}, "negativas": {"$sum": "$negativas"},
                            "celdas": {"$sum": 1}, "hotspots": {"$sum": {"$cond": ["$hotspot", 1, 0]}},
                            "fuentes": {"$addToSet": "$fuente"}}},
                {"$sort": {"_id": 1}}]
    filas = [{"inicio": r["_id"], "resenas": r["resenas"], "negativas": r["negativas"],
              "pct_negativo": r["negativas"] / r["resenas"] if r["resenas"] else None, "celdas": r["celdas"],
              "hotspots": r["hotspots"], "fuente": "streaming" if "streaming" in r["fuentes"] else "historico"}
             for r in db()["ventanas_geohash"].aggregate(pipeline)]
    return respuesta({"total": len(filas), "resultados": filas})


# ------------------------------------------------------------------ /alerts
def _plano_alerta(d):
    lon, lat = d["location"]["coordinates"]
    return {"business_id": d["_id"], "name": d.get("name"), "categoria_principal": d.get("categoria_principal"),
            "probabilidad": d.get("probabilidad"), "umbral": d.get("umbral"), "activa": d.get("activa"),
            "fecha_evento": d.get("fecha_evento"), "procesado_en": d.get("procesado_en"),
            "primera_alerta": d.get("primera_alerta"), "veces": d.get("veces"), "geohash": d.get("geohash"),
            "lat": lat, "lon": lon, "location": d["location"], "variables": d.get("variables")}


def _alertas(filtro):
    activas = parse_bool(request.args, "activas", default=True)
    limite = parse_limite(request.args, default=200)
    if activas:
        filtro["activa"] = True
    col = db()["alerts"]
    docs = [_plano_alerta(d) for d in col.find(filtro).sort("probabilidad", DESCENDING).limit(limite)]
    return respuesta({"total": col.count_documents(filtro), "devueltas": len(docs), "solo_activas": activas,
                      "resultados": docs})


@app.get("/alerts")
def alertas_radio():
    punto, radio = parse_punto_opcional(request.args)
    return _alertas({"location": _circulo(punto, radio)} if punto else {})


@app.post("/alerts")
def alertas_poligono():
    poligono = parse_poligono(request.get_json(silent=True))
    return _alertas({"location": {"$geoWithin": {"$geometry": poligono}}})


# ------------------------------------------------------------------ /survival
@app.get("/survival")
def supervivencia_todas():
    """Todas las curvas en formato plano (una fila por grupo y tiempo): lo consume Power BI."""
    tipo = request.args.get("tipo")
    if tipo:
        tipo = parse_tipo_grupo(request.args)
    pequenos = parse_bool(request.args, "incluir_pequenos")
    filtro = {"tipo": tipo} if tipo else {}
    if not pequenos:
        filtro["suficiente"] = True
    filas, grupos = [], []
    for d in db()["survival"].find(filtro, {"geometria": 0}):
        grupos.append({k: d.get(k) for k in ("tipo", "grupo", "nombre", "negocios", "cierres", "mediana_anios",
                                             "supervivencia_5a", "suficiente")})
        filas += [{"tipo": d["tipo"], "grupo": d["grupo"], "nombre": d["nombre"], **c} for c in d["curva"]]
    return respuesta({"grupos": grupos, "total_puntos": len(filas), "resultados": filas})


@app.get("/survival/<path:grupo>")
def supervivencia(grupo):
    tipo = parse_tipo_grupo(request.args)
    doc = db()["survival"].find_one({"tipo": tipo, "grupo": grupo}, {"geometria": 0})
    if doc is None:
        disponibles = sorted(d["grupo"] for d in db()["survival"].find({"tipo": tipo}, {"grupo": 1}))
        return respuesta({"error": f"No hay curva para {tipo} '{grupo}'", "disponibles": disponibles}, 404)
    doc["nota_conclusiones"] = None if doc.get("suficiente") else \
        "Grupo pequeño: la curva se muestra pero no se sacan conclusiones ni se reporta mediana"
    return respuesta(doc)


# ------------------------------------------------------------------ /risk/near
@app.get("/risk/near")
def riesgo_cerca():
    """Probabilidad de cierre (modelo de riesgo, actualizada por el streaming) de los negocios cercanos."""
    punto = parse_punto(request.args)
    radio = parse_radio(request.args)
    categoria = parse_categoria(request.args)
    limite = parse_limite(request.args)
    query = {"riesgo": {"$exists": True}}
    if categoria:
        query["categories"] = categoria
    pipeline = [{"$geoNear": {"near": punto, "distanceField": "distancia_m", "maxDistance": radio,
                              "spherical": True, "key": "location", "query": query}},
                {"$limit": limite},
                {"$project": {"_id": 0, "business_id": "$_id", "name": 1, "categoria_principal": 1,
                              "distancia_m": 1, "location": 1, "is_open_2022": "$is_open",
                              "probabilidad": "$riesgo.probabilidad", "alerta": "$riesgo.alerta",
                              "umbral": "$riesgo.umbral", "fecha_ref": "$riesgo.fecha_ref",
                              "fuente": "$riesgo.fuente"}}]
    docs = list(db()["businesses"].aggregate(pipeline))
    return respuesta({"centro": punto, "radio_m": radio, "categoria": categoria, "total": len(docs),
                      "en_alerta": sum(1 for d in docs if d.get("alerta")), "resultados": docs})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
