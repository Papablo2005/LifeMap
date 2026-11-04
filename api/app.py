# archivo: api/app.py
"""API Flask de LifeMap (Entrega 1).

  GET  /health                                  estado de cada servicio + hash del commit
  GET  /businesses/near?lat&lon&radio[&categoria&limite]   $near
  POST /businesses/within[?categoria&limite]    (cuerpo: GeoJSON Polygon)   $geoWithin
  GET  /zones/summary?lat&lon&radio[&categoria] agregación con $geoNear: sentimiento medio y tasa de cierre
  GET  /stats/closure?por=categoria|geohash&min_dias=N[&limite]   tasas de cierre (MapReduce)
  POST /predict                                 {"text": "..."} -> sentimiento
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
from validators import (ValidationError, parse_categoria, parse_limite, parse_min_dias, parse_por,
                        parse_poligono, parse_punto, parse_radio, parse_texto)

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
    informe, status = health.verificar(lambda: db().command("ping"))
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


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
