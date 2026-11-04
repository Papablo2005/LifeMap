# archivo: ingest/carga_mongo.py
"""Construcción de documentos y carga POR LOTES a MongoDB.

Modelo de datos:
  businesses : un documento por negocio, con location (GeoJSON Point, [lon, lat]),
               índice 2dsphere y la vida del negocio (días-negocio, evento, censura).
  reviews    : un documento por reseña, que referencia a su negocio con business_id.
"""
import pandas as pd
from pymongo import ASCENDING, GEOSPHERE, MongoClient


def _py(v):
    """Convierte tipos de numpy/pandas a tipos nativos que acepta Mongo."""
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    if hasattr(v, "item"):
        return v.item()
    return v


def docs_negocios(df):
    docs = []
    for r in df.itertuples(index=False):
        docs.append({
            "_id": r.business_id,
            "business_id": r.business_id,
            "name": r.name, "city": r.city, "state": r.state,
            "stars": float(r.stars), "review_count": int(r.review_count),
            "is_open": int(r.is_open),
            "categories": [str(c) for c in r.categories],
            "categoria_principal": r.categoria_principal,
            "geohash": r.geohash,
            # GeoJSON: el orden es [LONGITUD, LATITUD]
            "location": {"type": "Point", "coordinates": [float(r.longitude), float(r.latitude)]},
            "vida": {
                "primera_resena": _py(r.primera_resena), "ultima_resena": _py(r.ultima_resena),
                "n_resenas": int(r.n_resenas), "dias_negocio": int(r.dias_negocio),
                "evento": int(r.evento), "censurado": bool(r.censurado),
            },
        })
    return docs


def docs_resenas(df):
    return [{
        "_id": r.review_id, "review_id": r.review_id, "business_id": r.business_id,
        "stars": float(r.stars), "label": int(r.label), "text": r.text,
        "date": _py(r.date), "geohash": r.geohash, "categoria_principal": r.categoria_principal,
    } for r in df.itertuples(index=False)]


def insertar_lotes(coleccion, docs, lote=5000):
    """insert_many por lotes (mucho más rápido que documento por documento)."""
    n = 0
    for i in range(0, len(docs), lote):
        parte = docs[i:i + lote]
        coleccion.insert_many(parte, ordered=False)
        n += len(parte)
    return n


def cargar_resenas_particion(pdf, uri, db, lote, fabrica_cliente=MongoClient):
    """Se ejecuta EN CADA WORKER de Dask. El cliente de Mongo se crea aquí adentro
    porque no se puede serializar y enviar desde el proceso principal."""
    if pdf.empty:
        return pd.DataFrame({"insertados": [0]})
    cli = fabrica_cliente(uri)
    n = insertar_lotes(cli[db]["reviews"], docs_resenas(pdf), lote)
    cli.close()
    return pd.DataFrame({"insertados": [n]})


def crear_indices(db):
    """El 2dsphere es obligatorio: falla si hay geometrías inválidas."""
    db["businesses"].create_index([("location", GEOSPHERE)])
    db["businesses"].create_index([("categoria_principal", ASCENDING)])
    db["businesses"].create_index([("geohash", ASCENDING)])
    db["reviews"].create_index([("business_id", ASCENDING)])
    db["reviews"].create_index([("geohash", ASCENDING)])
    db["reviews"].create_index([("date", ASCENDING)])
