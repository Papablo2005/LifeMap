# archivo: api/tests/seed.py
"""Siembra datos mínimos en el entorno de PRUEBA de Jenkins (proyecto lifemap-ci). No toca los datos reales.

Cinco negocios alrededor de (38.627, -90.199), Saint Louis. Con estos números, la tasa de cierre de la
zona es EXACTAMENTE 25 %:  2 cierres / (2920 días-negocio / 365) * 100.
"""
import os
from datetime import datetime

from pymongo import ASCENDING, GEOSPHERE, MongoClient

db = MongoClient(os.environ["MONGO_URI"])[os.environ.get("MONGO_DB", "lifemap")]
LON0, LAT0 = -90.199, 38.627
#            evento, días, categoría, n_reseñas, sentimiento medio, % negativo
FILAS = [(1, 365, "Bars", 10, -0.2, 0.40), (0, 730, "Bars", 20, 0.5, 0.10), (0, 1095, "Pubs", 30, 0.6, 0.05),
         (1, 365, "Pubs", 10, 0.0, 0.20), (0, 365, "Bars", 10, 0.9, 0.00)]

db["businesses"].drop()
db["businesses"].insert_many([{
    "_id": f"b{i}", "business_id": f"b{i}", "name": f"Bar {i}", "city": "Saint Louis", "state": "MO",
    "stars": 4.0, "review_count": n, "is_open": 0 if ev else 1,
    "categories": [cat, "Nightlife"], "categoria_principal": cat, "geohash": "9yzgu",
    "location": {"type": "Point", "coordinates": [LON0 + i * 0.001, LAT0 + i * 0.001]},   # [lon, lat]
    "vida": {"primera_resena": datetime(2015, 1, 1), "ultima_resena": datetime(2017, 1, 1), "n_resenas": n,
             "dias_negocio": dias, "evento": ev, "censurado": ev == 0},
    "sentimiento": {"pct_negativo": neg, "media": media, "n_clasificadas": n, "modelo": "prueba"},
} for i, (ev, dias, cat, n, media, neg) in enumerate(FILAS)])
db["businesses"].create_index([("location", GEOSPHERE)])
db["businesses"].create_index([("categoria_principal", ASCENDING)])

db["closure_by_category"].drop()
db["closure_by_category"].insert_many([
    {"_id": "Bars", "categoria": "Bars", "negocios": 3, "cierres": 1, "dias_negocio": 1460, "tasa_cierre_pct": 25.0},
    {"_id": "Pubs", "categoria": "Pubs", "negocios": 2, "cierres": 1, "dias_negocio": 1460, "tasa_cierre_pct": 25.0},
    {"_id": "Karaoke", "categoria": "Karaoke", "negocios": 1, "cierres": 1, "dias_negocio": 100, "tasa_cierre_pct": 100.0},
])
db["closure_by_geohash"].drop()
db["closure_by_geohash"].insert_one({"_id": "9yzgu", "geohash": "9yzgu", "negocios": 5, "cierres": 2,
                                     "dias_negocio": 2920, "tasa_cierre_pct": 25.0,
                                     "location": {"type": "Point", "coordinates": [LON0, LAT0]}})
print("[seed] datos de prueba listos")
