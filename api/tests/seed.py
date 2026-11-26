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

# ------------------------------------------------------------------ Entrega 2
from common.geohash import encode  # noqa: E402
from common.hotspot import documento_ventana, evaluar_hotspot  # noqa: E402

# riesgo de cierre de cada negocio (como lo deja el streaming)
for i, p in enumerate([0.9, 0.2, 0.1, 0.7, 0.05]):
    db["businesses"].update_one({"_id": f"b{i}"}, {"$set": {"riesgo": {
        "probabilidad": p, "alerta": p >= 0.5, "umbral": 0.5, "fecha_ref": datetime(2020, 6, 1),
        "fuente": "streaming", "modelo_version": "1"}}})

# dos ventanas consecutivas de la celda del centro: el % negativo sube de 10 % a 40 % -> hotspot
GH = encode(LAT0, LON0, 5)
db["ventanas_geohash"].drop()
anterior = evaluar_hotspot(documento_ventana(GH, datetime(2020, 4, 15), datetime(2020, 5, 15), 20, 2, "streaming"),
                           None, 0.15, 10)
actual = evaluar_hotspot(documento_ventana(GH, datetime(2020, 5, 15), datetime(2020, 6, 14), 20, 8, "streaming"),
                         anterior, 0.15, 10)
lejana = evaluar_hotspot(documento_ventana("9yzfh", datetime(2020, 5, 15), datetime(2020, 6, 14), 12, 1, "streaming"),
                         None, 0.15, 10)
db["ventanas_geohash"].insert_many([anterior, actual, lejana])
db["ventanas_geohash"].create_index([("centro", GEOSPHERE)])

# alertas: una activa cerca del centro y una ya desactivada
db["alerts"].drop()
db["alerts"].insert_many([
    {"_id": "b0", "business_id": "b0", "name": "Bar 0", "categoria_principal": "Bars", "probabilidad": 0.9,
     "umbral": 0.5, "activa": True, "fecha_evento": datetime(2020, 6, 1), "procesado_en": datetime(2026, 11, 1),
     "veces": 2, "geohash": GH, "location": {"type": "Point", "coordinates": [LON0, LAT0]}},
    {"_id": "b3", "business_id": "b3", "name": "Bar 3", "categoria_principal": "Pubs", "probabilidad": 0.4,
     "umbral": 0.5, "activa": False, "fecha_evento": datetime(2020, 6, 1), "procesado_en": datetime(2026, 11, 1),
     "veces": 1, "geohash": GH, "location": {"type": "Point", "coordinates": [LON0 + 0.003, LAT0 + 0.003]}},
])
db["alerts"].create_index([("location", GEOSPHERE)])

# curvas de supervivencia (una suficiente y una de grupo pequeño)
db["survival"].drop()
curva = [{"t_anios": t, "supervivencia": s, "ic_inferior": s - 0.05, "ic_superior": min(1, s + 0.05), "en_riesgo": r}
         for t, s, r in [(0.0, 1.0, 120), (1.0, 0.9, 100), (5.0, 0.6, 40), (10.0, 0.45, 12)]]
db["survival"].insert_many([
    {"_id": "categoria:Bars", "tipo": "categoria", "grupo": "Bars", "nombre": "Bars", "negocios": 120,
     "cierres": 40, "suficiente": True, "mediana_anios": 8.5, "supervivencia_5a": 0.6, "curva": curva},
    {"_id": "categoria:Karaoke", "tipo": "categoria", "grupo": "Karaoke", "nombre": "Karaoke", "negocios": 6,
     "cierres": 3, "suficiente": False, "mediana_anios": None, "curva": curva[:2]},
    {"_id": "zona:centro", "tipo": "zona", "grupo": "centro", "nombre": "Centro", "negocios": 80, "cierres": 30,
     "suficiente": True, "mediana_anios": 7.0, "curva": curva},
])
print("[seed] datos de prueba listos")
