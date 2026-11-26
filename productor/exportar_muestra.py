# archivo: productor/exportar_muestra.py
"""Imprime N reseñas REALES posteriores a T (JSON por línea) desde la base de producción.
Jenkins lo usa en la prueba de humo del streaming:
    (prod)  python exportar_muestra.py --n 100  |  (entorno de prueba) python productor.py --stdin --velocidad 0
Así la prueba usa datos reales del dataset sin tocar la base de producción ni escribir archivos."""
import argparse
import sys

from bson import json_util
from pymongo import ASCENDING, MongoClient

sys.path.insert(0, "/app")
from common import config  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--n", type=int, default=100)
args = p.parse_args()
db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
docs = list(db["reviews"].find({"date": {"$gt": config.fecha_t()}}, {"label": 0})
            .sort("date", ASCENDING).limit(args.n))
if len(docs) < args.n:
    sys.exit(f"Solo hay {len(docs)} reseñas posteriores a T={config.FECHA_T} en la base (se esperaban {args.n})")
for d in docs:
    print(json_util.dumps(d))
