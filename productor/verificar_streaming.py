# archivo: productor/verificar_streaming.py
"""Prueba de humo del flujo (Jenkins): después de publicar N mensajes, espera a que aparezcan
clasificados en MongoDB (colección reviews_stream) con predicción, probabilidad y marca de procesamiento.
Termina con código 1 si no llegan a tiempo: Jenkins NO despliega."""
import argparse
import sys
import time

from pymongo import MongoClient

sys.path.insert(0, "/app")
from common import config  # noqa: E402

p = argparse.ArgumentParser()
p.add_argument("--esperadas", type=int, default=100)
p.add_argument("--timeout", type=int, default=300, help="segundos")
args = p.parse_args()

col = MongoClient(config.MONGO_URI)[config.MONGO_DB]["reviews_stream"]
completos = {"prediccion": {"$in": ["negativo", "neutral", "positivo"]},
             "probabilidad": {"$gte": 0, "$lte": 1}, "procesado_en": {"$type": "date"},
             "modelo_version": {"$exists": True}}
inicio = time.time()
while True:
    total, buenos = col.count_documents({}), col.count_documents(completos)
    print(f"[humo] {time.time() - inicio:5.0f} s  en reviews_stream={total}  completos={buenos}", flush=True)
    if buenos >= args.esperadas:
        ejemplo = col.find_one(completos, {"text": 0})
        print(f"[humo] OK: {buenos} reseñas clasificadas. Ejemplo: {ejemplo}")
        sys.exit(0)
    if time.time() - inicio > args.timeout:
        sys.exit(f"[humo] FALLA: solo {buenos}/{args.esperadas} reseñas clasificadas en {args.timeout} s")
    time.sleep(5)
