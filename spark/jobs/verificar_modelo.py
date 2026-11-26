# archivo: spark/jobs/verificar_modelo.py
"""Prueba de Jenkins (Entrega 2): el modelo que el streaming tiene CARGADO es el versionado.

Comprueba:
  1. /models/sentimiento_latest y /models/riesgo_latest tienen lifemap_version.json con la versión de
     config.env (SENT_VERSION, RIESGO_VERSION), entrenados con la misma FECHA_T, y su huella SHA-256
     coincide con los archivos (nadie los cambió después de versionarlos);
  2. el streaming registró en MongoDB (meta.streaming) que cargó esa misma versión y esa misma huella.
Si algo no coincide termina con código 1 y Jenkins NO despliega.
Se ejecuta dentro del contenedor streaming:  python3 /opt/jobs/verificar_modelo.py
"""
import os
import sys
import time

from common import config, versiones

SENT = os.path.join(config.MODELS_DIR, "sentimiento_latest")
RIESGO = os.path.join(config.MODELS_DIR, "riesgo_latest")


def main():
    problemas = versiones.verificar(SENT, config.SENT_VERSION, config.FECHA_T)
    problemas += versiones.verificar(RIESGO, config.RIESGO_VERSION, config.FECHA_T)

    from pymongo import MongoClient
    meta = None
    for _ in range(int(os.environ.get("ESPERA_SEG", "120")) // 5):     # el streaming registra al cargar
        meta = MongoClient(config.MONGO_URI)[config.MONGO_DB]["meta"].find_one({"_id": "streaming"})
        if meta and meta.get("modelo_huella"):
            break
        time.sleep(5)
    if not meta or not meta.get("modelo_huella"):
        problemas.append("el streaming no registró qué modelo cargó (meta.streaming vacío)")
    else:
        if str(meta.get("modelo_version")) != str(config.SENT_VERSION):
            problemas.append(f"streaming cargó sentimiento v{meta.get('modelo_version')}, se esperaba v{config.SENT_VERSION}")
        if meta.get("modelo_huella") != versiones.huella(SENT):
            problemas.append("la huella del modelo cargado por el streaming no coincide con el versionado")
        if str(meta.get("riesgo_version")) != str(config.RIESGO_VERSION):
            problemas.append(f"streaming cargó riesgo v{meta.get('riesgo_version')}, se esperaba v{config.RIESGO_VERSION}")

    if problemas:
        print("[modelo] FALLA:\n  - " + "\n  - ".join(problemas))
        sys.exit(1)
    print(f"[modelo] OK: sentimiento v{config.SENT_VERSION} y riesgo v{config.RIESGO_VERSION} (T={config.FECHA_T}) "
          f"son los versionados y son los que cargó el streaming")


if __name__ == "__main__":
    main()
