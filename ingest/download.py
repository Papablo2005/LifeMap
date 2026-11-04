# archivo: ingest/download.py
"""Paso 1 del pipeline de datos: obtiene el Yelp Open Dataset con la API de Kaggle.

Las credenciales llegan como variables de entorno desde Jenkins (o desde .env
cuando se corre a mano). Nunca se escriben en el repositorio.
Es idempotente: si el dataset ya está descargado, no lo vuelve a bajar.
"""
import os
import subprocess
import sys
from pathlib import Path

RAW_DIR = Path(os.environ.get("RAW_DIR", "/data/raw"))
DATASET = os.environ.get("KAGGLE_DATASET", "yelp-dataset/yelp-dataset")


def hay_credenciales():
    if os.environ.get("KAGGLE_API_TOKEN"):
        return True
    return bool(os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"))


def main():
    if not DATASET or DATASET.startswith("dueno/"):
        sys.exit("[download] Configura KAGGLE_DATASET en config.env")

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    marca = RAW_DIR / ".descargado"
    if marca.exists() and marca.read_text() == DATASET and os.environ.get("FORCE_DOWNLOAD") != "1":
        print(f"[download] {DATASET} ya está en {RAW_DIR}; se omite la descarga.")
        return

    if not hay_credenciales():
        sys.exit("[download] Faltan credenciales: KAGGLE_API_TOKEN o KAGGLE_USERNAME + KAGGLE_KEY")

    print(f"[download] Descargando {DATASET} con la API de Kaggle en {RAW_DIR} ...")
    subprocess.run(
        ["kaggle", "datasets", "download", DATASET, "-p", str(RAW_DIR), "--unzip"],
        check=True,   # si falla la descarga, el script termina con error
    )
    marca.write_text(DATASET)
    print("[download] Listo. Archivos:")
    for f in sorted(RAW_DIR.rglob("*")):
        if f.is_file() and f.name != ".descargado":
            print(f"  {f}  ({f.stat().st_size / 2**20:.1f} MB)")


if __name__ == "__main__":
    main()