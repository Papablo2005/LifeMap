# archivo: common/config.py
"""Configuración leída del entorno (la define config.env y docker-compose.yml)."""
import os


def _lista(valor):
    return [x.strip() for x in valor.split(",") if x.strip()]


RAW_DIR = os.environ.get("RAW_DIR", "/data/raw")
CLEAN_DIR = os.environ.get("CLEAN_DIR", "/data/clean")
REPORT_DIR = os.environ.get("REPORT_DIR", "/data/reportes")
MODELS_DIR = os.environ.get("MODELS_DIR", "/models")

# Área y familia de negocios (reportadas al docente)
AREA_STATES = _lista(os.environ.get("AREA_STATES", "MO,IL"))
# caja geográfica minLon,minLat,maxLon,maxLat (vacía = sin caja)
AREA_BBOX = os.environ.get("AREA_BBOX", "-90.90,38.30,-89.80,39.10").strip()
FAMILIA = os.environ.get("FAMILIA", "Nightlife")

GEOHASH_PRECISION = int(os.environ.get("GEOHASH_PRECISION", "5"))
MIN_DIAS_NEGOCIO = int(os.environ.get("MIN_DIAS_NEGOCIO", "3650"))

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")


def bbox():
    """(min_lon, min_lat, max_lon, max_lat) o None."""
    if not AREA_BBOX:
        return None
    mnlon, mnlat, mxlon, mxlat = map(float, AREA_BBOX.split(","))
    return mnlon, mnlat, mxlon, mxlat
