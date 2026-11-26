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

# ---------------------------------------------------------------- Entrega 2
FECHA_T = os.environ.get("FECHA_T", "2020-01-01")                 # fecha de corte (YYYY-MM-DD)
SENT_VERSION = os.environ.get("SENT_VERSION", "2")
RIESGO_VERSION = os.environ.get("RIESGO_VERSION", "1")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "reviews")
VELOCIDAD_DIAS_POR_SEG = float(os.environ.get("VELOCIDAD_DIAS_POR_SEG", "3"))
VENTANA_DIAS = int(os.environ.get("VENTANA_DIAS", "30"))
WATERMARK_DIAS = int(os.environ.get("WATERMARK_DIAS", "7"))
UMBRAL_HOTSPOT = float(os.environ.get("UMBRAL_HOTSPOT", "0.15"))
MIN_RESENAS_VENTANA = int(os.environ.get("MIN_RESENAS_VENTANA", "10"))
INICIO_OBSERVACION = os.environ.get("INICIO_OBSERVACION", "2010-01-01")
MIN_NEGOCIOS_KM = int(os.environ.get("MIN_NEGOCIOS_KM", "30"))
MIN_EVENTOS_KM = int(os.environ.get("MIN_EVENTOS_KM", "10"))
RECALL_OBJETIVO = float(os.environ.get("RECALL_OBJETIVO", "0.70"))
RADIO_COMPETENCIA_M = float(os.environ.get("RADIO_COMPETENCIA_M", "500"))

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "lifemap")


def bbox():
    """(min_lon, min_lat, max_lon, max_lat) o None."""
    if not AREA_BBOX:
        return None
    mnlon, mnlat, mxlon, mxlat = map(float, AREA_BBOX.split(","))
    return mnlon, mnlat, mxlon, mxlat


def fecha_t():
    """FECHA_T como datetime (00:00). Todo lo que sea <= T es "pasado" (se puede usar para entrenar)."""
    from datetime import datetime
    return datetime.strptime(FECHA_T, "%Y-%m-%d")
