# archivo: common/hotspot.py
"""Regla de hotspot (Entrega 2), igual en el job histórico y en el streaming.

Una celda de geohash es hotspot en una ventana de 30 días cuando su porcentaje de reseñas negativas
SUBE frente a la ventana anterior al menos `umbral` (p. ej. 0.15 = 15 puntos porcentuales) y las dos
ventanas tienen al menos `minimo` reseñas (con 3 reseñas, pasar de 0 % a 33 % es ruido, no una señal).
El umbral se justifica con la distribución histórica de cambios (spark/jobs/hotspots_historico.py).
"""
from datetime import datetime, timedelta, timezone

EPOCA = datetime(1970, 1, 1)


def inicio_ventana(fecha, dias=30):
    """Inicio de la ventana fija (tumbling) de `dias` que contiene `fecha`, alineada como en Spark
    (desde 1970-01-01 UTC). Así el batch y el streaming cortan las ventanas en los mismos días."""
    n = (fecha - EPOCA).days // dias
    return EPOCA + timedelta(days=n * dias)


def es_hotspot(n, pct, n_prev, pct_prev, umbral, minimo):
    """Devuelve (hotspot: bool, delta: float o None)."""
    if n_prev is None or pct_prev is None:
        return False, None
    delta = pct - pct_prev
    return (n >= minimo and n_prev >= minimo and delta >= umbral), delta


def sin_zona(dt):
    """Spark entrega datetimes sin zona (en UTC por configuración); MongoDB también los guarda en UTC."""
    if dt is not None and dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def documento_ventana(geohash, inicio, fin, n, negativas, fuente):
    """Documento de la colección ventanas_geohash: conteos + geometría consultable de la celda."""
    from common.geohash import poligono_geojson, punto_geojson
    return {"_id": f"{geohash}|{inicio:%Y-%m-%d}", "geohash": geohash, "inicio": inicio, "fin": fin,
            "n": int(n), "negativas": int(negativas), "pct_negativo": (negativas / n) if n else 0.0,
            "celda": poligono_geojson(geohash), "centro": punto_geojson(geohash), "fuente": fuente}


def evaluar_hotspot(doc, anterior, umbral, minimo):
    """Completa `doc` con la comparación frente a la ventana anterior (documento o None)."""
    hot, delta = es_hotspot(doc["n"], doc["pct_negativo"],
                            anterior["n"] if anterior else None,
                            anterior["pct_negativo"] if anterior else None, umbral, minimo)
    doc.update({"hotspot": hot, "delta": delta,
                "pct_negativo_anterior": anterior["pct_negativo"] if anterior else None,
                "n_anterior": anterior["n"] if anterior else None, "umbral": umbral})
    return doc
