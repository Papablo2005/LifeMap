# archivo: common/competencia.py
"""Densidad de competidores: negocios de la MISMA categoría principal a menos de `radio_m` metros,
contados con $geoNear sobre businesses.location (índice 2dsphere).

Para no mirar el futuro, un competidor "existe en la fecha ref" si su primera reseña es anterior a ref
y tuvo alguna reseña en los 6 meses previos a ref (no se usa nada posterior a ref).
"""
from datetime import timedelta


def contar_competidores(db, negocio, ref, radio_m=500.0):
    """`negocio`: documento de businesses con location y categoria_principal. Devuelve un entero."""
    if not negocio or "location" not in negocio:
        return 0
    filtro = {"_id": {"$ne": negocio["_id"]},
              "categoria_principal": negocio.get("categoria_principal"),
              "vida.primera_resena": {"$lte": ref},
              "vida.ultima_resena": {"$gte": ref - timedelta(days=182)}}
    pipeline = [{"$geoNear": {"near": negocio["location"], "distanceField": "d", "maxDistance": radio_m,
                              "spherical": True, "key": "location", "query": filtro}},
                {"$count": "n"}]
    r = list(db["businesses"].aggregate(pipeline))
    return int(r[0]["n"]) if r else 0
