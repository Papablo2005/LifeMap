# archivo: common/etiquetas.py
"""Etiqueta de sentimiento derivada de las estrellas de la reseña.

    1 y 2 estrellas -> 0 (negativo)
    3 estrellas     -> 1 (neutral)
    4 y 5 estrellas -> 2 (positivo)
"""
NOMBRES = ["negativo", "neutral", "positivo"]


def etiqueta_sentimiento(stars):
    """Devuelve 0, 1, 2, o None si las estrellas no son un valor válido (1..5)."""
    try:
        s = int(round(float(stars)))
    except (TypeError, ValueError):
        return None
    if s in (1, 2):
        return 0
    if s == 3:
        return 1
    if s in (4, 5):
        return 2
    return None
