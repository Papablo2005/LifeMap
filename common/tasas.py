# archivo: common/tasas.py
"""Tasa de cierre anualizada. UNA sola definición, usada por el MapReduce,
la API y las pruebas de Jenkins (si alguien cambia la fórmula, pytest falla).

    tasa de cierre (%) = cierres / (días-negocio / 365) * 100

Es la misma fórmula de la AFR del ejercicio de discos, con días-negocio en
lugar de días-disco. NO es la proporción simple de negocios cerrados.
"""


def tasa_cierre_pct(cierres, dias_negocio):
    """Devuelve la tasa anualizada en %, o None si no hay exposición (días <= 0)."""
    if dias_negocio is None or dias_negocio <= 0:
        return None
    return cierres / (dias_negocio / 365.0) * 100.0


def muestra_suficiente(dias_negocio, minimo_dias):
    """True si el grupo tiene al menos `minimo_dias` días-negocio de exposición."""
    return dias_negocio is not None and dias_negocio >= minimo_dias
