# archivo: common/riesgo.py
"""Variables y etiqueta del modelo de riesgo de cierre (Entrega 2).

La MISMA función calcula las variables en batch (con datos hasta T, para entrenar) y en streaming
(cada micro-lote, para puntuar): así no hay diferencias entre entrenamiento y producción.

El "estado" de un negocio es un diccionario pequeño que se guarda en MongoDB (colección estado_negocio)
con totales y conteos por mes. Se actualiza sumando reseñas nuevas; nunca hace falta releer todo.
Compatible con Python 3.8 (la imagen de Spark usa 3.8).
"""
import math
from datetime import datetime, timedelta

VARIABLES = ["sent_medio", "tendencia_neg", "estrellas", "log_resenas", "edad_anios",
             "competidores_500m", "resenas_6m"]
DIAS_TENDENCIA = 182          # "últimos 6 meses"


def mes_de(fecha):
    """'YYYY-MM' de una fecha."""
    return fecha.strftime("%Y-%m")


def estado_vacio(business_id, competidores=0):
    return {"_id": business_id, "n": 0, "neg": 0, "suma_sent": 0.0, "suma_estrellas": 0.0,
            "primera": None, "ultima": None, "meses": {}, "competidores_500m": int(competidores)}


def agregar(estado, mes, n, neg, suma_sent, suma_estrellas, primera, ultima):
    """Suma al estado las reseñas de un negocio en un mes (ya agregadas: n, negativas, sumas, fechas)."""
    if n <= 0:
        return estado
    estado["n"] += int(n)
    estado["neg"] += int(neg)
    estado["suma_sent"] += float(suma_sent)
    estado["suma_estrellas"] += float(suma_estrellas)
    if estado["primera"] is None or primera < estado["primera"]:
        estado["primera"] = primera
    if estado["ultima"] is None or ultima > estado["ultima"]:
        estado["ultima"] = ultima
    m = estado["meses"].setdefault(mes, [0, 0])
    m[0] += int(n)
    m[1] += int(neg)
    return estado


def calcular_variables(estado, ref):
    """Variables del negocio vistas en la fecha `ref` (T en entrenamiento; "ahora" en streaming).

    sent_medio      media de (P(positivo) - P(negativo)) del modelo de sentimiento, en [-1, 1]
    tendencia_neg   % negativo de los últimos 6 meses menos el % negativo anterior (sube = empeora)
    estrellas       media de estrellas de SUS reseñas hasta ref (no las estrellas de business.json,
                    que son de 2022 y mirarían el futuro)
    log_resenas     log(1 + número de reseñas)
    edad_anios      años desde la primera reseña
    competidores_500m   negocios de la misma categoría a 500 m (calculado con $geoNear)
    resenas_6m      reseñas en los últimos 6 meses (actividad reciente)
    """
    n = estado["n"]
    corte = mes_de(ref - timedelta(days=DIAS_TENDENCIA))
    n6 = neg6 = 0
    for mes, (nm, negm) in estado["meses"].items():
        if corte <= mes <= mes_de(ref):
            n6 += nm
            neg6 += negm
    n_antes, neg_antes = n - n6, estado["neg"] - neg6
    tendencia = (neg6 / n6 - neg_antes / n_antes) if (n6 > 0 and n_antes > 0) else 0.0
    edad = ((ref - estado["primera"]).days / 365.25) if estado["primera"] else 0.0
    return {
        "sent_medio": estado["suma_sent"] / n if n else 0.0,
        "tendencia_neg": float(tendencia),
        "estrellas": estado["suma_estrellas"] / n if n else 0.0,
        "log_resenas": math.log1p(n),
        "edad_anios": max(0.0, float(edad)),
        "competidores_500m": float(estado.get("competidores_500m", 0)),
        "resenas_6m": float(n6),
    }


def sumar_meses(fecha, meses):
    """fecha + `meses` meses (día ajustado al último día válido del mes)."""
    m = fecha.month - 1 + meses
    anio, mes = fecha.year + m // 12, m % 12 + 1
    for dia in (fecha.day, 30, 29, 28):
        try:
            return fecha.replace(year=anio, month=mes, day=dia)
        except ValueError:
            continue


def etiqueta(cerrado, ultima_resena, t, meses=12):
    """Etiqueta del enunciado. Devuelve 1, 0 o None (= se excluye).

    1    cerrado y su última reseña cae entre T y T + 12 meses
    0    sigue con actividad después de ese plazo, o sigue abierto (con actividad después de T)
    None sin actividad después de T (se excluye)
    """
    if ultima_resena is None or ultima_resena <= t:
        return None
    if cerrado and ultima_resena <= sumar_meses(t, meses):
        return 1
    return 0


def a_datetime(x):
    """Acepta datetime, date o texto ISO (lo que llega de Mongo, de Spark o de Kafka)."""
    if x is None or isinstance(x, datetime):
        return x
    if hasattr(x, "year") and hasattr(x, "month"):
        return datetime(x.year, x.month, x.day)
    return datetime.fromisoformat(str(x).replace("Z", "").split("+")[0])
