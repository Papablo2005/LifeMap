# archivo: ingest/limpieza.py
"""Reglas de limpieza (funciones puras de pandas, probadas con pytest).

Cada registro descartado cuenta en UNA sola regla (se evalúan en orden), así
la tabla del informe suma exactamente: leídos - descartados = conservados.
"""
import pandas as pd

from common import geohash
from common.categorias import categoria_principal, es_familia, lista_categorias
from common.etiquetas import etiqueta_sentimiento

COLS_NEGOCIO = ["business_id", "name", "city", "state", "latitude", "longitude",
                "stars", "review_count", "is_open", "categories"]
COLS_RESENA = ["review_id", "business_id", "stars", "text", "date"]
FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"      # formato de review.json: 2018-07-07 22:09:11

# Orden y nombre de las reglas (los nombres salen tal cual en el informe)
REGLAS_NEGOCIO = [
    "coordenadas nulas o no numéricas",
    "coordenadas fuera de rango",
    "punto (0, 0)",
    "is_open inválido (no es 0 ni 1)",
    "fuera del área (estado o caja geográfica)",
    "fuera de la familia de categorías",
    "business_id duplicado",
    "sin reseñas tras la limpieza",
]
REGLAS_RESENA = [
    "de negocios fuera del conjunto (unión reseña-negocio)",
    "sin texto",
    "estrellas inválidas",
    "fecha nula o inválida",
    "review_id duplicado",
]

_CIUDADES = {"st louis": "Saint Louis", "st. louis": "Saint Louis", "saint louis": "Saint Louis"}


def normalizar_ciudad(ciudad):
    if not isinstance(ciudad, str):
        return ciudad
    return _CIUDADES.get(ciudad.strip().lower(), ciudad.strip())


def _descartar(pdf, mascara, regla, descartes):
    """Quita las filas de `mascara` y las cuenta en `regla`."""
    descartes[regla] = descartes.get(regla, 0) + int(mascara.sum())
    return pdf[~mascara]


# ----------------------------------------------------------------- NEGOCIOS
def limpiar_negocios_particion(pdf, estados, bbox, familia, precision):
    """Limpia una partición de business.json. Devuelve (negocios_limpios, leidos, descartes)."""
    leidos = len(pdf)
    descartes = {}
    pdf = pdf[COLS_NEGOCIO].copy()
    pdf["latitude"] = pd.to_numeric(pdf["latitude"], errors="coerce")
    pdf["longitude"] = pd.to_numeric(pdf["longitude"], errors="coerce")

    pdf = _descartar(pdf, pdf.latitude.isna() | pdf.longitude.isna(), REGLAS_NEGOCIO[0], descartes)
    fuera = (pdf.latitude < -90) | (pdf.latitude > 90) | (pdf.longitude < -180) | (pdf.longitude > 180)
    pdf = _descartar(pdf, fuera, REGLAS_NEGOCIO[1], descartes)
    pdf = _descartar(pdf, (pdf.latitude == 0) & (pdf.longitude == 0), REGLAS_NEGOCIO[2], descartes)

    abierto = pd.to_numeric(pdf["is_open"], errors="coerce")
    pdf = _descartar(pdf, ~abierto.isin([0, 1]), REGLAS_NEGOCIO[3], descartes)
    pdf["is_open"] = abierto[pdf.index].astype(int)

    fuera_area = ~pdf["state"].isin(estados)
    if bbox:
        mnlon, mnlat, mxlon, mxlat = bbox
        fuera_area = fuera_area | ~(pdf.longitude.between(mnlon, mxlon) & pdf.latitude.between(mnlat, mxlat))
    pdf = _descartar(pdf, fuera_area, REGLAS_NEGOCIO[4], descartes)
    pdf = _descartar(pdf, ~pdf["categories"].apply(lambda c: es_familia(c, familia)),
                     REGLAS_NEGOCIO[5], descartes)

    pdf["city"] = pdf["city"].apply(normalizar_ciudad)
    pdf["categories"] = pdf["categories"].apply(lista_categorias)
    pdf["categoria_principal"] = pdf["categories"].apply(categoria_principal)
    pdf["geohash"] = [geohash.encode(la, lo, precision) for la, lo in zip(pdf.latitude, pdf.longitude)]
    return pdf.reset_index(drop=True), leidos, descartes


# ------------------------------------------------------------------ RESEÑAS
def limpiar_resenas_particion(pdf, ids_validos):
    """Limpia una partición de review.json.

    Devuelve (resenas_limpias, leidos, descartes, fecha_max_str).
    `fecha_max_str` es la fecha más reciente de TODA la partición (antes de
    descartar nada): sirve para la fecha de corte del dataset.
    """
    leidos = len(pdf)
    descartes = {}
    fechas_todas = pd.to_datetime(pdf["date"], format=FORMATO_FECHA, errors="coerce")
    fecha_max = fechas_todas.max()
    pdf = pdf[COLS_RESENA].copy()

    pdf = _descartar(pdf, ~pdf["business_id"].isin(ids_validos), REGLAS_RESENA[0], descartes)
    sin_texto = ~pdf["text"].apply(lambda t: isinstance(t, str) and t.strip() != "")
    pdf = _descartar(pdf, sin_texto, REGLAS_RESENA[1], descartes)

    pdf["stars"] = pd.to_numeric(pdf["stars"], errors="coerce")
    etiquetas = pdf["stars"].apply(etiqueta_sentimiento)
    pdf = _descartar(pdf, etiquetas.isna(), REGLAS_RESENA[2], descartes)
    pdf["label"] = etiquetas[pdf.index].astype(int)

    pdf["date"] = pd.to_datetime(pdf["date"], format=FORMATO_FECHA, errors="coerce")
    pdf = _descartar(pdf, pdf["date"].isna(), REGLAS_RESENA[3], descartes)
    pdf["text"] = pdf["text"].astype(str)
    return (pdf.reset_index(drop=True), leidos, descartes,
            None if pd.isna(fecha_max) else str(fecha_max))


def quitar_duplicados(pdf, columna):
    """Devuelve (sin_duplicados, cuántos_se_quitaron)."""
    n0 = len(pdf)
    pdf = pdf.drop_duplicates(subset=columna, keep="first")
    return pdf, n0 - len(pdf)


# --------------------------------------------------------------------- VIDA
def construir_vida(negocios, agregado):
    """Une los negocios con (primera, ultima, n_resenas) por negocio y calcula la vida.

    `agregado`: DataFrame con business_id, primera_resena, ultima_resena, n_resenas.
    Mismos criterios que common.vida: dias = (ultima - primera) en días + 1;
    evento = 1 si is_open == 0; censurado = (evento == 0).
    """
    v = negocios.merge(agregado, on="business_id", how="inner")
    v["dias_negocio"] = ((v["ultima_resena"].dt.normalize() - v["primera_resena"].dt.normalize())
                         .dt.days + 1).astype(int)
    v["evento"] = (v["is_open"] == 0).astype(int)
    v["censurado"] = v["evento"] == 0
    return v


def reporte_reglas(leidos, nombres_reglas, descartes):
    """Tabla antes/después por regla, en el mismo orden de evaluación."""
    filas, restante = [], leidos
    for r in nombres_reglas:
        d = int(descartes.get(r, 0))
        filas.append({"regla": r, "antes": restante, "descartados": d, "despues": restante - d,
                      "pct_descartado": round(d / restante * 100, 3) if restante else 0.0})
        restante -= d
    return filas
