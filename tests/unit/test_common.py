# archivo: tests/unit/test_common.py
"""Pruebas de las funciones puras compartidas (sin Mongo, sin Spark)."""
from datetime import date, datetime

import pytest

from common import geohash
from common.categorias import categoria_principal, es_familia, lista_categorias
from common.etiquetas import etiqueta_sentimiento
from common.tasas import muestra_suficiente, tasa_cierre_pct
from common.vida import dias_negocio, evento_y_censura


# ---------- FÓRMULA DE LA TASA DE CIERRE (la prueba que exige el enunciado) ----------
@pytest.mark.parametrize("cierres,dias,esperado", [
    (1, 365, 100.0),       # 1 cierre en 1 año-negocio  -> 100 %
    (2, 730, 100.0),       # 2 cierres en 2 años-negocio -> 100 %
    (0, 1000, 0.0),
    (1, 3650, 10.0),       # 1 cierre en 10 años-negocio -> 10 %
    (5, 36500, 5.0),
])
def test_formula_tasa_cierre_anualizada(cierres, dias, esperado):
    assert tasa_cierre_pct(cierres, dias) == pytest.approx(esperado)


def test_tasa_no_es_proporcion_simple():
    # 1 cerrado de 10 negocios NO es 10 %: depende de los días de exposición
    assert tasa_cierre_pct(1, 10 * 365) == pytest.approx(10.0)
    assert tasa_cierre_pct(1, 10 * 100) != pytest.approx(10.0)


def test_tasa_sin_exposicion():
    assert tasa_cierre_pct(0, 0) is None
    assert tasa_cierre_pct(1, None) is None


def test_muestra_suficiente():
    assert muestra_suficiente(3650, 3650)
    assert not muestra_suficiente(3649, 3650)
    assert not muestra_suficiente(None, 10)


# ---------- VIDA, EVENTO Y CENSURA ----------
def test_dias_negocio_incluye_ambos_extremos():
    assert dias_negocio(datetime(2020, 1, 1, 23, 0), datetime(2020, 1, 1, 1, 0)) == 1  # mismo día
    assert dias_negocio(date(2020, 1, 1), date(2020, 1, 31)) == 31
    assert dias_negocio(datetime(2019, 1, 1), datetime(2019, 12, 31)) == 365


def test_dias_negocio_orden_invalido():
    with pytest.raises(ValueError):
        dias_negocio(date(2020, 2, 1), date(2020, 1, 1))


def test_evento_y_censura():
    assert evento_y_censura(0) == (1, False)   # cerrado: evento observado
    assert evento_y_censura(1) == (0, True)    # abierto: censurado
    with pytest.raises(ValueError):
        evento_y_censura(None)


# ---------- ETIQUETAS ----------
@pytest.mark.parametrize("stars,esperado", [
    (1, 0), (2.0, 0), (3, 1), (3.0, 1), (4, 2), (5.0, 2), (0, None), (6, None), (None, None), ("x", None),
])
def test_etiqueta_sentimiento(stars, esperado):
    assert etiqueta_sentimiento(stars) == esperado


# ---------- CATEGORÍAS ----------
def test_lista_y_familia():
    assert lista_categorias("Bars, Nightlife,  Restaurants ") == ["Bars", "Nightlife", "Restaurants"]
    assert lista_categorias(None) == []
    assert es_familia("Restaurants, Nightlife")
    assert not es_familia("Restaurants, Food")
    assert not es_familia(None)


def test_categoria_principal_determinista():
    assert categoria_principal("Restaurants, Nightlife, Bars") == "Bars"
    assert categoria_principal("Nightlife, Sports Bars, Bars") == "Sports Bars"   # prioridad, no orden
    assert categoria_principal("Nightlife, Food Trucks") == "Otros Nightlife"
    assert categoria_principal(None) == "Otros Nightlife"


# ---------- GEOHASH ----------
def test_geohash_vectores_conocidos():
    assert geohash.encode(57.64911, 10.40744, 11) == "u4pruydqqvj"
    assert geohash.encode(42.6, -5.6, 5) == "ezs42"


def test_geohash_centro_dentro_de_su_celda():
    gh = geohash.encode(38.627, -90.199, 5)
    lat, lon = geohash.centro(gh)
    assert geohash.encode(lat, lon, 5) == gh
    lat_min, lat_max, lon_min, lon_max = geohash.bbox(gh)
    assert lat_min <= 38.627 <= lat_max and lon_min <= -90.199 <= lon_max


def test_geohash_geojson_orden_lon_lat():
    p = geohash.punto_geojson("9yzg")
    lon, lat = p["coordinates"]
    assert -180 <= lon <= 180 and -90 <= lat <= 90
    anillo = geohash.poligono_geojson("9yzg")["coordinates"][0]
    assert anillo[0] == anillo[-1] and len(anillo) == 5


def test_geohash_fuera_de_rango():
    with pytest.raises(ValueError):
        geohash.encode(95, 0)


# ---------- MÉTRICAS POR CLASE ----------
def test_metricas_desde_matriz():
    from common.metricas import metricas_desde_matriz
    # clase 0: 8 aciertos, 2 confundidos con 2 | clase 1: 0 aciertos | clase 2: 90 aciertos, 5 como 0
    m = metricas_desde_matriz([[8, 0, 2], [1, 0, 3], [5, 0, 90]])
    neg = m["por_clase"]["negativo"]
    assert neg["precision"] == pytest.approx(8 / 14) and neg["recall"] == pytest.approx(0.8)
    assert m["por_clase"]["neutral"]["f1"] == 0.0                  # clase ignorada por el modelo
    assert m["accuracy"] == pytest.approx(98 / 109)
    assert m["macro_f1"] < m["accuracy"]                           # el desbalance se nota en macro-F1
    assert m["por_clase"]["positivo"]["soporte"] == 95
