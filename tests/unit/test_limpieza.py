# archivo: tests/unit/test_limpieza.py
"""Pruebas de las reglas de limpieza y de la construcción de la vida (pandas puro)."""
import pandas as pd
import pytest

from common.vida import dias_negocio, evento_y_censura
from limpieza import (REGLAS_NEGOCIO, REGLAS_RESENA, construir_vida, limpiar_negocios_particion,
                      limpiar_resenas_particion, quitar_duplicados, reporte_reglas)

BBOX = (-90.9, 38.3, -89.8, 39.1)


def negocios_base():
    fila = dict(name="x", city="St Louis", state="MO", latitude=38.63, longitude=-90.2, stars=4.0,
                review_count=10, is_open=1, categories="Bars, Nightlife")
    filas = [
        dict(fila, business_id="ok"),
        dict(fila, business_id="sin_lat", latitude=None),
        dict(fila, business_id="rango", longitude=500),
        dict(fila, business_id="cero", latitude=0, longitude=0),
        dict(fila, business_id="is_open_nulo", is_open=None),
        dict(fila, business_id="otro_estado", state="AZ"),
        dict(fila, business_id="fuera_caja", latitude=40.0),
        dict(fila, business_id="no_nightlife", categories="Restaurants, Food"),
        dict(fila, business_id="cats_nulas", categories=None),
    ]
    return pd.DataFrame(filas)


def test_limpieza_negocios_cada_registro_cae_en_una_regla():
    limpio, leidos, d = limpiar_negocios_particion(negocios_base(), ["MO", "IL"], BBOX, "Nightlife", 5)
    assert leidos == 9
    assert list(limpio.business_id) == ["ok"]
    assert d[REGLAS_NEGOCIO[0]] == 1 and d[REGLAS_NEGOCIO[1]] == 1 and d[REGLAS_NEGOCIO[2]] == 1
    assert d[REGLAS_NEGOCIO[3]] == 1
    assert d[REGLAS_NEGOCIO[4]] == 2          # otro estado + fuera de la caja
    assert d[REGLAS_NEGOCIO[5]] == 2          # sin Nightlife + categorías nulas
    assert leidos - sum(d.values()) == len(limpio)     # nada se cuenta dos veces


def test_negocio_limpio_trae_campos_derivados():
    limpio, *_ = limpiar_negocios_particion(negocios_base(), ["MO"], BBOX, "Nightlife", 5)
    r = limpio.iloc[0]
    assert r.city == "Saint Louis" and r.categoria_principal == "Bars"
    assert len(r.geohash) == 5 and r.categories == ["Bars", "Nightlife"] and r.is_open == 1


def resenas_base():
    f = dict(business_id="ok", stars=5.0, text="great place", date="2020-01-02 10:00:00")
    return pd.DataFrame([
        dict(f, review_id="r1"),
        dict(f, review_id="r2", business_id="ajeno"),
        dict(f, review_id="r3", text="   "),
        dict(f, review_id="r4", text=None),
        dict(f, review_id="r5", stars=9.0),
        dict(f, review_id="r6", date="basura"),
        dict(f, review_id="r7", stars=1.0, date="2021-06-01 00:00:00"),
    ])


def test_limpieza_resenas():
    limpias, leidos, d, fecha_max = limpiar_resenas_particion(resenas_base(), {"ok"})
    assert leidos == 7 and list(limpias.review_id) == ["r1", "r7"]
    assert d[REGLAS_RESENA[0]] == 1 and d[REGLAS_RESENA[1]] == 2
    assert d[REGLAS_RESENA[2]] == 1 and d[REGLAS_RESENA[3]] == 1
    assert list(limpias.label) == [2, 0]                    # 5 estrellas -> positivo, 1 -> negativo
    assert fecha_max.startswith("2021-06-01")               # la fecha de corte mira TODAS las filas


def test_quitar_duplicados():
    df = pd.DataFrame({"review_id": ["a", "a", "b"], "x": [1, 2, 3]})
    sin, n = quitar_duplicados(df, "review_id")
    assert n == 1 and list(sin.x) == [1, 3]


def test_construir_vida_coincide_con_funciones_comunes():
    negocios = pd.DataFrame({"business_id": ["a", "b"], "is_open": [0, 1]})
    agg = pd.DataFrame({
        "business_id": ["a", "b"],
        "primera_resena": pd.to_datetime(["2020-01-01 23:00", "2019-01-01 00:00"]),
        "ultima_resena": pd.to_datetime(["2020-01-01 01:00", "2019-12-31 00:00"]),
        "n_resenas": [2, 50]})
    v = construir_vida(negocios, agg).set_index("business_id")
    assert v.loc["a", "dias_negocio"] == 1 == dias_negocio(agg.primera_resena[0], agg.ultima_resena[0])
    assert v.loc["b", "dias_negocio"] == 365
    assert (v.loc["a", "evento"], v.loc["a", "censurado"]) == (1, False) == evento_y_censura(0)
    assert (v.loc["b", "evento"], v.loc["b", "censurado"]) == (0, True) == evento_y_censura(1)


def test_reporte_reglas_suma_exacta():
    filas = reporte_reglas(100, ["a", "b"], {"a": 10, "b": 5})
    assert filas[0]["antes"] == 100 and filas[0]["despues"] == 90
    assert filas[1]["antes"] == 90 and filas[1]["despues"] == 85
