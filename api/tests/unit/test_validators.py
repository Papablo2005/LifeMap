# archivo: api/tests/unit/test_validators.py
import pytest

from validators import (ValidationError, parse_categoria, parse_limite, parse_min_dias, parse_por,
                        parse_poligono, parse_punto, parse_radio, parse_texto)

CUADRADO = [[[-74.0, 40.7], [-73.9, 40.7], [-73.9, 40.8], [-74.0, 40.8], [-74.0, 40.7]]]


def test_punto_valido_orden_lon_lat():
    assert parse_punto({"lat": "40.7", "lon": "-74.0"})["coordinates"] == [-74.0, 40.7]


@pytest.mark.parametrize("args", [
    {"lat": "91", "lon": "0"},
    {"lat": "0", "lon": "-181"},
    {"lat": "abc", "lon": "0"},
    {"lon": "0"},
])
def test_punto_invalido(args):
    with pytest.raises(ValidationError):
        parse_punto(args)


def test_radio_y_limite():
    assert parse_radio({"radio": "500"}) == 500
    assert parse_limite({}) == 100
    with pytest.raises(ValidationError):
        parse_radio({"radio": "0"})
    with pytest.raises(ValidationError):
        parse_limite({"limite": "5000"})


def test_poligono_valido_y_feature():
    assert parse_poligono({"type": "Polygon", "coordinates": CUADRADO})["type"] == "Polygon"
    feature = {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": CUADRADO}}
    assert parse_poligono(feature)["coordinates"] == CUADRADO


@pytest.mark.parametrize("cuerpo", [
    None,
    {"type": "Point", "coordinates": [0, 0]},
    {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1]]]},          # no cerrado
    {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [0, 0]]]},                  # muy corto
    {"type": "Polygon", "coordinates": [[[0, 0], [200, 0], [1, 1], [0, 0]]]},        # fuera de rango
])
def test_poligono_invalido(cuerpo):
    with pytest.raises(ValidationError):
        parse_poligono(cuerpo)


# ------------------------------------------------------------------ LifeMap
def test_categoria_opcional_y_validada():
    assert parse_categoria({}) is None
    assert parse_categoria({"categoria": "  Bars "}) == "Bars"
    with pytest.raises(ValidationError):
        parse_categoria({"categoria": "x" * 61})
    with pytest.raises(ValidationError):
        parse_categoria({"categoria": "mala\x00"})


def test_min_dias_y_por():
    assert parse_min_dias({}, 3650) == 3650
    assert parse_min_dias({"min_dias": "100"}, 3650) == 100
    for malo in ("-1", "abc", "99999999999"):
        with pytest.raises(ValidationError):
            parse_min_dias({"min_dias": malo}, 3650)
    assert parse_por({}) == "categoria" and parse_por({"por": "geohash"}) == "geohash"
    with pytest.raises(ValidationError):
        parse_por({"por": "ciudad"})


def test_texto_predict():
    assert parse_texto({"text": "  great bar  "}) == "great bar"
    for malo in (None, [], {}, {"text": 5}, {"text": "   "}, {"text": "x" * 5001}):
        with pytest.raises(ValidationError):
            parse_texto(malo)
