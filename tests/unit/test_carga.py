# archivo: tests/unit/test_carga.py
"""Prueba la construcción de documentos y la carga por lotes (con mongomock, sin Mongo real)."""
import mongomock
import pandas as pd
import pytest

from carga_mongo import cargar_resenas_particion, docs_negocios, docs_resenas, insertar_lotes


def negocio():
    return pd.DataFrame([dict(
        business_id="b1", name="Bar Uno", city="Saint Louis", state="MO", latitude=38.63, longitude=-90.2,
        stars=4.5, review_count=12, is_open=0, categories=["Bars", "Nightlife"], categoria_principal="Bars",
        geohash="9yzuh", primera_resena=pd.Timestamp("2018-01-01"), ultima_resena=pd.Timestamp("2018-12-31"),
        n_resenas=12, dias_negocio=365, evento=1, censurado=False)])


def test_documento_negocio_geojson_lon_lat_y_vida():
    d = docs_negocios(negocio())[0]
    assert d["location"] == {"type": "Point", "coordinates": [-90.2, 38.63]}      # [lon, lat]
    assert d["vida"]["dias_negocio"] == 365 and d["vida"]["evento"] == 1 and d["vida"]["censurado"] is False
    assert d["_id"] == "b1" and d["categories"] == ["Bars", "Nightlife"]


def test_carga_por_lotes_inserta_todo():
    col = mongomock.MongoClient().db.reviews
    docs = [{"_id": str(i)} for i in range(23)]
    assert insertar_lotes(col, docs, lote=10) == 23 and col.count_documents({}) == 23


def test_resenas_referencian_su_negocio():
    df = pd.DataFrame([dict(review_id="r1", business_id="b1", stars=5.0, label=2, text="great",
                            date=pd.Timestamp("2020-01-01"), geohash="9yzuh", categoria_principal="Bars")])
    d = docs_resenas(df)[0]
    assert d["business_id"] == "b1" and d["_id"] == "r1" and d["label"] == 2


def test_cargar_particion_worker(monkeypatch):
    cli = mongomock.MongoClient()
    df = pd.DataFrame([dict(review_id=f"r{i}", business_id="b1", stars=4.0, label=2, text="x",
                            date=pd.Timestamp("2020-01-01"), geohash="g", categoria_principal="Bars")
                       for i in range(7)])
    out = cargar_resenas_particion(df, "mongodb://x", "lifemap", 3, fabrica_cliente=lambda uri: cli)
    assert int(out["insertados"].iloc[0]) == 7
    assert cli["lifemap"]["reviews"].count_documents({"business_id": "b1"}) == 7
    assert int(cargar_resenas_particion(df.iloc[0:0], "x", "y", 3)["insertados"].iloc[0]) == 0
