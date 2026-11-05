# archivo: api/tests/unit/test_endpoints.py
"""Pruebas de la API sin Mongo real (mongomock). Los endpoints geoespaciales ($near, $geoWithin,
$geoNear) los prueban las pruebas de humo (tests/smoke.py) contra un Mongo de verdad en Jenkins."""
import mongomock
import pytest

import app as modulo
import health
import predictor


@pytest.fixture
def cliente(monkeypatch):
    base = mongomock.MongoClient()["lifemap_test"]
    monkeypatch.setattr(modulo, "db", lambda: base)
    modulo.app.config["TESTING"] = True
    return modulo.app.test_client(), base


# ---------------------------------------------------------- validación (400, nunca 500)
@pytest.mark.parametrize("url", [
    "/businesses/near",
    "/businesses/near?lat=200&lon=0&radio=100",
    "/businesses/near?lat=38&lon=-90&radio=0",
    "/businesses/near?lat=38&lon=-90&radio=100&categoria=" + "x" * 80,
    "/zones/summary?lat=38&lon=-90",
    "/stats/closure?por=ciudad",
    "/stats/closure?min_dias=-5",
])
def test_parametros_invalidos_dan_400(cliente, url):
    c, _ = cliente
    r = c.get(url)
    assert r.status_code == 400 and "error" in r.get_json()


def test_within_poligono_mal_formado_da_400(cliente):
    c, _ = cliente
    assert c.post("/businesses/within", json={"type": "Polygon", "coordinates": []}).status_code == 400
    assert c.post("/businesses/within", data="no es json").status_code == 400


def test_predict_validacion_y_modelo_ausente(cliente, monkeypatch):
    c, _ = cliente
    assert c.post("/predict", json={}).status_code == 400
    assert c.post("/predict", json={"text": "   "}).status_code == 400
    monkeypatch.setattr(predictor, "MODEL_PATH", "/ruta/que/no/existe")
    r = c.post("/predict", json={"text": "great place"})
    assert r.status_code == 503 and "Modelo" in r.get_json()["error"]


# ------------------------------------------------------------------ /stats/closure
def test_stats_closure_aplica_minimo_de_dias_negocio(cliente):
    c, base = cliente
    base["closure_by_category"].insert_many([
        {"_id": "Bars", "categoria": "Bars", "dias_negocio": 50_000, "cierres": 10, "tasa_cierre_pct": 7.3},
        {"_id": "Pubs", "categoria": "Pubs", "dias_negocio": 20_000, "cierres": 9, "tasa_cierre_pct": 16.4},
        {"_id": "Karaoke", "categoria": "Karaoke", "dias_negocio": 900, "cierres": 1, "tasa_cierre_pct": 40.5},
    ])
    j = c.get("/stats/closure?por=categoria&min_dias=3650").get_json()
    assert [g["categoria"] for g in j["resultados"]] == ["Pubs", "Bars"]       # ordenadas por tasa desc.
    assert j["grupos_excluidos_por_muestra"] == 1 and j["min_dias"] == 3650     # Karaoke: muy poca exposición
    assert c.get("/stats/closure?min_dias=0").get_json()["total"] == 3          # el mínimo es un parámetro


def test_stats_closure_geohash_no_devuelve_el_poligono(cliente):
    c, base = cliente
    base["closure_by_geohash"].insert_one({"_id": "9yzgu", "geohash": "9yzgu", "dias_negocio": 9000,
                                           "tasa_cierre_pct": 5.0, "celda": {"type": "Polygon"}})
    g = c.get("/stats/closure?por=geohash&min_dias=0").get_json()["resultados"][0]
    assert g["geohash"] == "9yzgu" and "celda" not in g


# ----------------------------------------------------------------------- /health
def test_health_informa_servicios_y_commit(cliente, monkeypatch):
    c, _ = cliente

    def sin_red(*a, **k):
        raise ConnectionError("sin red")
    monkeypatch.setattr(health, "_http", sin_red)
    monkeypatch.setenv("GIT_COMMIT", "abc123")

    monkeypatch.setattr(health, "REQUERIDOS", ["mongo"])
    r = c.get("/health")
    j = r.get_json()
    assert r.status_code == 200 and j["commit"] == "abc123"
    assert set(j["servicios"]) == {"mongo", "spark", "dask", "api"}
    assert j["servicios"]["mongo"]["estado"] == "ok" and j["servicios"]["spark"]["estado"] == "error"

    monkeypatch.setattr(health, "REQUERIDOS", ["mongo", "spark"])      # spark requerido y caído -> 503
    r = c.get("/health")
    assert r.status_code == 503 and r.get_json()["estado"] == "degradado"


def test_health_con_spark_y_dask_vivos(cliente, monkeypatch):
    c, _ = cliente
    monkeypatch.setattr(health, "REQUERIDOS", ["mongo", "spark", "dask"])
    monkeypatch.setattr(health, "_http", lambda url, timeout=2.5: (
        (200, '{"status": "ALIVE", "aliveworkers": 1}') if "json" in url else (200, "ok")))
    r = c.get("/health")
    assert r.status_code == 200 and r.get_json()["servicios"]["spark"]["workers"] == 1


def test_health_no_se_cuelga_si_un_servicio_tarda(cliente, monkeypatch):
    """Un servicio opcional lento (p. ej. DNS de spark-master en CI) no debe bloquear /health."""
    import time
    c, _ = cliente
    monkeypatch.setattr(health, "REQUERIDOS", ["mongo"])
    monkeypatch.setattr(health, "LIMITE_SEG", 0.5)

    def lento(url, timeout=2.5):
        time.sleep(3)
        raise OSError("lento")
    monkeypatch.setattr(health, "_http", lento)
    t0 = time.perf_counter()
    r = c.get("/health")
    assert time.perf_counter() - t0 < 2
    assert r.status_code == 200
    assert r.get_json()["servicios"]["spark"]["estado"] == "error"