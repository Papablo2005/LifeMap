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
    assert set(j["servicios"]) == {"mongo", "spark", "dask", "kafka", "streaming", "api"}
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

# ================================================================== Entrega 2
@pytest.mark.parametrize("url", [
    "/live/hotspots?lat=38.6&lon=-90.2",                         # falta radio
    "/live/hotspots?lat=38.6&lon=-90.2&radio=1000&ventana=ayer",
    "/live/hotspots?lat=38.6&lon=-90.2&radio=1000&solo_hotspots=quizas",
    "/alerts?lat=38.6&lon=-90.2&radio=-1",
    "/alerts?lat=38.6",                                          # lat sin lon
    "/survival/Bars?tipo=ciudad",
    "/risk/near?lat=38.6&lon=-90.2",
    "/risk/near?lat=95&lon=-90.2&radio=500",
])
def test_entrega2_parametros_invalidos_dan_400(cliente, url):
    c, _ = cliente
    r = c.get(url)
    assert r.status_code == 400 and "error" in r.get_json()


def test_alerts_poligono_mal_formado_da_400(cliente):
    c, _ = cliente
    assert c.post("/alerts", json={"type": "Point", "coordinates": [0, 0]}).status_code == 400


def test_survival_de_una_categoria_y_404_con_disponibles(cliente):
    c, base = cliente
    base["survival"].insert_many([
        {"_id": "categoria:Bars", "tipo": "categoria", "grupo": "Bars", "nombre": "Bars", "negocios": 100,
         "cierres": 30, "suficiente": True, "mediana_anios": 7.5,
         "curva": [{"t_anios": 0.0, "supervivencia": 1.0, "en_riesgo": 100}]},
        {"_id": "categoria:Karaoke", "tipo": "categoria", "grupo": "Karaoke", "nombre": "Karaoke", "negocios": 5,
         "cierres": 2, "suficiente": False, "mediana_anios": None,
         "curva": [{"t_anios": 0.0, "supervivencia": 1.0, "en_riesgo": 5}]},
    ])
    j = c.get("/survival/Bars").get_json()
    assert j["mediana_anios"] == 7.5 and j["curva"][0]["en_riesgo"] == 100 and j["nota_conclusiones"] is None
    assert "Grupo pequeño" in c.get("/survival/Karaoke").get_json()["nota_conclusiones"]
    r = c.get("/survival/Pubs")
    assert r.status_code == 404 and r.get_json()["disponibles"] == ["Bars", "Karaoke"]
    plano = c.get("/survival").get_json()
    assert [g["grupo"] for g in plano["grupos"]] == ["Bars"] and plano["resultados"][0]["grupo"] == "Bars"
    assert len(c.get("/survival?incluir_pequenos=1").get_json()["grupos"]) == 2


def test_retraso_del_consumidor_y_estado_del_streaming():
    from datetime import datetime, timedelta
    kafka = {"_limites": {"0": (0, 100), "1": (0, 50), "2": (10, 10)}}
    meta = {"offsets": {"0": 90, "1": 50}, "latido": datetime.utcnow(), "estado": "activo"}
    retraso, por_p = health.retraso_consumidor(kafka, meta)
    assert retraso == 10 and por_p == {"0": 10, "1": 0, "2": 0}
    assert health.estado_streaming(meta)["estado"] == "ok"
    viejo = dict(meta, latido=datetime.utcnow() - timedelta(minutes=5))
    assert health.estado_streaming(viejo)["estado"] == "error"
    assert health.estado_streaming(None)["estado"] == "error"
