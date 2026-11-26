# archivo: tests/unit/test_entrega2.py
"""Pruebas de la lógica de la Entrega 2 (sin Kafka, Spark ni Mongo): variables y etiqueta del modelo de
riesgo, regla de hotspot, productor (solo reseñas > T, en orden, reloj acelerado), supervivencia
(censura y entrada tardía) y versionado de modelos."""
import math
import os
from datetime import datetime, timedelta

import pytest

from common import versiones
from common.hotspot import es_hotspot, evaluar_hotspot, documento_ventana, inicio_ventana
from common.riesgo import (VARIABLES, a_datetime, agregar, calcular_variables, estado_vacio, etiqueta,
                           sumar_meses)

T = datetime(2020, 1, 1)


# ------------------------------------------------------------------ riesgo: etiqueta
@pytest.mark.parametrize("cerrado, ultima, esperado", [
    (True, datetime(2020, 6, 1), 1),        # cerró dentro de los 12 meses siguientes a T
    (True, datetime(2021, 1, 1), 1),        # justo en T + 12 meses (incluido)
    (True, datetime(2021, 3, 1), 0),        # cerrado, pero con actividad después del plazo
    (False, datetime(2020, 2, 1), 0),       # sigue abierto y tuvo actividad después de T
    (True, datetime(2019, 5, 1), None),     # sin actividad después de T: se excluye
    (False, T, None),                       # última reseña exactamente en T: sin actividad posterior
])
def test_etiqueta_sigue_la_definicion(cerrado, ultima, esperado):
    assert etiqueta(cerrado, ultima, T) == esperado


def test_sumar_meses_ajusta_fin_de_mes():
    assert sumar_meses(datetime(2020, 1, 31), 1) == datetime(2020, 2, 29)
    assert sumar_meses(datetime(2019, 7, 1), 12) == datetime(2020, 7, 1)


# ------------------------------------------------------------------ riesgo: variables
def _estado():
    e = estado_vacio("b1", competidores=3)
    agregar(e, "2018-01", 10, 1, 6.0, 45.0, datetime(2018, 1, 3), datetime(2018, 1, 28))   # 10 % negativo
    agregar(e, "2019-10", 5, 3, -1.0, 12.0, datetime(2019, 10, 2), datetime(2019, 10, 20))  # 60 % negativo
    return e


def test_variables_usan_solo_lo_agregado_y_son_coherentes():
    v = calcular_variables(_estado(), T)
    assert set(v) == set(VARIABLES)
    assert v["sent_medio"] == pytest.approx(5.0 / 15)
    assert v["estrellas"] == pytest.approx(57.0 / 15)
    assert v["log_resenas"] == pytest.approx(math.log1p(15))
    assert v["resenas_6m"] == 5                          # solo octubre de 2019 cae en los últimos 6 meses
    assert v["tendencia_neg"] == pytest.approx(0.6 - 0.1)  # empeoró 50 puntos
    assert v["edad_anios"] == pytest.approx((T - datetime(2018, 1, 3)).days / 365.25)
    assert v["competidores_500m"] == 3


def test_agregar_es_acumulativo_como_en_el_streaming():
    """Sumar mes a mes (streaming) da lo mismo que sumarlo todo junto (batch)."""
    a = _estado()
    b = estado_vacio("b1", competidores=3)
    agregar(b, "2019-10", 5, 3, -1.0, 12.0, datetime(2019, 10, 2), datetime(2019, 10, 20))
    agregar(b, "2018-01", 10, 1, 6.0, 45.0, datetime(2018, 1, 3), datetime(2018, 1, 28))
    assert calcular_variables(a, T) == calcular_variables(b, T)


def test_estado_vacio_no_rompe():
    v = calcular_variables(estado_vacio("x"), T)
    assert v["log_resenas"] == 0 and v["tendencia_neg"] == 0 and v["edad_anios"] == 0


def test_a_datetime_acepta_texto_iso():
    assert a_datetime("2020-03-04T05:06:07") == datetime(2020, 3, 4, 5, 6, 7)
    assert a_datetime(None) is None


# ------------------------------------------------------------------ hotspot
def test_hotspot_exige_subida_y_volumen_minimo():
    assert es_hotspot(20, 0.40, 20, 0.10, 0.15, 10) == (True, pytest.approx(0.30))
    assert es_hotspot(20, 0.20, 20, 0.10, 0.15, 10)[0] is False        # subió solo 10 puntos
    assert es_hotspot(5, 0.80, 20, 0.10, 0.15, 10)[0] is False         # muy pocas reseñas
    assert es_hotspot(20, 0.40, None, None, 0.15, 10) == (False, None)  # sin ventana anterior


def test_ventana_alineada_como_spark_y_con_geometria():
    ini = inicio_ventana(datetime(2020, 1, 15, 13, 0), 30)
    assert (ini - datetime(1970, 1, 1)).days % 30 == 0 and ini <= datetime(2020, 1, 15) < ini + timedelta(days=30)
    d = documento_ventana("9yzgu", ini, ini + timedelta(days=30), 20, 5, "streaming")
    assert d["pct_negativo"] == 0.25 and d["celda"]["type"] == "Polygon" and d["centro"]["type"] == "Point"
    assert evaluar_hotspot(d, None, 0.15, 10)["hotspot"] is False


# ------------------------------------------------------------------ productor
def test_productor_nunca_publica_resenas_hasta_t():
    from productor.productor import solo_posteriores
    docs = [{"date": T - timedelta(days=1)}, {"date": T}, {"date": T + timedelta(seconds=1)}]
    assert [d["date"] for d in solo_posteriores(docs, T)] == [T + timedelta(seconds=1)]


def test_mensaje_lleva_clave_tiempo_de_evento_y_ubicacion():
    from productor.productor import mensaje
    doc = {"_id": "r1", "review_id": "r1", "business_id": "b1", "stars": 2, "text": "bad",
           "date": datetime(2020, 2, 3, 4, 5, 6), "geohash": "9yzgu", "categoria_principal": "Bars"}
    m = mensaje(doc, {"b1": {"name": "Bar", "location": {"type": "Point", "coordinates": [-90.2, 38.6]}}})
    assert m["date"] == "2020-02-03T04:05:06" and m["business_id"] == "b1" and (m["lon"], m["lat"]) == (-90.2, 38.6)


def test_reloj_acelerado_espera_lo_que_corresponde():
    from productor.productor import esperar_reloj
    reloj = {"t": 0.0}
    dormidas = []

    def dormir(s):
        dormidas.append(s)
        reloj["t"] += s
    # 3 días por segundo: una reseña 30 días después de T sale a los ~10 s
    sim = esperar_reloj(T + timedelta(days=30), T, 0.0, 3.0, dormir=dormir, ahora=lambda: reloj["t"])
    assert sim >= T + timedelta(days=30) and 9.9 <= reloj["t"] <= 10.6
    assert esperar_reloj(T + timedelta(days=30), T, 0.0, 0) == T + timedelta(days=30)   # sin reloj


# ------------------------------------------------------------------ supervivencia
def _negocio(i, primera, ultima, cerrado, cat="Bars"):
    return {"_id": f"b{i}", "categoria_principal": cat,
            "vida": {"primera_resena": primera, "ultima_resena": ultima, "evento": int(cerrado)}}


def test_preparar_censura_entrada_tardia_y_exclusion():
    from analitica.supervivencia import en_riesgo, preparar
    corte, inicio = datetime(2022, 1, 19), datetime(2010, 1, 1)
    filas, excluidos = preparar([
        _negocio(1, datetime(2012, 1, 1), datetime(2015, 1, 1), True),    # cierre observado
        _negocio(2, datetime(2012, 1, 1), datetime(2016, 1, 1), False),   # abierto: censurado en la fecha de corte
        _negocio(3, datetime(2006, 1, 1), datetime(2014, 1, 1), True),    # entrada tardía (4 años)
        _negocio(4, datetime(2005, 1, 1), datetime(2008, 1, 1), True),    # cerró antes de observar: fuera
    ], corte, inicio)
    assert excluidos == 1
    f = {x["id"]: x for x in filas}
    assert f["b1"]["evento"] == 1 and f["b1"]["entrada"] == 0
    assert f["b2"]["evento"] == 0 and f["b2"]["duracion"] == pytest.approx(((corte - datetime(2012, 1, 1)).days + 1) / 365.25)
    assert f["b3"]["entrada"] == pytest.approx((inicio - datetime(2006, 1, 1)).days / 365.25)
    assert en_riesgo(filas, 0.0) == 2           # b3 todavía no ha entrado al grupo en riesgo en t = 0
    assert en_riesgo(filas, 5.0) == 2           # b1 cerró a los 3 años; b2 y b3 siguen


def test_kaplan_meier_con_censura_conocida():
    pytest.importorskip("lifelines")
    from analitica.supervivencia import ajustar
    # 4 negocios: cierres en 1 y 3 años, censurados en 2 y 4  ->  S(1)=3/4, S(3)=3/4*1/2
    filas = [{"duracion": d, "evento": e, "entrada": 0.0} for d, e in [(1, 1), (2, 0), (3, 1), (4, 0)]]
    km = ajustar(filas, "prueba")
    assert km.survival_function_at_times([1.5]).values[0] == pytest.approx(0.75)
    assert km.survival_function_at_times([3.5]).values[0] == pytest.approx(0.375)


# ------------------------------------------------------------------ versionado de modelos
def test_versionado_detecta_version_t_y_cambios(tmp_path):
    carpeta = tmp_path / "modelo"
    (carpeta / "stages").mkdir(parents=True)
    (carpeta / "stages" / "part-0").write_bytes(b"pesos")
    versiones.escribir(str(carpeta), "2", "2020-01-01", modelo="RL")
    assert versiones.verificar(str(carpeta), "2", "2020-01-01") == []
    assert versiones.verificar(str(carpeta), "3", "2020-01-01")                       # otra versión
    assert versiones.verificar(str(carpeta), "2", "2019-01-01")                       # otra T
    (carpeta / "stages" / "part-0").write_bytes(b"pesos alterados")
    assert any("huella" in p for p in versiones.verificar(str(carpeta), "2", "2020-01-01"))
    assert versiones.verificar(str(tmp_path / "no_existe"), "2", "2020-01-01")
