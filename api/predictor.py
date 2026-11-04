# archivo: api/predictor.py
"""Sentimiento de un texto nuevo con el PipelineModel guardado por spark/jobs/sentimiento.py.

Carga PEREZOSA: Spark (local[1]) arranca en la primera petición a /predict, no al iniciar la API.
El modelo ya incluye la limpieza (SQLTransformer), así que se le pasa el texto crudo.
"""
import os
import threading

from common.etiquetas import NOMBRES

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/sentimiento_latest")
_lock = threading.Lock()
_estado = {"spark": None, "modelo": None}


class ModeloNoDisponible(Exception):
    pass


def _cargar():
    if _estado["modelo"] is not None:
        return
    if not os.path.isdir(MODEL_PATH):
        raise ModeloNoDisponible(f"No hay modelo en {MODEL_PATH}: entrene primero (spark/jobs/sentimiento.py)")
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[1]").appName("lifemap-predict")
             .config("spark.driver.memory", os.environ.get("PREDICT_DRIVER_MEMORY", "768m"))
             .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "1").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    _estado["spark"], _estado["modelo"] = spark, PipelineModel.load(MODEL_PATH)


def predecir(texto):
    with _lock:
        _cargar()
        df = _estado["spark"].createDataFrame([(texto,)], ["text"])
        fila = _estado["modelo"].transform(df).select("prediction", "probability").first()
    clase = int(fila["prediction"])
    return {"clase": NOMBRES[clase], "clase_id": clase,
            "probabilidades": {NOMBRES[i]: round(float(p), 6) for i, p in enumerate(fila["probability"])},
            "modelo": os.path.basename(os.path.realpath(MODEL_PATH))}
