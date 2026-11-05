# archivo: api/health.py
"""Estado de cada servicio + hash del commit desplegado, para el endpoint /health."""
import json
import os
from concurrent.futures import ThreadPoolExecutor, wait
import urllib.request
from datetime import datetime, timezone

SPARK_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
DASK_UI = os.environ.get("DASK_DASHBOARD", "http://dask-scheduler:8787")
# Servicios que deben estar sanos para que /health responda 200 (en CI solo hay mongo y api)
REQUERIDOS = [s.strip() for s in os.environ.get("HEALTH_REQUIRED", "mongo,spark,dask").split(",") if s.strip()]
# Tiempo máximo TOTAL de /health. El timeout de urlopen no cubre la resolución DNS: en el entorno de
# prueba (sin spark-master ni dask-scheduler) buscar esos nombres puede tardar varios segundos.
LIMITE_SEG = float(os.environ.get("HEALTH_TIMEOUT", "3"))
_POOL = ThreadPoolExecutor(max_workers=6)


def _http(url, timeout=2.5):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def _probar(funcion):
    try:
        return {"estado": "ok", **(funcion() or {})}
    except Exception as e:                    # cualquier fallo = servicio no disponible
        return {"estado": "error", "detalle": f"{type(e).__name__}: {e}"[:200]}


def _spark():
    _, cuerpo = _http(SPARK_UI.rstrip("/") + "/json/")
    j = json.loads(cuerpo)
    if j.get("status") != "ALIVE" or int(j.get("aliveworkers", 0)) < 1:
        raise RuntimeError(f"maestro {j.get('status')} con {j.get('aliveworkers', 0)} workers")
    return {"workers": int(j["aliveworkers"])}


def _dask():
    status, cuerpo = _http(DASK_UI.rstrip("/") + "/health")
    if status != 200 or cuerpo.strip() != "ok":
        raise RuntimeError(f"respuesta inesperada: {cuerpo[:50]!r}")
    return {}


def verificar(ping_mongo):
    """Devuelve (informe, http_status). `ping_mongo` es una función que lanza si Mongo no responde."""
    pruebas = {"mongo": lambda: ping_mongo() and {}, "spark": _spark, "dask": _dask}
    futuros = {nombre: _POOL.submit(_probar, f) for nombre, f in pruebas.items()}   # en paralelo
    wait(futuros.values(), timeout=LIMITE_SEG)
    servicios = {nombre: (fut.result() if fut.done() else
                          {"estado": "error", "detalle": f"sin respuesta en {LIMITE_SEG:g} s"})
                 for nombre, fut in futuros.items()}
    servicios["api"] = {"estado": "ok"}
    sano = all(servicios[s]["estado"] == "ok" for s in REQUERIDOS if s in servicios)
    informe = {"estado": "ok" if sano else "degradado",
               "commit": os.environ.get("GIT_COMMIT", "desconocido"),
               "servicios": servicios,
               "hora_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    return informe, (200 if sano else 503)