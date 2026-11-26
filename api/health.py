# archivo: api/health.py
"""Estado de cada servicio + hash del commit desplegado, para el endpoint /health."""
import json
import os
from concurrent.futures import ThreadPoolExecutor, wait
import urllib.request
from datetime import datetime, timezone

SPARK_UI = os.environ.get("SPARK_MASTER_UI", "http://spark-master:8080")
DASK_UI = os.environ.get("DASK_DASHBOARD", "http://dask-scheduler:8787")
KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "reviews")
LATIDO_MAX_SEG = 90            # si el streaming no da señales en 90 s, se informa como caído
# Servicios que deben estar sanos para que /health responda 200 (en CI solo hay mongo y api)
REQUERIDOS = [s.strip() for s in os.environ.get("HEALTH_REQUIRED", "mongo,spark,dask,kafka").split(",") if s.strip()]
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


def _kafka():
    """Broker vivo, topic con sus particiones y offset final (high watermark) de cada partición."""
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient
    md = AdminClient({"bootstrap.servers": KAFKA, "socket.timeout.ms": 2000, "log_level": 0}).list_topics(topic=TOPIC, timeout=2)
    tp = md.topics.get(TOPIC)
    if tp is None or tp.error is not None or not tp.partitions:
        raise RuntimeError(f"el topic '{TOPIC}' no existe o no tiene particiones")
    c = Consumer({"bootstrap.servers": KAFKA, "group.id": "lifemap-health", "enable.auto.commit": False,
                  "log_level": 0})
    try:
        limites = {str(p): c.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=2) for p in sorted(tp.partitions)}
    finally:
        c.close()
    return {"topic": TOPIC, "particiones": len(limites), "mensajes_en_topic": sum(a - b for b, a in limites.values()),
            "_limites": limites}


def retraso_consumidor(kafka, meta_streaming):
    """Retraso (lag) = mensajes publicados que el streaming aún no procesó, sumando todas las particiones."""
    procesados = (meta_streaming or {}).get("offsets", {})
    retraso, por_particion = 0, {}
    for p, (bajo, alto) in kafka.pop("_limites", {}).items():
        r = max(0, alto - max(bajo, int(procesados.get(p, bajo))))
        por_particion[p] = r
        retraso += r
    return retraso, por_particion


def estado_streaming(meta_streaming):
    if not meta_streaming:
        return {"estado": "error", "detalle": "el job de streaming no se ha registrado"}
    latido = meta_streaming.get("latido")
    edad = (datetime.utcnow() - latido).total_seconds() if latido else None
    vivo = edad is not None and edad <= LATIDO_MAX_SEG
    return {"estado": "ok" if vivo and meta_streaming.get("estado") == "activo" else "error",
            "fase": meta_streaming.get("estado"), "segundos_desde_latido": round(edad, 1) if edad is not None else None,
            "ultimo_lote": meta_streaming.get("ultimo_lote"), "reloj_evento": meta_streaming.get("reloj_evento"),
            "resenas_procesadas": meta_streaming.get("resenas_procesadas", 0),
            "modelo_version": meta_streaming.get("modelo_version")}


def verificar(ping_mongo, leer_meta_streaming=lambda: None):
    """Devuelve (informe, http_status). `ping_mongo` es una función que lanza si Mongo no responde;
    `leer_meta_streaming` devuelve el documento meta.streaming (offsets procesados, latido)."""
    pruebas = {"mongo": lambda: ping_mongo() and {}, "spark": _spark, "dask": _dask, "kafka": _kafka,
               "streaming": leer_meta_streaming}
    futuros = {nombre: _POOL.submit(_probar, f) for nombre, f in pruebas.items()}   # en paralelo
    wait(futuros.values(), timeout=LIMITE_SEG)
    servicios = {nombre: (fut.result() if fut.done() else
                          {"estado": "error", "detalle": f"sin respuesta en {LIMITE_SEG:g} s"})
                 for nombre, fut in futuros.items()}
    meta_streaming = futuros["streaming"].result().get("meta") if futuros["streaming"].done() else None
    servicios["streaming"] = estado_streaming(meta_streaming)
    if servicios["kafka"]["estado"] == "ok":
        servicios["kafka"]["retraso_consumidor"], servicios["kafka"]["retraso_por_particion"] = \
            retraso_consumidor(servicios["kafka"], meta_streaming)
    servicios["kafka"].pop("_limites", None)
    servicios["api"] = {"estado": "ok"}
    sano = all(servicios[s]["estado"] == "ok" for s in REQUERIDOS if s in servicios)
    informe = {"estado": "ok" if sano else "degradado",
               "commit": os.environ.get("GIT_COMMIT", "desconocido"),
               "servicios": servicios,
               "hora_utc": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    return informe, (200 if sano else 503)