# archivo: productor/productor.py
"""Entrega 2 · Productor Kafka con reloj acelerado.

Publica en el topic `reviews` (3 particiones, clave = business_id) SOLO las reseñas posteriores a la
fecha de corte T, en orden de fecha. Las reseñas hasta T se usaron para entrenar; por eso se reservan.

Reloj acelerado: el tiempo simulado arranca en T y avanza VELOCIDAD días por cada segundo real
(3 días/s = un mes de reseñas cada 10 s). Cada reseña se publica cuando el reloj llega a su fecha.

Uso (dentro del contenedor, ver docker-compose: servicio `productor`):
  python productor.py                         # todas las reseñas > T, velocidad de config.env
  python productor.py --velocidad 6           # dos meses cada 10 s
  python productor.py --hasta 2020-06-30      # solo hasta esa fecha
  python productor.py --limite 500 --velocidad 0     # 500 reseñas lo más rápido posible
  python exportar_muestra.py | python productor.py --stdin --velocidad 0   # prueba de humo de Jenkins
"""
import argparse
import json
import sys
import time
from datetime import datetime, timedelta

sys.path.insert(0, "/app")
from common import config  # noqa: E402


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Productor Kafka de reseñas con reloj acelerado")
    p.add_argument("--velocidad", type=float, default=config.VELOCIDAD_DIAS_POR_SEG,
                   help="días simulados por segundo real (0 = sin reloj, lo más rápido posible)")
    p.add_argument("--desde", default=None, help="YYYY-MM-DD (por defecto T); nunca puede ser anterior a T")
    p.add_argument("--hasta", default=None, help="YYYY-MM-DD (por defecto, hasta el final del dataset)")
    p.add_argument("--limite", type=int, default=0, help="máximo de mensajes (0 = sin límite)")
    p.add_argument("--topic", default=config.KAFKA_TOPIC)
    p.add_argument("--stdin", action="store_true", help="leer reseñas (JSON por línea) de la entrada estándar")
    return p.parse_args(argv)


def mensaje(doc, negocios):
    """Reseña de MongoDB -> JSON del mensaje. Incluye la ubicación para las ventanas y las alertas."""
    neg = negocios.get(doc["business_id"], {})
    lon, lat = (neg.get("location") or {}).get("coordinates", [None, None])
    return {
        "review_id": doc.get("review_id") or doc.get("_id"),
        "business_id": doc["business_id"],
        "stars": float(doc["stars"]),
        "text": doc["text"],
        "date": doc["date"].strftime("%Y-%m-%dT%H:%M:%S"),          # tiempo del EVENTO
        "geohash": doc.get("geohash") or neg.get("geohash"),
        "categoria_principal": doc.get("categoria_principal") or neg.get("categoria_principal"),
        "name": neg.get("name"), "lat": lat, "lon": lon,
        "publicado_en": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
    }


def solo_posteriores(docs, t):
    """Garantía del criterio E2.2: nunca se publica una reseña con fecha <= T."""
    for d in docs:
        if d["date"] > t:
            yield d


def esperar_reloj(fecha, inicio_sim, t0, velocidad, dormir=time.sleep, ahora=time.monotonic):
    """Bloquea hasta que el reloj simulado alcance `fecha`. Devuelve el tiempo simulado actual."""
    if velocidad <= 0:
        return fecha
    while True:
        sim = inicio_sim + timedelta(days=(ahora() - t0) * velocidad)
        if fecha <= sim:
            return sim
        faltan_s = (fecha - sim).total_seconds() / 86400 / velocidad
        dormir(min(0.5, max(0.01, faltan_s)))


def fuente_mongo(args, t):
    from pymongo import ASCENDING, MongoClient
    db = MongoClient(config.MONGO_URI)[config.MONGO_DB]
    negocios = {n["_id"]: n for n in db["businesses"].find(
        {}, {"name": 1, "location": 1, "geohash": 1, "categoria_principal": 1})}
    desde = max(t, datetime.strptime(args.desde, "%Y-%m-%d")) if args.desde else t
    filtro = {"date": {"$gt": desde}}
    if args.hasta:
        filtro["date"]["$lte"] = datetime.strptime(args.hasta, "%Y-%m-%d") + timedelta(days=1)
    cursor = db["reviews"].find(filtro, {"label": 0}, no_cursor_timeout=True).sort("date", ASCENDING)
    return desde, cursor, negocios


def fuente_stdin(t):
    from bson import json_util
    docs = [json_util.loads(linea) for linea in sys.stdin if linea.strip()]
    docs.sort(key=lambda d: d["date"])
    return t, docs, {}


def main(argv=None):
    args = parse_args(argv)
    from confluent_kafka import Producer
    t = config.fecha_t()
    desde, docs, negocios = fuente_stdin(t) if args.stdin else fuente_mongo(args, t)
    prod = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP, "linger.ms": 50, "acks": "all"})
    errores = []

    def informe(err, _msg):
        if err is not None:
            errores.append(str(err))

    print(f"[productor] topic={args.topic}  T={t:%Y-%m-%d}  desde={desde:%Y-%m-%d}  "
          f"velocidad={args.velocidad} días/s  ({'sin reloj' if args.velocidad <= 0 else '1 mes cada %.0f s' % (30 / args.velocidad)})",
          flush=True)
    t0, n, mes_actual = time.monotonic(), 0, None
    for doc in solo_posteriores(docs, t):
        sim = esperar_reloj(doc["date"], desde, t0, args.velocidad)
        m = mensaje(doc, negocios)
        prod.produce(args.topic, key=m["business_id"].encode(), value=json.dumps(m).encode(), on_delivery=informe)
        prod.poll(0)
        n += 1
        if doc["date"].strftime("%Y-%m") != mes_actual:
            mes_actual = doc["date"].strftime("%Y-%m")
            print(f"[productor] reloj simulado {sim:%Y-%m-%d}  publicadas={n:,}", flush=True)
        if args.limite and n >= args.limite:
            break
    prod.flush(30)
    if errores:
        sys.exit(f"[productor] {len(errores)} mensajes fallaron: {errores[:3]}")
    print(f"[productor] listo: {n:,} reseñas publicadas en {time.monotonic() - t0:.0f} s", flush=True)


if __name__ == "__main__":
    main()
