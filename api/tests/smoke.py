# archivo: api/tests/smoke.py
"""Pruebas de humo contra la API ya levantada (se ejecuta DENTRO del contenedor api, tras seed.py).
Si algo falla, termina con código 1 y Jenkins NO despliega."""
import json
import sys
import time
import urllib.error
import urllib.request

BASE = "http://localhost:5000"
LAT, LON = 38.627, -90.199
fallos = []


def pedir(ruta, cuerpo=None):
    """Devuelve (status, json) también para respuestas 4xx/5xx."""
    datos = json.dumps(cuerpo).encode() if cuerpo is not None else None
    req = urllib.request.Request(BASE + ruta, data=datos, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def revisar(nombre, condicion):
    print(("OK   " if condicion else "FALLA") + " " + nombre)
    if not condicion:
        fallos.append(nombre)


for _ in range(30):                     # esperar a que la API responda
    try:
        pedir("/health")
        break
    except Exception:
        time.sleep(2)
else:
    sys.exit("La API nunca respondió en /health")

s, d = pedir("/health")
revisar("/health: mongo ok, trae commit y servicios", s == 200 and d["servicios"]["mongo"]["estado"] == "ok"
        and "commit" in d and {"mongo", "spark", "dask", "api"} <= set(d["servicios"]))

s, d = pedir(f"/businesses/near?lat={LAT}&lon={LON}&radio=1000")
revisar("near ($near): devuelve los 5 negocios", s == 200 and d["total"] == 5)
revisar("near: ordenados del más cercano al más lejano",
        [r["business_id"] for r in d["resultados"]] == ["b0", "b1", "b2", "b3", "b4"])
s, d = pedir(f"/businesses/near?lat={LAT}&lon={LON}&radio=1000&categoria=Pubs")
revisar("near con categoría: solo Pubs", s == 200 and d["total"] == 2)
s, d = pedir(f"/businesses/near?lat={LAT}&lon={LON}&radio=50")
revisar("near con radio pequeño: parámetros variables", s == 200 and d["total"] < 5)
s, d = pedir("/businesses/near?lat=999&lon=0&radio=10")
revisar("near con lat inválida: 400", s == 400)

poligono = {"type": "Polygon", "coordinates": [[[-90.21, 38.62], [-90.19, 38.62], [-90.19, 38.64],
                                                [-90.21, 38.64], [-90.21, 38.62]]]}
s, d = pedir("/businesses/within", poligono)
revisar("within ($geoWithin): 5 negocios en el polígono", s == 200 and d["total_en_poligono"] == 5)
s, d = pedir("/businesses/within?categoria=Bars", poligono)
revisar("within con categoría: 3 Bars", s == 200 and d["total_en_poligono"] == 3)
s, d = pedir("/businesses/within", {"type": "Polygon", "coordinates": []})
revisar("within con polígono malo: 400", s == 400)

s, d = pedir(f"/zones/summary?lat={LAT}&lon={LON}&radio=1000")
revisar("zones/summary ($geoNear): 5 negocios, 2 cierres, 2920 días-negocio",
        s == 200 and d["negocios"] == 5 and d["cierres"] == 2 and d["dias_negocio"] == 2920)
revisar("zones/summary: tasa anualizada = 25 %", abs(d["tasa_cierre_pct"] - 25.0) < 1e-9)
revisar("zones/summary: sentimiento medio ponderado por reseñas = 35/80",
        abs(d["sentimiento_medio"] - 35 / 80) < 1e-9)      # (10*-0.2 + 20*0.5 + 30*0.6 + 10*0 + 10*0.9) / 80
s, d = pedir(f"/zones/summary?lat={LAT}&lon={LON}&radio=1000&categoria=Pubs")
revisar("zones/summary con categoría: 2 negocios", s == 200 and d["negocios"] == 2)

s, d = pedir("/stats/closure?por=categoria&min_dias=1000")
revisar("stats/closure: excluye grupos con pocos días-negocio",
        s == 200 and {g["categoria"] for g in d["resultados"]} == {"Bars", "Pubs"}
        and d["grupos_excluidos_por_muestra"] == 1)
s, d = pedir("/stats/closure?por=categoria&min_dias=0")
revisar("stats/closure: el mínimo es un parámetro", s == 200 and d["total"] == 3)
s, d = pedir("/stats/closure?por=geohash&min_dias=0")
revisar("stats/closure por geohash", s == 200 and d["total"] == 1)

s, d = pedir("/predict", {"text": ""})
revisar("predict con texto vacío: 400", s == 400)
s, d = pedir("/predict", {"text": "great place, friendly staff"})
revisar("predict: 200 con clase, o 503 si aún no hay modelo entrenado",
        (s == 200 and "clase" in d) or (s == 503 and "error" in d))

if fallos:
    sys.exit(f"Fallaron {len(fallos)} pruebas: {fallos}")
print("Todas las pruebas de humo pasaron.")
