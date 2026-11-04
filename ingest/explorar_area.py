# archivo: ingest/explorar_area.py
"""Mira business.json ANTES de limpiar, para decidir y justificar el área y la
categoría principal. Imprime:
  1) negocios por estado,
  2) ciudades de los estados del área con negocios Nightlife,
  3) las categorías más frecuentes dentro de Nightlife (para ajustar common/categorias.py).
Uso:  docker compose run --rm ingest python explorar_area.py
"""
import glob
import os
from collections import Counter

import pandas as pd

from common import config
from common.categorias import es_familia, lista_categorias

ruta = (glob.glob(os.path.join(config.RAW_DIR, "**", "*business*.json"), recursive=True) or [None])[0]
if not ruta:
    raise SystemExit(f"No encuentro business.json en {config.RAW_DIR}: corra primero download.py")

estados, ciudades, cats = Counter(), Counter(), Counter()
n_nl = n_area = 0
bbox = config.bbox()
for chunk in pd.read_json(ruta, lines=True, chunksize=50_000, dtype=False, convert_dates=False):
    estados.update(chunk["state"].dropna())
    for _, r in chunk[chunk["state"].isin(config.AREA_STATES)].iterrows():
        if not es_familia(r["categories"], config.FAMILIA):
            continue
        n_nl += 1
        ciudades[(r["state"], r["city"])] += 1
        en_caja = (bbox is None or (bbox[0] <= r["longitude"] <= bbox[2] and bbox[1] <= r["latitude"] <= bbox[3]))
        if en_caja:
            n_area += 1
            cats.update(c for c in lista_categorias(r["categories"]) if c != config.FAMILIA)

print("== Negocios por estado (top 15) ==")
for k, v in estados.most_common(15):
    print(f"  {k:<5}{v:>8,}")
print(f"\n== Negocios {config.FAMILIA} en {config.AREA_STATES}: {n_nl:,}  (dentro de la caja: {n_area:,}) ==")
for (e, c), v in ciudades.most_common(25):
    print(f"  {e:<4}{c:<28}{v:>6,}")
print(f"\n== Categorías más frecuentes dentro de {config.FAMILIA} (en la caja) ==")
for k, v in cats.most_common(30):
    print(f"  {k:<28}{v:>6,}")
