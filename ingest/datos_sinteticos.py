# archivo: ingest/datos_sinteticos.py
"""Genera archivos con el MISMO formato del Yelp Open Dataset (JSON por líneas) y
datos de mentira, para probar TODO el pipeline en minutos sin bajar 4 GB.
Incluye a propósito datos sucios (coordenadas malas, textos vacíos, duplicados...)
para que cada regla de limpieza tenga algo que descartar.

Uso:  docker compose run --rm ingest python datos_sinteticos.py [n_negocios] [carpeta]
Por defecto: 600 negocios en /data/raw_sintetico  (apunte RAW_DIR allí para usarlos).
"""
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

N = int(sys.argv[1]) if len(sys.argv) > 1 else 600
DESTINO = Path(sys.argv[2] if len(sys.argv) > 2 else "/data/raw_sintetico")
random.seed(42)

POS = "great amazing awesome delicious friendly fantastic perfect wonderful love best excellent fun cozy lovely".split()
NEG = "terrible awful rude slow dirty worst horrible disgusting bad overpriced cold never boring noisy".split()
NEU = "okay average fine decent normal alright mediocre ordinary typical standard".split()
RUIDO = "bar drinks beer night music place staff service order table friends time good food menu patio".split()
CATS_NL = ["Bars", "Sports Bars", "Dive Bars", "Cocktail Bars", "Pubs", "Lounges", "Dance Clubs",
           "Music Venues", "Karaoke", "Breweries", "Wine Bars"]
OTRAS = ["Restaurants", "Food", "American (New)", "Pizza", "Burgers", "Coffee & Tea"]


def texto(estrellas):
    base = POS if estrellas >= 4 else NEG if estrellas <= 2 else NEU
    palabras = [random.choice(base) for _ in range(random.randint(3, 8))] + \
               [random.choice(RUIDO) for _ in range(random.randint(4, 12))]
    if random.random() < 0.15:                       # algo de ruido cruzado
        palabras.append(random.choice(POS + NEG))
    random.shuffle(palabras)
    return " ".join(palabras).capitalize() + "."


def main():
    DESTINO.mkdir(parents=True, exist_ok=True)
    negocios, ids_ok = [], []
    for i in range(N):
        bid = f"biz{i:05d}"
        fuera = random.random() < 0.12                 # negocios de otro estado
        nl = random.random() < 0.80                    # 80 % Nightlife
        cats = random.sample(CATS_NL, k=random.randint(1, 3)) + (["Nightlife"] if nl else []) + \
               random.sample(OTRAS, k=random.randint(0, 2))
        random.shuffle(cats)
        negocios.append({
            "business_id": bid, "name": f"Lugar {i}", "address": "x",
            "city": random.choice(["Saint Louis", "St Louis", "Clayton", "Kirkwood", "St. Louis"]) if not fuera else "Phoenix",
            "state": "MO" if not fuera else "AZ", "postal_code": "63101",
            "latitude": round(random.gauss(38.63, 0.05), 6) if not fuera else 33.45,
            "longitude": round(random.gauss(-90.20, 0.06), 6) if not fuera else -112.07,
            "stars": round(random.uniform(1, 5) * 2) / 2, "review_count": random.randint(5, 300),
            "is_open": 0 if random.random() < 0.30 else 1,
            "attributes": {"BikeParking": "True"}, "categories": ", ".join(cats), "hours": None,
        })
        if not fuera and nl:
            ids_ok.append(bid)
    # datos sucios en negocios
    negocios[1]["latitude"] = None
    negocios[2]["longitude"] = 500
    negocios[3]["latitude"], negocios[3]["longitude"] = 0, 0
    negocios[4]["is_open"] = None
    negocios.append(dict(negocios[10]))                # duplicado exacto
    with open(DESTINO / "yelp_academic_dataset_business.json", "w", encoding="utf-8") as f:
        for n in negocios:
            f.write(json.dumps(n) + "\n")

    resenas, k = [], 0
    for n in negocios[:N]:
        total = random.randint(1, 120)
        inicio = datetime(2009, 1, 1) + timedelta(days=random.randint(0, 3000))
        for _ in range(total):
            e = random.choices([1, 2, 3, 4, 5], weights=[8, 8, 14, 32, 38])[0]
            f = inicio + timedelta(days=random.randint(0, 900), seconds=random.randint(0, 86399))
            resenas.append({"review_id": f"rev{k:07d}", "user_id": "u", "business_id": n["business_id"],
                            "stars": float(e), "useful": 0, "funny": 0, "cool": 0, "text": texto(e),
                            "date": f.strftime("%Y-%m-%d %H:%M:%S")})
            k += 1
    random.shuffle(resenas)
    validas = [r for r in resenas if r["business_id"] in set(ids_ok)]   # reseñas que SÍ pasan el filtro de área
    validas[0]["text"] = ""                            # sin texto
    validas[1]["text"] = "   "
    validas[2]["stars"] = 9.0                          # estrellas inválidas
    validas[3]["date"] = "no es una fecha"             # fecha inválida
    resenas[4]["business_id"] = "huerfano"             # negocio inexistente
    resenas.append(dict(validas[10]))                  # review_id duplicado
    with open(DESTINO / "yelp_academic_dataset_review.json", "w", encoding="utf-8") as f:
        for r in resenas:
            f.write(json.dumps(r) + "\n")
    print(f"[sintetico] {len(negocios)} negocios y {len(resenas)} reseñas en {DESTINO}")


if __name__ == "__main__":
    main()
