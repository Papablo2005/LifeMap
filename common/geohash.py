# archivo: common/geohash.py
"""Geohash en Python puro (sin dependencias), igual para Dask, Spark y la API.

Un geohash divide el mapa en celdas rectangulares; cuantos más caracteres,
más pequeña la celda (precisión 5 ~ 4,9 x 4,9 km; precisión 6 ~ 1,2 x 0,6 km).
"""
_BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"
_DECODIFICA = {c: i for i, c in enumerate(_BASE32)}


def encode(lat, lon, precision=5):
    """Devuelve el geohash de (lat, lon) con `precision` caracteres."""
    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        raise ValueError(f"Coordenadas fuera de rango: lat={lat}, lon={lon}")
    lat_i, lon_i = [-90.0, 90.0], [-180.0, 180.0]
    gh, bits, ch, par_lon = [], 0, 0, True
    while len(gh) < precision:
        intervalo, valor = (lon_i, lon) if par_lon else (lat_i, lat)
        medio = (intervalo[0] + intervalo[1]) / 2
        if valor >= medio:
            ch = ch * 2 + 1
            intervalo[0] = medio
        else:
            ch = ch * 2
            intervalo[1] = medio
        par_lon = not par_lon
        bits += 1
        if bits == 5:
            gh.append(_BASE32[ch])
            bits, ch = 0, 0
    return "".join(gh)


def bbox(geohash):
    """Devuelve (lat_min, lat_max, lon_min, lon_max) de la celda."""
    lat_i, lon_i, par_lon = [-90.0, 90.0], [-180.0, 180.0], True
    for c in geohash:
        valor = _DECODIFICA[c]
        for mascara in (16, 8, 4, 2, 1):
            intervalo = lon_i if par_lon else lat_i
            medio = (intervalo[0] + intervalo[1]) / 2
            if valor & mascara:
                intervalo[0] = medio
            else:
                intervalo[1] = medio
            par_lon = not par_lon
    return lat_i[0], lat_i[1], lon_i[0], lon_i[1]


def centro(geohash):
    """Devuelve (lat, lon) del centro de la celda."""
    lat_min, lat_max, lon_min, lon_max = bbox(geohash)
    return (lat_min + lat_max) / 2, (lon_min + lon_max) / 2


def poligono_geojson(geohash):
    """Polígono GeoJSON (anillo cerrado, orden [lon, lat]) de la celda."""
    lat_min, lat_max, lon_min, lon_max = bbox(geohash)
    return {"type": "Polygon", "coordinates": [[
        [lon_min, lat_min], [lon_max, lat_min], [lon_max, lat_max],
        [lon_min, lat_max], [lon_min, lat_min]]]}


def punto_geojson(geohash):
    """Punto GeoJSON (orden [lon, lat]) con el centro de la celda."""
    lat, lon = centro(geohash)
    return {"type": "Point", "coordinates": [lon, lat]}
