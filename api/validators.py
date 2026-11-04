# archivo: api/validators.py
"""Validación de parámetros. Separada de app.py para poder probarla con pytest
sin necesidad de tener MongoDB corriendo."""


class ValidationError(ValueError):
    pass


def _es_numero(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def parse_float(args, nombre, minimo, maximo, default=None):
    valor = args.get(nombre)
    if valor is None or valor == "":
        if default is not None:
            return default
        raise ValidationError(f"Falta el parámetro '{nombre}'")
    try:
        v = float(valor)
    except (TypeError, ValueError):
        raise ValidationError(f"'{nombre}' debe ser numérico")
    if not (minimo <= v <= maximo):
        raise ValidationError(f"'{nombre}' debe estar entre {minimo} y {maximo}")
    return v


def parse_punto(args):
    lat = parse_float(args, "lat", -90, 90)
    lon = parse_float(args, "lon", -180, 180)
    # GeoJSON: [longitud, latitud]
    return {"type": "Point", "coordinates": [lon, lat]}


def parse_radio(args, maximo=50_000):
    return parse_float(args, "radio", 1, maximo)  # metros


def parse_limite(args, default=100, maximo=1000):
    return int(parse_float(args, "limite", 1, maximo, default=default))


def parse_poligono(cuerpo):
    """Acepta un GeoJSON Polygon o un Feature cuyo geometry sea Polygon."""
    if not isinstance(cuerpo, dict):
        raise ValidationError("El cuerpo debe ser un objeto GeoJSON")
    if cuerpo.get("type") == "Feature":
        cuerpo = cuerpo.get("geometry") or {}
    if cuerpo.get("type") != "Polygon":
        raise ValidationError("Se espera un GeoJSON de tipo 'Polygon'")

    anillos = cuerpo.get("coordinates")
    if not isinstance(anillos, list) or not anillos:
        raise ValidationError("'coordinates' debe ser una lista de anillos")
    for anillo in anillos:
        if not isinstance(anillo, list) or len(anillo) < 4:
            raise ValidationError("Cada anillo necesita al menos 4 posiciones")
        for pos in anillo:
            if (not isinstance(pos, list) or len(pos) < 2
                    or not _es_numero(pos[0]) or not _es_numero(pos[1])):
                raise ValidationError("Cada posición debe ser [lon, lat]")
            if not (-180 <= pos[0] <= 180 and -90 <= pos[1] <= 90):
                raise ValidationError(f"Posición fuera de rango: {pos}")
        if anillo[0][:2] != anillo[-1][:2]:
            raise ValidationError("El polígono debe estar cerrado (primer punto = último)")
    return {"type": "Polygon", "coordinates": anillos}


# ------------------------------------------------------------------ LifeMap
def parse_categoria(args, maximo=60):
    """Categoría opcional (coincidencia exacta con una categoría de Yelp)."""
    valor = args.get("categoria")
    if valor is None or valor.strip() == "":
        return None
    valor = valor.strip()
    if len(valor) > maximo or any(ord(c) < 32 for c in valor):
        raise ValidationError(f"'categoria' inválida (máximo {maximo} caracteres, sin controles)")
    return valor


def parse_min_dias(args, default):
    """Mínimo de días-negocio para que un grupo cuente en las tasas."""
    return int(parse_float(args, "min_dias", 0, 10_000_000, default=default))


def parse_por(args):
    por = args.get("por", "categoria")
    if por not in ("categoria", "geohash"):
        raise ValidationError("'por' debe ser 'categoria' o 'geohash'")
    return por


def parse_texto(cuerpo, maximo=5000):
    """Cuerpo de /predict: {"text": "..."}."""
    if not isinstance(cuerpo, dict) or not isinstance(cuerpo.get("text"), str):
        raise ValidationError('El cuerpo debe ser JSON con el campo "text" (texto)')
    texto = cuerpo["text"].strip()
    if not texto:
        raise ValidationError("'text' no puede estar vacío")
    if len(texto) > maximo:
        raise ValidationError(f"'text' excede {maximo} caracteres")
    return texto
