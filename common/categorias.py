# archivo: common/categorias.py
"""Familia de categorías (Nightlife) y categoría principal de un negocio.

Dentro de Nightlife casi todos los negocios comparten esa etiqueta, así que
"la primera categoría" no discrimina. Regla de categoría principal:

  la PRIMERA de esta lista de prioridad que el negocio tenga entre sus categorías;
  si no tiene ninguna, "Otros Nightlife".

Es determinista (no depende del orden en que Yelp liste las categorías).
Ajuste la lista mirando la salida de ingest/explorar_area.py y JUSTIFÍQUELA en el informe.
"""
FAMILIA = "Nightlife"

PRIORIDAD = [
    "Dive Bars", "Sports Bars", "Cocktail Bars", "Wine Bars", "Beer Bar", "Whiskey Bars",
    "Gay Bars", "Hookah Bars", "Piano Bars", "Irish Pub", "Pubs", "Breweries", "Brewpubs",
    "Beer Gardens", "Lounges", "Dance Clubs", "Music Venues", "Jazz & Blues", "Karaoke",
    "Comedy Clubs", "Casinos", "Pool Halls", "Arcades", "Adult Entertainment", "Bars",
]
OTROS = "Otros Nightlife"


def lista_categorias(categories):
    """Convierte la cadena 'A, B, C' de Yelp en lista ['A','B','C'] ([] si es nula)."""
    if categories is None:
        return []
    if isinstance(categories, (list, tuple)):
        return [str(c).strip() for c in categories if str(c).strip()]
    if not isinstance(categories, str):
        return []
    return [c.strip() for c in categories.split(",") if c.strip()]


def es_familia(categories, familia=FAMILIA):
    return familia in lista_categorias(categories)


def categoria_principal(categories):
    cats = set(lista_categorias(categories))
    for c in PRIORIDAD:
        if c in cats:
            return c
    return OTROS
