# archivo: common/vida.py
"""Vida de un negocio: de su primera a su última reseña (aproximación).

Limitaciones (se discuten en el informe):
  * El dataset no trae fecha de apertura ni de cierre.
  * Un negocio puede existir antes de su primera reseña (entrada tardía).
  * is_open es una foto al momento del dataset, no la fecha del cierre.
"""


def dias_negocio(primera, ultima):
    """Días entre la primera y la última reseña, contando ambos extremos (+1).

    El +1 evita que un negocio con una sola reseña tenga 0 días de exposición.
    Acepta datetime, date o pandas.Timestamp.
    """
    d1 = primera.date() if hasattr(primera, "date") else primera
    d2 = ultima.date() if hasattr(ultima, "date") else ultima
    dias = (d2 - d1).days + 1
    if dias < 1:
        raise ValueError("La última reseña es anterior a la primera")
    return dias


def evento_y_censura(is_open):
    """(evento, censurado): is_open == 0 -> cierre observado; is_open == 1 -> censurado.

    Un negocio abierto NO es un cierre: solo sabemos que seguía abierto en la
    fecha de corte del dataset (censura a la derecha).
    """
    if is_open not in (0, 1):
        raise ValueError(f"is_open inválido: {is_open!r}")
    evento = 1 if is_open == 0 else 0
    return evento, evento == 0
