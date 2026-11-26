# archivo: common/versiones.py
"""Versionado de modelos guardados (Entrega 2).

Al guardar un modelo de Spark se escribe dentro de su carpeta `lifemap_version.json` con la versión,
la fecha de corte T y una huella SHA-256 de todos sus archivos. Al cargarlo, el streaming recalcula la
huella y la registra en MongoDB; la prueba de Jenkins (verificar_modelo.py) comprueba que lo cargado
es exactamente la versión declarada en config.env y que nadie lo cambió.
"""
import hashlib
import json
import os
from datetime import datetime

ARCHIVO = "lifemap_version.json"


def huella(carpeta):
    """SHA-256 del contenido de la carpeta del modelo (rutas relativas + bytes), sin el archivo de versión
    ni los .crc que escribe Hadoop."""
    h = hashlib.sha256()
    for raiz, _, archivos in sorted(os.walk(carpeta)):
        for nombre in sorted(archivos):
            if nombre == ARCHIVO or nombre.endswith(".crc"):
                continue
            ruta = os.path.join(raiz, nombre)
            h.update(os.path.relpath(ruta, carpeta).encode())
            with open(ruta, "rb") as f:
                for bloque in iter(lambda: f.read(1 << 20), b""):
                    h.update(bloque)
    return h.hexdigest()


def escribir(carpeta, version, fecha_t, **extra):
    info = {"version": str(version), "fecha_t": fecha_t, "huella": huella(carpeta),
            "guardado": datetime.now().isoformat(timespec="seconds")}
    info.update(extra)
    with open(os.path.join(carpeta, ARCHIVO), "w", encoding="utf-8") as f:
        json.dump(info, f, indent=2, ensure_ascii=False)
    return info


def leer(carpeta):
    ruta = os.path.join(carpeta, ARCHIVO)
    if not os.path.exists(ruta):
        return None
    with open(ruta, encoding="utf-8") as f:
        return json.load(f)


def verificar(carpeta, version_esperada, fecha_t_esperada):
    """Lista de problemas (vacía = todo bien)."""
    info = leer(carpeta)
    if info is None:
        return [f"{carpeta}: no tiene {ARCHIVO} (modelo sin versionar)"]
    problemas = []
    if str(info.get("version")) != str(version_esperada):
        problemas.append(f"{carpeta}: versión {info.get('version')} != esperada {version_esperada}")
    if info.get("fecha_t") != fecha_t_esperada:
        problemas.append(f"{carpeta}: entrenado con T={info.get('fecha_t')} != FECHA_T={fecha_t_esperada}")
    if huella(carpeta) != info.get("huella"):
        problemas.append(f"{carpeta}: la huella no coincide (el modelo cambió después de versionarlo)")
    return problemas
