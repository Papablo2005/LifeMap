# archivo: common/metricas.py
"""Métricas de clasificación por clase a partir de una matriz de confusión
(Python puro, para poder probarlas con pytest sin Spark)."""
from common.etiquetas import NOMBRES


def metricas_desde_matriz(matriz, nombres=None):
    """`matriz[i][j]` = reseñas de clase real i que el modelo predijo como j.

    Devuelve precision, recall, F1 y soporte por clase, más accuracy, macro-F1
    (promedio simple de los F1: trata igual a las clases pequeñas) y weighted-F1.
    """
    nombres = nombres or NOMBRES
    k = len(nombres)
    total = sum(sum(f) for f in matriz)
    por_clase = {}
    for c, nombre in enumerate(nombres):
        tp = matriz[c][c]
        fp = sum(matriz[i][c] for i in range(k)) - tp
        fn = sum(matriz[c]) - tp
        p = tp / (tp + fp) if (tp + fp) else 0.0
        r = tp / (tp + fn) if (tp + fn) else 0.0
        f1 = 2 * p * r / (p + r) if (p + r) else 0.0
        por_clase[nombre] = {"precision": p, "recall": r, "f1": f1, "soporte": sum(matriz[c])}
    accuracy = sum(matriz[i][i] for i in range(k)) / total if total else 0.0
    macro = sum(m["f1"] for m in por_clase.values()) / k
    ponderado = sum(m["f1"] * m["soporte"] for m in por_clase.values()) / total if total else 0.0
    return {"por_clase": por_clase, "accuracy": accuracy, "macro_f1": macro, "weighted_f1": ponderado,
            "matriz_confusion": [list(map(int, f)) for f in matriz]}
