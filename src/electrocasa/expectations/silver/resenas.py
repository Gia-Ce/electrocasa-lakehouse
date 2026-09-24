def expectations_resenas():
    return {"calificacion_valida": "calificacion BETWEEN 1 AND 5"}


def condicion_cuarentena_resenas():
    return "calificacion IS NULL OR calificacion < 1 OR calificacion > 5"
