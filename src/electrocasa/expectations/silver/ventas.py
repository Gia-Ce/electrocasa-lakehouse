def expectations_ventas():
    """Un monto nulo, cero o negativo no es utilizable para métricas de ventas."""
    return {
        "monto_total_valido": "monto_total > 0"
    }


def condicion_cuarentena_ventas():
    return "monto_total IS NULL OR monto_total <= 0"
