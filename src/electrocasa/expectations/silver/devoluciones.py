def expectations_devoluciones():
    return {"monto_reembolso_valido": "monto_reembolso >= 0"}


def condicion_cuarentena_devoluciones():
    return "monto_reembolso IS NULL OR monto_reembolso < 0"
