from pyspark.sql.types import StructType, StructField, StringType


def schema_devoluciones():
    """Contrato raw de devoluciones; montos y fechas se convierten en Silver."""
    return StructType([
        StructField("devolucion_id", StringType(), True),
        StructField("pedido_id", StringType(), True),
        StructField("sucursal_id", StringType(), True),
        StructField("producto_id", StringType(), True),
        StructField("motivo", StringType(), True),
        StructField("monto_reembolso", StringType(), True),
        StructField("fecha_devolucion", StringType(), True),
    ])
