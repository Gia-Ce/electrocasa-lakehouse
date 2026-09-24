from pyspark import pipelines as dp
from pyspark.sql.functions import col, current_timestamp, lit, when

from src.electrocasa.transforms.silver.ventas import transform_ventas
from src.electrocasa.expectations.silver.ventas import (
    expectations_ventas,
    condicion_cuarentena_ventas,
)


@dp.table(
    name="ventas",
    comment="Ventas limpias y validadas",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop(expectations_ventas())
def silver_ventas():
    return spark.read.table("bronze.ventas").transform(transform_ventas)


@dp.table(
    name="ventas_cuarentena",
    comment="Ventas rechazadas por monto invalido con trazabilidad",
    table_properties={"quality": "silver"},
)
def ventas_cuarentena():
    return (
        spark.read.table("bronze.ventas")
        .transform(transform_ventas)
        .filter(condicion_cuarentena_ventas())
        .withColumn(
            "motivo_rechazo",
            when(col("monto_total").isNull(), lit("monto_total_nulo"))
            .otherwise(lit("monto_total_no_positivo")),
        )
        .withColumn("fec_rechazo", current_timestamp())
    )
