from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, when, current_timestamp

from src.electrocasa.transforms.silver.devoluciones import transform_devoluciones
from src.electrocasa.expectations.silver.devoluciones import (
    expectations_devoluciones,
    condicion_cuarentena_devoluciones,
)


def base_devoluciones():
    return transform_devoluciones(
        spark.read.table("bronze.devoluciones"),
        spark.read.table("silver.productos"),
    )


@dp.table(name="devoluciones", comment="Devoluciones limpias y validadas", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(expectations_devoluciones())
def silver_devoluciones():
    return base_devoluciones()


@dp.table(name="devoluciones_cuarentena", comment="Devoluciones rechazadas con trazabilidad", table_properties={"quality": "silver"})
def devoluciones_cuarentena():
    return (
        base_devoluciones()
        .filter(condicion_cuarentena_devoluciones())
        .withColumn(
            "motivo_rechazo",
            when(col("monto_reembolso").isNull(), lit("monto_reembolso_nulo"))
            .otherwise(lit("monto_reembolso_negativo")),
        )
        .withColumn("fec_rechazo", current_timestamp())
    )
