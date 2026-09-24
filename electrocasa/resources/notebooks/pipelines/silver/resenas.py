from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit, when, current_timestamp

from src.electrocasa.transforms.silver.resenas import transform_resenas
from src.electrocasa.expectations.silver.resenas import (
    expectations_resenas,
    condicion_cuarentena_resenas,
)


def base_resenas():
    return transform_resenas(
        spark.read.table("bronze.resenas"),
        spark.read.table("silver.productos"),
    )


@dp.table(name="resenas", comment="Resenas limpias y validadas", table_properties={"quality": "silver"})
@dp.expect_all_or_drop(expectations_resenas())
def silver_resenas():
    return base_resenas()


@dp.table(name="resenas_cuarentena", comment="Resenas rechazadas con trazabilidad", table_properties={"quality": "silver"})
def resenas_cuarentena():
    return (
        base_resenas()
        .filter(condicion_cuarentena_resenas())
        .withColumn(
            "motivo_rechazo",
            when(col("calificacion").isNull(), lit("calificacion_nula"))
            .when(col("calificacion") < 1, lit("calificacion_menor_1"))
            .otherwise(lit("calificacion_mayor_5")),
        )
        .withColumn("fec_rechazo", current_timestamp())
    )
