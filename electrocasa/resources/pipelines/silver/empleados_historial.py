from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr


@dp.temporary_view(name="empleados_cdc", comment="Eventos validos preparados para AUTO CDC")
def empleados_cdc():
    return spark.readStream.table("silver.empleados_eventos")


dp.create_streaming_table(
    name="empleados_historial",
    comment="Historial SCD tipo 2 de empleados",
    table_properties={"quality": "silver"},
)


dp.create_auto_cdc_flow(
    target="empleados_historial",
    source="empleados_cdc",
    keys=["id_empleado"],
    sequence_by=col("fecha_evento"),
    apply_as_deletes=expr("tipo_evento = 'baja'"),
    except_column_list=[
        "tipo_evento", "fecha_evento", "fec_ingesta", "sistema_origen",
        "archivo_origen", "id_lote", "_rescued_data",
    ],
    stored_as_scd_type="2",
    name="auto_cdc_empleados_scd2",
)
