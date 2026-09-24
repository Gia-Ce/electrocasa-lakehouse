from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, concat_ws, lit


def read_autoloader_stream(
    spark: SparkSession,
    source_path: str,
    schema_location: str,
    schema,
    file_format: str,
    header: bool = True,
    delimiter: str = ",",
    multi_line: bool = False,
) -> DataFrame:
    """Lee una fuente concreta con Auto Loader.

    La configuracion es explicita por fuente. Para ElectroCasa hay pocas fuentes
    y cada una tiene particularidades conocidas, por lo que se prioriza claridad
    y trazabilidad sobre un framework metadata-driven generico.
    """

    reader = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", file_format)
        .option("cloudFiles.schemaLocation", schema_location)
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
    )

    if file_format == "csv":
        reader = (
            reader
            .option("header", str(header).lower())
            .option("delimiter", delimiter)
        )

    if file_format == "json":
        reader = reader.option("multiLine", str(multi_line).lower())

    return reader.load(source_path)


def add_audit_columns(df: DataFrame, system_source: str = "file") -> DataFrame:
    """Agrega trazabilidad tecnica comun a las tablas Bronze basadas en archivos."""

    return (
        df
        .withColumn("fec_ingesta", current_timestamp())
        .withColumn("sistema_origen", lit(system_source))
        .withColumn("archivo_origen", col("_metadata.file_name"))
        .withColumn(
            "id_lote",
            concat_ws(
                "_",
                col("_metadata.file_name"),
                col("_metadata.file_modification_time").cast("string"),
            ),
        )
    )
