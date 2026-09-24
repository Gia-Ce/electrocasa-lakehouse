# Databricks notebook source
catalogo = dbutils.widgets.get("catalogo")
path_landing = dbutils.widgets.get("path_landing")
ruta = f"{path_landing}/productos"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.bronze.productos
""")

spark.sql(f"""
COPY INTO {catalogo}.bronze.productos
FROM (
    SELECT
        *,
        current_timestamp() AS fec_ingesta,
        'file' AS sistema_origen,
        _metadata.file_name AS archivo_origen,
        concat(
            _metadata.file_name,
            '_',
            cast(_metadata.file_modification_time AS STRING)
        ) AS id_lote
    FROM '{ruta}'
)
FILEFORMAT = JSON
FORMAT_OPTIONS (
    'mergeSchema' = 'true',
    'multiLine' = 'true'
)
COPY_OPTIONS (
    'mergeSchema' = 'true'
)
""")