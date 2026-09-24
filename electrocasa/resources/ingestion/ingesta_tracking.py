# Databricks notebook source
catalogo = dbutils.widgets.get("catalogo")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalogo}.bronze.tracking_envios AS
SELECT
    *,
    current_timestamp() AS fec_ingesta,
    'azure_sql' AS sistema_origen,
    CAST(NULL AS STRING) AS archivo_origen,
    concat(
        'tracking_',
        date_format(current_timestamp(), 'yyyyMMdd_HHmmss')
    ) AS id_lote
FROM electrocasa_sql_catalog.dbo.trackingenvios
""")