# Databricks notebook source
# Notebook de exemplo:
# - le datos desde a capa silver
# - crea unha agregación por país
# - garda o resultado na capa gold

# COMMAND ----------
# Cela 1. Definir parámetros de entrada

dbutils.widgets.text("source_table", "workspace.silver.invoices_silver")
dbutils.widgets.text("target_catalog", "workspace")
dbutils.widgets.text("target_schema", "gold")
dbutils.widgets.text("target_table", "sales_by_country_gold")

# COMMAND ----------
# Cela 2. Imports e recuperación de parámetros

from pyspark.sql import functions as F

source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")

target_table_fqn = f"{target_catalog}.{target_schema}.{target_table}"

print(f"Orixe: {source_table}")
print(f"Destino: {target_table_fqn}")

# COMMAND ----------
# MAGIC %sql
# MAGIC -- Cela 3. Crear esquema de destino
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:target_catalog || '.' || :target_schema);

# COMMAND ----------
# Cela 4. Lectura da capa silver

df_silver = spark.table(source_table)

display(df_silver)

# COMMAND ----------
# Cela 5. Agregación da capa gold por país

df_gold = (
    df_silver
    .groupBy("Country")
    .agg(
        F.count("*").alias("num_linhas"),
        F.countDistinct("InvoiceNo").alias("num_pedidos"),
        F.round(F.sum("total_price"), 2).alias("total_vendas"),
        F.round(F.avg("total_price"), 2).alias("ticket_medio")
    )
    .orderBy(F.col("total_vendas").desc())
)

display(df_gold)

# COMMAND ----------
# Cela 6. Escritura da capa gold

(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table_fqn)
)

print(f"Táboa creada: {target_table_fqn}")

# COMMAND ----------
# Cela 7. Comprobación final

display(spark.table(target_table_fqn))

# COMMAND ----------
# Cela 8. Consulta de validación

spark.sql(f"""
SELECT *
FROM {target_table_fqn}
ORDER BY total_vendas DESC
""").show()
