# Databricks notebook source
# Notebook de exemplo:
# - le datos desde a capa bronze
# - realiza limpeza e transformación
# - garda os datos na capa silver

# COMMAND ----------
# Cela 1. Definir parámetros de entrada

dbutils.widgets.text("source_table", "workspace.bronze.invoices_bronze")
dbutils.widgets.text("target_catalog", "workspace")
dbutils.widgets.text("target_schema", "silver")
dbutils.widgets.text("target_table", "invoices_silver")

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
# Cela 4. Lectura da capa bronze

df_bronze = spark.table(source_table)

display(df_bronze)

# COMMAND ----------
# Cela 5. Limpeza e transformación da capa silver

df_silver = (
    df_bronze
    .filter(F.col("InvoiceNo").isNotNull())
    .filter(F.col("CustomerID").isNotNull())
    .filter(F.col("Quantity") > 0)
    .filter(F.col("UnitPrice") > 0)
    .withColumn("Country", F.trim(F.col("Country")))
    .withColumn("Description", F.trim(F.col("Description")))
    .withColumn("CustomerID", F.col("CustomerID").cast("string"))
    .withColumn("total_price", F.col("Quantity") * F.col("UnitPrice"))
)

display(df_silver)

# COMMAND ----------
# Cela 6. Escritura da capa silver

(
    df_silver.write
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
SELECT Country, COUNT(*) AS num_pedidos, SUM(total_price) AS total
FROM {target_table_fqn}
GROUP BY Country
ORDER BY total DESC
""").show()
