# Databricks notebook source
# Notebook de exemplo:
# - le datos desde a capa silver
# - crea unha agregación mensual
# - garda o resultado na capa gold

# COMMAND ----------
# Cela 1. Definir parámetros de entrada

dbutils.widgets.text("source_table", "workspace.silver.invoices_silver")
dbutils.widgets.text("target_catalog", "workspace")
dbutils.widgets.text("target_schema", "gold")
dbutils.widgets.text("target_table", "sales_by_month_gold")

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
# Cela 5. Preparación da data e agregación mensual

df_gold = (
    df_silver
    .withColumn("invoice_ts", F.to_timestamp("InvoiceDate", "M/d/yyyy H:mm"))
    .withColumn("invoice_month", F.date_format(F.col("invoice_ts"), "yyyy-MM"))
    .filter(F.col("invoice_month").isNotNull())
    .groupBy("invoice_month")
    .agg(
        F.countDistinct("InvoiceNo").alias("num_pedidos"),
        F.countDistinct("CustomerID").alias("num_clientes"),
        F.round(F.sum("total_price"), 2).alias("total_vendas")
    )
    .orderBy("invoice_month")
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
ORDER BY invoice_month
""").show()
