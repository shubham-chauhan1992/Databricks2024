# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/bronze')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from parquet.`/FileStore/tables/bronze/customers_20240710.parquet`

# COMMAND ----------


