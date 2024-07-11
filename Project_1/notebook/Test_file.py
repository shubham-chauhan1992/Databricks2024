# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/bronze/customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from parquet.`/FileStore/tables/bronze/customers/*`

# COMMAND ----------


