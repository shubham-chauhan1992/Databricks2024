# Databricks notebook source
# MAGIC %run "../../../Databricks2024/Data_ingestion_generic_framework/Functions/customer_source_2_parquet_bronze"

# COMMAND ----------

import datetime

current_time = datetime.datetime.now()
print("Starting extraction process : "+ current_time.strftime("%Y-%m-%d %H:%M:%S.%f"))

params_file="Project_1/configs/customers_extraction_param.json"
sequence_date=${sequence_date}

#READ CSV FILE BY CALLING THE SOURCE_2_BRONZE FUNCTION BY PASSING THE PARAMETER FILE PRESENT INSIDE CONFIG FLOLDER
bronze_2_silver(params_file,spark,sequence_date)

print("Extraction process completed successfully : " + datetime.datetime.now().current_time.strftime("%Y-%m-%d %H:%M:%S.%f"))
