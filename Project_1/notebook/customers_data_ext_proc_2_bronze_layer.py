# Databricks notebook source
# MAGIC %run "../../DataIngestionGenericFramework/Functions/customerSourceToParquetBronze" 

# COMMAND ----------


from datetime import datetime,timedelta
import json
import os
#business_day=dbutils.widgets.get("business_day")
business_day=1
current_time = datetime.now().strftime("%Y%m%d")
seq_date = (datetime.now() - timedelta(days=int(business_day))).strftime("%Y%m%d")
params_file="../../../Databricks2024/Project_1/configs/customers_extraction_param.json"

# PARSE THE PARAMETER FILE
with open(params_file) as f:
    param=json.load(f)

print("Starting extraction process : "+ datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
print("\n")
print("BUSINESS_DATE : "+seq_date)
print("PARAMETER FILE PATH : ",params_file)
print("PARAMETER DETAILS : ",param["param"])


#READ CSV FILE BY CALLING THE SOURCE_2_BRONZE FUNCTION BY PASSING THE PARAMETER FILE PRESENT INSIDE CONFIG FLOLDER
bronze_2_silver(param["param"],spark,seq_date)

print("\n")
print("Extraction process completed successfully : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
