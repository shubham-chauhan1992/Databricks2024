# Databricks notebook source
# MAGIC %run "../../DataIngestionGenericFramework/Functions/selectSourceToReadFrom" 

# COMMAND ----------

# MAGIC %run "../../DataIngestionGenericFramework/Functions/writeTarget"

# COMMAND ----------

from datetime import datetime
import json

params_file="../../../Databricks2024/eds_ingestion/configs/customers_metadata.json"
with open(params_file) as f:
    param=json.load(f)

print("Starting metadata extraction process : "+ datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
print("\n")
print("PARAMETER FILE PATH : ",params_file)
print("PARAMETER DETAILS : ",param["param"])
#READ  FILE BY CALLING THE SOURCE_2_BRONZE FUNCTION BY PASSING THE PARAMETER FILE PRESENT INSIDE CONFIG FLOLDER

sourceDf=selectSourceFormat(param["param"],spark,"YYYYMMDD")
display(sourceDf)
write_metadata_json(sourceDf,param["param"])

# print("\n")
# print("Extraction process completed successfully : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
