# Databricks notebook source
# MAGIC %run "../../DataIngestionGenericFramework/Functions/selectSourceToReadFrom" 

# COMMAND ----------

# MAGIC %run "../../DataIngestionGenericFramework/Functions/writeTarget"

# COMMAND ----------


from datetime import datetime,timedelta
import json

#business_day=dbutils.widgets.get("business_day")
business_day=2
current_time = datetime.now().strftime("%Y%m%d")
seq_date = (datetime.now() - timedelta(days=int(business_day))).strftime("%Y%m%d")

params_file="../../../Databricks2024/eds_ingestion/configs/customer_data_transformation_bronze_2_silver.json"

# PARSE THE PARAMETER FILE
with open(params_file) as f:
    param=json.load(f)

print("Starting extraction process : "+ datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))
print("\n")
print("BUSINESS_DATE : "+seq_date)
print("PARAMETER FILE PATH : ",params_file)
print("PARAMETER DETAILS : ",param["param"])


#READ CSV FILE BY CALLING THE SOURCE_2_BRONZE FUNCTION BY PASSING THE PARAMETER FILE PRESENT INSIDE CONFIG FLOLDER
sourceDf=selectSourceFormat(param["param"],spark,seq_date)
print(sourceDf.count())
writeDF=sourceDf
write_target(param["param"],writeDF,seq_date)

print("\n")
print("Extraction process completed successfully : " + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"))

# COMMAND ----------


