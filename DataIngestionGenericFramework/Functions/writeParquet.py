# Databricks notebook source
def write_target(param_json,dataframe,target_path,target_file_options):
    print("Writing Target file in dataframe with following options " , target_file_options )
    writeDf=dataframe.write.format('parquet').options(**target_file_options).mode(param_json["write_mode"]).save(target_path)

   
 

    

# COMMAND ----------


