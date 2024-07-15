# Databricks notebook source
# MAGIC %run "./parseFileName"

# COMMAND ----------

import os

def write_target(param_json,dataframe,sequence_date):
    target_file_options=param_json["target_file_options"]
    file_name=create_target_file_name(param_json,sequence_date)
    print("Writing Target file in dataframe with following options " , target_file_options )
    dataframe.write.format('parquet').options(**target_file_options).mode(param_json["write_mode"]).save(file_name)


def write_metadata_json(dataframe,param):
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    workspace_path=notebook_path.split(param["workspace_name"])[0]
    json_output = dataframe.toJSON().collect()
    file_path="/Workspace"+workspace_path+param["workspace_name"]+param["target_file_dir"]+"/"+param["target_file_name"]
    print(file_path)
    # Write the JSON output to a file
    with open(file_path, "w") as f:
        for line in json_output:
            f.write(line + "\n")

   
   
 

    
