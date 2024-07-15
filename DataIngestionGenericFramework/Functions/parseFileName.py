# Databricks notebook source
import sys

# CREATE A FULL FILE NAME 
def create_source_file_name(param_json,sequence_date):
    filename=param_json["source_file_name"]
    
    if param_json["timestamp_flag"]!="Y":
        temp_file_nm=param_json["source_file_dir"]+"/"+param_json["source_file_name"]
        if temp_file_nm.startswith("../"):
            file_name=temp_file_nm
        else:
            file_name="dbfs:"+param_json["source_file_dir"]+"/"+param_json["source_file_name"]
            
        
    else:
        
        file_name="dbfs:"+param_json["source_file_dir"]+"/"+filename.replace(param_json["source_timestamp_pattern"],sequence_date)

    print("Source File name : "+file_name)

   
    try:
        dbutils.fs.ls(file_name)
        return file_name
    except Exception as e:
        if 'java.io.FileNotFoundException':
            print("Source file not found")
            sys.exit()
        else:
            raise

    
       


def create_target_file_name(param_json,sequence_date):
    filename=param_json["target_file_name"]
    temp_file_nm=param_json["target_file_dir"]+"/"+param_json["target_file_name"]
    if not sequence_date:
        if temp_file_nm.startswith("../"):
            file_name=temp_file_nm
        else:
            file_name="dbfs:"+param_json["target_file_dir"]+"/"+param_json["target_file_name"]
    else:
        if temp_file_nm.startswith("../"):
            file_name=temp_file_nm
        else:
            file_name="dbfs:"+param_json["target_file_dir"]+"/"+filename+"/"+str(sequence_date)
    
   
    print(file_name)
   
    try:
        dbutils.fs.ls(file_name)
        return file_name
    except Exception as e:
        if 'java.io.FileNotFoundException':
            print("Source file not found")
            sys.exit()
        else:
            raise
        
        

    return file_name

# COMMAND ----------

import os
print(os.getcwd())
