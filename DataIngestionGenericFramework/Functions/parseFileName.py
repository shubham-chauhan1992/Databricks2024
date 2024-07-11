# Databricks notebook source

import os

# CREATE A FULL FILE NAME 
def create_source_file_name(param_json,sequence_date):
    filename=param_json["source_file_name"]
    if param_json["timestamp_flag"]!="Y":
        file_name=param_json["source_file_dir"]+"/"+param_json["source_file_name"]
    else:
        
        file_name=param_json["source_file_dir"]+"/"+filename.replace(param_json["source_timestamp_pattern"],sequence_date)

        print("Source File name : ", file_name)

    try:
        if os.path.exists(file_name):
            print("")
        else:
            raise RuntimeError("Source path not found")
    except RuntimeError as e:
        print("Please check source dataset path")

        return file_name


def create_target_file_name(param_json,sequence_date):
    filename=param_json["target_file_name"]
    if not sequence_date:
        file_name=param_json["target_file_dir"]+"/"+param_json["target_file_name"]
    else:
        file_name=param_json["target_file_dir"]+"/"+filename+"/"+str(sequence_date)
    
   

    try:
        if os.path.exists(file_name):
            print("")
        else:
            raise RuntimeError("Target path not found")
    except RuntimeError as e:
        print("please check target dataset path")

    return file_name
