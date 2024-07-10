# Databricks notebook source
def write_target(dataframe,target_path,target_file_options):
    writeDf=dataframe.write.format('parquet').options(**target_options).save(target_path)

   
 

    
