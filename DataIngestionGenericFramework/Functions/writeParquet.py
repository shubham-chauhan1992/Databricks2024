# Databricks notebook source
def write_target(dataframe,target_path,target_file_options):
    print("Writing Target file in dataframe with following options " , target_file_options )
    writeDf=dataframe.write.format('parquet').options(**target_file_options).save(target_path)

   
 

    
