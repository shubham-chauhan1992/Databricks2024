# Databricks notebook source

# parameter details that we will use after parsing the parameter file created in json format in the configs dir

# source_type: Choose from the value ("file","table")
# file_type: Choose from the value ("json","csv","excel","parquet","text")
# source_file_name: Specify the file name or file name pattern present in the DBFS mnt path
# source_file_dir: Specify the DBricks file system path
# source_schema_name: Specify the schema name in case you source type is a table
# surce_table_name: Specify the table name in case you source type is a table
# basic_mapping: Set it to "true" or "false" -> It applys basic trimming functions on decimal and string fields
# target_type:Choose from the value ("file","table")
# target_file_nm: Mostly it is "parquet" 
# target_file_dir: Specify the DBricks file system path
# target_schema_name: Specify the schema name in case you target type is a table
# target_table_name: Specify the table name in case you target type is a table



# COMMAND ----------

# MAGIC %run "./readSourceFiles"
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "./parseFileName"

# COMMAND ----------

# MAGIC %run "./writeParquet"

# COMMAND ----------



# READ DATA FROM A SOURCE FILE AND WRITE IT INTO THE BRONZE ZONE
def bronze_2_silver(param_json,spark,sequence_date):
  
    source_file_options=param_json["source_file_options"]
    target_file_options=param_json["target_file_options"]

    # CREATE FILE NAME
    file_name=create_source_file_name(param_json,sequence_date)

    # CHOOSE THE FILE TYPE WE WANT TO PROCESS
    if(param_json["source_type"]=="file" and param_json["file_type"]=="csv"):
         sourceDf=read_csv(spark,file_name,source_file_options)
    
    if(param_json["source_type"]=="file" and param_json["file_type"]=="json"):
         sourceDf=read_json(spark,file_name,source_file_options)

    if(param_json["source_type"]=="file" and param_json["file_type"]=="text"):
         sourceDf=read_text(spark,file_name,source_file_options)
    
    #if(param_json["source_type"]=="file" and param_json["file_type"]=="xml"):
         #sourceDf=read_xml(spark,file_name,source_file_options)

    if(param_json["source_type"]=="file" and param_json["file_type"]=="excel"):
         sourceDf=read_excel(spark,file_name,source_file_options)


    #CHOOSE THE CUSTOM FUNCTION FOR CUSTOM MAPPING THAT YOU WANT TO PASS -- placeholder for future development

    #DEFAULT MAPPING 
    writeDf=sourceDf
    #CREATE TARGET FILE NAME
    file_name=create_target_file_name(param_json,sequence_date)
    #WRITE THE DATA INTO THE TARGET
    write_target(param_json,writeDf,file_name,target_file_options)

        

    







     
    


    
   





    




