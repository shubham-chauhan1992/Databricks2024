# Databricks notebook source
def read_csv(spark,file_name,source_file_options):
    dataframe = spark.read.format("csv").options(**source_file_options).load(file_name)
   return dataframe

def read_excel(spark,file_name,source_file_options):
    dataframe = spark.read.format("com.crealytics.spark.excel").options(**source_file_options).load(file_name)
    return dataframe

def read_json(spark,file_name,source_file_options):
    dataframe= spark.read.format('json').options(**source_file_options).load(file_name)
    return dataframe

def read_text(spark,file_name,source_file_options):
    dataframe=spark.read.format('text').options(**source_file_options).load(file_name)
    return dataframe


