# Databricks notebook source
import os
import pandas as pd


def read_csv(spark,file_name,source_file_options):
    print("Loading Source file in dataframe with following options " , source_file_options )
    dataframe = spark.read.format("csv").options(**source_file_options).load(file_name)
    return dataframe

def read_excel(spark,file_name,source_file_options):
    print("Loading Source file in dataframe with following options " , source_file_options )
    excelDf= pd.read_excel(file_name.replace("dbfs:","/dbfs/"))
    dataframe=spark.createDataFrame(excelDf)
    return dataframe

def read_json(spark,file_name,source_file_options):
    print("Loading Source file in dataframe with following options " , source_file_options )
    dataframe= spark.read.format('json').options(**source_file_options).load(file_name)
    return dataframe

def read_text(spark,file_name,source_file_options):
    print("Loading Source file in dataframe with following options " , source_file_options )
    dataframe=spark.read.format('text').options(**source_file_options).load(file_name)
    return dataframe


