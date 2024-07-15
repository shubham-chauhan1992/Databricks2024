# Databricks notebook source
import json

def write_schema(dataframe,bronze_layer_file_path,schema_name):

    # Get the column names and types from the DataFrame
    columns = df.dtypes

    # Create a dictionary with column names as keys and types as values
    column_dict = {name: col_type for name, col_type in columns}

    # Convert the dictionary to a JSON string
    json_string = json.dumps(column_dict)

    return json_string



