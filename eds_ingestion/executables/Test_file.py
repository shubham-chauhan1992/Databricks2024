# Databricks notebook source
dbutils.fs.ls('/FileStore/tables/')


# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------


df=spark.read.option("header","True").option("inferSchema","True").parquet("/FileStore/tables/bronze/customers/20240713")
df.printSchema()


# COMMAND ----------

df.printSchema()
df.dtypes

# COMMAND ----------

import json

# Get the column names and types from the DataFrame
columns = df.dtypes

# Create a dictionary with column names as keys and types as values
column_dict = {name: col_type for name, col_type in columns}

# Convert the dictionary to a JSON string
json_string = json.dumps(column_dict)

print(json_string)

# COMMAND ----------



# COMMAND ----------

import json
import ipywidgets as widgets
from IPython.display import display

# Get the column names and types from the DataFrame
columns = df.dtypes

# Create a dictionary with column names as keys and types as values
column_dict = {name: col_type for name, col_type in columns}

# Convert the dictionary to a JSON string
json_string = json.dumps(column_dict)

# Create a list to hold the keys from the JSON file
keys = list(json.loads(json_string).keys())

# Create a Dropdown widget with the keys as options and set the name as "column_name"
dropdown = widgets.Dropdown(options=keys, description="column_name")

# Set the widget style to overflow-wrap: anywhere to allow the full description to be visible
dropdown.style.description_width = "auto"

# Display the Dropdown widget
display(dropdown)


basic_checks={
"check_name":"{'NULL','BLANK','TRIM','DECIMAL_STRP','IS_STRING','IS_DECIMAL','IS_ALPHA_NUMERIC','HAS_SPECIAL_CHARACTER','CAST_TO_DECIMAL','CAST_TO_STRING','CAST_TO_DATE','CAST_TO_DATETIME'}",
"check_type":"{'single_step','multi_step'}",
"do_check":"{'Y','N'}",
"format":"{'date','datetime'}",
"drop":"{'Y','N'}",
"collect_rejects":"{'Y','N'}" }

# Create the dropdown widget
dropdown = widgets.Dropdown(options=options, description="Select a value")

# Display the dropdown widget
display(dropdown)

# Create the dropdown widget
dropdown = widgets.Dropdown(options=options, description="Select a value")

# Display the dropdown widget
display(dropdown)

# Create the dropdown widget
dropdown = widgets.Dropdown(options=options, description="Select a value")

# Display the dropdown widget
display(dropdown)

# Create the dropdown widget
dropdown = widgets.Dropdown(options=options, description="Select a value")

# Display the dropdown widget
display(dropdown)



                 




# COMMAND ----------

# DBTITLE 1,h


# COMMAND ----------

import ipywidgets as widgets
from IPython.display import display

# Define the key value pairs
key_value_pairs = {
    "check_name": ['NULL','BLANK','TRIM','DECIMAL_STRP','IS_STRING','IS_DECIMAL','IS_ALPHA_NUMERIC','HAS_SPECIAL_CHARACTER','CAST_TO_DECIMAL','CAST_TO_STRING','CAST_TO_DATE','CAST_TO_DATETIME'],
    "check_type": ['single_step','multi_step'],
    "do_check": ['Y','N'],
    "format": ['date','datetime'],
    "drop": ['Y','N'],
    "collect_rejects": ['Y','N']
}

# Create a dictionary to hold the dropdown widgets
dropdowns = {}

# Create a separate dropdown widget for each key value pair
for key, value in key_value_pairs.items():
    dropdown = widgets.Dropdown(options=value, description=key)
    dropdown.layout.width = 'auto'  # Set the width of the dropdown to auto
    dropdowns[key] = dropdown

# Create a HBox layout to align the dropdown widgets horizontally
hbox = widgets.HBox(list(dropdowns.values()))

# Display the HBox layout
display(hbox)

# COMMAND ----------

import ipywidgets as widgets
from IPython.display import display

# Define the key value pairs
key_value_pairs = {
    "check_name": ['NULL','BLANK','TRIM','DECIMAL_STRP','IS_STRING','IS_DECIMAL','IS_ALPHA_NUMERIC','HAS_SPECIAL_CHARACTER','CAST_TO_DECIMAL','CAST_TO_STRING','CAST_TO_DATE','CAST_TO_DATETIME'],
    "check_type": ['single_step','multi_step'],
    "do_check": ['Y','N'],
    "format": ['date','datetime'],
    "drop": ['Y','N'],
    "collect_rejects": ['Y','N']
}

# Create a dictionary to hold the dropdown widgets
dropdowns = {}

# Create a separate dropdown widget for each key value pair
for key, value in key_value_pairs.items():
    dropdown = widgets.Dropdown(options=value, description=key)
    dropdown.layout.width = 'auto'  # Set the width of the dropdown to auto
    dropdowns[key] = dropdown

# Create a HBox layout to align the dropdown widgets horizontally
hbox = widgets.HBox(list(dropdowns.values()))

# Display the HBox layout
display(hbox)

# COMMAND ----------

display(spark.read.csv("/FileStore/tables/customers_20240712.csv"))
