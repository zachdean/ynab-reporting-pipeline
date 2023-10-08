# Databricks notebook source
import ynab_helpers

ynab_helpers.set_up_storage_account(spark, dbutils)

raw_filename="accounts.json"
YNAB_ENDPOINT = "accounts"
destination_path = f"abfss://{ynab_helpers.CONTAINER}@{ynab_helpers.STORAGE_ENDPOINT}/raw/{raw_filename}"

# COMMAND ----------

# DBTITLE 1,Save Data to ADLS
import json

# raw_json = fetch_raw_json()
raw_json = ynab_helpers.fetch_raw_json(YNAB_ENDPOINT, dbutils)

# convert the JSON object to a string
raw_json_str = json.dumps(raw_json)

# # write the contents of the JSON string to a file in ADLS Gen2 using abfss:// protocol
dbutils.fs.put(destination_path, raw_json_str, overwrite=True)

