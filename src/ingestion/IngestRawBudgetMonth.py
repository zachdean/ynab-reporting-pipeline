# Databricks notebook source
import ynab_helpers

ynab_helpers.set_up_storage_account(spark, dbutils)

destination_base_dir = f"abfss://{ynab_helpers.CONTAINER}@{ynab_helpers.STORAGE_ENDPOINT}/raw/month/"

# COMMAND ----------

# DBTITLE 1,Save Current Month Budget Data to ADLS
import json
import datetime

current_date = datetime.date.today()
first_day_of_month = datetime.date(current_date.year, current_date.month, 1).strftime("%Y-%m-%d")
current_date_str = current_date.strftime("%Y-%m-%d")

route = f"months/{first_day_of_month}"
raw_json = ynab_helpers.fetch_raw_json(route, dbutils)

# convert the JSON object to a string
raw_json_str = json.dumps(raw_json)
destination_path = f"{destination_base_dir}{first_day_of_month}/{current_date_str}.json"

# # write the contents of the JSON string to a file in ADLS Gen2 using abfss:// protocol
dbutils.fs.put(destination_path, raw_json_str, overwrite=True)


# COMMAND ----------

# DBTITLE 1,Save Offset Month Budget Data to ADLS
import json
import datetime

current_date = datetime.date.today()
month_offset = -1
if current_date.day > 15:
    month_offset = 1

first_day_of_month = datetime.date(current_date.year, current_date.month + month_offset, 1).strftime("%Y-%m-%d")
current_date_str = current_date.strftime("%Y-%m-%d")

route = f"months/{first_day_of_month}"
raw_json = ynab_helpers.fetch_raw_json(route, dbutils)

# convert the JSON object to a string
raw_json_str = json.dumps(raw_json)
destination_path = f"{destination_base_dir}{first_day_of_month}/{current_date_str}.json"

# # write the contents of the JSON string to a file in ADLS Gen2 using abfss:// protocol
dbutils.fs.put(destination_path, raw_json_str, overwrite=True)

