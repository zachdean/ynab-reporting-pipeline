# Databricks notebook source
# TODO: change these values when I get the new Resource group created with IoC
storage_end_point = "csci622store.dfs.core.windows.net" 
container = "ynab"
raw_filename="transactions.json"
key_vault_scope = "csci622Kv"
storage_key = "csci622-store"
ynab_key="YnabUserToken"
ynab_budget="YnabBudgetId"
YNAB_BASE_ENDPOINT="https://api.ynab.com/v1/"
ENCODING = "utf-8"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=key_vault_scope, key=storage_key))

ynab_api_key = dbutils.secrets.get(scope=key_vault_scope, key=ynab_key)
ynab_budget_id = dbutils.secrets.get(scope=key_vault_scope, key=ynab_budget)

destination_base_dir = f"abfss://{container}@{storage_end_point}/raw/month/"

# COMMAND ----------

# DBTITLE 1,Define fetch method
import requests
import json

def fetch_raw_json(budget_month: str):
    
    headers = {"Authorization": f"Bearer {ynab_api_key}"}
    api_response = requests.get(f"{YNAB_BASE_ENDPOINT}/budgets/{ynab_budget_id}/months/{budget_month}",
                                headers=headers)

    if api_response.status_code != 200:
        print(f"failed to fetch data, response code {api_response.status_code}")
        print(api_response.content.decode(ENCODING))
        raise Exception("failed to fetch data")
    return json.loads(api_response.content)

# COMMAND ----------

# DBTITLE 1,Save Current Month Budget Data to ADLS
import json

import datetime

current_date = datetime.date.today()
first_day_of_month = datetime.date(current_date.year, current_date.month, 1).strftime("%Y-%m-%d")
current_date_str = current_date.strftime("%Y-%m-%d")

raw_json = fetch_raw_json(first_day_of_month)

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

raw_json = fetch_raw_json(first_day_of_month)

# convert the JSON object to a string
raw_json_str = json.dumps(raw_json)
destination_path = f"{destination_base_dir}{first_day_of_month}/{current_date_str}.json"

# # write the contents of the JSON string to a file in ADLS Gen2 using abfss:// protocol
dbutils.fs.put(destination_path, raw_json_str, overwrite=True)

