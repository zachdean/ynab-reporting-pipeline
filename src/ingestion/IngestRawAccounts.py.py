# Databricks notebook source
# TODO: change these values when I get the new Resource group created with IoC
storage_end_point = "csci622store.dfs.core.windows.net" 
container = "ynab"
raw_filename="accounts.json"
key_vault_scope = "csci622Kv"
storage_key = "csci622-store"
ynab_key="YnabUserToken"
ynab_budget="YnabBudgetId"
YNAB_ENDPOINT = "accounts"
YNAB_BASE_ENDPOINT="https://api.ynab.com/v1/"
ENCODING = "utf-8"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=key_vault_scope, key=storage_key))

ynab_api_key = dbutils.secrets.get(scope=key_vault_scope, key=ynab_key)
ynab_budget_id = dbutils.secrets.get(scope=key_vault_scope, key=ynab_budget)

destination_path = f"abfss://{container}@{storage_end_point}/raw/{raw_filename}"

# COMMAND ----------

# DBTITLE 1,Define fetch method
import requests
import json

def fetch_raw_json():
    
    headers = {"Authorization": f"Bearer {ynab_api_key}"}
    api_response = requests.get(f"{YNAB_BASE_ENDPOINT}/budgets/{ynab_budget_id}/{YNAB_ENDPOINT}",
                                headers=headers)

    if api_response.status_code != 200:
        print(f"failed to fetch data, response code {api_response.status_code}")
        print(api_response.content.decode(ENCODING))
        raise Exception("failed to fetch data")
    return json.loads(api_response.content)

# COMMAND ----------

# DBTITLE 1,Save Data to ADLS
import json

raw_json = fetch_raw_json()

# convert the JSON object to a string
raw_json_str = json.dumps(raw_json)

# # write the contents of the JSON string to a file in ADLS Gen2 using abfss:// protocol
dbutils.fs.put(destination_path, raw_json_str, overwrite=True)

