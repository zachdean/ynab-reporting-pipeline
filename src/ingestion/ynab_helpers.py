import requests
import json
import os

# TODO: change these values when I get the new Resource group created with IoC
STORAGE_ENDPOINT = "csci622store.dfs.core.windows.net" 
CONTAINER = "ynab"
KEY_VAULT_SCOPE = "csci622Kv"
STORAGE_KEY = "csci622-store"
YNAB_USER_TOKEN_KEY="YnabUserToken"
YNAB_BUDGET_KEY="YnabBudgetId"
YNAB_BASE_ENDPOINT="https://api.ynab.com/v1/"
ENCODING = "utf-8"

def set_up_storage_account(spark, dbutils):
    spark.conf.set(
        "fs.azure.account.key." + STORAGE_ENDPOINT,
        dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=STORAGE_KEY))

def fetch_raw_json(endpoint: str, dbutils):
    ynab_api_key = dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=YNAB_USER_TOKEN_KEY)
    ynab_budget_id = dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=YNAB_BUDGET_KEY)

    headers = {"Authorization": f"Bearer {ynab_api_key}"}
    request_uri = os.path.join(f"{YNAB_BASE_ENDPOINT}/budgets/{ynab_budget_id}", endpoint)
    api_response = requests.get(request_uri, headers=headers)

    if api_response.status_code != 200:
        print(f"failed to fetch data, response code {api_response.status_code}")
        print(api_response.content.decode(ENCODING))
        raise Exception("failed to fetch data")
    return json.loads(api_response.content)