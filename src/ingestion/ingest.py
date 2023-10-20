from azure.storage.blob import BlobServiceClient, ContentSettings
import logging
import json
import datetime
import requests
import os

YNAB_USER_TOKEN_KEY=os.getenv('YNAB_USER_TOKEN_KEY')
YNAB_BUDGET_ID=os.getenv('YNAB_BUDGET_ID')
YNAB_BASE_ENDPOINT=os.getenv('YNAB_BASE_ENDPOINT')
ENCODING = "utf-8"

def _fetch_raw_json(endpoint: str) -> dict:

  headers = {"Authorization": f"Bearer {YNAB_USER_TOKEN_KEY}"}
  request_uri = os.path.join(YNAB_BASE_ENDPOINT,"budgets", YNAB_BUDGET_ID, endpoint).replace('\\','/')
  api_response = requests.get(request_uri, headers=headers)

  logging.info(f"fetched data from {request_uri}, response: {api_response.status_code}")

  if api_response.status_code != 200:
    logging.error(f"failed to fetch data, response code {api_response.status_code}")
    logging.error(api_response.content.decode(ENCODING))
    raise Exception("failed to fetch data")
  return json.loads(api_response.content)

def _upload_blob(connect_str: str, blob_name: str, raw_json: str) -> int:
  # Create the BlobServiceClient object which will be used to create a container client
  blob_service_client = BlobServiceClient.from_connection_string(connect_str)
  blob_client = blob_service_client.get_blob_client(
      container="ynab", blob=blob_name)
  blob_client.upload_blob(raw_json, overwrite=True, timeout=60, content_settings=ContentSettings(content_type="application/json"))
  byte_count = len(raw_json.encode(ENCODING))
  logging.info(f"uploaded blob `{blob_name}` with {byte_count} bytes")
  return byte_count


def load_transactions(connect_str: str) -> int:
  """Loads transactions from the ynab api and saves to blob storage

        :param str conn_str:
            A connection string to an Azure Storage account.
  """

  # fetch raw transaction json
  raw_json_obj = _fetch_raw_json("transactions")
  raw_json = json.dumps(raw_json_obj)

  blob_name = "bronze/transactions.json"
  return _upload_blob(connect_str, blob_name, raw_json)


def load_accounts(connect_str: str) -> int:
  """Loads accounts from the ynab api and saves to blob storage

        :param str conn_str:
            A connection string to an Azure Storage account.
  """

  # fetch raw accounts json
  raw_json_obj = _fetch_raw_json("accounts")
  raw_json = json.dumps(raw_json_obj)

  blob_name = "bronze/accounts.json"

  return _upload_blob(connect_str, blob_name, raw_json)


def load_current_budget_month(connect_str: str) -> int:
  """Loads current budget month from the ynab api and saves to blob storage

        :param str conn_str:
            A connection string to an Azure Storage account.
  """

  current_date = datetime.date.today()
  first_day_of_month = datetime.date(
      current_date.year, current_date.month, 1).strftime("%Y-%m-%d")
  current_date_str = current_date.strftime("%Y-%m-%d")

  # fetch raw accounts json
  route = f"months/{first_day_of_month}"
  raw_json_obj = _fetch_raw_json(route)
  raw_json = json.dumps(raw_json_obj)

  blob_name = f"bronze/month/{first_day_of_month}/{current_date_str}.json"
  return _upload_blob(connect_str, blob_name, raw_json)


def load_previous_budget_month(connect_str: str) -> int:
  """Loads the previous budget month from the ynab api and saves to blob storage

        :param str conn_str:
            A connection string to an Azure Storage account.
  """

  # decide which month to load
  current_date = datetime.date.today()
  month_offset = -1
  if current_date.day > 15:
      return

  first_day_of_month = datetime.date(
      current_date.year, current_date.month + month_offset, 1).strftime("%Y-%m-%d")
  current_date_str = current_date.strftime("%Y-%m-%d")

  # fetch raw budget month json
  route = f"months/{first_day_of_month}"
  raw_json_obj = _fetch_raw_json(route)
  raw_json = json.dumps(raw_json_obj)

  blob_name = f"bronze/month/{first_day_of_month}/{current_date_str}.json"
  return _upload_blob(connect_str, blob_name, raw_json)
