from azure.storage.blob import BlobServiceClient, ContentSettings
from date_helpers import add_month
import logging
import json
import datetime
import requests
import os
import gzip

YNAB_USER_TOKEN_KEY = os.getenv('YNAB_USER_TOKEN_KEY')
YNAB_BUDGET_ID = os.getenv('YNAB_BUDGET_ID')
YNAB_BASE_ENDPOINT = os.getenv('YNAB_BASE_ENDPOINT')
ENCODING = "utf-8"


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


def load_current_budget_month(connect_str: str, current_date: datetime.datetime) -> int:
    """Loads current budget month from the ynab api and saves to blob storage

          :param str conn_str:
              A connection string to an Azure Storage account.
    """

    first_day_of_month = current_date.strftime("%Y-%m-01")
    current_date_str = current_date.strftime("%Y-%m-%d")

    # fetch raw accounts json
    route = f"months/{first_day_of_month}"
    raw_json_obj = _fetch_raw_json(route)
    raw_json = json.dumps(raw_json_obj)

    blob_name = f"bronze/month/{first_day_of_month}/{current_date_str}.json"
    return _upload_blob(connect_str, blob_name, raw_json)


def load_previous_budget_month(connect_str: str, current_date: datetime.datetime) -> int:
    """Loads the previous budget month from the ynab api and saves to blob storage

          :param str conn_str:
              A connection string to an Azure Storage account.
    """

    # load previous month
    month_offset = -1
    first_day_of_month = add_month(current_date, month_offset).strftime("%Y-%m-01")
    current_date_str = current_date.strftime("%Y-%m-%d")

    # fetch raw budget month json
    route = f"months/{first_day_of_month}"
    raw_json_obj = _fetch_raw_json(route)
    raw_json = json.dumps(raw_json_obj)

    blob_name = f"bronze/month/{first_day_of_month}/{current_date_str}.json"
    return _upload_blob(connect_str, blob_name, raw_json)


def _fetch_raw_json(endpoint: str) -> dict:

    headers = {"Authorization": f"Bearer {YNAB_USER_TOKEN_KEY}"}
    request_uri = os.path.join(
        YNAB_BASE_ENDPOINT, "budgets", YNAB_BUDGET_ID, endpoint).replace('\\', '/')
    api_response = requests.get(request_uri, headers=headers)

    logging.info(
        f"fetched data from {request_uri}, response: {api_response.status_code}")

    if api_response.status_code != 200:
        logging.error(
            f"failed to fetch data, response code {api_response.status_code}")
        logging.error(api_response.content.decode(ENCODING))
        raise Exception("failed to fetch data")
    return json.loads(api_response.content)


def _upload_blob(connect_str: str, blob_name: str, raw_json: str) -> int:
    # Compress the raw_json string
    compressed_data = gzip.compress(raw_json.encode(ENCODING))

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container="ynab", blob=blob_name)

    # Upload the compressed data to the blob storage
    blob_client.upload_blob(compressed_data, overwrite=True, timeout=60, content_settings=ContentSettings(
        content_type="application/json", content_encoding="gzip"))

    byte_count = len(compressed_data)
    logging.info(
        f"uploaded compressed blob `{blob_name}` with {byte_count} bytes")
    return byte_count
