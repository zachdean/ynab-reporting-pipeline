from typing import Generator
from azure.storage.blob import BlobServiceClient, ContentSettings
import logging
import json
import pandas as pd

def _download_blob(connect_str: str, blob_name: str,) -> dict:
  # Create the BlobServiceClient object
  blob_service_client = BlobServiceClient.from_connection_string(connect_str)

  # Get a reference to the blob
  blob_client = blob_service_client.get_blob_client(container="ynab", blob=blob_name)

  # Download the blob data
  blob_data = blob_client.download_blob().read_all()

  # Parse the blob data as a JSON string
  json_data = json.loads(blob_data)

  return json_data

def _upload_blob(connect_str: str, blob_name: str, raw_json: str) -> int:
  # Create the BlobServiceClient object which will be used to create a container client
  blob_service_client = BlobServiceClient.from_connection_string(connect_str)
  blob_client = blob_service_client.get_blob_client(
      container="ynab", blob=blob_name)
  blob_client.upload_blob(raw_json, overwrite=True, timeout=60, content_settings=ContentSettings(content_type="application/json"))
  logging.info(f"uploaded blob `{blob_name}` with {byte_count} bytes")
  return byte_count

def _transform_transactions(raw_json_obj: dict) -> dict:
  return raw_json_obj

def _unnest_subtransactions(transaction: dict) -> Generator[dict, None, None]:
  """ Unnests subtransactions from a transaction, returns transaction if there are no sub transactions
  """

  if len(transaction["subtransactions"]) == 0 :
    yield transaction
    return transaction
  
  for subtransaction in transaction["subtransactions"]:
    
    yield {
      "id": transaction["id"],
      "date": transaction["date"],
      "amount": subtransaction["amount"],
      "memo": subtransaction["memo"],
      "cleared": transaction["cleared"],
      "approved": transaction["approved"],
      "flag_color": transaction["flag_color"],
      "account_id": transaction["account_id"],
      "account_name": transaction["account_name"],
      "payee_id": transaction["payee_id"],
      "payee_name": transaction["payee_name"],
      "category_id": subtransaction["category_id"],
      "category_name": subtransaction["category_name"],
      "transfer_account_id": transaction["transfer_account_id"],
      "transfer_transaction_id": transaction["transfer_transaction_id"],
      "matched_transaction_id": transaction["matched_transaction_id"],
      "import_id": transaction["import_id"],
      "import_payee_name": transaction["import_payee_name"],
      "import_payee_name_original": transaction["import_payee_name_original"],
      "debt_transaction_type": transaction["debt_transaction_type"],
      "deleted": transaction["deleted"],
    }

def _clean_transaction(transaction: dict) -> dict:
  """ Cleans a transaction by removing subtransactions and unnesting them
  """
  # unnest subtransactions
  return {
    "id": transaction["id"],
    "date": transaction["date"],
    "amount": transaction["amount"] / 1000,
    "memo": transaction["memo"],
    "cleared": transaction["cleared"],
    "approved": transaction["approved"],
    "flag_color": transaction["flag_color"],
    "account_id": transaction["account_id"],
    "account_name": transaction["account_name"],
    "payee_id": transaction["payee_id"],
    "payee_name": transaction["payee_name"],
    "category_id": transaction["category_id"],
    "category_name": transaction["category_name"],
    "transfer_account_id": transaction["transfer_account_id"],
    "transfer_transaction_id": transaction["transfer_transaction_id"],
    "debt_transaction_type": transaction["debt_transaction_type"],
  }

def transform_transactions(connect_str: str) -> int:
  """ Transforms transactions from the ynab api and saves to blob storage

        :param str conn_str:
            A connection string to an Azure Storage account.
  """

  # fetch raw transaction json
  raw_json_obj = _download_blob(connect_str, "raw/transactions.json")

  # transform transaction json
  transformed_json_obj = _transform_transactions(raw_json_obj)

  # save transformed transaction json
  transformed_json = json.dumps(transformed_json_obj)
  blob_name = "transformed/transactions.json"
  return _upload_blob(connect_str, blob_name, transformed_json)