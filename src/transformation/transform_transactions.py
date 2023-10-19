from typing import Generator, Iterable
from azure.storage.blob import BlobServiceClient, ContentSettings
import logging
import json
import pandas as pd
import uuid


def _download_blob(connect_str: str, blob_name: str,) -> dict:
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Get a reference to the blob
    blob_client = blob_service_client.get_blob_client(
        container="ynab", blob=blob_name)

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
    blob_client.upload_blob(raw_json, overwrite=True, timeout=60,
                            content_settings=ContentSettings(content_type="application/json"))
    logging.info(f"uploaded blob `{blob_name}` with {byte_count} bytes")
    return byte_count


def _transform_transactions(raw_json_obj: dict) -> dict:
    return raw_json_obj


def _unnest_subtransactions(transaction: dict) -> Generator[dict, None, None]:
    """ Unnests subtransactions from a transaction, returns transaction if there are no sub transactions
    """

    if len(transaction["subtransactions"]) == 0:
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
    """ Cleans a transaction by removing unneeded fields and converting the amount to a decimal
    """
    return {
        "id": transaction["id"],
        "date": transaction["date"],
        # there is issues with converting to a decimal at this point, so we drop the extra zero
        # and convert to decimal in a later step. Future work allow for the decimal floating point
        # to be dynamically determined from the /budgets/{budgetId}/settings endpoint
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


def _add_mortgage_payments(transactions: list[dict], accounts: list[dict]) -> list[dict]:
    transactions_df = pd.DataFrame(transactions)

    transaction_groups = transactions_df.groupby("account_id")

    account_id: str
    group: pd.DataFrame
    for account_id, group in transaction_groups:

        account_arr = [
            account for account in accounts if account["id"] == account_id]
        if len(account_arr) == 0:
            continue

        account = account_arr[0]

        if len(account["debt_interest_rates"]) == 0:
            continue

        # convert to datetime so that date functions will work
        group["date"] = group['date'].apply(lambda x: x[:8] + "01")
        group["date"] = pd.to_datetime(group["date"])

        # Group by date and sum the amounts
        months_df = group.groupby(pd.Grouper(key="date", freq="MS"))[
            "amount"].sum().reset_index()

        # Loop through row to calculate interest
        runningTotal = 0
        for index, row in months_df.sort_values("date").iterrows():

            if index == 0:
                runningTotal += row["amount"]
                continue

            interest_transaction = _create_interest_transaction(
                row, runningTotal, account)
            runningTotal += interest_transaction["amount"]
            transactions.append(interest_transaction)

            if len(account["debt_escrow_amounts"]) > 0:
                escrow_transaction = _create_escrow_transaction(row, account)
                runningTotal += escrow_transaction["amount"]
                transactions.append(escrow_transaction)

            runningTotal += row["amount"]

    return transactions


def _create_interest_transaction(row: pd.Series, runningTotal: float, account: dict) -> dict:

    # calculate interest
    interest_rate = _fetch_value_from_date(
        account["debt_interest_rates"], row["date"]) / 100000
    interest = int(round(runningTotal * (interest_rate / 12.0)))
    return {
        "id": str(uuid.uuid4()),
        "date": row["date"].strftime("%Y-%m-%d"),
        "amount": interest,
        "memo": "",
        "cleared": "reconciled",
        "approved": True,
        "flag_color": None,
        "account_id": account["id"],
        "account_name": account["name"],
        "payee_id": account["id"],
        "payee_name": f"{account['name']} Interest Payment",
        "category_id": None,
        "category_name": None,
        "transfer_account_id": None,
        "transfer_transaction_id": None,
        "debt_transaction_type": "interest"
    }


def _create_escrow_transaction(row: pd.Series, account: dict) -> dict:
    escrow_amount = _fetch_value_from_date(
        account["debt_escrow_amounts"], row["date"])
    return {
        "id": str(uuid.uuid4()),
        "date": row["date"].strftime("%Y-%m-%d"),
        "amount": -escrow_amount,
        "memo": "",
        "cleared": "reconciled",
        "approved": True,
        "flag_color": None,
        "account_id": account["id"],
        "account_name": account["name"],
        "payee_id": account["id"],
        "payee_name": f"{account['name']} Escrow Payment",
        "category_id": None,
        "category_name": None,
        "transfer_account_id": None,
        "transfer_transaction_id": None,
        "debt_transaction_type": "escrow"
    }


def _fetch_value_from_date(json_obj: dict, date: pd.Timestamp) -> str:

    sorted_items = sorted(json_obj.items(), key=lambda x: x[0], reverse=True)

    # Loop through the sorted items
    for key, value in sorted_items:
        if pd.to_datetime(key) <= date:
            return value

    return sorted_items[0][1]


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
