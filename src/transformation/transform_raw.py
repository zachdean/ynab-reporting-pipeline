from typing import Generator, Iterable
from azure.storage.blob import BlobServiceClient
import logging
import json
import pandas as pd
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import blob_helpers


def transform_transactions(connect_str: str) -> int:
    """ Transforms transactions from the ynab api and saves to blob storage

          :param str conn_str:
              A connection string to an Azure Storage account.
    """

    # fetch raw transaction json
    raw_transactions = _download_blob(
        connect_str, "bronze/transactions.json")["data"]["transactions"]
    accounts = _download_blob(
        connect_str, "bronze/accounts.json")["data"]["accounts"]

    # transform transaction json
    transactions = _transform_transactions(raw_transactions, accounts)

    # Define the transaction schema
    schema = {
        "id": str,
        "date": "datetime64[ns]",
        "amount": float,
        "memo": str,
        "cleared": str,
        "approved": bool,
        "flag_color": str,
        "account_id": str,
        "account_name": str,
        "payee_id": str,
        "payee_name": str,
        "category_id": str,
        "category_name": str,
        "transfer_account_id": str,
        "transfer_transaction_id": str,
        "debt_transaction_type": str
    }
    blob_name = "silver/transactions.snappy.parquet"
    return _upload_blob(connect_str, blob_name, transactions, schema)


def transform_accounts(connect_str: str) -> int:
    """ Transforms accounts from the ynab api and saves to blob storage

          :param str conn_str:
              A connection string to an Azure Storage account.
    """

    # fetch raw transaction json
    raw_accounts = _download_blob(
        connect_str, "bronze/accounts.json")["data"]["accounts"]

    # transform transaction json
    accounts = map(_clean_account, raw_accounts)

    # Define the schema as a dictionary
    schema = {
        "id": str,
        "name": str,
        "type": str,
        "on_budget": bool,
        "closed": bool,
        "note": str,
        "balance": float,
        "cleared_balance": float,
        "uncleared_balance": float,
        "deleted": bool
    }

    blob_name = "silver/accounts.snappy.parquet"
    return _upload_blob(connect_str, blob_name, accounts, schema)


def transform_budget_month(connect_str: str, month: str) -> int:
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_service = blob_service_client.get_container_client(
        container="ynab")

    # Get a list of blobs in the folder
    blobs = container_service.list_blobs(
        name_starts_with=f"bronze/month/{month}/")

    months = []
    for blob in blobs:
        print(blob.name)
        snapshot_date = blob.name.split("/")[-1].replace(".json", "")
        print(f"snapshot_date: {snapshot_date}")
        raw_month = _download_blob(connect_str, blob.name)["data"]["month"]
        months.extend(_clean_budget_month(raw_month, snapshot_date))

    schema = {
        "id": str,
        "month": "datetime64[ns]",
        "snapshot_date": "datetime64[ns]",
        "category_group_id": str,
        "category_group_name": str,
        "name": str,
        "hidden": bool,
        "budgeted": float,
        "activity": float,
        "balance": float,
    }

    blob_name = f"silver/budget_months/{month}.snappy.parquet"
    return _upload_blob(connect_str, blob_name, months, schema)


def _download_blob(connect_str: str, blob_name: str) -> dict:
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Get a reference to the blob
    blob_client = blob_service_client.get_blob_client(
        container="ynab", blob=blob_name)

    # Download the blob data
    blob_data = blob_client.download_blob().readall()

    # Parse the blob data as a JSON string
    json_data = json.loads(blob_data)

    return json_data


def _upload_blob(connect_str: str, blob_name: str, data: Iterable[dict], schema: dict) -> int:

    # save data as parquet using pyarrow
    df = pd.DataFrame(data, columns=schema.keys()).astype(schema)
    return blob_helpers.upload_parquet(connect_str, blob_name, df)


def _transform_transactions(transactions: list[dict], accounts: list[dict]) -> Iterable[dict]:

    transactions = _add_mortgage_payments(transactions, accounts)

    clean_transactions = []
    for transaction in transactions:
        for subTransaction in _unnest_subtransactions(transaction):
            clean_transactions.append(_clean_transaction(subTransaction))

    return clean_transactions


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
        # Future work allow for the decimal floating point to be dynamically determined
        # from the /budgets/{budgetId}/settings endpoint
        "amount": round(transaction["amount"] / 1000, 2),
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
        "payee_name": f"{account['name']} Interest/Escrow Payment",
        "category_id": None,
        "category_name": None,
        "transfer_account_id": None,
        "transfer_transaction_id": None,
        "debt_transaction_type": "interest",
        "subtransactions": []
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
        "payee_name": f"{account['name']} Interest/Escrow Payment",
        "category_id": None,
        "category_name": None,
        "transfer_account_id": None,
        "transfer_transaction_id": None,
        "debt_transaction_type": "escrow",
        "subtransactions": []
    }


def _fetch_value_from_date(json_obj: dict, date: pd.Timestamp) -> int:

    sorted_items = sorted(json_obj.items(), key=lambda x: x[0], reverse=True)

    # Loop through the sorted items
    for key, value in sorted_items:
        if pd.to_datetime(key) <= date:
            return value

    return 0


def _clean_account(account: dict) -> dict:
    return {
        "id": account["id"],
        "name": account["name"],
        "type": account["type"],
        "on_budget": account["on_budget"],
        "closed": account["closed"],
        "note": account["note"],
        # Future work allow for the decimal floating point to be dynamically determined
        # from the /budgets/{budgetId}/settings endpoint
        "balance": round(account["balance"] / 1000, 2),
        "cleared_balance": round(account["cleared_balance"] / 1000, 2),
        "uncleared_balance": round(account["uncleared_balance"] / 1000, 2),
        "deleted": account["deleted"],
    }


def _clean_budget_month(budget_month: dict, snapshot_date: str) -> Generator[dict, None, None]:

    for raw_category in budget_month["categories"]:
        category = _clean_category(raw_category)

        category["month"] = budget_month["month"]
        category["snapshot_date"] = snapshot_date
        yield category


def _clean_category(category: dict) -> dict:
    return {
        "id": category["id"],
        "category_group_id": category["category_group_id"],
        "category_group_name": category["category_group_name"],
        "name": category["name"],
        "hidden": category["hidden"],
        "budgeted": round(category["budgeted"] / 1000, 2),
        "activity": round(category["activity"] / 1000, 2),
        "balance": round(category["balance"] / 1000, 2),
    }
