from datetime import datetime
import logging
import pandas as pd
import blob_helpers


def validate_transactions_fact(connect_str: str):
  # load the accounts_dim table
  transactions_df = blob_helpers.download_parquet(
    connect_str, "gold/transactions_fact.snappy.parquet")
  accounts_df = blob_helpers.download_parquet(
    connect_str, "gold/accounts_dim.snappy.parquet")
  
  _validate_transactions_fact(transactions_df, accounts_df)  

def validate_net_worth_fact(connect_str: str):
  # load the accounts_dim table
  net_worth_df = blob_helpers.download_parquet(
    connect_str, "gold/net_worth_fact.snappy.parquet")
  accounts_df = blob_helpers.download_parquet(
    connect_str, "gold/accounts_dim.snappy.parquet")
  
  _validate_net_worth_fact(net_worth_df, accounts_df)

def _validate_transactions_fact(transactions_df: pd.DataFrame, accounts_df: pd.DataFrame):
  transactions_grouped = transactions_df.groupby("account_id").sum("amount")
  failed_count = 0
  for index, account in accounts_df.iterrows():
    account_id = account["account_id"]
    account_balance = account["balance"]
    if account_id in transactions_grouped.index:
      transaction_balance = transactions_grouped.loc[account_id]["amount"]

      # round to 3 decimal places to fix floating point errors
      if account_balance != round(transaction_balance, 3):
        account_name = account["name"]
        failed_count += 1
        logging.warn(f"Account {account_name} has a balance of {account_balance} but a transaction balance of {transaction_balance}, with a difference of {account_balance - transaction_balance}")
  
  if (failed_count == 0):
    logging.info("All accounts validated successfully")
    return
  
  raise Exception(f"{failed_count} account(s) failed validation")

def _validate_net_worth_fact(net_worth_df: pd.DataFrame, accounts_df: pd.DataFrame):
  net_worth_delta = round(net_worth_df["delta"].sum(), 3)
  actual_net_worth = round(accounts_df["balance"].sum(), 3)
  
  if net_worth_delta != actual_net_worth:
    raise Exception(f"Net worth delta of {net_worth_delta} does not match actual net worth of {actual_net_worth}")
  
  logging.info("Net worth validated successfully")