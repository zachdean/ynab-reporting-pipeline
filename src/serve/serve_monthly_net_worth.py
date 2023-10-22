import logging
import pandas as pd
import blob_helpers

# although this table is not strictly necessary, it does make working in power bi a little easier
def create_net_worth_fact(connect_str: str):
    transactions_fact = blob_helpers.download_parquet(
    connect_str, "gold/transactions_fact.snappy.parquet")
    
    accounts_dim = blob_helpers.download_parquet(
    connect_str, "gold/accounts_dim.snappy.parquet")

    # order by date desc
    net_worth_fact = _compute_monthly_net_worth(transactions_fact, accounts_dim)

    blob_helpers.upload_parquet(connect_str, "gold/net_worth_fact.snappy.parquet", net_worth_fact)

def _compute_monthly_net_worth(transactions_fact: pd.DataFrame, accounts_dim: pd.DataFrame) -> pd.DataFrame:
    # convert date column to datetime
    transactions_fact["date"] = pd.to_datetime(transactions_fact["date"])

    # merge transactions_fact and accounts_dim DataFrames
    df = pd.merge(transactions_fact, accounts_dim, how="left", left_on="account_id", right_on="id")

    # group by month and asset type, and aggregate amount column
    net_worth_fact = df.groupby([pd.Grouper(key="date", freq="MS"), "asset_type"]).agg({"amount": "sum"}).reset_index()

    # pivot DataFrame to reshape it
    net_worth_fact = net_worth_fact.pivot(index="date", columns="asset_type", values="amount").fillna(0).stack().reset_index(name="amount")

    # add computed fields
    net_worth_fact["running_total"] = net_worth_fact.groupby("asset_type")["amount"].cumsum()
    net_worth_fact["asset_running_total"] = net_worth_fact.apply(lambda row: row["running_total"] if row["asset_type"] == "asset" else None, axis=1)
    net_worth_fact["liability_running_total"] = net_worth_fact.apply(lambda row: row["running_total"] if row["asset_type"] == "liability" else None, axis=1)

    # rename columns
    net_worth_fact = net_worth_fact.rename(columns={"amount": "delta"})

    # order by date desc
    net_worth_fact = net_worth_fact.sort_values(["date", "asset_type"], ascending=[True, True]).reset_index(drop=True)

    return net_worth_fact