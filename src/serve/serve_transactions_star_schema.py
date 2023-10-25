from datetime import datetime
import blob_helpers


def create_transactions_fact(connect_str: str):
    # Define the list of column names that you want to keep
    keep_cols = ["id", "date", "amount", "account_id",
                 "payee_id", "category_id", "debt_transaction_type"]

    df = blob_helpers.download_parquet(
        connect_str, "silver/transactions.snappy.parquet")

    # Drop columns that are not in the list of column names that you want to keep
    drop_cols = set(df.columns) - set(keep_cols)
    df = df.drop(columns=drop_cols)

    blob_helpers.upload_parquet(
        connect_str, "gold/transactions_fact.snappy.parquet", df)


def create_category_dim(connect_str: str):
    # Define the list of column names that you want to keep
    keep_cols = ["id", "name", "category_group_id",
                 "category_group_name", "hidden"]

    # we could use the scd table, but we always know that the current month should be available allowing us to
    # process in parallel
    now = datetime.today()
    month = datetime(now.year, now.month, 1).strftime("%Y-%m-%d")
    df = blob_helpers.download_parquet(
        connect_str, f"silver/budget_months/{month}.snappy.parquet")

    # Group the DataFrame by the relevant columns and get the index of the row with the maximum `snapshot_date`
    idx = df.groupby(keep_cols)["snapshot_date"].idxmax()

    # Select the rows with the maximum `snapshot_date`
    df = df.loc[idx, keep_cols]
    
    df = df.reset_index(drop=True)

    df = df.rename(columns={"id": "category_id"})

    blob_helpers.upload_parquet(
        connect_str, "gold/category_dim.snappy.parquet", df)


def create_accounts_dim(connect_str: str):
    # asset liability map
    asset_map = {
        'checking': 'asset',
        'savings': 'asset',
        'cash': 'asset',
        'otherAsset': 'asset',
        'creditCard': 'liability',
        'lineOfCredit': 'liability',
        'otherLiability': 'liability',
        'mortgage': 'liability',
        'autoLoan': 'liability',
        'studentLoan': 'liability',
        'personalLoan': 'liability',
        'medicalDebt': 'liability',
        'otherDebt': 'liability'
    }
    # Define the list of column names that you want to keep
    keep_cols = [
        "id",
        "name",
        "type",
        "asset_type",
        "on_budget",
        "closed",
        "balance",
        "deleted",
    ]

    df = blob_helpers.download_parquet(
        connect_str, "silver/accounts.snappy.parquet")

    df["asset_type"] = df["type"].map(asset_map)

    drop_cols = set(df.columns) - set(keep_cols)
    df = df.drop(columns=drop_cols)
    df = df.rename(columns={"id": "account_id"})

    

    blob_helpers.upload_parquet(
        connect_str, "gold/accounts_dim.snappy.parquet", df)


def create_payee_dim(connect_str: str):
    df = blob_helpers.download_parquet(
        connect_str, "silver/transactions.snappy.parquet")

    df = df[["payee_id", "payee_name"]]\
        .drop_duplicates()\
        .rename(columns={"payee_name": "name"})\
        .reset_index(drop=True)

    blob_helpers.upload_parquet(
        connect_str, "gold/payee_dim.snappy.parquet", df)
