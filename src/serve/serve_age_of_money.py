import pandas as pd
import blob_helpers
import queue
INCOME_CATEGORY = "Inflow: Ready to Assign"


def create_age_of_money_fact(connect_str: str) -> int:
    transactions_fact = blob_helpers.download_parquet(
        connect_str, "silver/transactions.snappy.parquet")

    accounts_dim = blob_helpers.download_parquet(
        connect_str, "gold/accounts_dim.snappy.parquet")

    # order by date desc
    age_of_money_fact = _compute_monthly_age_of_money(
        transactions_fact, accounts_dim)

    return blob_helpers.upload_parquet(connect_str, "gold/age_of_money_fact.snappy.parquet", age_of_money_fact)


def _compute_monthly_age_of_money(transactions_fact: pd.DataFrame, accounts_dim: pd.DataFrame) -> pd.DataFrame:
    # convert date column to datetime
    transactions_fact["date"] = pd.to_datetime(transactions_fact["date"])

    # merge transactions_fact and accounts_dim DataFrames
    df = pd.merge(transactions_fact, accounts_dim, how="inner",
                  left_on="account_id", right_on="account_id")

    # group by month and asset type, and aggregate amount column
    grouped_df = df[df["on_budget"]].groupby([pd.Grouper(
        key="date", freq="D"), "category_name"]).agg({"amount": "sum"}).reset_index()

    inflow_df = grouped_df[grouped_df["category_name"] == INCOME_CATEGORY].sort_values(
        "date", ascending=True).reset_index(drop=True)
    outflow_df = grouped_df[grouped_df["category_name"] != INCOME_CATEGORY].groupby(
        pd.Grouper(key="date", freq="D")).agg({"amount": "sum"}).reset_index()

    # create a queue
    income_buckets = queue.Queue()

    # fill the queue with inflow_df rows
    for i in range(len(inflow_df)):
        row = inflow_df.loc[i]
        income_buckets.put({
            "date": row["date"],
            "amount": row["amount"]
        })

    carrying_balance = 0
    age_of_money = []
    bucket = income_buckets.get()
    for _, row in outflow_df.iterrows():
        bucket["amount"] += row["amount"]

        if (bucket["amount"] <= 0):
            carrying_balance = bucket["amount"]

            # if there are no more buckets then age of money is negative,
            # this allows for that edge case and age of money can go negative
            if (not income_buckets.empty()):
                bucket = income_buckets.get()

            bucket["amount"] += carrying_balance

        # add rows to get daily age of money
        age_of_money.append(
            {"date": row["date"], "age_of_money": (row["date"] - bucket["date"]).days})

    age_of_money_fact = pd.DataFrame(age_of_money)
    return age_of_money_fact
