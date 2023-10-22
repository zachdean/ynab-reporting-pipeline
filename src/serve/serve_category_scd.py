from azure.storage.blob import BlobServiceClient
import logging
import pandas as pd
import blob_helpers

def create_category_scd(connect_str: str):
    df = pd.DataFrame()
    
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    container_service = blob_service_client.get_container_client(
        container="ynab")

    # Get a list of blobs in the folder
    blobs = container_service.list_blobs(
        name_starts_with="silver/budget_months/")
    
    # union all blobs in silver/budget_months/

    for blob in blobs:
        logging.info(f"downloading {blob.name}")
        new_df = blob_helpers.download_parquet(connect_str, blob.name)
        df = pd.concat([ df, new_df])

    scd_df = _create_category_sdc(df)

    blob_helpers.upload_parquet(connect_str, "gold/category_scd.snappy.parquet", scd_df)

def _create_category_sdc(df: pd.DataFrame) -> pd.DataFrame:
    agg_funcs = {
        "snapshot_date": ["min", "max"]
    }

    # Apply the aggregation functions to the DataFrame
    grouped_df = df.groupby(["month", "id", "category_group_id", "name", "category_group_name", "budgeted"])\
                .agg(agg_funcs)

    # Flatten the column names of the resulting DataFrame
    grouped_df.columns = ["_".join(col).strip() for col in grouped_df.columns.values]

    # rename columns
    grouped_df = grouped_df.rename(columns={"snapshot_date_min": "start_date", "snapshot_date_max": "end_date"})
    grouped_df = grouped_df.sort_values(by=["month", "start_date"])

    grouped_df["start_date"] = pd.to_datetime(grouped_df["start_date"])
    grouped_df["end_date"] = pd.to_datetime(grouped_df["end_date"])

    # Reset the index of the resulting DataFrame
    grouped_df = grouped_df.reset_index()

    # Group the DataFrame by `month` and `id`, apply the custom function to each group,
    # and retain all of the other columns
    grouped_df = grouped_df.groupby(["month", "id"]).apply(_replace_max_with_none)
    grouped_df = grouped_df.reset_index(drop=True)

    grouped_df = grouped_df.groupby(["month", "id"]).apply(_apply_end_date)

    # Reset the index of the resulting DataFrame
    grouped_df = grouped_df.reset_index(drop=True)

    return grouped_df

def _replace_max_with_none(group):
    group.loc[group["end_date"] == group["end_date"].max(), "end_date"] = None
    return group

def _apply_end_date(group):
    # Shift the `start_date` column by one row and subtract one day from it to get the end date of the next row
    next_start_date = group["start_date"].shift(-1) - pd.Timedelta(days=1)

    # Set the `end_date` value to the end date of the next row
    group.loc[group["end_date"] == group["end_date"].max(), "end_date"] = next_start_date

    return group