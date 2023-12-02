from azure.storage.blob import BlobServiceClient
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def download_parquet(connect_str: str, blob_name: str) -> pd.DataFrame:
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Get a reference to the blob
    blob_client = blob_service_client.get_blob_client(
        container="ynab", blob=blob_name)

    # Download the blob data
    blob_data = blob_client.download_blob().readall()
    reader = pa.BufferReader(blob_data)
    table = pq.read_table(reader)
    df = table.to_pandas()
    logging.info(f"downloaded blob `{blob_name}` with {len(blob_data)} bytes")
    return df


def upload_parquet(connect_str: str, blob_name: str, df: pd.DataFrame) -> int:

    # save data as parquet using pyarrow
    table = pa.Table.from_pandas(df)

    # Write the table to a buffer as a Parquet file
    buffer = pa.BufferOutputStream()
    pq.write_table(table, buffer)
    data = buffer.getvalue().to_pybytes()

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container="ynab", blob=blob_name)
    blob_client.upload_blob(data, overwrite=True)
    byte_count = len(data)
    logging.info(f"uploaded blob `{blob_name}` with {byte_count} bytes")
    return byte_count
