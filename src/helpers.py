from azure.storage.blob import BlobServiceClient, ContentSettings
import logging

def upload_blob(connect_str: str, blob_name: str, raw_json: str) -> int:
    ENCODING = "utf-8"
    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    blob_client = blob_service_client.get_blob_client(
        container="ynab", blob=blob_name)
    blob_client.upload_blob(raw_json, overwrite=True, timeout=60, content_settings=ContentSettings(content_type="application/json"))
    byte_count = len(raw_json.encode(ENCODING))
    logging.info(f"uploaded blob `{blob_name}` with {byte_count} bytes")
    return byte_count