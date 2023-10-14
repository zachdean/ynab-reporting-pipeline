from pulumi_azure_native import storage, resources
from pulumi import Output

def signed_blob_read_url(blob: storage.Blob, container: storage.BlobContainer, account: storage.StorageAccount, resource_group: resources.ResourceGroup) -> Output[str]:
  
  blob_sas = storage.list_storage_account_service_sas_output(
    account_name=account.name,
    protocols=storage.HttpProtocol.HTTPS,
    shared_access_expiry_time="2030-01-01",
    shared_access_start_time="2021-01-01",
    resource_group_name=resource_group.name,
    resource=storage.SignedResource.C,
    permissions=storage.Permissions.R,
    canonicalized_resource=Output.format("/blob/{0}/{1}", account.name, container.name),
    content_type="application/json",
    cache_control="max-age=5",
    content_disposition="inline",
    content_encoding="deflate"
  )
  token = blob_sas.service_sas_token
  return Output.format("https://{0}.blob.core.windows.net/{1}/{2}?{3}", account.name, container.name, blob.name, token)