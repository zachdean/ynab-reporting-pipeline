import pulumi
from pulumi_azure_native import authorization, resources, storage, keyvault, databricks

DATABRICKS_PRINCIPLE_ID = "be99d3e2-db24-4c41-b1bc-5c9d6ee14815"

 # Create an Azure Resource Group
client_config = authorization.get_client_config()
resource_group = resources.ResourceGroup('YnabDataPipeline')
resource_group = resource_group

# Create an Azure Storage Account
storage_account = storage.StorageAccount('sa',
                                          resource_group_name=resource_group.name,
                                          location=resource_group.location,
                                          sku=storage.SkuArgs(
                                              name=storage.SkuName.STANDARD_LRS),
                                          kind=storage.Kind.STORAGE_V2)

# create the ynab container
container = storage.BlobContainer('ynab_container',
    account_name=storage_account.name,
    resource_group_name=resource_group.name,
    container_name='ynab'
)

# Get the primary key of the Storage Account
account_keys = pulumi.Output.all(storage_account.name, resource_group.name).apply(lambda args:
                                                                                  storage.list_storage_account_keys(resource_group_name=args[1], account_name=args[0]))
primary_key = account_keys.apply(
    lambda account_keys: account_keys.keys[0].value)

# Create a DataBricks Workspace
workspace = databricks.Workspace('YnabPipeline',
                                      resource_group_name=resource_group.name,
                                      location=resource_group.location,
                                      managed_resource_group_id=f'/subscriptions/{client_config.subscription_id}/resourceGroups/Managed_Databricks',
                                      sku=databricks.SkuArgs(name="standard"))

# Create an Azure Key Vault
key_vault = keyvault.Vault('kv',
                                resource_group_name=resource_group.name,
                                location=resource_group.location,
                                properties=keyvault.VaultPropertiesArgs(
                                    tenant_id=client_config.tenant_id,
                                    sku=keyvault.SkuArgs(
                                        family=keyvault.SkuFamily.A, name=keyvault.SkuName.STANDARD),
                                    enable_rbac_authorization=False,
                                    access_policies=[
                                        keyvault.AccessPolicyEntryArgs(
                                            object_id=client_config.object_id,
                                            permissions=keyvault.PermissionsArgs(                                              
                                                secrets= ["get", "list", "set", "delete", "backup", "restore", "recover", "purge"],
                                                certificates= ["get", "list", "delete", "create", "import", "update", "managecontacts", "getissuers", "listissuers", "setissuers", "deleteissuers", "manageissuers", "recover", "backup", "restore", "manageissuers", "setissuers", "deleteissuers"]
                                            ),
                                            tenant_id=client_config.tenant_id
                                        ),
                                        keyvault.AccessPolicyEntryArgs(
                                            object_id=DATABRICKS_PRINCIPLE_ID,
                                            permissions=keyvault.PermissionsArgs(
                                                secrets= ["get", "list"],
                                            ),
                                            tenant_id=client_config.tenant_id
                                        )]))

# Create a secret in Azure Key Vault for the storage account key
storage_key_secret = keyvault.Secret('storageKey',
                                          vault_name=key_vault.name,
                                          resource_group_name=resource_group.name,
                                          properties=keyvault.SecretPropertiesArgs(value=primary_key))

# use key vault to store the storage account host instead of setting a env
storage_host = storage_account.primary_endpoints.apply(lambda endpoints: endpoints.blob.replace('https://', '').replace('/', ''))
storage_host_secret = keyvault.Secret('storageHost',
                                          vault_name=key_vault.name,
                                          resource_group_name=resource_group.name,
                                          properties=keyvault.SecretPropertiesArgs(value=storage_host))

# Export the Stack outputs
pulumi.export('ResourceGroupId', resource_group.id)
pulumi.export('StorageAccountId', storage_account.id)
pulumi.export('StorageAccountHost', storage_host)
pulumi.export('VaultId', key_vault.id)
pulumi.export('VaultUrl', key_vault.name.apply(lambda v: f"https://{v}.vault.azure.net/"))
pulumi.export('WorkspaceId', workspace.id)