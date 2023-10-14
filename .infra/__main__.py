from pulumi_azure_native import authorization, resources, storage, keyvault, web
import pulumi_azure as azure
import pulumi
import helpers

 # Create an Azure Resource Group
client_config = authorization.get_client_config()
resource_group = resources.ResourceGroup('YnabDataPipeline')
resource_group = resource_group

config = pulumi.Config()

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

deploy_container = storage.BlobContainer('zip_container',
    account_name=storage_account.name,
    resource_group_name=resource_group.name,
    container_name='zip'
)

blob = storage.Blob("zip",
    resource_group_name=resource_group.name,
    account_name=storage_account.name,
    container_name="$web",
    blob_name="functionapp.zip",
    type="Block",
    source=pulumi.FileArchive("./build"))

# Get the primary key of the Storage Account
account_keys = pulumi.Output.all(storage_account.name, resource_group.name).apply(lambda args:
                                                                                  storage.list_storage_account_keys(resource_group_name=args[1], account_name=args[0]))
primary_key = account_keys.apply(
    lambda account_keys: account_keys.keys[0].value)

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
                                        )]))

app_service_plan = web.AppServicePlan("appServicePlan",
                                       resource_group_name=resource_group.name,
                                       location=storage_account.location,
                                       kind="Linux", 
                                       reserved=True,
                                       sku=web.SkuDescriptionArgs(
                                           tier="Standard",
                                           name="S1"))

# Create a secret in Azure Key Vault for the storage account key
storage_key_secret = keyvault.Secret('storageKey',
                                          vault_name=key_vault.name,
                                          resource_group_name=resource_group.name,
                                          properties=keyvault.SecretPropertiesArgs(value=primary_key))

ynab_token_secret = keyvault.Secret('YnabUserToken',
                                          vault_name=key_vault.name,
                                          resource_group_name=resource_group.name,
                                          properties=keyvault.SecretPropertiesArgs(value=config.require_secret('ynabUserToken')))

ynab_budget_id_secret = keyvault.Secret('YnabBudgetId',
                                          vault_name=key_vault.name,
                                          resource_group_name=resource_group.name,
                                          properties=keyvault.SecretPropertiesArgs(value=config.require_secret('ynabBudgetId')))

# use key vault to store the storage account host instead of setting a env
storage_host = storage_account.primary_endpoints.apply(lambda endpoints: endpoints.blob.replace('https://', '').replace('/', ''))
storage_host_secret = keyvault.Secret('storageHost',
                                          vault_name=key_vault.name,
                                          resource_group_name=resource_group.name,
                                          properties=keyvault.SecretPropertiesArgs(value=storage_host))

conn_string = helpers.signed_blob_read_url(blob, deploy_container, storage_account, resource_group)

storage_key_url = pulumi.Output.all(key_vault.name, storage_key_secret.id).apply(lambda l: f"@Microsoft.KeyVault(SecretUri=https://{l[0]}.vault.azure.net/secrets/{storage_key_secret.name}/{l[1]})")
ynab_token_url = pulumi.Output.all(key_vault.name, ynab_token_secret.id).apply(lambda l: f"@Microsoft.KeyVault(SecretUri=https://{l[0]}.vault.azure.net/secrets/{ynab_token_secret.name}/{l[1]})")
ynab_budget_id_url = pulumi.Output.all(key_vault.name, ynab_budget_id_secret.id).apply(lambda l: f"@Microsoft.KeyVault(SecretUri=https://{l[0]}.vault.azure.net/secrets/{ynab_budget_id_secret.name}/{l[1]})")

# Create a Function App
app = web.WebApp("fa",
    resource_group_name=resource_group.name,
    location=resource_group.location,
    server_farm_id=app_service_plan.id,
    site_config=web.SiteConfigArgs(
        app_settings=[
            web.NameValuePairArgs(name="WEBSITE_RUN_FROM_PACKAGE", value=conn_string),
            web.NameValuePairArgs(name="FUNCTIONS_EXTENSION_VERSION", value="~4"),
            web.NameValuePairArgs(name="FUNCTIONS_WORKER_RUNTIME", value="python"),
            web.NameValuePairArgs(name="AzureWebJobsStorage", value=storage_key_url),
            web.NameValuePairArgs(name="YNAB_BASE_ENDPOINT", value=config.require('ynabBaseEndpoint')),
            web.NameValuePairArgs(name="YNAB_BUDGET_ID", value=ynab_budget_id_url),
            web.NameValuePairArgs(name="YNAB_USER_TOKEN_KEY", value=ynab_token_url),
        ],
    ),
    identity=web.ManagedServiceIdentityArgs(
        type=web.ManagedServiceIdentityType.SYSTEM_ASSIGNED,
    ))

# create access policy
access_policy = azure.keyvault.AccessPolicy("accessPolicy",
    key_vault_id=key_vault.id,
    tenant_id=app.identity.tenant_id,
    object_id=app.identity.principal_id,
    key_permissions=[
        "get"
    ],
    secret_permissions=[
        "get"
    ])

# Export the Stack outputs
pulumi.export('ResourceGroupId', resource_group.id)
pulumi.export('StorageAccountId', storage_account.id)
pulumi.export('StorageAccountHost', storage_host)
pulumi.export('VaultId', key_vault.id)
pulumi.export('VaultUrl', key_vault.name.apply(lambda v: f"https://{v}.vault.azure.net/"))
pulumi.export('AppServicePlanName', app_service_plan.name.apply(lambda v: v))