$resourceGroupName = "YnabDataPipeline-rg"

# Get the function app name using the Azure CLI
$functionAppNameStr = az functionapp list --resource-group $resourceGroupName --query "[].name"

Write-Host $functionAppNameStr.GetType()

# Get the first value in the array
$functionAppName = $functionAppNameStr[1].Trim().Trim('"')

# Get the function app's resource ID
$resourceId = az functionapp show --name $functionAppName --resource-group $resourceGroupName --query id --output tsv

# Get the master key
$masterKey = az rest --method post --uri "$resourceId/host/default/listkeys?api-version=2018-11-01" --query masterKey --output tsv

# Print the master key
Write-Host "Master Key"
Write-Host $masterKey
Write-Host
Write-Host "Function App Name"
Write-Host $functionAppName

# # Print the function app name to the console
# Write-Host "Invoking function" + $functionAppName

# # Send the POST request
Write-Host "Invoking function https://$functionAppName.azurewebsites.net/runtime/webhooks/durabletask/orchestrators/ynab_pipeline_orchestrator?code=$masterKey"
$response = Invoke-RestMethod -Uri "https://$functionAppName.azurewebsites.net/runtime/webhooks/durabletask/orchestrators/ynab_pipeline_orchestrator?code=$masterKey" -Method Post

# # Print the response
Write-Host $response
