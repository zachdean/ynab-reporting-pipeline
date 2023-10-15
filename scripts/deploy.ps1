$resourceGroupName = "YnabDataPipeline-rg"

# Get the function app name using the Azure CLI
$functionAppNameStr = az functionapp list --resource-group $resourceGroupName --query "[].name"

Write-Host $functionAppNameStr.GetType()

# Get the first value in the array
$functionAppName = $functionAppNameStr[1].Trim().Trim('"')

# Print the function app name to the console
Write-Host "Deploy to function" + $functionAppName

# Navigate to the function app directory
Set-Location src

# Publish the function app
func azure functionapp publish $functionAppName --resource-group $resourceGroupName