targetScope='subscription'

param resourceGroupName string = 'ynabdatapipeline'
param resourceGroupLocation string = 'southcentralus'

@secure()
@description('The YNAB API token for the user that will be used for the data pipeline')
param ynabUserToken string

@description('The ID of the budget to be used for the data pipeline')
param ynabBudgetId string

param ynabBasUrl string = 'https://api.ynab.com/v1/'

resource ynabRG 'Microsoft.Resources/resourceGroups@2022-09-01' = {
  name: resourceGroupName
  location: resourceGroupLocation
}

module functionApp 'modules/function-app.bicep' = {
  name: 'functionApp'
  scope: ynabRG
  params: {
    location: ynabRG.location
    ynabUserToken: ynabUserToken
    ynabBudgetId: ynabBudgetId
    ynabBaseUrl: ynabBasUrl
  }
}
