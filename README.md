[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12156503&assignment_repo_type=AssignmentRepo)
# CSCI 622 Project - YNAB Data Pipeline

You Need a Budget ([YNAB](https://ynab.com)) is a personal budgeting software that allows users to make budgets and then track transactions to stay on that plan. Although the software is great at budgeting and transaction tracking, its reporting functionality is limited. By extracting and cleaning the data, a power bi report can be created to show robust analytics including things like a slowing changing dimension to compare the changes in a budget from the beginning of the month to the end when it is closed out.

# Getting started

## Architecture change log
There has been several architectural changes to the app, the changes are captured in [./docs/decision_history](./docs/decision_history).

## Prerequisites
In order for this project to work, you must follow the steps below:

1. Create a [Azure Subscription](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/create-subscription)
1. Install the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
1. Install the [Azure Bicep](https://learn.microsoft.com/en-us/azure/azure-resource-manager/bicep/install)
1. Install [Azure Functions Core Tools](https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local?tabs=windows%2Cisolated-process%2Cnode-v4%2Cpython-v2%2Chttp-trigger%2Ccontainer-apps&pivots=programming-language-powershell#install-the-azure-functions-core-tools)
1. For local development, create local settings file `src/local.settings.json`, it should have the following at a minimum

``` json
{
  "IsEncrypted": false,
  "Values": {
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "AzureWebJobsFeatureFlags": "EnableWorkerIndexing",
    "AzureWebJobsStorage": "The storage account connection string",
    "YNAB_BASE_ENDPOINT":"https://api.ynab.com/v1/",
    "YNAB_USER_TOKEN_KEY":"Your YNAB user token",
    "YNAB_BUDGET_ID":"Your budget id"
  }
}
```

## Deploy Infrastructure

- create `local.parameters.json` based on `sample.parameters.json` adding the following keys. This step can omitted if you want to enter the params during the deployment
  - `ynabUserToken`
  - `ynabBudgetId`
- open this directory in a terminal
- run 
``` PowerShell
az deployment sub create --location southcentralus --template-file .\ynab-data-pipeline.bicep --parameters .\local.parameters.json
```
  - Note: if you omit `--parameters` then you will be prompted to enter required parameters. The command would be 
``` PowerShell
az deployment sub create --location southcentralus --template-file .\ynab-data-pipeline.bicep
```

## Deploy Function

`# Deploy the function app code
run `./script/deploy.ps1`

Note: you may need to unblock powershell script execution on your machine, you can do it by running the following command `Set-ExecutionPolicy -ExecutionPolicy Undefined -Scope CurrentUser`

## YNAB Api User Token

The YNAB Api token must be fetched from the YNAB application (see [quick start](https://api.ynab.com/)). After the token is fetched it must be added to the create Azure Key Vault. In addition, you must get the budget to the run the pipeline from the YNAB application url (`https://app.ynab.com/{Take this value}/budget/202310`, the value should like this `f2b1c1f9-5d5d-4e2d-8c6e-9c2b7d5c4d6e`, the full url would be `https://app.ynab.com/f2b1c1f9-5d5d-4e2d-8c6e-9c2b7d5c4d6e/budget/202310`).

After the values are collected, they need to be stored in the create Azure Key Vault. Deploy the infrastructure to get the values properly in KeyVault and app settings.

## Ingestion

Note: to use mocked data, change the function application settings `YNAB_BASE_ENDPOINT` to `https://<functionName>.azurewebsites.net/api/mocks/`. This load static files from the function itself that should demonstrate the pipeline

### Source

the [YNAB API](https://api.ynab.com/v1) provides all of the necessary endpoints to get data from. Below are the specific endpoints currently used.

#### Get Transactions

**Specification:** [Get Transactions](https://api.ynab.com/v1#/Transactions/getTransactions)

**Description of use:** after the data is cleaned it is used for the bulk of the analysis downstream.

**Poll Frequency:** Daily at 0242 CST  
**Durability:** Replaced daily  
**Storage Type:** Raw payload  
**Storage Location:** `ynab/raw/transactions.json`
#### Get Accounts

**Specification:** [Get Accounts](https://api.ynab.com/v1#/Accounts/getAccounts)

**Description of use:** Holds two important pieces of information, the type of the account (asset vs liability) and if it is a debt account, the interest rate and minimum payment information. Also used to validate the clean of the transactions.

**Poll Frequency:** Daily at 0242 CST  
**Durability:** Replaced daily  
**Storage Type:** Raw payload  
**Storage Location:** `ynab/raw/accounts.json`

#### Get Budget Month

**Specification:** [Get Budget Month](https://api.ynab.com/v1#/Months/getBudgetMonth)

**Description of use:** Used to get the budgeted amount on for each category every month

**Poll Frequency:** Each month is polled daily at 0242 CST from the 16th of the previous month to the 15th of the current month. For an example, on 14 October 2023, the Budget for September 2023 and October 2023 will be polled and stored. On 20 October 2023, the Budget for October 2023 and November 2023 will be polled and stored. The reason for this is that a user may be fixing there budget a couple of days into the next month and we want to capture the changes. (TODO: decide if I want to update this to only poll a month from the first day of the month to the 15th of the next month)  
**Durability:** Replaced daily  
**Storage Type:** Raw payload  
**Storage Location:** `ynab/raw/month/{BudgetMonth}/{RunDay}.json`

