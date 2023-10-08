[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=12156503&assignment_repo_type=AssignmentRepo)
# CSCI 622 Project - YNAB Data Pipeline

You Need a Budget ([YNAB](https://ynab.com)) is a personal budgeting software that allows users to make budgets and then track transactions to stay on that plan. Although the software is great at budgeting and transaction tracking, its reporting functionality is limited. By extracting and cleaning the data, a power bi report can be created to show robust analytics including things like a slowing changing dimension to compare the changes in a budget from the beginning of the month to the end when it is closed out.

# Getting started

## Prerequisites
In order for this project to work, you must follow the steps below:

1. Create a [Azure Subscription](https://learn.microsoft.com/en-us/azure/cost-management-billing/manage/create-subscription)
1. Install the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
1. Create a [Pulumi Account](https://www.pulumi.com/docs/pulumi-cloud/accounts/)
1. Install the [Pulumi CLI](https://www.pulumi.com/docs/install/)

## Deploy Infrastructre
After Pulumi is configured on your machine, you can deploy infrastructure by doing the following

1. Checkout repo
1. Open `./.infra` in a terminal
1. run `pulumi up -s dev` to create the stack



## Ingestion
<Design documentation for ingestion goes here.  Where does the data come from?  How is it ingested?  Where is it stored (be specific)?>

Note - if you came here looking for assignment instructions, go to SupplementaryInfo\CourseInstructions