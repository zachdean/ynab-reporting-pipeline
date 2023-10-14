# Discoveries

## Common traits

all data sources had some common traits listed below

- All amount fields are in miliunits and need to be divided by 1000 to convert to USD
- datas are in the form yyyy-MM-dd

## Transactions

schema
``` json
{
        "id": "3be0b817-2a3b-48f1-a1ec-a40cdbad155f",
        "date": "2020-09-01",
        "amount": -190670,
        "memo": "",
        "cleared": "reconciled",
        "approved": true,
        "flag_color": null,
        "account_id": "ddf0f0a9-40ea-4e0d-ab77-594034b53916",
        "account_name": "Cash Flow",
        "payee_id": "d16a8efd-1ac0-442a-9757-487698f120b7",
        "payee_name": "Sam's Club",
        "category_id": "7efdb98e-42bc-4973-a82b-6d4ff34c9da9",
        "category_name": "Split",
        "transfer_account_id": null,
        "transfer_transaction_id": null,
        "matched_transaction_id": "3147d5a8-fced-481f-ae38-de4c90d024c3",
        "import_id": null,
        "import_payee_name": null,
        "import_payee_name_original": null,
        "debt_transaction_type": null,
        "deleted": false,
        "subtransactions": [
          {
            "id": "e393d466-19a7-4c16-b6ec-f03f8f0a1633",
            "transaction_id": "3be0b817-2a3b-48f1-a1ec-a40cdbad155f",
            "amount": -72000,
            "memo": null,
            "payee_id": null,
            "payee_name": null,
            "category_id": "cbe8569d-fa46-4c70-823a-84ba40d3705b",
            "category_name": "👨‍👩‍👦‍👦 Kids",
            "transfer_account_id": null,
            "transfer_transaction_id": null,
            "deleted": false
          },
          {
            "id": "3893474b-fae6-4e51-b253-bc134d47c651",
            "transaction_id": "3be0b817-2a3b-48f1-a1ec-a40cdbad155f",
            "amount": -153190,
            "memo": null,
            "payee_id": null,
            "payee_name": null,
            "category_id": "f793ec38-d669-4013-a4ba-dd4e26413288",
            "category_name": "🛒 Groceries",
            "transfer_account_id": null,
            "transfer_transaction_id": null,
            "deleted": false
          },
          {
            "id": "e6649666-c994-4758-9d01-86b4c9fd82d7",
            "transaction_id": "3be0b817-2a3b-48f1-a1ec-a40cdbad155f",
            "amount": -14480,
            "memo": null,
            "payee_id": null,
            "payee_name": null,
            "category_id": "b3075bbc-4f32-4ae9-a3e8-f7ffd2f228c1",
            "category_name": "🧻 Home Supplies",
            "transfer_account_id": null,
            "transfer_transaction_id": null,
            "deleted": false
          }
        ]
      }
```

the traits unique to transactions are as follows

1. The sum of all transactions in a account should equal the account balance in accounts
1. When a transaction is split between categories, the splits are in subtransactions, the sum of amounts of the subtransactions equals the amount of the transaction
2. mortgage accounts are special accounts that calculate the escrow and interest payments and the transactions do not appear in the transaction list. As such, the escrow and interest payments need to be calculated so that the account balance matches the mortgage balance

## Accounts

schema
``` json
{
  "data": {
    "accounts": [
      {
        "id": "5faa6501-dff7-49e8-8868-ae715a349d8c",
        "name": "A Cool House",
        "type": "mortgage",
        "on_budget": false,
        "closed": false,
        "note": null,
        "balance": -100000000,
        "cleared_balance": 0,
        "uncleared_balance": 0,
        "transfer_payee_id": "246d3216-0659-4422-b773-b80a98c18739",
        "direct_import_linked": false,
        "direct_import_in_error": false,
        "last_reconciled_at": "2023-09-01T16:30:00.000Z",
        "debt_original_balance": 0,
        "debt_interest_rates": {
          "2021-10-01": 5000
        },
        "debt_minimum_payments": {
          "2021-10-01": 800000
        },
        "debt_escrow_amounts": {
          "2021-10-01": 250000,
          "2022-02-01": 200000,
          "2023-09-01": 300000
        },
        "deleted": false
      },
    ],
    "server_knowledge": 22000
  }
}
```

the traits unique to accounts are as follows

- contains whether or not the account is tracked on the budget `on_budget`. Transactions are not categorized on accounts not on the budget.
- the account balance can be used for validation after the transaction un-nesting.
- for debt accounts (primarily mortgage accounts, unsure of other accounts that behave the same way), special fields are added that allow for the computation of interest and escrow amounts.
  - `debt_interest_rates`: in a miliunits, for an example `5000` converts to `5.0%` or `.05` in decimal form
  - `debt_escrow_amounts`: the amount that is applied to the escrow every month in miliunits


## Categories

schema
``` json
{
  "data": {
    "month": {
      "month": "2022-09-01",
      "note": null,
      "income": 10000000,
      "budgeted": 10000000,
      "activity": -5000000,
      "to_be_budgeted": 0,
      "age_of_money": 30,
      "deleted": false,
      "categories": [
        {
          "id": "4310255e-f731-4a55-85a8-54ca42b9add7",
          "category_group_id": "4e6d7949-9f14-481e-91da-c29cbfeb7f94",
          "category_group_name": "Utilities",
          "name": "⚡/💧 Electric/Water",
          "hidden": false,
          "original_category_group_id": null,
          "note": null,
          "budgeted": 100000,
          "activity": -100000,
          "balance": 0,
          "goal_type": null,
          "goal_day": null,
          "goal_cadence": null,
          "goal_cadence_frequency": null,
          "goal_creation_month": null,
          "goal_target": 0,
          "goal_target_month": null,
          "goal_percentage_complete": null,
          "goal_months_to_budget": null,
          "goal_under_funded": null,
          "goal_overall_funded": null,
          "goal_overall_left": null,
          "deleted": false
        }
      ]
    }
  }
}
```

the traits unique to Categories are as follows
- all categories in `categories` property
- `category_group` is on every category and this is the only way to derive the groups