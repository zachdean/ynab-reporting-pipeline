import sys
from typing import Generator
import unittest
import pandas as pd
import json
import os

# Add the directory containing the modules to the `PYTHONPATH`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import src.transformation.transform_raw as transform

# Get the path of the current file
current_file_path = os.path.dirname(__file__)

class SubTransactionsTestCase(unittest.TestCase):

    def test_given_nested_transactions_when_unnest_subtransactions_then_subtransctions_unnested(self):
        # arrange
        with open(os.path.join(current_file_path, 'resources/nested_transaction.json'), 'r', encoding='utf-8') as f:
            transactions = json.load(f)
        with open(os.path.join(current_file_path, 'resources/expected_subtransactions.json'), 'r', encoding='utf-8') as f:
            expected = json.load(f)

        # act
        unnested = list(transform._unnest_subtransactions(transactions))

        # assert
        self.assertEqual(len(unnested), 2)
        self.assertListEqual(unnested, expected)

    def test_given_unnested_transactions_when_unnest_subtransactions_then_transctions_returned(self):
        # arrange
        with open(os.path.join(current_file_path, 'resources/unnested_transaction.json'), 'r', encoding='utf-8') as f:
            transaction = json.load(f)

        # act
        unnested = list(transform._unnest_subtransactions(transaction))

        # assert
        self.assertEqual(len(unnested), 1)
        self.assertDictEqual(unnested[0], transaction)


class TransactionsTestCase(unittest.TestCase):

    def test_given_raw_transactions_when_transaction_cleaned_then_transaction_data_shaped(self):
        # arrange
        with open(os.path.join(current_file_path, 'resources/unnested_transaction.json'), 'r', encoding='utf-8') as f:
            transaction = json.load(f)
        with open(os.path.join(current_file_path, 'resources/expected_cleaned_transaction.json'), 'r', encoding='utf-8') as f:
            expected = json.load(f)

        # act
        cleaned_transaction = transform._clean_transaction(transaction)

        # assert
        self.assertDictEqual(cleaned_transaction, expected)


class MortgageTransactionsTestCase(unittest.TestCase):

    def test_given_accounts_when_transaction_cleaned_then_mortgage_payments_added(self):
        # arrange
        with open(os.path.join(current_file_path, 'resources/mortgage_transactions.json'), 'r', encoding='utf-8') as f:
            transactions = json.load(f)
        with open(os.path.join(current_file_path, 'resources/accounts.json'), 'r', encoding='utf-8') as f:
            accounts = json.load(f)
        with open(os.path.join(current_file_path, 'resources/expected_mortgage_transactions.json'), 'r', encoding='utf-8') as f:
            expected = json.load(f)

        # act
        actual = transform._add_mortgage_payments(transactions, accounts)

        # assert
        actual_df = pd.DataFrame(actual)
        expected_df = pd.DataFrame(expected)

        self.assertEqual(len(actual_df), len(expected_df))

        # convert to two digit decimal since that is the final format of the data
        actual_df['amount'] = round(actual_df['amount'] / 1000, 2)
        expected_df['amount'] = round(expected_df['amount'] / 1000, 2)

        # assert on difference because of floating point comparison errors
        self.assertEqual(
            abs(actual_df["amount"].sum() - expected_df["amount"].sum()) < 0.01, True)


class TestEscrowAmountFromDate(unittest.TestCase):
    def setUp(self):
        # Arrange: Set up the account as a class field
        self.account = {"id": "123", "name": "Test Account", "debt_escrow_amounts": {"2022-01-01": 100}}

    def test_create_escrow_transaction(self):
        # Arrange: Set up the row and expected keys
        row = pd.Series({"date": pd.Timestamp('2022-01-01')})
        expected_keys = ["id", "date", "amount", "memo", "cleared", "approved", "flag_color", "account_id", "account_name", "payee_id", "payee_name", "category_id", "category_name", "transfer_account_id", "transfer_transaction_id", "debt_transaction_type", "subtransactions"]

        # Act: Call the _create_escrow_transaction() function with the row and account
        actual = transform._create_escrow_transaction(row, self.account)

        # Assert: Check that the actual dictionary has the expected keys
        self.assertEqual(list(actual.keys()), expected_keys)

    def test_create_escrow_transaction_amount(self):
        # Arrange: Set up the row and expected amount
        row = pd.Series({"date": pd.Timestamp('2022-01-01')})
        expected_amount = -100

        # Act: Call the _create_escrow_transaction() function with the row and account
        actual = transform._create_escrow_transaction(row, self.account)

        # Assert: Check that the actual amount matches the expected amount
        self.assertEqual(actual["amount"], expected_amount)

    def test_create_escrow_transaction_date(self):
        # Arrange: Set up the row and expected date
        row = pd.Series({"date": pd.Timestamp('2022-01-01')})
        expected_date = "2022-01-01"

        # Act: Call the _create_escrow_transaction() function with the row and account
        actual = transform._create_escrow_transaction(row, self.account)

        # Assert: Check that the actual date matches the expected date
        self.assertEqual(actual["date"], expected_date)


class TestCreateInterestTransaction(unittest.TestCase):

    def setUp(self):
        # Arrange: Set up the account as a class field
        self.account = {"id": "123", "name": "Test Account", "debt_interest_rates": {"2022-01-01": 50000}}

    def test_create_interest_transaction(self):
        # Arrange: Set up the row and runningTotal, and expected keys
        row = pd.Series({"date": pd.Timestamp('2022-01-01')})
        runningTotal = 1000
        expected_keys = ["id", "date", "amount", "memo", "cleared", "approved", "flag_color", "account_id", "account_name", "payee_id", "payee_name", "category_id", "category_name", "transfer_account_id", "transfer_transaction_id", "debt_transaction_type", "subtransactions"]

        # Act: Call the _create_interest_transaction() function with the row, runningTotal, and account
        actual = transform._create_interest_transaction(row, runningTotal, self.account)

        # Assert: Check that the actual dictionary has the expected keys
        self.assertEqual(list(actual.keys()), expected_keys)

    def test_create_interest_transaction_amount(self):
        # Arrange: Set up the row and runningTotal, and expected amount
        row = pd.Series({"date": pd.Timestamp('2022-01-01')})
        runningTotal = -1000000
        expected_amount = -41667

        # Act: Call the _create_interest_transaction() function with the row, runningTotal, and account
        actual = transform._create_interest_transaction(row, runningTotal, self.account)

        # Assert: Check that the actual amount matches the expected amount
        self.assertEqual(actual["amount"], expected_amount)

    def test_create_interest_transaction_date(self):
        # Arrange: Set up the row and runningTotal, and expected date
        row = pd.Series({"date": pd.Timestamp('2022-01-01')})
        runningTotal = 1000
        expected_date = "2022-01-01"

        # Act: Call the _create_interest_transaction() function with the row, runningTotal, and account
        actual = transform._create_interest_transaction(row, runningTotal, self.account)

        # Assert: Check that the actual date matches the expected date
        self.assertEqual(actual["date"], expected_date)


class TestFetchValueFromDate(unittest.TestCase):

    def setUp(self):
        # class level set up
        self.json_obj = {'2022-01-01': 100, '2022-02-01': 200, '2022-03-01': 300}

    def test_within_range(self):
        # Arrange: Set up the date and expected value
        date = pd.Timestamp('2022-01-02')
        expected_value = 100

        # Act: Call the _fetch_value_from_date() function with the JSON object, date, and expected value
        actual_value = transform._fetch_value_from_date(self.json_obj, date)

        # Assert: Check that the actual value matches the expected value
        self.assertEqual(actual_value, expected_value)

    def test_before_range(self):
        # Arrange: Set up the date and expected value
        date = pd.Timestamp('2021-12-31')
        expected_value = 0

        # Act: Call the _fetch_value_from_date() function with the JSON object, date, and expected value
        actual_value = transform._fetch_value_from_date(self.json_obj, date)

        # Assert: Check that the actual value matches the expected value
        self.assertEqual(actual_value, expected_value)

    def test_after_range(self):
        # Arrange: Set up the date and expected value
        date = pd.Timestamp('2022-04-04')
        expected_value = 300

        # Act: Call the _fetch_value_from_date() function with the JSON object, date, and expected value
        actual_value = transform._fetch_value_from_date(self.json_obj, date)

        # Assert: Check that the actual value matches the expected value
        self.assertEqual(actual_value, expected_value)

    def test_empty_json(self):
        # Arrange: Set up an empty JSON object, date, and expected value
        json_obj = {}
        date = pd.Timestamp('2022-01-01')
        expected_value = 0

        # Act: Call the _fetch_value_from_date() function with the empty JSON object, date, and expected value
        actual_value = transform._fetch_value_from_date(json_obj, date)

        # Assert: Check that the actual value matches the expected value
        self.assertEqual(actual_value, expected_value)


class TestCleanAccount(unittest.TestCase):
    
    def setUp(self):
        self.account = {"id": "123", "name": "Test Account", "type": "checking", "on_budget": True, "closed": False, "note": "Test note", "balance": 100000, "cleared_balance": 50000, "uncleared_balance": 70000, "deleted": False}

    def test_clean_account(self):
        # Test case 1: Check that the function returns a dictionary with the correct keys
        expected_keys = ["id", "name", "type", "on_budget", "closed", "note", "balance", "cleared_balance", "uncleared_balance", "deleted"]
        actual = transform._clean_account(self.account)
        self.assertEqual(list(actual.keys()), expected_keys)

    def test_clean_account_balance(self):
        # Test case 2: Check that the function returns a dictionary with the correct balance value
        # arrange
        expected_balance = 100.0
        
        # act
        actual = transform._clean_account(self.account)
        
        # assert
        self.assertEqual(actual["balance"], expected_balance)

    def test_clean_account_cleared_balance(self):
        # Test case 3: Check that the function returns a dictionary with the correct cleared_balance value
        # arrange
        expected_cleared_balance = 50.0
        
        # act
        actual = transform._clean_account(self.account)
        
        # assert
        self.assertEqual(actual["cleared_balance"], expected_cleared_balance)

    def test_clean_account_uncleared_balance(self):
        # Test case 4: Check that the function returns a dictionary with the correct uncleared_balance value
        # arrange
        expected_uncleared_balance = 70.0
        
        # act
        actual = transform._clean_account(self.account)

        # assert
        self.assertEqual(actual["uncleared_balance"], expected_uncleared_balance)


class TestCleanCategory(unittest.TestCase):

    def setUp(self):
        # Define some sample category data
        self.category_data = {
            "id": "123",
            "category_group_id": "456",
            "category_group_name": "Groceries",
            "name": "Food",
            "hidden": False,
            "budgeted": 5000,
            "activity": 2500,
            "balance": 2500,
        }

    def test_clean_category_returns_dict(self):
        # Arrange
        category = self.category_data

        # Act
        result = transform._clean_category(category)

        # Assert
        self.assertIsInstance(result, dict)

    def test_clean_category_returns_expected_keys(self):
        # Arrange
        category = self.category_data

        # Act
        result = transform._clean_category(category)

        # Assert
        expected_keys = ["id", "category_group_id", "category_group_name", "name", "hidden", "budgeted", "activity", "balance"]
        self.assertCountEqual(result.keys(), expected_keys)

    def test_clean_category_returns_expected_values(self):
        # Arrange
        category = self.category_data

        # Act
        result = transform._clean_category(category)

        # Assert
        expected_values = {
            "id": "123",
            "category_group_id": "456",
            "category_group_name": "Groceries",
            "name": "Food",
            "hidden": False,
            "budgeted": 5.0,
            "activity": 2.5,
            "balance": 2.5,
        }
        self.assertDictEqual(result, expected_values)

class TestCleanBudgetMonth(unittest.TestCase):

    def setUp(self):
        # Define some sample budget month data
        self.budget_month_data = {
            "month": "2023-09",
            "categories": [
                {
                    "id": "123",
                    "category_group_id": "456",
                    "category_group_name": "Groceries",
                    "name": "Food",
                    "hidden": False,
                    "budgeted": 5000,
                    "activity": 2500,
                    "balance": 2500,
                },
                {
                    "id": "456",
                    "category_group_id": "789",
                    "category_group_name": "Housing",
                    "name": "Rent",
                    "hidden": False,
                    "budgeted": 10000,
                    "activity": 0,
                    "balance": 10000,
                },
            ],
        }
        self.snapshot_date = "2023-09-30"

    def test_clean_budget_month_returns_expected_number_of_categories(self):
        # Arrange
        budget_month = self.budget_month_data
        snapshot_date = self.snapshot_date

        # Act
        result = list(transform._clean_budget_month(budget_month, snapshot_date))

        # Assert
        expected_number_of_categories = 2
        self.assertEqual(len(result), expected_number_of_categories)

    def test_clean_budget_month_returns_expected_category_data(self):
        # Arrange
        budget_month = self.budget_month_data
        snapshot_date = self.snapshot_date

        # Act
        result = list(transform._clean_budget_month(budget_month, snapshot_date))

        # Assert
        expected_category_data = [
            {
                "id": "123",
                "category_group_id": "456",
                "category_group_name": "Groceries",
                "name": "Food",
                "hidden": False,
                "budgeted": 5.0,
                "activity": 2.5,
                "balance": 2.5,
                "month": "2023-09",
                "snapshot_date": "2023-09-30",
            },
            {
                "id": "456",
                "category_group_id": "789",
                "category_group_name": "Housing",
                "name": "Rent",
                "hidden": False,
                "budgeted": 10.0,
                "activity": 0.0,
                "balance": 10.0,
                "month": "2023-09",
                "snapshot_date": "2023-09-30",
            },
        ]
        self.assertListEqual(result, expected_category_data)

if __name__ == '__main__':
    unittest.main()
