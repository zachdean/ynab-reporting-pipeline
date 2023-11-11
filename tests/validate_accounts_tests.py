import os
import sys
import unittest
import pandas as pd

# Add the directory containing the modules to the `PYTHONPATH`
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src")))

import src.validate.validate_accounts as validate_accounts

# Get the path of the current file
current_file_path = os.path.dirname(__file__)


class TestValidateTransactionsFact(unittest.TestCase):
  def setUp(self):
    self.transactions_df = pd.DataFrame({
      "account_id": [1, 2, 1, 3],
      "amount": [100, 200.85, 300, 400]
    })
    self.accounts_df = pd.DataFrame({
      "account_id": [1, 2, 3],
      "balance": [400, 200.85, 400],
      "name": ["Account1", "Account2", "Account3"]
    })

  def test_validate_transactions_fact_no_failures(self):
    # Arrange

    # Act
    try:
      validate_accounts._validate_transactions_fact(self.transactions_df, self.accounts_df)
    except Exception as e:
      self.fail(f"Test failed with exception: {e}")

    # Assert
    # No assertion needed as the test will fail if an exception is raised

  def test_validate_transactions_fact_with_failures(self):
    # Arrange
    self.accounts_df.at[0, 'balance'] = 500 

    # Act
    with self.assertRaises(Exception) as context:
      validate_accounts._validate_transactions_fact(self.transactions_df, self.accounts_df)

    # Assert
    self.assertIn('1 account(s) failed validation', str(context.exception))

class TestValidateNetWorthFact(unittest.TestCase):
    def setUp(self):
        self.net_worth_df = pd.DataFrame({
            "delta": [100.0, 200.0, 300.0]
        })
        self.accounts_df = pd.DataFrame({
            "balance": [100.0, 200.0, 300.0]
        })

    def test_validate_net_worth_fact_no_failures(self):
        # Act
        validate_accounts._validate_net_worth_fact(self.net_worth_df, self.accounts_df)

    def test_validate_net_worth_fact_with_failures(self):
        # Arrange
        self.accounts_df.at[0, 'balance'] = 500.0  # Change balance to cause validation failure

        # Act
        with self.assertRaises(Exception) as context:
            validate_accounts._validate_net_worth_fact(self.net_worth_df, self.accounts_df)
          
        # Assert
        self.assertIn('Net worth delta of 600.0 does not match actual net worth of 1000.0', str(context.exception))

if __name__ == '__main__':
  unittest.main()