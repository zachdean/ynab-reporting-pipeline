import os
import sys
import pandas as pd
import numpy as np
import unittest

# Add the directory containing the modules to the `PYTHONPATH`
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")))

from src.serve.serve_monthly_net_worth import _compute_monthly_net_worth

class TestComputeMonthlyNetWorth(unittest.TestCase):
    def setUp(self):
        # create sample data
        self.transactions_fact = pd.DataFrame({
            "date": ["2020-01-01", "2020-01-01", "2020-02-01", "2020-02-01"],
            "account_id": [1, 2, 1, 2],
            "amount": [100, -50, 200, -75]
        })
        self.accounts_dim = pd.DataFrame({
            "id": [1, 2],
            "asset_type": ["asset", "liability"]
        })

    def test_compute_monthly_net_worth_columns(self):
        # arrange
        expected_columns = ["date", "asset_type", "delta", "running_total",
                            "asset_running_total", "liability_running_total"]

        # act
        net_worth_fact = _compute_monthly_net_worth(
            self.transactions_fact, self.accounts_dim)
        actual_columns = list(net_worth_fact.columns)

        # assert
        self.assertListEqual(actual_columns, expected_columns)

    def test_compute_monthly_net_worth_running_total(self):
        # arrange
        expected_running_total = [100, -50, 300, -125]

        # act
        net_worth_fact = _compute_monthly_net_worth(
            self.transactions_fact, self.accounts_dim)
        actual_running_total = list(net_worth_fact["running_total"])

        # assert
        self.assertListEqual(actual_running_total, expected_running_total)

    def test_compute_monthly_net_worth_asset_running_total(self):
        # arrange
        expected_asset_running_total = [100.0, np.nan, 300.0, np.nan]

        # act
        net_worth_fact = _compute_monthly_net_worth(
            self.transactions_fact, self.accounts_dim)
        actual_asset_running_total = list(
            net_worth_fact["asset_running_total"])

        # assert
        self._assert_list_equal_with_nan(actual_asset_running_total,
                             expected_asset_running_total)

    def test_compute_monthly_net_worth_liability_running_total(self):
        # arrange
        expected_liability_running_total = [np.nan, -50.0, np.nan, -125.0]

        # act
        net_worth_fact = _compute_monthly_net_worth(
            self.transactions_fact, self.accounts_dim)
        actual_liability_running_total = list(
            net_worth_fact["liability_running_total"])

        # assert
        self._assert_list_equal_with_nan(actual_liability_running_total,
                             expected_liability_running_total)

    def test_compute_monthly_net_worth_delta(self):
        # arrange
        expected_delta = [100, -50, 200, -75]

        # act
        net_worth_fact = _compute_monthly_net_worth(
            self.transactions_fact, self.accounts_dim)
        actual_delta = list(net_worth_fact["delta"])

        # assert
        self.assertListEqual(actual_delta, expected_delta)

    def test_compute_monthly_net_worth_sorting(self):
        # arrange
        expected_date = [pd.Timestamp("2020-01-01"), pd.Timestamp(
            "2020-01-01"), pd.Timestamp("2020-02-01"), pd.Timestamp("2020-02-01")]
        expected_asset_type = ["asset", "liability", "asset", "liability"]

        # act
        net_worth_fact = _compute_monthly_net_worth(
            self.transactions_fact, self.accounts_dim)
        actual_date = list(net_worth_fact["date"])
        actual_asset_type = list(net_worth_fact["asset_type"])

        # assert
        self.assertListEqual(actual_date, expected_date)
        self.assertListEqual(actual_asset_type, expected_asset_type)

    def _assert_list_equal_with_nan(self, actual_list: list[float], expected_list: list[float]) -> None:
        for i in range(len(actual_list)):
            if np.isnan(actual_list[i]) and np.isnan(expected_list[i]):
                continue
            if actual_list[i] != expected_list[i]:
                self.fail(f"Expected: {expected_list}\nActual: {actual_list}")

if __name__ == "__main__":
    unittest.main()
