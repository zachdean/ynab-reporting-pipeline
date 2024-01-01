import os
import sys
import unittest
import pandas as pd

# Add the directory containing the modules to the `PYTHONPATH`
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")))

from src.serve.serve_age_of_money import _compute_monthly_age_of_money  # noqa:E402 (module level import not at top of file)


class TestComputeMonthlyAgeOfMoney(unittest.TestCase):
    def setUp(self):
        self.transactions_fact = pd.DataFrame({
            "date": ["2021-01-01", "2021-01-02", "2021-01-03"],
            "account_id": [1, 2, 3],
            "amount": [300, 200, 300]
        })
        self.accounts_dim = pd.DataFrame({
            "account_id": [1, 2, 3],
            "on_budget": [True, True, True],
            "category_name": ["Inflow: Ready to Assign", "Outflow", "Inflow: Ready to Assign"]
        })

    # TODO: think of a better, more robust way to test this function, possibly by decomposing it into smaller functions
    def test_compute_monthly_age_of_money(self):
        result = _compute_monthly_age_of_money(
            self.transactions_fact, self.accounts_dim)
        expected_result = pd.DataFrame({
            "date": pd.to_datetime(["2021-01-02"]),
            "age_of_money": [1]
        })

        pd.testing.assert_frame_equal(result, expected_result)


if __name__ == '__main__':
    unittest.main()
