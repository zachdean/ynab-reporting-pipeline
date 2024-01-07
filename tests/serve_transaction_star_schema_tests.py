import os
import sys
import unittest
import pandas as pd

# Add the directory containing the modules to the `PYTHONPATH`
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")))

from src.serve.serve_transactions_star_schema import \
    _create_category_dim  # noqa:E402 (module level import not at top of file)


class TestCreateTransactionStarSchema(unittest.TestCase):

    def test_create_category_dim(self):
        # Create a sample DataFrame
        df = pd.DataFrame({
            "id": ["A", "A", "B", "B"],
            "month": ["2021-01-01", "2021-01-01", "2021-01-01", "2021-01-01"],
            "snapshot_date": ["2021-01-01", "2021-01-02", "2021-02-01", "2021-01-01"],
            "category_group_id": [1, 1, 2, 2],
            "category_group_name": ["Group 1", "Group 1", "Group 2", "Group 2"],
            "name": ["Category 1", "Category 1", "Category 2", "Category 2"],
            "hidden": [True, False, False, False],
            "budgeted": [0, 200, 300, 400],
            "activity": [10, 20, 30, 40],
            "balance": [90, 180, 270, 360],
        })

        # Apply the `_create_category_sdc` function to the DataFrame
        sdc_df = _create_category_dim(df)

        # Print out the resulting DataFrame and the expected DataFrame
        expected_df = pd.DataFrame({
            "category_id": ["A", "B"],
            "name": ["Category 1", "Category 2"],
            "category_group_id": [1, 2],
            "category_group_name": ["Group 1", "Group 2"],
            "hidden": [False, False]
        })

        # Verify that the resulting DataFrame has the expected shape and values
        pd.testing.assert_frame_equal(sdc_df, expected_df)


if __name__ == "__main__":
    unittest.main()
