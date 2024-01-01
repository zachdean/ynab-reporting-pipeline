import os
import sys
import unittest
import pandas as pd

# Add the directory containing the modules to the `PYTHONPATH`
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")))

from src.serve.serve_category_scd import \
    _replace_max_with_none, \
    _create_category_sdc, \
    _create_category_variance  # noqa:E402 (module level import not at top of file)


class TestCreateCategorySDC(unittest.TestCase):

    def test_create_category_sdc(self):
        # Create a sample DataFrame
        df = pd.DataFrame({
            "month": ["2021-01", "2021-01", "2021-02", "2021-02"],
            "id": ["A", "A", "B", "B"],
            "category_group_id": [1, 1, 2, 2],
            "name": ["Category 1", "Category 2", "Category 3", "Category 4"],
            "category_group_name": ["Group 1", "Group 1", "Group 2", "Group 2"],
            "budgeted": [100, 200, 300, 400],
            "snapshot_date": ["2021-01-01", "2021-01-31", "2021-02-01", "2021-02-28"]
        })

        # Apply the `_create_category_sdc` function to the DataFrame
        sdc_df = _create_category_sdc(df)

        # Print out the resulting DataFrame and the expected DataFrame
        expected_df = pd.DataFrame({
            "month": ["2021-01", "2021-01", "2021-02", "2021-02"],
            "category_id": ["A", "A", "B", "B"],
            "category_group_id": [1, 1, 2, 2],
            "name": ["Category 1", "Category 2", "Category 3", "Category 4"],
            "category_group_name": ["Group 1", "Group 1", "Group 2", "Group 2"],
            "budgeted": [100, 200, 300, 400],
            "start_date": ["2021-01-01", "2021-01-31", "2021-02-01", "2021-02-28"],
            "end_date": ["2021-01-30", None, "2021-02-27", None]
        })

        expected_df["start_date"] = pd.to_datetime(expected_df["start_date"])
        expected_df["end_date"] = pd.to_datetime(expected_df["end_date"])

        # Verify that the resulting DataFrame has the expected shape and values
        pd.testing.assert_frame_equal(sdc_df, expected_df)


class TestReplaceMaxWithNone(unittest.TestCase):
    def test_replace_max_with_none(self):
        # Create a sample DataFrame
        df = pd.DataFrame({
            "id": ["A", "A", "B", "B"],
            "start_date": ["2021-01-01", "2021-02-01", "2021-01-01", "2021-02-01"],
            "end_date": ["2021-01-31", "2021-03-31", "2021-02-28", "2021-03-31"]
        })

        # Group the DataFrame by `id` and apply the `_replace_max_with_none` function
        grouped_df = df.groupby("id").apply(_replace_max_with_none)

        # Reset the index of both DataFrames
        grouped_df = grouped_df.reset_index(drop=True)
        expected_df = pd.DataFrame({
            "id": ["A", "A", "B", "B"],
            "start_date": ["2021-01-01", "2021-02-01", "2021-01-01", "2021-02-01"],
            "end_date": ["2021-01-31", None, "2021-02-28", None]
        }).reset_index(drop=True)

        # Verify that the maximum `end_date` values were replaced with `None`
        pd.testing.assert_frame_equal(grouped_df, expected_df)


class TestCreateCategoryVariance(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame({
            "category_id": ['c1', 'c1', 'c2', 'c2'],
            "name": ['cat1', 'cat1', 'cat2', 'cat2'],
            "month": ['2022-01', '2022-01', '2022-02', '2022-02'],
            "start_date": ['2022-01-01', '2022-01-02', '2022-02-01', '2022-02-02'],
            "budgeted": [100, 200, 300, 400]
        })

    def test_create_category_variance(self):
        # arrange
        expected_df = pd.DataFrame({
            "category_id": ['c1', 'c2'],
            "name": ['cat1', 'cat2'],
            "month": ['2022-01', '2022-02'],
            "variance": [100, 100]
        })

        # act
        actual_df = _create_category_variance(self.df)

        # assert
        pd.testing.assert_frame_equal(actual_df, expected_df)


if __name__ == "__main__":
    unittest.main()
