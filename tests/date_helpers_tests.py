import os
import sys
import unittest
import pandas as pd
from parameterized import parameterized
# Add the directory containing the modules to the `PYTHONPATH`
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "src")))

from src.date_helpers import add_month  # noqa:E402 (module level import not at top of file)


class TestDateHelpers(unittest.TestCase):
    @parameterized.expand([
        ("2021-01-02", -1, "2020-12-02"),
        ("2023-03-30", -1, "2023-02-28"),
        ("2023-05-31", 1, "2023-06-30"),
        ("2023-06-30", -1, "2023-05-30"),
    ])
    def test_add_month(self, date_str, delta, expected_date_str):
        actual_date = add_month(pd.to_datetime(date_str), delta)
        expected_date = pd.to_datetime(expected_date_str)

        self.assertEqual(actual_date, expected_date)


if __name__ == '__main__':
    unittest.main()
