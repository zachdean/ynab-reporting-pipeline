import unittest
import src.transformation.transform_transactions as transform
import json
import os

# Get the path of the current file
current_file_path = os.path.dirname(__file__)

class SubTransactionsTestCase(unittest.TestCase):

  def test_given_nested_transactions_when_unnest_subtransactions_then_subtransctions_unnested(self):
    # arrange
    with open(os.path.join(current_file_path,'resources/nested_transaction.json'), 'r', encoding='utf-8') as f:
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
    with open(os.path.join(current_file_path,'resources/unnested_transaction.json'), 'r', encoding='utf-8') as f:
      transaction = json.load(f)

    # act
    unnested = list(transform._unnest_subtransactions(transaction))
    
    # assert
    self.assertEqual(len(unnested), 1)
    self.assertDictEqual(unnested[0], transaction)

class TransactionsTestCase(unittest.TestCase):
  
  def test_given_raw_transactions_when_transaction_cleaned_then_transaction_data_shaped(self):
    # arrange
    with open(os.path.join(current_file_path,'resources/unnested_transaction.json'), 'r', encoding='utf-8') as f:
      transaction = json.load(f)
    with open(os.path.join(current_file_path, 'resources/expected_cleaned_transaction.json'), 'r', encoding='utf-8') as f:
      expected = json.load(f)

    # act
    cleaned_transaction = transform._clean_transaction(transaction)
    
    # assert
    self.assertDictEqual(cleaned_transaction, expected)

if __name__ == '__main__':
  unittest.main()