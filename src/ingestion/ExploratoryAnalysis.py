# Databricks notebook source
# DBTITLE 1,Mount Storage
STORAGE_ENDPOINT_KEY = "storageHost"
CONTAINER = "ynab"
KEY_VAULT_SCOPE = "azureKeyVault"
STORAGE_KEY = "storageKey"

spark.conf.set("fs.azure.account.key." + get_storage_account_host(dbutils),
                dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=STORAGE_KEY))

storage_host = dbutils.secrets.get(scope=KEY_VAULT_SCOPE, key=STORAGE_ENDPOINT_KEY)

# COMMAND ----------

# DBTITLE 1,Display Account Data
import json
from pyspark.sql.functions import unix_timestamp,to_date
from pyspark.sql.types import *

accounts_path = f"abfss://{CONTAINER}@{storage_host}/raw/accounts.json"

accounts_json_string = dbutils.fs.head(accounts_path)

schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True),
    StructField("on_budget", BooleanType(), True),
    StructField("closed", BooleanType(), True),
    StructField("note", StringType(), True),
    StructField("balance", IntegerType(), True),
    StructField("cleared_balance", IntegerType(), True),
    StructField("uncleared_balance", IntegerType(), True),
    StructField("transfer_payee_id", StringType(), True),
    StructField("direct_import_linked", BooleanType(), True),
    StructField("direct_import_in_error", BooleanType(), True),
    StructField("last_reconciled_at", StringType(), True),
    StructField("debt_original_balance", IntegerType(), True),
    StructField("debt_interest_rates", MapType(StringType(), IntegerType()), True),
    StructField("debt_minimum_payments", MapType(StringType(), IntegerType()), True),
    StructField("debt_escrow_amounts", MapType(StringType(), IntegerType()), True),
    StructField("deleted", BooleanType(), True)
])

accounts_json_list = json.loads(accounts_json_string)["data"]["accounts"]

accounts_df = spark.createDataFrame(accounts_json_list, schema=schema)

display(accounts_df)

# balances match web ui, also gives information about each account (type, if it is abudget account, etc..) that will be useful later in the pipeline

# COMMAND ----------

# DBTITLE 1,Display Transactions Data
import json
from pyspark.sql.functions import unix_timestamp, to_date, explode, current_catalog, col
from pyspark.sql.types import *


transactions_path = f"abfss://{CONTAINER}@{storage_host}/raw/transactions.json"

# Define the schema of the dataframe
schema = StructType([
    StructField("data", StructType([
        StructField("transactions", ArrayType(
            StructType([
                StructField("id", StringType(), True),
                StructField("date", DateType(), True),
                StructField("amount", IntegerType(), True),
                StructField("memo", StringType(), True),
                StructField("cleared", StringType(), True),
                StructField("approved", BooleanType(), True),
                StructField("flag_color", StringType(), True),
                StructField("account_id", StringType(), True),
                StructField("account_name", StringType(), True),
                StructField("payee_id", StringType(), True),
                StructField("payee_name", StringType(), True),
                StructField("category_id", StringType(), True),
                StructField("category_name", StringType(), True),
                StructField("transfer_account_id", StringType(), True),
                StructField("transfer_transaction_id", StringType(), True),
                StructField("matched_transaction_id", StringType(), True),
                StructField("import_id", StringType(), True),
                StructField("import_payee_name", StringType(), True),
                StructField("import_payee_name_original", StringType(), True),
                StructField("debt_transaction_type", StringType(), True),
                StructField("deleted", BooleanType(), True),
                StructField("subtransactions", ArrayType(
                    StructType([
                        StructField("id", StringType(), True),
                        StructField("transaction_id", StringType(), True),
                        StructField("amount", IntegerType(), True),
                        StructField("memo", StringType(), True),
                        StructField("payee_id", StringType(), True),
                        StructField("payee_name", StringType(), True),
                        StructField("category_id", StringType(), True),
                        StructField("category_name", StringType(), True),
                        StructField("transfer_account_id", StringType(), True),
                        StructField("transfer_transaction_id", StringType(), True),
                        StructField("deleted", BooleanType(), True)
                    ])
                ), True)
            ])
        ))
    ]))
])

# Read the JSON file and select only the 'transactions' column
transactions_df = spark.read.json(transactions_path).select("data.transactions")

# Explode the transactions array column into separate rows
exploded_transactions_df = transactions_df.select(explode(transactions_df['transactions']).alias('transaction'))

# Explode the subtransactions array column into new rows
exploded_subtransactions_df = exploded_transactions_df\
  .select(
    'transaction.id', 
    'transaction.date', 
    'transaction.amount', 
    'transaction.memo', 
    'transaction.cleared', 
    'transaction.approved', 
    'transaction.flag_color', 
    'transaction.account_id', 
    'transaction.account_name', 
    'transaction.payee_id', 
    'transaction.payee_name', 
    'transaction.category_id', 
    'transaction.category_name', 
    'transaction.transfer_account_id', 
    'transaction.transfer_transaction_id', 
    'transaction.matched_transaction_id', 
    'transaction.import_id', 
    'transaction.import_payee_name', 
    'transaction.import_payee_name_original', 
    'transaction.debt_transaction_type', 
    'transaction.deleted', 
    explode('transaction.subtransactions').alias('subtransaction')
  )

# Select the fields you need from the exploded subtransactions
unnested_transaction_df = exploded_subtransactions_df\
  .select(
    'id',
    'date',
    'amount',
    'memo',
    'cleared',
    'approved',
    'flag_color',
    'account_id',
    'account_name',
    'payee_id',
    'payee_name',
    'category_id',
    'category_name',
    'transfer_account_id',
    'transfer_transaction_id',
    'matched_transaction_id',
    'import_id',
    'import_payee_name',
    'import_payee_name_original',
    'debt_transaction_type',
    'deleted',
    col('subtransaction.id').alias('subtransaction_id'),
    col('subtransaction.amount').alias('subtransaction_amount'),
    col('subtransaction.memo').alias('subtransaction_memo'),
    col('subtransaction.payee_id').alias('subtransaction_payee_id'),
    col('subtransaction.category_id').alias('subtransaction_category_id'),
    col('subtransaction.transfer_account_id').alias('subtransaction_transfer_account_id'),
    col('subtransaction.transfer_transaction_id').alias('subtransaction_transfer_transaction_id'),
    col('subtransaction.deleted').alias('subtransaction_deleted'),
  )
 
# Display the output DataFrame
display(unnested_transaction_df)



# COMMAND ----------

# DBTITLE 1,Display Budget Data
import json
from pyspark.sql.functions import unix_timestamp,to_date
from pyspark.sql.types import *

months_root_path = f"abfss://{CONTAINER}@{storage_host}/raw/month"

# list budget months
display(dbutils.fs.ls(months_root_path))

# list budgets in budget month
# List the directories within the specified path
directories = [file.path for file in dbutils.fs.ls(months_root_path) if file.isDir()]

# Loop through each directory and display its files
for directory in directories:
    print(f"Contents of directory {directory}:")
    display(dbutils.fs.ls(directory))

month_json_string = dbutils.fs.head(months_root_path + '/2023-09-01/2023-10-03.json')

month_json = json.loads(month_json_string)["data"]["month"]

schema = StructType([
            StructField("id", StringType(), True),
            StructField("category_group_id", StringType(), True),
            StructField("category_group_name", StringType(), True),
            StructField("name", StringType(), True),
            StructField("hidden", BooleanType(), True),
            StructField("original_category_group_id", StringType(), True),
            StructField("note", StringType(), True),
            StructField("budgeted", LongType(), True),
            StructField("activity", LongType(), True),
            StructField("balance", LongType(), True),
            StructField("goal_type", StringType(), True),
            StructField("goal_day", LongType(), True),
            StructField("goal_cadence", StringType(), True),
            StructField("goal_cadence_frequency", StringType(), True),
            StructField("goal_creation_month", StringType(), True),
            StructField("goal_target", LongType(), True),
            StructField("goal_target_month", StringType(), True),
            StructField("goal_percentage_complete", LongType(), True),
            StructField("goal_months_to_budget", LongType(), True),
            StructField("goal_under_funded", LongType(), True),
            StructField("goal_overall_funded", LongType(), True),
            StructField("goal_overall_left", LongType(), True),
            StructField("deleted", BooleanType(), True),
        ])

# create dataframe to display catagories
categories_df = spark.createDataFrame(month_json['categories'], schema=schema)

display(categories_df)

# note special categories of Ready to Assign nd Uncatorgized

# COMMAND ----------

# DBTITLE 1,Explore Transactions
from pyspark.sql.functions import sum, format_number, col

transactions_df = exploded_transactions_df.select(
    'transaction.id',
    'transaction.payee_name',
    'transaction.date',
    (exploded_transactions_df['transaction.amount'] / 1000).alias('amount'),
    'transaction.cleared',
    'transaction.account_id',
    'transaction.account_name',
  )

print("the sum of all transactions should match the net worth in YNAB")
net_worth = transactions_df.agg(sum(col('amount')).alias("sum_amount")).select(format_number(col("sum_amount"), 2)).collect()[0][0]
print(f"Net Worth: ${net_worth}")

print("Net worth does not match, so show sum by account")
grouped_df = transactions_df.groupBy("account_name") \
    .agg(sum("amount").alias("total_amount")) \
    .withColumn("account_balance", format_number("total_amount", 2)).drop("total_amount")

display(grouped_df)

print("The Mortage account (a special account in YNAB) balance does not match")
mortgage_df = transactions_df.filter(col("account_name") == "Mortage")

display(mortgage_df)

# final analysis is that the ledger is missing the escrow and interest payment. In the YNAB UI these are display almost as sub trnasactions, 
# but are accounted for in mortage balance
