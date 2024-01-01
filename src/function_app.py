from datetime import datetime
import logging
import azure.functions as func
import azure.durable_functions as df
import os
import ingestion.ingest as ingest
import transformation.transform_raw as transform_raw
import serve.serve_category_scd as serve_category_scd
import serve.serve_monthly_net_worth as serve_monthly_net_worth
import serve.serve_age_of_money as serve_age_of_money
import serve.serve_transactions_star_schema as serve_transaction_star_schema
import validate.validate_accounts as validate_accounts

app = df.DFApp()
connect_str = os.getenv('AzureWebJobsStorage')


@app.orchestration_trigger(context_name="context")
def ynab_pipeline_orchestrator(context: df.DurableOrchestrationContext):

    # ******Bronze******
    first_retry_interval_in_milliseconds = 60000
    max_number_of_attempts = 3

    retry_options = df.RetryOptions(
        first_retry_interval_in_milliseconds, max_number_of_attempts)
    logging.info('ingestion start')
    # auto retry api calls in the event of a transient failure of the YNAB api
    bronze_tasks = [
        context.call_activity_with_retry(load_transactions, retry_options),
        context.call_activity_with_retry(load_accounts, retry_options),
        context.call_activity_with_retry(
            load_current_budget_month, retry_options, context.current_utc_datetime.strftime("%Y-%m-%d")),
    ]

    if context.current_utc_datetime.day <= 15:
        bronze_tasks.append(context.call_activity_with_retry(
            load_previous_budget_month, retry_options, context.current_utc_datetime.strftime("%Y-%m-%d")))

    yield context.task_all(bronze_tasks)

    logging.info('All files ingested')

    # ******Silver******

    # transform raw form into parquet files
    silver_tasks = [
        context.call_activity(transform_transactions),
        context.call_activity(transform_accounts),
        context.call_activity(transform_current_budget_month,
                              context.current_utc_datetime.strftime("%Y-%m-01")),
    ]

    if context.current_utc_datetime.day <= 15:
        silver_tasks.append(context.call_activity(
            transform_previous_budget_month, context.current_utc_datetime.strftime("%Y-%m-01")))

    yield context.task_all(silver_tasks)

    logging.info('transform complete')

    # # ******Gold******
    gold_tasks = [
        # catagories SCD
        context.call_activity(serve_category_scd_activity),

        # transaction star schema
        context.call_activity(create_transactions_fact_activity),
        context.call_activity(serve_category_dim_activity),
        context.call_activity(serve_accounts_dim_activity),
        context.call_activity(serve_payee_dim_activity),

        # helper fact tables
        context.call_activity(serve_age_of_money_activity)
    ]

    yield context.task_all(gold_tasks)

    logging.info('gold complete')

    additional_tasks = [
        context.call_activity(serve_category_variance_activity),
        context.call_activity(serve_net_worth_fact_activity)
    ]
    # calculate net worth
    yield context.task_all(additional_tasks)

    # validate results
    # TODO: there is a bug in the YNAB api so I cannot properly calculate the interest and escrow amounts
    # for my mortgage account, commenting out for now since it will always fail
    # validation_tasks = [
    #     context.call_activity('validate_transactions_fact'),
    #     context.call_activity('validate_net_worth_fact_activity'),
    # ]

    # yield context.task_all(validation_tasks)

# region orchestrator triggers


@app.schedule(schedule="0 42 2 * * *",
              arg_name="timer",
              # run_on_startup=True, # uncomment for local dev
              use_monitor=False)
@app.durable_client_input(client_name="client")
async def ynab_pipeline_orchestrator_trigger(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    instance_id = await client.start_new('ynab_pipeline_orchestrator')

    logging.info(f"Started orchestration with ID = '{instance_id}'.")

#  endregion

# region Bronze


@app.activity_trigger(input_name="input")
def load_transactions(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    upload_size = ingest.load_transactions(connect_str)
    logging.info(f"load_transactions: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def load_accounts(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return ingest.load_accounts(connect_str)
    upload_size = ingest.load_accounts(connect_str)
    logging.info(f"load_accounts: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def load_current_budget_month(input: str):
    connect_str = os.getenv('AzureWebJobsStorage')
    date = datetime.strptime(input, '%Y-%m-%d')
    # return ingest.load_current_budget_month(connect_str, date)
    upload_size = ingest.load_current_budget_month(connect_str, date)
    logging.info(f"load_current_budget_month: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def load_previous_budget_month(input: str):
    connect_str = os.getenv('AzureWebJobsStorage')
    date = datetime.strptime(input, '%Y-%m-%d')
    # return ingest.load_previous_budget_month(connect_str, date)
    upload_size = ingest.load_previous_budget_month(connect_str, date)
    logging.info(f"load_previous_budget_month: Uploaded {upload_size} bytes")
    return upload_size
#  endregion

# region Silver


@app.activity_trigger(input_name="input")
def transform_transactions(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return transform_raw.transform_transactions(connect_str)
    upload_size = transform_raw.transform_transactions(connect_str)
    logging.info(f"transform_transactions: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def transform_accounts(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return transform_raw.transform_accounts(connect_str)
    upload_size = transform_raw.transform_accounts(connect_str)
    logging.info(f"transform_accounts: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def transform_previous_budget_month(input: str):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return transform_raw.transform_budget_month(connect_str, input)
    upload_size = transform_raw.transform_budget_month(connect_str, input)
    logging.info(f"transform_budget_month: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def transform_current_budget_month(input: str):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return transform_raw.transform_budget_month(connect_str, input)
    upload_size = transform_raw.transform_budget_month(connect_str, input)
    logging.info(f"transform_budget_month: Uploaded {upload_size} bytes")
    return upload_size
# endregion

# region Gold

# region Transaction Star Schema


@app.activity_trigger(input_name="input")
def create_transactions_fact_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_transaction_star_schema.create_transactions_fact(connect_str)
    upload_size = serve_transaction_star_schema.create_transactions_fact(
        connect_str)
    logging.info(f"create_transactions_fact: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def serve_category_dim_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_transaction_star_schema.create_category_dim(connect_str)
    upload_size = serve_transaction_star_schema.create_category_dim(
        connect_str)
    logging.info(f"create_category_dim: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def serve_accounts_dim_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_transaction_star_schema.create_accounts_dim(connect_str)
    upload_size = serve_transaction_star_schema.create_accounts_dim(
        connect_str)
    logging.info(f"create_accounts_dim: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def serve_payee_dim_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_transaction_star_schema.create_payee_dim(connect_str)
    upload_size = serve_transaction_star_schema.create_payee_dim(connect_str)
    logging.info(f"create_payee_dim: Uploaded {upload_size} bytes")
    return upload_size

# endregion Transaction Star Schema

# region Views


@app.activity_trigger(input_name="input")
def serve_net_worth_fact_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_monthly_net_worth.create_net_worth_fact(connect_str)
    upload_size = serve_monthly_net_worth.create_net_worth_fact(connect_str)
    logging.info(f"create_net_worth_fact: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def serve_category_scd_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_category_scd.create_category_scd(connect_str)
    upload_size = serve_category_scd.create_category_scd(connect_str)
    logging.info(f"create_category_scd: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def serve_category_variance_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_category_scd.create_category_variance(connect_str)
    upload_size = serve_category_scd.create_category_variance(connect_str)
    logging.info(f"create_category_variance: Uploaded {upload_size} bytes")
    return upload_size


@app.activity_trigger(input_name="input")
def serve_age_of_money_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    # return serve_age_of_money.create_age_of_money_fact(connect_str)
    upload_size = serve_age_of_money.create_age_of_money_fact(connect_str)
    logging.info(f"create_age_of_money_fact: Uploaded {upload_size} bytes")
    return upload_size
# endregion Views

# endregion Gold

# region Validation


@app.activity_trigger(input_name="input")
def validate_transactions_fact(input) -> bool:
    connect_str = os.getenv('AzureWebJobsStorage')
    return validate_accounts.validate_transactions_fact(connect_str)


@app.activity_trigger(input_name="input")
def validate_net_worth_fact_activity(input) -> bool:
    connect_str = os.getenv('AzureWebJobsStorage')
    return validate_accounts.validate_net_worth_fact(connect_str)

# endregion Validation
