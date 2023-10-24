from datetime import datetime
import logging
import azure.functions as func
import azure.durable_functions as df
import os
import ingestion.ingest as ingest
import transformation.transform_raw as transform_raw
import serve.serve_category_scd as serve_category_scd
import serve.serve_monthly_net_worth as serve_monthly_net_worth
import serve.serve_transactions_star_schema as serve_transaction_star_schema

app = df.DFApp()
connect_str = os.getenv('AzureWebJobsStorage')


@app.orchestration_trigger(context_name="context")
def ynab_pipeline_orchestrator(context: df.DurableOrchestrationContext) -> None:

    # ******Bronze******
    first_retry_interval_in_milliseconds = 60000
    max_number_of_attempts = 3

    retry_options = df.RetryOptions(
        first_retry_interval_in_milliseconds, max_number_of_attempts)

    # auto retry api calls in the event of a transient failure of the YNAB api
    # TODO: retry is not working as expected. Look into it more
    tasks = [
        context.call_activity('load_transactions'),
        context.call_activity('load_accounts'),
        context.call_activity('load_current_budget_month'),
        context.call_activity('load_previous_budget_month')
    ]

    yield context.task_all(tasks)

    logging.info('All files ingested')

    # ******Silver******
    # TODO: this is not quite right yet, it is giving a non deterministic error
    # transform raw form into parquet files
    # tasks = [
    #     context.call_activity('transform_transactions'),
    #     context.call_activity('transform_accounts'),
    #     context.call_activity('transform_previous_budget_month'),
    #     context.call_activity('transform_current_budget_month'),
    # ]

    # yield context.task_all(tasks)

    # # ******Gold******
    # tasks = [
    #     # catagories SCD
    #     context.call_activity('serve_category_scd_activity'),

    #     # transaction star schema
    #     context.call_activity('create_transactions_fact_activity'),
    #     context.call_activity('serve_category_dim_activity'),
    #     context.call_activity('serve_accounts_dim_activity'),
    #     context.call_activity('serve_payee_dim_activity'),

    #     # monthly net worth helper fact table
    #     context.call_activity('serve_net_worth_fact_activity')
    # ]

    # yield context.task_all(tasks)

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

# region stubbed data


@app.route('mocks/budgets/{budgetId}/transactions', methods=['GET'])
def transaction_mock_http(req: func.HttpRequest) -> func.HttpResponse:
    filename = 'static/transactions.json'
    with open(filename, 'rb') as f:
        return func.HttpResponse(f.read(), mimetype='application/json')


@app.route('mocks/budgets/{budgetId}/accounts', methods=['GET'])
def accounts_mock_http(req: func.HttpRequest) -> func.HttpResponse:
    filename = 'static/accounts.json'
    with open(filename, 'rb') as f:
        return func.HttpResponse(f.read(), mimetype='application/json')


@app.route('mocks/budgets/{budgetId}/months/{month}', methods=['GET'])
def month_mock_http(req: func.HttpRequest) -> func.HttpResponse:
    filename = 'static/month.json'
    with open(filename, 'rb') as f:
        return func.HttpResponse(f.read(), mimetype='application/json')

# endregion

# region Bronze


@app.activity_trigger(input_name="input")
def load_transactions(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return ingest.load_transactions(connect_str)


@app.activity_trigger(input_name="input")
def load_accounts(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return ingest.load_accounts(connect_str)


@app.activity_trigger(input_name="input")
def load_current_budget_month(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return ingest.load_current_budget_month(connect_str)


@app.activity_trigger(input_name="input")
def load_previous_budget_month(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return ingest.load_previous_budget_month(connect_str)

#  endregion

# region Silver


@app.activity_trigger(input_name="input")
def transform_transactions(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return transform_raw.transform_transactions(connect_str)


@app.activity_trigger(input_name="input")
def transform_accounts(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return transform_raw.transform_accounts(connect_str)


@app.activity_trigger(input_name="input")
def transform_previous_budget_month(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    now = datetime.today()
    if now.date > 15:
        return

    first_day_of_month = datetime(
        now.year, now.month - 1, 1).strftime("%Y-%m-%d")
    return transform_raw.transform_budget_month(connect_str, first_day_of_month)


@app.activity_trigger(input_name="input")
def transform_current_budget_month(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    now = datetime.today()
    first_day_of_month = datetime(now.year, now.month, 1).strftime("%Y-%m-%d")
    return transform_raw.transform_budget_month(connect_str, first_day_of_month)

# endregion

# region Gold

# region Transaction Star Schema

@app.activity_trigger(input_name="input")
def create_transactions_fact_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return serve_transaction_star_schema.create_transactions_fact(connect_str)

@app.activity_trigger(input_name="input")
def serve_category_dim_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return serve_transaction_star_schema.create_category_dim(connect_str)

@app.activity_trigger(input_name="input")
def serve_accounts_dim_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return serve_transaction_star_schema.create_accounts_dim(connect_str)

@app.activity_trigger(input_name="input")
def serve_payee_dim_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return serve_transaction_star_schema.create_payee_dim(connect_str)

# endregion Transaction Star Schema

# region Views
@app.activity_trigger(input_name="input")
def serve_net_worth_fact_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return serve_monthly_net_worth.create_net_worth_fact(connect_str)

@app.activity_trigger(input_name="input")
def serve_category_scd_activity(input):
    connect_str = os.getenv('AzureWebJobsStorage')
    return serve_category_scd.create_category_scd(connect_str)
# endregion Views

# endregion Gold
