import datetime
import logging
import azure.functions as func
import azure.durable_functions as df
import os
import ingestion.ingest as ingest
import transformation.transform_raw as transform_raw

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
    tasks = [
        context.call_activity('load_transactions', retry_options),
        context.call_activity('load_accounts', retry_options),
        context.call_activity('load_current_budget_month', retry_options),
        context.call_activity('load_previous_budget_month', retry_options)
    ]

    yield context.task_all(tasks)

    logging.info('All files ingested')

    # ******Silver******

    # transform raw form into parquet files
    tasks = [
        context.call_activity('transform_transactions'),
        context.call_activity('transform_accounts'),
        context.call_activity('transform_previous_budget_month'),
        context.call_activity('transform_current_budget_month'),
    ]

    yield context.task_all(tasks)

# region orchestrator triggers


@app.schedule(schedule="0 42 2 * * *",
              arg_name="timer",
              # run_on_startup=True, # uncomment for local dev
              use_monitor=False)
@app.durable_client_input(client_name="client")
async def ynab_pipeline_orchestrator_trigger(timer: func.TimerRequest, client: df.DurableOrchestrationClient) -> None:
    instance_id = await client.start_new('ynab_pipeline_orchestrator')

    logging.info(f"Started orchestration with ID = '{instance_id}'.")


@app.durable_client_input(client_name="client")
@app.route('orchestrators/ynab_pipeline_orchestrator', methods=['POST'])
async def ynab_pipeline_orchestrator_http(req: func.HttpRequest, client: df.DurableOrchestrationClient) -> func.HttpResponse:
    instance_id = await client.start_new('ynab_pipeline_orchestrator')
    response = client.create_check_status_response(req, instance_id)
    return response

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
