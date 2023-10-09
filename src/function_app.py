import logging
import azure.functions as func
import azure.durable_functions as df
import os
import ingestion.ingest as ingest

app = df.DFApp()
connect_str = os.getenv('AzureWebJobsStorage')

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

@app.orchestration_trigger(context_name="context")
def ynab_pipeline_orchestrator(context: df.DurableOrchestrationContext) -> None:
  tasks = [
      context.call_activity('load_transactions'),
      context.call_activity('load_accounts'),
      context.call_activity('load_current_budget_month'),
      context.call_activity('load_previous_budget_month')
  ]
  
  yield context.task_all(tasks)

  logging.info('All files ingested')

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