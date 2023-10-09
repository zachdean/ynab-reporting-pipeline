import pulumi
from pulumi_databricks import SecretScope, SecretScopeKeyvaultMetadataArgs, get_current_user, Job, JobNewClusterArgs, JobNotebookTaskArgs, JobEmailNotificationsArgs, Notebook, JobTaskArgs, WorkspaceFile, JobScheduleArgs

# Get the authenticated user's workspace home directory path and email address.
user_home_path     = get_current_user().home
pulumi.log.info(f"User home path: {user_home_path}")
user_email_address = get_current_user().user_name

config = pulumi.Config()

# resource_prefix = config.require('resource-prefix')

# Define cluster resource settings.
node_type = config.require('node-type')

# get infrastructure detials
stack = pulumi.get_stack()
org = pulumi.get_organization()

stack_ref = pulumi.StackReference(f"{org}/ynab-data-pipeline/{stack}")

# TODO: this does not work yet, need to figure out why
# Secret Scope
# secrets_scope = SecretScope("azure_secrets",
#                             backend_type="AZURE_KEYVAULT",
#                             initial_manage_principal="users",
#                             keyvault_metadata=SecretScopeKeyvaultMetadataArgs(
#                                 dns_name=stack_ref.get_output("VaultUrl"),
#                                 resource_id=stack_ref.get_output("VaultId"),
#                             ))

# Create a Notebook resource.
ingest_transaction_notebook = Notebook(
    resource_name="IngestRawTransactions",
    path=f"{user_home_path}/src/ingestion/IngestRawTransactions.py",
    source="../src/ingestion/IngestRawTransactions.py"
)

ingest_accounts_notebook = Notebook(
    resource_name="IngestRawAccounts",
    path=f"{user_home_path}/src/ingestion/IngestRawAccounts.py",
    source="../src/ingestion/IngestRawAccounts.py"
)

ingest_budget_month_notebook = Notebook(
    resource_name="IngestRawBudgetMonth",
    path=f"{user_home_path}/src/ingestion/IngestRawBudgetMonth.py",
    source="../src/ingestion/IngestRawBudgetMonth.py"
)

ynab_helper = WorkspaceFile("ynab_helpers.py", 
                            path=f"{user_home_path}/src/ingestion/ynab_helpers.py",
                            source="../src/ingestion/ynab_helpers.py")

# Create Tasks for the Job.
def create_job_task(resource_name: str, resource_path: str, node_type: str) -> JobTaskArgs:
    notebook_task = JobNotebookTaskArgs(
        notebook_path=resource_path,
    )    
    return JobTaskArgs(
        task_key=resource_name,
        notebook_task=notebook_task,
        # for some reason, we cannot share the same cluster between jobs in pulumi,
        # this quickly leads to running out of available ip address in the region.
        new_cluster= JobNewClusterArgs(
            num_workers=1,
            spark_version="13.3.x-scala2.12",
            node_type_id=node_type,
        )
    )

# Usage:
tasks = [
    create_job_task("ingest-raw-transactions", ingest_transaction_notebook.path, node_type),
    create_job_task("ingest-raw-accounts", ingest_accounts_notebook.path, node_type),
    create_job_task("ingest-raw-budget_month", ingest_budget_month_notebook.path, node_type),
]

# Create the job
job = Job(
    resource_name="ynab-ingest-job",
    tasks=tasks,
    # there seems to be a bug with pulumi that this does not work
    # trigger=JobScheduleArgs(
    #     quartz_cron_expression='0 42 2 * * ?',
    #     timezone_id="America/Chicago",
    #     pause_status='UNPAUSED'),
    email_notifications=JobEmailNotificationsArgs(
        on_successes=[user_email_address],
        on_failures=[user_email_address]
    )
)

pulumi.export('Job URL', job.url)
