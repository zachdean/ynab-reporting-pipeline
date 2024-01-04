from datetime import datetime
import json
import os
import sys
import logging
import argparse
# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

ch.setFormatter(formatter)

logger.addHandler(ch)

current_file_path = os.path.dirname(__file__)

# Load the contents of the local.settings.json file into a dictionary
with open(os.path.join(current_file_path, '../src/local.settings.json'), 'r') as f:
    settings = json.load(f)

# Get the value of the ConnectionString key
connection_string = settings['Values']['AzureWebJobsStorage']

for key, value in settings['Values'].items():
    os.environ[key] = value

# Add the directory containing the modules to the `PYTHONPATH`
src_dir = os.path.abspath(os.path.join(current_file_path, '..', 'src'))
sys.path.insert(0, src_dir)

# the imports rely on env variables that are set above
import ingestion.ingest as ingest  # noqa:E402 (module level import not at top of file)
import transformation.transform_raw as transform  # noqa:E402 (module level import not at top of file)

# Parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('--max_date', type=str, help='Maximum date for backfilling',
                    default=datetime.today().strftime('%Y-%m-%d'))
args = parser.parse_args()

months = ingest._fetch_raw_json("months")['data']['months']

backfill_count = 0
last_month = None
for month in months:
    month_date = month['month']
    if args.max_date and datetime.strptime(month_date, '%Y-%m-%d') > datetime.strptime(args.max_date, '%Y-%m-%d'):
        break
    logger.info(f"Backfilling {month_date}")
    ingest.load_current_budget_month(
        connection_string, datetime.strptime(month_date, '%Y-%m-%d'))
    transform.transform_budget_month(connection_string, month_date)
    backfill_count += 1
    last_month = month_date

logger.info(f"Backfill complete. {backfill_count} months backfilled, Last month: {last_month}")
