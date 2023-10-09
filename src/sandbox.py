# import json
# import os

# with open('local.settings.json') as f:
#     settings = json.load(f)

# for key, value in settings['Values'].items():
#     os.environ[key] = value

# import ingestion.ingest as ingest

# connect_str=os.getenv('AzureWebJobsStorage')
# ingest.load_transactions(connect_str)
# print("loaded")