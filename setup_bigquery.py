from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()

client = bigquery.Client()

# Create raw dataset
raw_dataset = bigquery.Dataset(f"{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_RAW')}")
raw_dataset.location = "US"
client.create_dataset(raw_dataset, exists_ok=True)
print("Raw dataset created!")

# Create transformed dataset
transformed_dataset = bigquery.Dataset(f"{os.getenv('BQ_PROJECT_ID')}.{os.getenv('BQ_DATASET_TRANSFORMED')}")
transformed_dataset.location = "US"
client.create_dataset(transformed_dataset, exists_ok=True)
print("Transformed dataset created!")