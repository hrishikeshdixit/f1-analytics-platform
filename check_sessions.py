from google.cloud import bigquery
from dotenv import load_dotenv
import os

load_dotenv()
client = bigquery.Client()
project = os.getenv('BQ_PROJECT_ID')

query = f"SELECT DISTINCT RoundNumber, SessionType FROM `{project}.raw.laps` ORDER BY RoundNumber"

df = client.query(query).to_dataframe()
print(df.to_string())