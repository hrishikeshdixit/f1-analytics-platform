import fastf1
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os

# Load credentials from .env file
load_dotenv()

# Enable FastF1 cache (saves data locally so you don't re-download)
fastf1.Cache.enable_cache('cache/')

def extract_session(year, round_number, session_type='R'):
    """
    Pull a session from FastF1
    session_type: 'R' = Race, 'Q' = Qualifying, 'FP1/FP2/FP3' = Practice
    """
    print(f"Extracting {session_type} session - Year: {year}, Round: {round_number}")
    session = fastf1.get_session(year, round_number, session_type)
    session.load()
    return session

def process_laps(session):
    """
    Extract lap data from the session and clean it up
    """
    laps = session.laps.copy()
    
    # Add useful context columns
    laps['Year'] = session.event['EventDate'].year
    laps['RaceName'] = session.event['EventName']
    laps['RoundNumber'] = session.event['RoundNumber']
    laps['SessionType'] = session.name
    
    # Convert ALL timedelta columns to seconds (BigQuery doesn't understand timedeltas)
    for col in laps.columns:
        if laps[col].dtype == 'timedelta64[ns]':
            laps[col] = laps[col].dt.total_seconds()
    
    # Reset index
    laps = laps.reset_index(drop=True)
    
    print(f"Processed {len(laps)} laps")
    return laps

def load_to_bigquery(df, table_id):
    """
    Load a dataframe into BigQuery
    table_id format: 'project.dataset.table'
    """
    client = bigquery.Client()
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",  # Overwrite table if it exists
        autodetect=True                       # Auto detect column types
    )
    
    print(f"Loading data to BigQuery table: {table_id}")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for job to finish
    
    print(f"Successfully loaded {len(df)} rows to {table_id}")

if __name__ == "__main__":
    PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    RAW_DATASET = os.getenv('BQ_DATASET_RAW')

    races = [
        (2026, 1, 'R'),   # Australian GP
        (2026, 2, 'R'),   # Chinese GP
    ]

    for year, round_num, session_type in races:
        try:
            session = extract_session(year, round_num, session_type)
            laps_df = process_laps(session)
            load_to_bigquery(laps_df, f"{PROJECT_ID}.{RAW_DATASET}.laps")
            print(f"Round {round_num} done!")
        except Exception as e:
            print(f"Round {round_num} failed: {e}")
