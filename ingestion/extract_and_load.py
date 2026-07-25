import fastf1
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime
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

'''
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

if __name__ == "__main__":
    PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    RAW_DATASET = os.getenv('BQ_DATASET_RAW')
    YEAR = 2026

    # Dynamically get all completed rounds so far
    schedule = fastf1.get_event_schedule(YEAR, include_testing=False)
    
    # Filter only rounds that have already happened
    from datetime import datetime
    completed = schedule[schedule['EventDate'] < datetime.now()]

    # Check what's already in BigQuery
    from google.cloud import bigquery
    client = bigquery.Client()
    query = f"SELECT DISTINCT RoundNumber FROM `{PROJECT_ID}.{RAW_DATASET}.laps`"
    
    try:
        existing_rounds = set(
            row.RoundNumber for row in client.query(query).result()
        )
    except:
        existing_rounds = set()

    print(f"Already loaded rounds: {existing_rounds}")

    # Only load rounds we don't have yet
    for _, event in completed.iterrows():
        round_num = event['RoundNumber']
        if round_num in existing_rounds:
            print(f"⏭️ Round {round_num} already loaded, skipping")
            continue
        try:
            session = extract_session(YEAR, round_num, 'R')
            laps_df = process_laps(session)
            load_to_bigquery(laps_df, f"{PROJECT_ID}.{RAW_DATASET}.laps")
            print(f"✅ Round {round_num} done!")
        except Exception as e:
            print(f"❌ Round {round_num} failed: {e}")
'''

if __name__ == "__main__":
    PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    RAW_DATASET = os.getenv('BQ_DATASET_RAW')
    YEAR = 2026

    # All possible session types to load
    #ALL_SESSIONS = ['FP1', 'FP2', 'FP3', 'SQ', 'S', 'Q', 'R']

    # Dynamically get all completed rounds
    schedule = fastf1.get_event_schedule(YEAR, include_testing=False)

    current_time = pd.Timestamp.now()

    # Check what's already in BigQuery
    client = bigquery.Client()
    query = f"""
        SELECT DISTINCT RoundNumber, SessionType 
        FROM `{PROJECT_ID}.{RAW_DATASET}.laps`
    """

    try:
        existing = client.query(query).to_dataframe()
        existing_sessions = set(
            zip(existing['RoundNumber'], existing['SessionType'])
        )
    except:
        existing_sessions = set()

    print(f"Already loaded sessions: {existing_sessions}")

    for _, event in schedule.iterrows():
        round_num = event['RoundNumber']
        race_name = event['EventName']
        print(f"\n── Round {round_num}: {race_name} ──")

        # Dynamically get available sessions from FastF1
        available_sessions = []
        try:
            event_obj = fastf1.get_event(YEAR, round_num)
            for i in range(1, 6):  # FastF1 stores up to 5 sessions per event
                session_name = event_obj.get(f'Session{i}')
                if session_name and pd.notna(session_name) and session_name != '':
                    available_sessions.append(session_name)
            print(f"  Available sessions: {available_sessions}")
        except Exception as e:
            print(f"  Could not get sessions for Round {round_num}: {e}")
            continue

        for session_type in available_sessions:
            # Check if already loaded
            if (round_num, session_type) in existing_sessions:
                print(f"  ⏭️ {session_type} already loaded, skipping")
                continue

            try:
                # Find the scheduled time for this session
                session_date = None

                for i in range(1, 6):
                    if event.get(f"Session{i}") == session_type:
                        session_date = event.get(f"Session{i}DateUtc")
                        break

                # Skip sessions that haven't happened yet
                if pd.isna(session_date):
                    continue

                session_date = pd.Timestamp(session_date)

                if session_date > current_time:
                    print(f"  ⏳ {session_type} has not occurred yet")
                    continue
                session = extract_session(YEAR, round_num, session_type)
                laps_df = process_laps(session)

                if laps_df.empty:
                    print(f"  ⚠️ {session_type} — no lap data available")
                    continue

                load_to_bigquery(
                    laps_df,
                    f"{PROJECT_ID}.{RAW_DATASET}.laps"
                )
                print(f"  ✅ {session_type} done!")

            except Exception as e:
                print(f"  ❌ {session_type} failed: {e}")