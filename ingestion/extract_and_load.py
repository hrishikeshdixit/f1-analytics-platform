import sys
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
    session.load(
        laps = True,
        telemetry = False,
        weather = False,
        messages = False,
      )
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

def load_telemetry_to_bigquery(df, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
        clustering_fields=["Driver", "SessionType", "RoundNumber"]
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"  ✅ Loaded {len(df)} rows")

def extract_telemetry(session, round_number, year):
    """
    Extract telemetry for all drivers from a session.
    Loads telemetry for every lap and stores in BigQuery.
    """
    print(f"  Extracting telemetry...")

    # Check if telemetry data is available for this session
    try:
        test_driver = session.drivers[0]
        test_lap = session.laps.pick_driver(test_driver).iloc[0]
        _ = test_lap.get_telemetry()
    except Exception:
        print(f"    ⏭️ {session.name} yet to be loaded")
        return

    all_telemetry = []

    for driver in session.drivers:
        try:
            driver_laps = session.laps.pick_driver(driver)

            if driver_laps.empty:
                continue

            for _, lap in driver_laps.iterrows():
                try:
                    tel = lap.get_telemetry()

                    if tel.empty:
                        continue

                    # Check required columns
                    required = ['X', 'Y', 'Speed', 'Distance']
                    if not all(col in tel.columns for col in required):
                        continue

                    # Add lap metadata
                    tel["Driver"] = session.get_driver(driver)["Abbreviation"]
                    tel["RaceName"] = session.event["EventName"]
                    tel["RoundNumber"] = round_number
                    tel["Year"] = year
                    tel["SessionType"] = session.name
                    tel["LapNumber"] = lap["LapNumber"]

                    # Select only needed columns
                    cols = [
                        "Driver", "RaceName", "RoundNumber", "Year",
                        "SessionType", "LapNumber", "X", "Y",
                        "Distance", "Speed"
                    ]

                    optional = ["Z", "Throttle", "Brake", "nGear", "IsAccurate"]
                    for col in optional:
                        if col in tel.columns:
                            cols.append(col)

                    all_telemetry.append(tel[cols])

                except Exception:
                    continue

        except Exception as e:
            print(f"    ⚠️ No telemetry for {driver}: {e}")
            continue
    
    if all_telemetry:
        df = pd.concat(all_telemetry, ignore_index=True)
        
        # Convert timedelta columns
        for col in df.columns:
            if df[col].dtype == 'timedelta64[ns]':
                df[col] = df[col].dt.total_seconds()
        
        print(f"  Processed {len(df)} telemetry points")
        return df
    
    return None

if __name__ == "__main__":
    PROJECT_ID = os.getenv('BQ_PROJECT_ID')
    RAW_DATASET = os.getenv('BQ_DATASET_RAW')
    YEAR = 2026

    # All possible session types to load
    #ALL_SESSIONS = ['FP1', 'FP2', 'FP3', 'SQ', 'S', 'Q', 'R']

    # Dynamically get all completed rounds
    schedule = fastf1.get_event_schedule(YEAR, include_testing=False)

    current_time = pd.Timestamp.now(tz='UTC')

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
    except Exception:
        existing_sessions = set()

    print(f"Already loaded sessions: {existing_sessions}")

    # Check existing telemetry in BigQuery
    telemetry_query = f"""
        SELECT DISTINCT RoundNumber, SessionType
        FROM `{PROJECT_ID}.{RAW_DATASET}.telemetry`
    """

    try:
        existing_tel = client.query(telemetry_query).to_dataframe()
        existing_telemetry = set(
            zip(existing_tel["RoundNumber"], existing_tel["SessionType"])
        )
    except Exception:
        existing_telemetry = set()

    print(f"Already loaded telemetry sessions: {existing_telemetry}")

    pipeline_failed = False

    for _, event in schedule.iterrows():
        if pipeline_failed:
            break

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
            # Could not even fetch the schedule for this round — treat as a
            # real failure and stop, rather than silently skipping the round.
            print(f"  ❌ Could not get sessions for Round {round_num}: {e}")
            pipeline_failed = True
            break

        for session_type in available_sessions:
            # 1) Already loaded --> skip
            if (round_num, session_type) in existing_sessions:
                print(f"  ⏭️ {session_type} already loaded, skipping")
                continue

            # 2) Not present because it hasn't happened yet --> skip (not a failure)
            session_start = event_obj.get(f'Session{available_sessions.index(session_type) + 1}DateUtc')
            if session_start is not None and pd.notna(session_start):
                if pd.Timestamp(session_start, tz='UTC') > current_time:
                    print(f"  ⏳ {session_type} hasn't happened yet, skipping")
                    continue

            # 3) Not present and already happened --> load; stop pipeline on first real failure
            try:
                session = extract_session(YEAR, round_num, session_type)
                laps_df = process_laps(session)

                if laps_df.empty:
                    print(f"  ⚠️ {session_type} — no lap data (treating as failure)")
                    pipeline_failed = True
                    break

                # Load laps
                load_to_bigquery(
                    laps_df,
                    f"{PROJECT_ID}.{RAW_DATASET}.laps"
                )
                print(f"  ✅ {session_type} laps done!")

                # Load telemetry if not already loaded
                if (round_num, session_type) not in existing_telemetry:
                    tel_df = extract_telemetry(session, round_num, YEAR)

                    if tel_df is not None and not tel_df.empty:
                        load_telemetry_to_bigquery(
                            tel_df,
                            f"{PROJECT_ID}.{RAW_DATASET}.telemetry"
                        )
                        print(f"  ✅ {session_type} telemetry done!")
                    else:
                        print(f"  ⚠️ {session_type} — no telemetry data")
                else:
                    print(f"  ⏭️ {session_type} telemetry already loaded")

            except Exception as e:
                print(f"  ❌ {session_type} failed: {e}")
                pipeline_failed = True
                break

    if pipeline_failed:
        print("\n🛑 Pipeline stopped due to failure.")
        sys.exit(1)
    else:
        print("\n✅ Pipeline completed successfully.")