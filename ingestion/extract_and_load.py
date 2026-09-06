import time
import fastf1
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
from datetime import datetime
import os

# Load credentials from .env file
load_dotenv()

# Enable FastF1 cache
cache_dir = '/tmp/fastf1_cache' if os.environ.get('GITHUB_ACTIONS') else 'cache/'
os.makedirs(cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

def extract_session(year, round_number, session_type='R', max_retries=5, retry_delay=60):
    """
    Pull a session from FastF1 with retry logic and validation.
    """
    print(f"Extracting {session_type} session - Year: {year}, Round: {round_number}")

    for attempt in range(max_retries):
        try:
            session = fastf1.get_session(year, round_number, session_type)
            session.load(
                laps=True,
                telemetry=False,
                weather=False,
                messages=False
            )

            # Validate data actually loaded
            if len(session.drivers) == 0:
                raise ValueError(f"No drivers loaded — API may have failed or data not available yet")

            print(f"  Loaded {len(session.drivers)} drivers successfully")
            return session

        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  ⚠️ Attempt {attempt + 1}/{max_retries} failed: {e}")
                print(f"  Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise Exception(f"Failed after {max_retries} attempts: {e}")

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

    # Convert ALL timedelta columns to seconds
    for col in laps.columns:
        if laps[col].dtype == 'timedelta64[ns]':
            laps[col] = laps[col].dt.total_seconds()

    laps = laps.reset_index(drop=True)
    print(f"Processed {len(laps)} laps")
    return laps

def load_to_bigquery(df, table_id):
    """Load a dataframe into BigQuery"""
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )
    print(f"Loading data to BigQuery table: {table_id}")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
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
    Reloads session with telemetry=True.
    """
    print(f"  Extracting telemetry...")

    try:
        # Reload with telemetry
        session.load(
            laps=True,
            telemetry=True,
            weather=False,
            messages=False
        )
    except Exception as e:
        print(f"  ⚠️ Could not reload telemetry: {e}")
        return None

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

                    required = ['X', 'Y', 'Speed', 'Distance']
                    if not all(col in tel.columns for col in required):
                        continue

                    tel["Driver"] = session.get_driver(driver)["Abbreviation"]
                    tel["RaceName"] = session.event["EventName"]
                    tel["RoundNumber"] = round_number
                    tel["Year"] = year
                    tel["SessionType"] = session.name
                    tel["LapNumber"] = lap["LapNumber"]

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

    schedule = fastf1.get_event_schedule(YEAR, include_testing=False)
    current_time = pd.Timestamp.now(tz='UTC')

    # Check existing laps in BigQuery
    client = bigquery.Client()
    try:
        existing = client.query(f"""
            SELECT DISTINCT RoundNumber, SessionType
            FROM `{PROJECT_ID}.{RAW_DATASET}.laps`
        """).to_dataframe()
        existing_sessions = set(zip(existing['RoundNumber'], existing['SessionType']))
    except Exception:
        existing_sessions = set()

    print(f"Already loaded sessions: {existing_sessions}")

    # Check existing telemetry in BigQuery
    try:
        existing_tel = client.query(f"""
            SELECT DISTINCT RoundNumber, SessionType
            FROM `{PROJECT_ID}.{RAW_DATASET}.telemetry`
        """).to_dataframe()
        existing_telemetry = set(zip(existing_tel["RoundNumber"], existing_tel["SessionType"]))
    except Exception:
        existing_telemetry = set()

    print(f"Already loaded telemetry sessions: {existing_telemetry}")

    any_failure = False

    for _, event in schedule.iterrows():
        round_num = event['RoundNumber']
        race_name = event['EventName']
        print(f"\n── Round {round_num}: {race_name} ──")

        # Dynamically get available sessions
        available_sessions = []
        try:
            event_obj = fastf1.get_event(YEAR, round_num)
            for i in range(1, 6):
                session_name = event_obj.get(f'Session{i}')
                if session_name and pd.notna(session_name) and session_name != '':
                    available_sessions.append(session_name)
            print(f"  Available sessions: {available_sessions}")
        except Exception as e:
            print(f"  ⚠️ Could not get sessions for Round {round_num}: {e}")
            continue  # Skip round, don't stop pipeline

        for session_type in available_sessions:
            # Already loaded → skip
            if (round_num, session_type) in existing_sessions:
                print(f"  ⏭️ {session_type} already loaded, skipping")
                continue

            # Check if session has happened yet
            try:
                session_idx = available_sessions.index(session_type) + 1
                session_start = event_obj.get(f'Session{session_idx}DateUtc')
                if session_start is not None and pd.notna(session_start):
                    if pd.Timestamp(session_start, tz='UTC') > current_time:
                        print(f"  ⏳ {session_type} hasn't happened yet, skipping")
                        continue
            except Exception:
                pass

            # Load session
            try:
                session = extract_session(YEAR, round_num, session_type)
                laps_df = process_laps(session)

                if laps_df.empty:
                    print(f"  ⚠️ {session_type} — no lap data, skipping")
                    continue

                load_to_bigquery(
                    laps_df,
                    f"{PROJECT_ID}.{RAW_DATASET}.laps"
                )
                print(f"  ✅ {session_type} laps done!")

                # Load telemetry
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
                any_failure = True
                continue  # Don't stop — move to next session

    if any_failure:
        print("\n⚠️ Pipeline completed with some failures — check logs above.")
        exit(1)  # Exit with error so GitHub Actions sends failure email
    else:
        print("\n✅ Pipeline completed successfully.")