from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st

SESSION_TABLE_MAP = {
    "Race": "fact_laps",
    "Qualifying": "stg_laps_qualifying",
    "Sprint Qualifying": "stg_laps_qualifying",
    "Sprint": "stg_laps_sprint",
    "Practice 1": "stg_laps_practice",
    "Practice 2": "stg_laps_practice",
    "Practice 3": "stg_laps_practice",
}

@st.cache_resource
def get_bq_client():

    """Create and cache BigQuery client."""
    return bigquery.Client()

'''    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )

    client = bigquery.Client(
        credentials=credentials,
        project=st.secrets["gcp_service_account"]["project_id"]
    )

    return client'''



@st.cache_data
def get_races():
    """Get all available races from BigQuery."""
    client = get_bq_client()
    query = """
        SELECT DISTINCT
            race_name,
            round_number,
            year
        FROM `f1-analytics-491120.transformed.fact_laps`

        UNION DISTINCT

        SELECT DISTINCT
            race_name,
            round_number,
            year
        FROM `f1-analytics-491120.transformed.stg_laps_qualifying`

        UNION DISTINCT

        SELECT DISTINCT
            race_name,
            round_number,
            year
        FROM `f1-analytics-491120.transformed.stg_laps_sprint`

        UNION DISTINCT

        SELECT DISTINCT
            race_name,
            round_number,
            year
        FROM `f1-analytics-491120.transformed.stg_laps_practice`

        ORDER BY round_number
    """
    return client.query(query).to_dataframe()

@st.cache_data
def get_drivers(race_name, session_type):
    """Get drivers for selected race and session."""

    client = get_bq_client()

    table = SESSION_TABLE_MAP.get(
        session_type,
        "fact_all_sessions"
    )

    session_filter = f"AND session_type = '{session_type}'" \
        if session_type != 'Race' else ""

    query = f"""
        SELECT DISTINCT 
            driver_code,
            team
        FROM `f1-analytics-491120.transformed.{table}`
        WHERE race_name = '{race_name}'
        ORDER BY driver_code
    """
    return client.query(query).to_dataframe()

@st.cache_data
def get_max_laps(race_name, session_type='Race'):
    """Get total number of laps for a race and session type."""
    client = get_bq_client()

    # Map session type to correct table
    table_map = SESSION_TABLE_MAP

    table = table_map.get(session_type, 'fact_laps')
    session_filter = f"AND session_type = '{session_type}'" \
        if session_type != 'Race' else ""

    query = f"""
        SELECT MAX(lap_number) as max_lap
        FROM `f1-analytics-491120.transformed.{table}`
        WHERE race_name = '{race_name}'
        {session_filter}
    """

    try:
        result = client.query(query).to_dataframe()['max_lap'].iloc[0]
        return int(result) if result else 1
    except Exception:
        return 1
'''
def get_max_laps(race_name):
    """Get total number of laps for a race."""
    client = get_bq_client()
    query = f"""
        SELECT MAX(lap_number) as max_lap
        FROM `f1-analytics-491120.transformed.fact_laps`
        WHERE race_name = '{race_name}'
    """
    return int(client.query(query).to_dataframe()['max_lap'].iloc[0])
'''


@st.cache_data
def get_driver_features():
    """Get aggregated driver features for ML model — normalised per circuit."""
    client = get_bq_client()
    query = """
        WITH circuit_baselines AS (
            SELECT
                race_name,
                MIN(lap_time_seconds) AS circuit_fastest_lap
            FROM `f1-analytics-491120.transformed.fact_laps`
            WHERE is_race_representative = TRUE
            AND lap_time_seconds IS NOT NULL
            GROUP BY race_name
        ),

        normalised_laps AS (
            SELECT
                f.driver_code,
                f.team,
                f.lap_time_delta,
                f.speed_longest_straight,
                f.tyre_compound,
                (f.lap_time_seconds - c.circuit_fastest_lap)
                    / c.circuit_fastest_lap * 100                AS norm_lap_time,
                (f.sector_1_seconds - MIN(f.sector_1_seconds)
                    OVER (PARTITION BY f.race_name))
                    / MIN(f.sector_1_seconds)
                    OVER (PARTITION BY f.race_name) * 100        AS norm_sector_1,
                (f.sector_2_seconds - MIN(f.sector_2_seconds)
                    OVER (PARTITION BY f.race_name))
                    / MIN(f.sector_2_seconds)
                    OVER (PARTITION BY f.race_name) * 100        AS norm_sector_2,
                (f.sector_3_seconds - MIN(f.sector_3_seconds)
                    OVER (PARTITION BY f.race_name))
                    / MIN(f.sector_3_seconds)
                    OVER (PARTITION BY f.race_name) * 100        AS norm_sector_3
            FROM `f1-analytics-491120.transformed.fact_laps` f
            JOIN circuit_baselines c ON f.race_name = c.race_name
            WHERE f.is_race_representative = TRUE
            AND f.lap_time_seconds IS NOT NULL
            AND f.sector_1_seconds IS NOT NULL
            AND f.sector_2_seconds IS NOT NULL
            AND f.sector_3_seconds IS NOT NULL
        ),

        race_stats AS (
            SELECT
                driver_code,
                team,
                AVG(norm_lap_time)                          AS avg_norm_lap_time,
                STDDEV(norm_lap_time)                       AS lap_consistency,
                AVG(norm_sector_1)                          AS avg_norm_sector_1,
                AVG(norm_sector_2)                          AS avg_norm_sector_2,
                AVG(norm_sector_3)                          AS avg_norm_sector_3,
                AVG(CASE
                    WHEN lap_time_delta IS NOT NULL
                    THEN lap_time_delta
                    ELSE 0
                END)                                        AS avg_lap_delta,
                MAX(speed_longest_straight)                 AS top_speed,
                AVG(speed_longest_straight)                 AS avg_speed,
                COUNTIF(tyre_compound = 'SOFT') / COUNT(*) AS soft_preference,
                COUNTIF(tyre_compound = 'HARD') / COUNT(*) AS hard_preference,
                COUNT(*)                                    AS total_laps
            FROM normalised_laps
            GROUP BY driver_code, team
            HAVING COUNT(*) >= 30
        ),

        sprint_stats AS (
            SELECT
                driver_code,
                AVG(lap_time_seconds)                       AS avg_sprint_lap_time,
                MIN(lap_time_seconds)                       AS best_sprint_lap,
                STDDEV(lap_time_seconds)                    AS sprint_consistency,
                MAX(speed_longest_straight)                 AS sprint_top_speed
            FROM `f1-analytics-491120.transformed.stg_laps_sprint`
            WHERE lap_time_seconds IS NOT NULL
            AND sector_1_seconds IS NOT NULL
            GROUP BY driver_code
        )

        SELECT
            r.*,
            CASE r.driver_code
                WHEN 'ALB' THEN 'Alexander Albon'
                WHEN 'ALO' THEN 'Fernando Alonso'
                WHEN 'ANT' THEN 'Kimi Antonelli'
                WHEN 'BEA' THEN 'Oliver Bearman'
                WHEN 'BOR' THEN 'Gabriel Bortoleto'
                WHEN 'BOT' THEN 'Valtteri Bottas'
                WHEN 'COL' THEN 'Franco Colapinto'
                WHEN 'GAS' THEN 'Pierre Gasly'
                WHEN 'HAD' THEN 'Isack Hadjar'
                WHEN 'HAM' THEN 'Lewis Hamilton'
                WHEN 'HUL' THEN 'Nico Hulkenberg'
                WHEN 'LAW' THEN 'Liam Lawson'
                WHEN 'LEC' THEN 'Charles Leclerc'
                WHEN 'LIN' THEN 'Arvid Lindblad'
                WHEN 'NOR' THEN 'Lando Norris'
                WHEN 'OCO' THEN 'Esteban Ocon'
                WHEN 'PER' THEN 'Sergio Perez'
                WHEN 'PIA' THEN 'Oscar Piastri'
                WHEN 'RUS' THEN 'George Russell'
                WHEN 'SAI' THEN 'Carlos Sainz'
                WHEN 'STR' THEN 'Lance Stroll'
                WHEN 'VER' THEN 'Max Verstappen'
                ELSE r.driver_code
            END                                                   AS full_name,
            COALESCE(s.avg_sprint_lap_time, r.avg_norm_lap_time) AS avg_sprint_lap_time,
            COALESCE(s.best_sprint_lap, 0)                        AS best_sprint_lap,
            COALESCE(s.sprint_consistency, r.lap_consistency)     AS sprint_consistency,
            COALESCE(s.sprint_top_speed, r.top_speed)             AS sprint_top_speed
        FROM race_stats r
        LEFT JOIN sprint_stats s ON r.driver_code = s.driver_code
        ORDER BY r.driver_code
    """
    return client.query(query).to_dataframe()

@st.cache_data
def get_available_sessions(race_name):
    """
    Get available session types for a specific race from BigQuery.
    Only returns sessions that actually have data.
    """
    client = get_bq_client()
    query = f"""
        SELECT DISTINCT SessionType
        FROM `f1-analytics-491120.raw.laps`
        WHERE RaceName = '{race_name}'
        ORDER BY SessionType
    """
    df = client.query(query).to_dataframe()
    return df['SessionType'].tolist()