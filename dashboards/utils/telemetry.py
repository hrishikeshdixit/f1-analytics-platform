import streamlit as st
from dashboards.utils.bigquery import get_bq_client
import fastf1
import pandas as pd

@st.cache_data
def get_telemetry(year, round_number, driver_code, session_identifier='R'):
    client = get_bq_client()
    
    session_map = {
        'R': 'Race', 'Q': 'Qualifying', 'Sprint': 'Sprint',
        'FP1': 'Practice 1', 'FP2': 'Practice 2', 'FP3': 'Practice 3',
        'Sprint Qualifying': 'Sprint Qualifying'
    }
    session_type = session_map.get(session_identifier, session_identifier)
    
    query = f"""
        WITH lap_distances AS (
            SELECT 
                lap_number,
                MAX(distance) AS total_distance
            FROM `f1-analytics-491120.transformed.stg_telemetry`
            WHERE driver_code = '{driver_code}'
            AND round_number = {round_number}
            AND year = {year}
            AND session_type = '{session_type}'
            GROUP BY lap_number
        ),
        fastest_lap AS (
            SELECT lap_number
            FROM lap_distances
            ORDER BY total_distance DESC
            LIMIT 1
        )
        SELECT
            pos_x       AS X,
            pos_y       AS Y,
            pos_z       AS Z,
            speed       AS Speed,
            throttle    AS Throttle,
            CAST(brake AS INT64) * 100 AS Brake,
            gear        AS Gear,
            distance    AS Distance
        FROM `f1-analytics-491120.transformed.stg_telemetry`
        WHERE driver_code = '{driver_code}'
        AND round_number = {round_number}
        AND year = {year}
        AND session_type = '{session_type}'
        AND lap_number = (SELECT lap_number FROM fastest_lap)
        ORDER BY distance
    """
    
    try:
        df = client.query(query).to_dataframe()
        return df if not df.empty else None
    except Exception:
        return None
    
@st.cache_data
def get_lap_telemetry(year, round_number, driver_code,
                      lap_number, session_identifier='R'):
    client = get_bq_client()
    
    session_map = {
        'R': 'Race', 'Q': 'Qualifying', 'Sprint': 'Sprint',
        'FP1': 'Practice 1', 'FP2': 'Practice 2', 'FP3': 'Practice 3',
        'Sprint Qualifying': 'Sprint Qualifying'
    }
    session_type = session_map.get(session_identifier, session_identifier)

    query = f"""
        SELECT
            pos_x       AS X,
            pos_y       AS Y,
            speed       AS Speed,
            throttle    AS Throttle,
            CAST(brake AS INT64) * 100 AS Brake,
            gear        AS Gear,
            distance    AS Distance
        FROM `f1-analytics-491120.transformed.stg_telemetry`
        WHERE driver_code = '{driver_code}'
        AND round_number = {round_number}
        AND year = {year}
        AND session_type = '{session_type}'
        AND lap_number = {lap_number}
        ORDER BY distance
    """

    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            return None

        # Detect lap type
        fastest_query = f"""

            SELECT lap_number
            FROM `f1-analytics-491120.transformed.fact_all_sessions`
            WHERE driver_code = '{driver_code}'
            AND round_number = {round_number}
            AND year = {year}
            AND session_type = '{session_type}'
            AND lap_time_seconds IS NOT NULL
    ORDER BY lap_time_seconds ASC
    LIMIT 1
        """
        fastest = client.query(fastest_query).to_dataframe()
        fastest_lap = int(fastest.iloc[0]["lap_number"]) if not fastest.empty else None

        is_flying = fastest_lap is not None and int(lap_number) == fastest_lap
        df["is_flying"] = is_flying
        df["lap_type"] = "flying" if is_flying else "cool_down"

        return df

    except Exception:
        return None
    
@st.cache_data
def get_championship_standings(year):
    """
    Get WDC and WCC standings from FastF1.
    Includes both Race and Sprint points.
    """
    try:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        completed = schedule[schedule['EventDate'] < pd.Timestamp.now()]

        # Sprint weekends in 2026
        # Dynamically detect sprint rounds from the schedule
        sprint_rounds = []
        for _, event in schedule.iterrows():
            round_num = int(event['RoundNumber'])
            try:
                # Check if this event has a Sprint session
                event_obj = fastf1.get_event(2026, round_num)
                sessions = [
                    event_obj.get('Session1', ''),
                    event_obj.get('Session2', ''),
                    event_obj.get('Session3', ''),
                    event_obj.get('Session4', ''),
                    event_obj.get('Session5', ''),
                ]
                if 'Sprint' in sessions:
                    sprint_rounds.append(round_num)
            except Exception:
                continue

        print(f"Sprint rounds detected: {sprint_rounds}")

        # Points maps
        race_points = {1:25, 2:18, 3:15, 4:12, 5:10, 6:8, 7:6, 8:4, 9:2, 10:1}
        sprint_points = {1:8, 2:7, 3:6, 4:5, 5:4, 6:3, 7:2, 8:1}

        driver_points = {}
        constructor_points = {}

        def process_session(results, points_map, include_fastest_lap=False):
            if results is None or results.empty:
                return

            # Find fastest lap driver if applicable
            fastest_lap_driver = None
            if include_fastest_lap and 'FastestLap' in results.columns:
                fastest_lap_rows = results[results['FastestLap'] == True]
                if not fastest_lap_rows.empty:
                    fl_row = fastest_lap_rows.iloc[0]
                    # Only counts if driver finished in top 10
                    if pd.notna(fl_row['Position']) and int(fl_row['Position']) <= 10:
                        fastest_lap_driver = fl_row['Abbreviation']

            for _, row in results.iterrows():
                driver = row['Abbreviation']
                team = row['TeamName']
                position = row['Position']
                pts = points_map.get(int(position), 0) if pd.notna(position) else 0

                # Add fastest lap bonus point (Race only, not Sprint)
                if include_fastest_lap and driver == fastest_lap_driver:
                    pts += 1

                if driver not in driver_points:
                    driver_points[driver] = {
                        'driver_code': driver,
                        'full_name': row['FullName'],
                        'team': team,
                        'points': 0
                    }
                driver_points[driver]['points'] += pts

                if team not in constructor_points:
                    constructor_points[team] = {
                        'team': team,
                        'points': 0
                    }
                constructor_points[team]['points'] += pts

        for _, event in completed.iterrows():
            round_num = int(event['RoundNumber'])

            # Load Race session
            try:
                session = fastf1.get_session(year, round_num, 'R')
                session.load(
                    laps=False, telemetry=False,
                    weather=False, messages=False
                )
                process_session(session.results, race_points, include_fastest_lap=True)
            except Exception:
                pass

            # Load Sprint session if applicable
            if round_num in sprint_rounds:
                try:
                    sprint = fastf1.get_session(year, round_num, 'Sprint')
                    sprint.load(
                        laps=False, telemetry=False,
                        weather=False, messages=False
                    )
                    process_session(sprint.results, sprint_points, include_fastest_lap=True)
                except Exception:
                    pass

        drivers_df = pd.DataFrame(
            driver_points.values()
        ).sort_values('points', ascending=False).reset_index(drop=True)
        drivers_df['position'] = drivers_df.index + 1

        constructors_df = pd.DataFrame(
            constructor_points.values()
        ).sort_values('points', ascending=False).reset_index(drop=True)
        constructors_df['position'] = constructors_df.index + 1

        return drivers_df, constructors_df

    except Exception as e:
        return None, None
    
@st.cache_data
def get_pit_exit(year, round_number, driver_code):
    client = get_bq_client()
    
    query = f"""
        SELECT pos_x AS x, pos_y AS y
        FROM `f1-analytics-491120.transformed.stg_telemetry`
        WHERE driver_code = '{driver_code}'
        AND round_number = {round_number}
        AND year = {year}
        AND session_type = 'Race'
        AND lap_number = (
            SELECT MIN(lap_number) + 1
            FROM `f1-analytics-491120.transformed.stg_telemetry`
            WHERE driver_code = '{driver_code}'
            AND round_number = {round_number}
            AND year = {year}
            AND session_type = 'Race'
        )
        ORDER BY distance
        LIMIT 1
    """
    
    try:
        df = client.query(query).to_dataframe()
        if df.empty:
            return None
        return {'x': float(df['x'].iloc[0]), 'y': float(df['y'].iloc[0])}
    except Exception:
        return None
    
@st.cache_data
def get_driver_race_results(driver_code, year):
    try:
        schedule = fastf1.get_event_schedule(year, include_testing=False)
        completed = schedule[schedule['EventDate'] < pd.Timestamp.now()]

        race_points_map = {
            1:25, 2:18, 3:15, 4:12, 5:10,
            6:8, 7:6, 8:4, 9:2, 10:1
        }

        sprint_points_map = {
            1:8, 2:7, 3:6, 4:5, 5:4, 6:3, 7:2, 8:1
        }

        results = []
        for _, event in completed.iterrows():
            round_num = int(event['RoundNumber'])
            race_name = event['EventName']

            try:
                event_obj = fastf1.get_event(year, round_num)
                available_sessions = [
                    event_obj.get(f'Session{i}')
                    for i in range(1, 6)
                    if event_obj.get(f'Session{i}')
                    and pd.notna(event_obj.get(f'Session{i}'))
                ]
            except Exception:
                available_sessions = ['Race']

            race_pts = 0
            sprint_pts = 0
            race_pos = None

            try:
                session = fastf1.get_session(year, round_num, 'R')
                session.load(
                    laps=False, telemetry=False,
                    weather=False, messages=False
                )
                driver_result = session.results[
                    session.results['Abbreviation'] == driver_code
                ]
                if not driver_result.empty:
                    pos = driver_result.iloc[0]['Position']
                    race_pos = pos if pd.notna(pos) else None
                    race_pts = race_points_map.get(int(pos), 0) \
                        if pd.notna(pos) else 0
            except Exception:
                pass

            if 'Sprint' in available_sessions:
                try:
                    sprint = fastf1.get_session(year, round_num, 'Sprint')
                    sprint.load(
                        laps=False, telemetry=False,
                        weather=False, messages=False
                    )
                    sprint_result = sprint.results[
                        sprint.results['Abbreviation'] == driver_code
                    ]
                    if not sprint_result.empty:
                        sprint_pos = sprint_result.iloc[0]['Position']
                        sprint_pts = sprint_points_map.get(int(sprint_pos), 0) \
                            if pd.notna(sprint_pos) else 0
                except Exception:
                    pass

            results.append({
                'round_number': round_num,
                'race_name': race_name,
                'position': race_pos,
                'points': race_pts,
                'sprint_points': sprint_pts,
                'total_points': race_pts + sprint_pts
            })

        return pd.DataFrame(results)

    except Exception:
        return None
