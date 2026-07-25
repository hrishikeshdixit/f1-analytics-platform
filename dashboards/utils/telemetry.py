import streamlit as st
import fastf1
import pandas as pd

@st.cache_data
def get_telemetry(year, round_number, driver_code, session_identifier='R'):
    """
    Load fastest lap telemetry for a driver from FastF1.
    Returns DataFrame with X, Y, Speed columns or None if unavailable.
    """
    try:
        session = fastf1.get_session(year, round_number, session_identifier)
        session.load(telemetry=True, laps=True)
        driver_laps = session.laps.pick_driver(driver_code)

        if driver_laps.empty:
            return None

        fastest_lap = driver_laps.pick_fastest()

        if fastest_lap is None:
            return None

        telemetry = fastest_lap.get_telemetry()

        if 'X' not in telemetry.columns or 'Y' not in telemetry.columns:
            return None

        return telemetry[['X', 'Y', 'Speed', 'Distance']]

    except Exception:
        return None
    
@st.cache_data
def get_lap_telemetry(year, round_number, driver_code, lap_number, session_identifier = 'R'):
    """
    Get full telemetry for a specific lap number.
    Returns Speed, Throttle, Brake, Gear, Distance, X, Y columns.
    """
    try:
        session = fastf1.get_session(year, round_number, session_identifier)
        session.load(telemetry=True, laps=True)
        driver_laps = session.laps.pick_driver(driver_code)

        # Get specific lap
        lap = driver_laps[driver_laps['LapNumber'] == lap_number]

        if lap.empty:
            return None

        lap = lap.iloc[0]
        telemetry = lap.get_telemetry()

        required = ['Speed', 'Throttle', 'Brake', 'nGear', 'Distance', 'X', 'Y']
        for col in required:
            if col not in telemetry.columns:
                return None
        driver_laps_all = session.laps.pick_driver(driver_code)
        all_lap_times = driver_laps_all['LapTime'].dt.total_seconds().dropna()
        current_lap_seconds = lap['LapTime'].total_seconds() \
            if pd.notna(lap['LapTime']) else None

        if current_lap_seconds and len(all_lap_times) > 0:
            fastest_time_seconds = all_lap_times.min()
            is_flying = bool(current_lap_seconds <= fastest_time_seconds * 1.10)
        else:
            is_flying = False

        # Detect out lap — lap immediately after pit or first lap of session
        is_out_lap = pd.notna(lap.get('PitOutTime', None))

        # Lap type: 'flying', 'out_lap', 'cool_down'
        if is_out_lap:
            lap_type = 'out_lap'
        elif is_flying:
            lap_type = 'flying'
        else:
            lap_type = 'cool_down'

        telemetry = telemetry[required].copy()
        telemetry = telemetry.rename(columns={'nGear': 'Gear'})
        telemetry['Brake'] = telemetry['Brake'].astype(int) * 100
        telemetry['is_flying'] = is_flying
        telemetry['lap_type'] = lap_type

        return telemetry

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
    """
    Get approximate pit exit GPS coordinates from lap data.
    """
    try:
        session = fastf1.get_session(year, round_number, 'R')
        session.load(telemetry=True, laps=True)
        driver_laps = session.laps.pick_driver(driver_code)

        # Find a lap where driver came out of pits
        pit_out_laps = driver_laps[driver_laps['PitOutTime'].notna()]

        if pit_out_laps.empty:
            return None

        # Get telemetry for first pit out lap
        pit_lap = pit_out_laps.iloc[0]
        tel = pit_lap.get_telemetry()

        if tel.empty or 'X' not in tel.columns:
            return None

        # Pit exit is at the very start of the out lap
        return {
            'x': float(tel['X'].iloc[5]),
            'y': float(tel['Y'].iloc[5])
        }

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