import fastf1
import pandas as pd
import os

cache_dir = 'cache/'
os.makedirs(cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

YEAR = 2026

print("Building FastF1 cache for all completed sessions...")

schedule = fastf1.get_event_schedule(YEAR, include_testing=False)
completed = schedule[schedule['EventDate'] < pd.Timestamp.now()]

for _, event in completed.iterrows():
    round_num = int(event['RoundNumber'])
    race_name = event['EventName']
    print(f"\n── Round {round_num}: {race_name} ──")

    try:
        event_obj = fastf1.get_event(YEAR, round_num)
        available_sessions = [
            event_obj.get(f'Session{i}')
            for i in range(1, 6)
            if event_obj.get(f'Session{i}')
            and pd.notna(event_obj.get(f'Session{i}'))
        ]
    except Exception:
        available_sessions = ['Race']

    for session_name in available_sessions:
        try:
            print(f"  Caching {session_name}...")
            session = fastf1.get_session(YEAR, round_num, session_name)
            session.load(telemetry=True, laps=True)
            print(f"  ✅ {session_name} cached!")
        except Exception as e:
            print(f"  ❌ {session_name}: {e}")

print("\n✅ Cache build complete!")
