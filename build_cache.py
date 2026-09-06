import fastf1
import pandas as pd
import os
import time

cache_dir = 'cache/'
os.makedirs(cache_dir, exist_ok=True)
fastf1.Cache.enable_cache(cache_dir)

YEAR = 2026

print("Building FastF1 cache — only new sessions...")

schedule = fastf1.get_event_schedule(YEAR, include_testing=False)
completed = schedule[schedule['EventDate'] < pd.Timestamp.now()]

# Track which sessions are already cached
cache_log_file = 'cache/cached_sessions.txt'

# Load existing cached sessions log
if os.path.exists(cache_log_file):
    with open(cache_log_file, 'r') as f:
        already_cached = set(line.strip() for line in f.readlines())
else:
    already_cached = set()

print(f"Already cached sessions: {len(already_cached)}")

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
    except Exception as e:
        print(f"  ⚠️ Could not get sessions: {e}")
        available_sessions = ['Race']

    for session_name in available_sessions:
        cache_key = f"{YEAR}_{round_num}_{session_name}"

        # Skip if already cached
        if cache_key in already_cached:
            print(f"  ⏭️ {session_name} already cached, skipping")
            continue

        for attempt in range(3):
            try:
                print(f"  Caching {session_name} (attempt {attempt+1}/3)...")
                session = fastf1.get_session(YEAR, round_num, session_name)
                session.load(
                    laps=True,
                    telemetry=True,
                    weather=False,
                    messages=False
                )
                print(f"  ✅ {session_name} cached!")

                # Log this session as cached
                with open(cache_log_file, 'a') as f:
                    f.write(f"{cache_key}\n")
                already_cached.add(cache_key)

                # Wait 3 minutes between sessions to avoid rate limit
                print(f"  ⏳ Waiting 3 minutes before next session...")
                time.sleep(180)
                break

            except Exception as e:
                if '500 calls/h' in str(e) or 'rate' in str(e).lower():
                    print(f"  ⚠️ Rate limited — waiting 10 minutes...")
                    time.sleep(600)
                    # Don't count this as an attempt — retry immediately
                elif attempt < 2:
                    print(f"  ⚠️ Attempt {attempt+1} failed: {e} — retrying in 60s")
                    time.sleep(60)
                else:
                    print(f"  ❌ {session_name} failed after 3 attempts: {e}")

print("\n✅ Cache build complete!")
