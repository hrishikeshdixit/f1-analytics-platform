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

# Separate logs for laps and telemetry
laps_log_file = 'cache/cached_laps.txt'
tel_log_file = 'cache/cached_telemetry.txt'

# Load existing logs
if os.path.exists(laps_log_file):
    with open(laps_log_file, 'r') as f:
        cached_laps = set(line.strip() for line in f.readlines())
else:
    cached_laps = set()

if os.path.exists(tel_log_file):
    with open(tel_log_file, 'r') as f:
        cached_telemetry = set(line.strip() for line in f.readlines())
else:
    cached_telemetry = set()

print(f"Already cached laps: {len(cached_laps)}")
print(f"Already cached telemetry: {len(cached_telemetry)}")

# ── PASS 1 — Laps ──
print("\n" + "="*60)
print("PASS 1 — Caching Laps")
print("="*60)

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

        if cache_key in cached_laps:
            print(f"  ⏭️ {session_name} laps already cached, skipping")
            continue

        for attempt in range(3):
            try:
                print(f"  Caching {session_name} laps (attempt {attempt+1}/3)...")
                session = fastf1.get_session(YEAR, round_num, session_name)
                session.load(
                    laps=True,
                    telemetry=False,
                    weather=False,
                    messages=False
                )
                print(f"  ✅ {session_name} laps cached!")

                with open(laps_log_file, 'a') as f:
                    f.write(f"{cache_key}\n")
                cached_laps.add(cache_key)

                print(f"  ⏳ Waiting 60 seconds...")
                time.sleep(60)
                break

            except Exception as e:
                if '500 calls/h' in str(e) or 'rate' in str(e).lower():
                    print(f"  ⚠️ Rate limited — waiting 10 minutes...")
                    time.sleep(600)
                elif attempt < 2:
                    print(f"  ⚠️ Attempt {attempt+1} failed: {e} — retrying in 60s")
                    time.sleep(60)
                else:
                    print(f"  ❌ {session_name} laps failed after 3 attempts: {e}")

# ── PASS 2 — Telemetry ──
print("\n" + "="*60)
print("PASS 2 — Caching Telemetry")
print("="*60)

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

        if cache_key in cached_telemetry:
            print(f"  ⏭️ {session_name} telemetry already cached, skipping")
            continue

        # Only cache telemetry if laps were cached successfully
        if cache_key not in cached_laps:
            print(f"  ⏭️ {session_name} laps not cached yet — skipping telemetry")
            continue

        for attempt in range(3):
            try:
                print(f"  Caching {session_name} telemetry (attempt {attempt+1}/3)...")
                session = fastf1.get_session(YEAR, round_num, session_name)
                session.load(
                    laps=True,
                    telemetry=True,
                    weather=False,
                    messages=False
                )
                print(f"  ✅ {session_name} telemetry cached!")

                with open(tel_log_file, 'a') as f:
                    f.write(f"{cache_key}\n")
                cached_telemetry.add(cache_key)

                print(f"  ⏳ Waiting 3 minutes...")
                time.sleep(180)
                break

            except Exception as e:
                if '500 calls/h' in str(e) or 'rate' in str(e).lower():
                    print(f"  ⚠️ Rate limited — waiting 10 minutes...")
                    time.sleep(600)
                elif attempt < 2:
                    print(f"  ⚠️ Attempt {attempt+1} failed: {e} — retrying in 60s")
                    time.sleep(60)
                else:
                    print(f"  ❌ {session_name} telemetry failed after 3 attempts: {e}")

print("\n✅ Cache build complete!")
print(f"Laps cached: {len(cached_laps)} sessions")
print(f"Telemetry cached: {len(cached_telemetry)} sessions")
