🏎️ F1 Analytics Platform

An end-to-end data analytics platform for the 2026 Formula 1 season — built to demonstrate modern data engineering, machine learning, and interactive visualisation on Google Cloud.

---

## What This Project Does

The platform automatically ingests all F1 race weekend sessions every week, transforms the data through a structured dbt modeling layer with automated quality testing, and serves it through two front ends:

- **Streamlit Web App** — interactive circuit replay, telemetry analysis, driver fingerprinting, and championship standings
- **Looker Studio Dashboard** — 5-page BI dashboard covering race, qualifying, practice, and sprint sessions

---

## Architecture

<img width="2720" height="2960" alt="f1_analytics_architecture" src="https://github.com/user-attachments/assets/0142effc-b277-4ba4-bad6-3d24c94da5b8" />

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **FastF1** | F1 telemetry and lap data from the official timing API |
| **Google BigQuery** | Cloud data warehouse |
| **dbt** | SQL transformation layer with automated testing |
| **Streamlit** | Interactive web application |
| **Looker Studio** | Business intelligence dashboard |
| **Plotly** | Interactive charts and track visualisations |
| **scikit-learn** | Gaussian Mixture Model clustering |
| **Google Cloud Platform** | Cloud infrastructure |
| **GitHub Actions** | Automated weekly pipeline (planned) |
| **Docker** | Containerisation (planned) |
| **Apache Airflow** | Pipeline orchestration (planned) |

---

## Data Coverage

- **Season:** 2026 F1 World Championship
- **Rounds loaded:** 9 (through British GP)
- **Sessions per round:** Practice 1/2/3, Sprint Qualifying, Sprint, Qualifying, Race
- **Raw rows:** ~150,000+ laps across all sessions
- **dbt models:** 7 staging + mart models
- **Data quality tests:** 24 automated tests

---

## Streamlit App Pages

### 🏎️ Circuit Replay
- Race/session selector with dynamic session detection per race weekend
- 2D sector-coloured track map (S1=Red, S2=Yellow, S3=Purple)
- Turn number annotations, start/finish flag, pit exit marker
- Scroll to zoom, hover for telemetry (speed, gear, throttle, brake)
- 4-panel telemetry analysis: Speed, Throttle, Brake, Gear vs distance
- Flying laps in team colour · Cool down laps in white · Out laps in sky blue
- Teammate line style differentiation (solid vs dashed)
- Driver photos and team background theming

### 🧬 Driver Fingerprinting
- **Gaussian Mixture Model** clustering of all 22 drivers
- **Circuit-normalised features** — lap times expressed as % gap to circuit fastest (not raw seconds) for fair cross-circuit comparison
- 14 features: race pace, consistency, sector times, tyre management, sprint pace
- PCA scatter plot with cluster legend
- Radar chart: Race Pace, Consistency, Top Speed, Tyre Management, Sprint Pace
- Similar drivers panel using Euclidean distance
- GMM probability table showing soft cluster membership per driver

### Championship Standings
- Live WDC and WCC standings from FastF1 results
- Dynamic sprint round detection — no hardcoded round numbers
- Race + sprint points calculated separately
- Click any driver → drill down showing:
  - Season stats (wins, podiums, best finish, DNFs)
  - Points progression chart
  - Race-by-race results table with sprint points

### Home
- Coming soon

---

## Looker Studio Dashboard

| Page | Content |
|---|---|
| Race Overview | Fastest laps, team pace, tyre usage by team |
| Driver Lap Analysis | Lap time evolution, sector pivot, full lap table |
| Tyre Strategy & Speed | Strategy chart, 4 speed trap comparisons |
| Qualifying Analysis | Fastest Q laps, gap to pole, sector scorecards |
| Practice & Long Runs | Session pace comparison, driver improvement |

---

## ML Model — Gaussian Mixture Models

GMM was chosen over K-Means because F1 drivers don't fit neatly into hard clusters. A driver like Russell is simultaneously competitive on one-lap pace but inconsistent on race management. GMM produces **soft cluster membership probabilities** per driver — more statistically honest than a binary assignment.

**Key design decision — circuit normalisation:**
Raw lap times can't be compared across circuits (Monza laps ≈ 80s, Monaco laps ≈ 74s). All lap times are expressed as **% gap to the fastest lap at that circuit** before being fed into the model.

**Features used:**
- `avg_norm_lap_time` — normalised race pace
- `lap_consistency` — stddev of normalised lap times
- `avg_norm_sector_1/2/3` — normalised sector strengths
- `avg_lap_delta` — tyre degradation proxy
- `top_speed` / `avg_speed` — straight line speed (absolute)
- `soft_preference` / `hard_preference` — tyre strategy
- `avg_sprint_lap_time` / `best_sprint_lap` — sprint pace
- `sprint_consistency` / `sprint_top_speed` — sprint characteristics

---

## Project Structure

```
f1-analytics-platform/
├── ingestion/
│   └── extract_and_load.py     # FastF1 → BigQuery ingestion
├── dbt/
│   └── f1/
│       └── models/
│           ├── staging/         # 4 staging models + tests
│           └── marts/           # fact_laps, dim_drivers, fact_all_sessions
├── dashboards/
│   ├── app.py                   # Streamlit entry point
│   ├── views/                   # Page modules
│   ├── utils/                   # BigQuery, telemetry, styles
│   └── components/              # Track map, driver cards
├── ml/
│   └── driver_fingerprints.py  # GMM clustering model
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Getting Started

### Prerequisites
- Python 3.11+
- Google Cloud account with BigQuery enabled
- GCP Service Account with BigQuery Admin role

### Setup

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/f1-analytics-platform.git
cd f1-analytics-platform

# Create virtual environment
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux

# Install dependencies
pip install -r requirements.txt
```

### Environment Variables

Create a `.env` file in the root:
```
GOOGLE_APPLICATION_CREDENTIALS=./secrets/gcp_key.json
BQ_PROJECT_ID=your-gcp-project-id
BQ_DATASET_RAW=raw
BQ_DATASET_TRANSFORMED=transformed
```

Place your GCP service account JSON key at `secrets/gcp_key.json`.

### Run the Pipeline

```bash
# Load F1 data into BigQuery
python ingestion/extract_and_load.py

# Transform with dbt
cd dbt/f1
dbt run
dbt test

# Launch the app
cd ../..
streamlit run dashboards/app.py
```

---

## Weekly Workflow

After each race weekend:

```bash
# 1. Load new race data
python ingestion/extract_and_load.py

# 2. Rebuild dbt models
cd dbt/f1
dbt build

# 3. App and Looker Studio update automatically
```

---

## Security Notes

The following are excluded from this repo via `.gitignore`:
- `secrets/` — GCP service account credentials
- `.env` — environment variables
- `venv/` — Python virtual environment
- `cache/` — FastF1 data cache
- `dbt/f1/target/` — compiled dbt files

---

## Status

| Phase | Status |
|---|---|
| Data Pipeline — all sessions | ✅ Complete |
| dbt Transformations | ✅ Complete |
| Streamlit App | ✅ Complete |
| Looker Studio Dashboard | ✅ Complete |
| GitHub Actions automation | 🔜 Planned |
| Docker containerisation | 🔜 Planned |
| Airflow orchestration | 🔜 Planned |

---

## Author

Built as a portfolio project demonstrating end-to-end data engineering skills.
Master's student in Information Systems.

*Stack: Python · FastF1 · Google BigQuery · dbt · Streamlit · Looker Studio · GMM · Plotly · GCP*
