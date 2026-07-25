import pandas as pd
import numpy as np
from google.cloud import bigquery
from sklearn.preprocessing import StandardScaler
from sklearn.mixture import GaussianMixture
from sklearn.decomposition import PCA
from dotenv import load_dotenv
import os

load_dotenv()

# ── Fetch Driver Features from BigQuery ──
def get_driver_features():
    
    client = bigquery.Client()
    
    query = """
        WITH circuit_baselines AS (
            -- Get the fastest lap per circuit to normalise against
            SELECT
                race_name,
                MIN(lap_time_seconds)   AS circuit_fastest_lap,
                AVG(lap_time_seconds)   AS circuit_avg_lap
            FROM `f1-analytics-491120.transformed.fact_laps`
            WHERE is_race_representative = TRUE
            AND lap_time_seconds IS NOT NULL
            GROUP BY race_name
        ),

        normalised_laps AS (
            -- Express each lap as % gap to circuit fastest
            SELECT
                f.driver_code,
                f.team,
                f.race_name,
                f.lap_time_seconds,
                f.sector_1_seconds,
                f.sector_2_seconds,
                f.sector_3_seconds,
                f.lap_time_delta,
                f.speed_longest_straight,
                f.tyre_compound,
                -- Normalised lap time: gap to fastest lap in % 
                (f.lap_time_seconds - c.circuit_fastest_lap) 
                    / c.circuit_fastest_lap * 100           AS norm_lap_time,
                -- Normalised sector times
                (f.sector_1_seconds - MIN(f.sector_1_seconds) 
                    OVER (PARTITION BY f.race_name)) 
                    / MIN(f.sector_1_seconds) 
                    OVER (PARTITION BY f.race_name) * 100   AS norm_sector_1,
                (f.sector_2_seconds - MIN(f.sector_2_seconds) 
                    OVER (PARTITION BY f.race_name)) 
                    / MIN(f.sector_2_seconds) 
                    OVER (PARTITION BY f.race_name) * 100   AS norm_sector_2,
                (f.sector_3_seconds - MIN(f.sector_3_seconds) 
                    OVER (PARTITION BY f.race_name)) 
                    / MIN(f.sector_3_seconds) 
                    OVER (PARTITION BY f.race_name) * 100   AS norm_sector_3
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
                -- Normalised pace (% gap to fastest — lower is better)
                AVG(norm_lap_time)                          AS avg_norm_lap_time,
                -- Consistency (stddev of normalised lap times)
                STDDEV(norm_lap_time)                       AS lap_consistency,
                -- Normalised sector strengths
                AVG(norm_sector_1)                          AS avg_norm_sector_1,
                AVG(norm_sector_2)                          AS avg_norm_sector_2,
                AVG(norm_sector_3)                          AS avg_norm_sector_3,
                -- Tyre management
                AVG(CASE 
                    WHEN lap_time_delta IS NOT NULL 
                    THEN lap_time_delta 
                    ELSE 0 
                END)                                        AS avg_lap_delta,
                -- Speed (absolute — comparable across circuits)
                MAX(speed_longest_straight)                 AS top_speed,
                AVG(speed_longest_straight)                 AS avg_speed,
                -- Tyre preferences
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
            COALESCE(s.avg_sprint_lap_time, r.avg_norm_lap_time) AS avg_sprint_lap_time,
            COALESCE(s.best_sprint_lap, 0)                        AS best_sprint_lap,
            COALESCE(s.sprint_consistency, r.lap_consistency)     AS sprint_consistency,
            COALESCE(s.sprint_top_speed, r.top_speed)             AS sprint_top_speed

        FROM race_stats r
        LEFT JOIN sprint_stats s ON r.driver_code = s.driver_code
        ORDER BY r.driver_code
    """
    
    df = client.query(query).to_dataframe()
    print(f"Loaded {len(df)} drivers")
    return df


# ── Feature Engineering ──
def prepare_features(df):
    feature_cols = [
        'avg_norm_lap_time',      # normalised pace
        'lap_consistency',         # stddev of normalised laps
        'avg_norm_sector_1',      # normalised sector 1
        'avg_norm_sector_2',      # normalised sector 2
        'avg_norm_sector_3',      # normalised sector 3
        'avg_lap_delta',          # tyre degradation
        'top_speed',              # absolute speed
        'avg_speed',
        'soft_preference',
        'hard_preference',
        'avg_sprint_lap_time',
        'best_sprint_lap',
        'sprint_consistency',
        'sprint_top_speed'
    ]

    X = df[feature_cols].fillna(0)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return X_scaled, feature_cols, scaler


# ── GMM Clustering ──
def run_clustering(X_scaled, n_components=4):
    """
    Gaussian Mixture Model clustering.
    Unlike K-Means, GMM gives each driver a probability
    of belonging to each cluster — soft clustering.
    n_components = number of gaussian distributions to fit
    """
    gmm = GaussianMixture(
        n_components=n_components,
        covariance_type='full',
        random_state=42,
        n_init=10,
        max_iter=200
    )
    
    clusters = gmm.fit_predict(X_scaled)
    
    # Get probability of each driver belonging to each cluster
    probabilities = gmm.predict_proba(X_scaled)
    
    print(f"GMM converged: {gmm.converged_}")
    print(f"GMM BIC score: {gmm.bic(X_scaled):.2f}")
    
    return clusters, gmm, probabilities


# ── PCA for 2D Visualization ──
def run_pca(X_scaled):
    pca = PCA(n_components=2, random_state=42)
    X_pca = pca.fit_transform(X_scaled)
    explained = pca.explained_variance_ratio_
    print(f"PCA explains {explained[0]*100:.1f}% + {explained[1]*100:.1f}% = {sum(explained)*100:.1f}% of variance")
    return X_pca, pca


# ── Dynamic Cluster Labels ──
def assign_cluster_labels(df, clusters):
    """
    Dynamically assign labels based on composite performance score.
    """
    df = df.copy()
    df['cluster'] = clusters

    cluster_stats = df.groupby('cluster').agg(
        avg_lap_time=('avg_norm_lap_time', 'mean'),
        avg_top_speed=('top_speed', 'mean'),
        avg_consistency=('lap_consistency', 'mean'),
        avg_sector_1=('avg_norm_sector_1', 'mean'),
        avg_sector_2=('avg_norm_sector_2', 'mean'),
        avg_sector_3=('avg_norm_sector_3', 'mean'),
        best_sprint=('best_sprint_lap', 'mean'),
        driver_count=('avg_norm_lap_time', 'count'),
    ).reset_index()

    def normalise(series, invert=False):
        min_val = series.min()
        max_val = series.max()
        if max_val == min_val:
            return series * 0 + 0.5
        normalised = (series - min_val) / (max_val - min_val)
        return 1 - normalised if invert else normalised

    cluster_stats['score'] = (
        normalise(cluster_stats['avg_lap_time'], invert=True) * 0.30 +
        normalise(cluster_stats['avg_top_speed']) * 0.15 +
        normalise(cluster_stats['avg_consistency'], invert=True) * 0.15 +
        normalise(cluster_stats['avg_sector_1'], invert=True) * 0.08 +
        normalise(cluster_stats['avg_sector_2'], invert=True) * 0.08 +
        normalise(cluster_stats['avg_sector_3'], invert=True) * 0.09 +
        normalise(cluster_stats['best_sprint'], invert=True) * 0.15
    )

    cluster_stats = cluster_stats.sort_values(
        'score', ascending=False
    ).reset_index(drop=True)

    n_clusters = len(cluster_stats)
    # Use Outlier only for clusters with very few drivers (<=2)
    # Otherwise use performance-based labels
    label_map = {}
    performance_labels = ['Front Runner', 'Midfield', 'Struggling', 'Back Marker']
    perf_idx = 0

    for _, row in cluster_stats.iterrows():
        if row['driver_count'] <= 2:
            label_map[int(row['cluster'])] = 'Outlier'
        else:
            label_map[int(row['cluster'])] = performance_labels[
                min(perf_idx, len(performance_labels) - 1)
            ]
            perf_idx += 1

    return label_map


# ── Driver Similarity ──
def get_similar_drivers(df, X_scaled, driver_code, n=3):
    from sklearn.metrics.pairwise import euclidean_distances

    driver_idx = df[df['driver_code'] == driver_code].index[0]
    idx_in_array = df.index.get_loc(driver_idx)

    distances = euclidean_distances(
        X_scaled[idx_in_array].reshape(1, -1),
        X_scaled
    )[0]

    similar_indices = distances.argsort()[1:n+1]
    similar_drivers = df.iloc[similar_indices][['driver_code', 'team']].copy()
    similar_drivers['similarity_score'] = (
        1 / (1 + distances[similar_indices])
    ).round(3)

    return similar_drivers


# ── Radar Chart Data ──
def get_radar_data(df, driver_code):
    driver = df[df['driver_code'] == driver_code].iloc[0]

    def normalise(value, col, invert=False):
        min_val = df[col].min()
        max_val = df[col].max()
        if max_val == min_val:
            return 50
        normalised = (value - min_val) / (max_val - min_val) * 100
        return 100 - normalised if invert else normalised

    return {
        'Race Pace':        normalise(driver['avg_norm_lap_time'], 'avg_norm_lap_time', invert=True),
        'Consistency':      normalise(driver['lap_consistency'], 'lap_consistency', invert=True),
        'Top Speed':        normalise(driver['top_speed'], 'top_speed'),
        'Tyre Management':  normalise(driver['avg_lap_delta'], 'avg_lap_delta', invert=True),
        'Sprint Pace':      normalise(driver['best_sprint_lap'], 'best_sprint_lap', invert=True),
    }


# ── GMM Probability Summary ──
def get_probability_summary(df, probabilities, cluster_labels):
    """
    Returns a DataFrame with each driver's probability
    of belonging to each named cluster.
    """
    n_clusters = probabilities.shape[1]
    
    # Map cluster IDs to names
    prob_df = pd.DataFrame(
        probabilities,
        columns=[
            cluster_labels.get(i, f'Cluster {i}')
            for i in range(n_clusters)
        ]
    )
    
    prob_df['driver_code'] = df['driver_code'].values
    prob_df = prob_df.set_index('driver_code')
    
    # Round to percentages
    prob_df = (prob_df * 100).round(1)
    
    return prob_df


# ── Main (for testing) ──
if __name__ == "__main__":
    df = get_driver_features()
    print(df[['driver_code', 'team', 'avg_norm_lap_time', 'lap_consistency']].to_string())

    X_scaled, feature_cols, scaler = prepare_features(df)
    clusters, gmm, probabilities = run_clustering(X_scaled)
    df['cluster'] = clusters

    print("\nRaw cluster assignments:")
    print(df[['driver_code', 'cluster']].sort_values('cluster').to_string())
    
    print("\nDrivers per cluster:")
    print(df['cluster'].value_counts().sort_index())
    
    print("\nGMM Probabilities (first 5 rows):")
    print(probabilities[:5].round(3))

    X_pca, pca = run_pca(X_scaled)
    df['pca_x'] = X_pca[:, 0]
    df['pca_y'] = X_pca[:, 1]

    cluster_labels = assign_cluster_labels(df, clusters)
    df['cluster_name'] = df['cluster'].map(cluster_labels)

    print("\nDriver Clusters:")
    print(df[[
        'driver_code', 'team', 'avg_norm_lap_time',
        'best_sprint_lap', 'top_speed', 'cluster_name'
    ]].sort_values('cluster_name').to_string())

    print("\nGMM Probability Summary (%):")
    prob_summary = get_probability_summary(df, probabilities, cluster_labels)
    print(prob_summary.to_string())