import streamlit as st
import fastf1
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import sys
sys.path.append('ml')
from driver_fingerprints import (
    get_driver_features, prepare_features,
    run_clustering, run_pca,
    get_similar_drivers, get_radar_data
)

# ── Setup ──
load_dotenv()
fastf1.Cache.enable_cache('cache/')

# ── Session State ──
if 'animating' not in st.session_state:
    st.session_state.animating = False
if 'current_lap' not in st.session_state:
    st.session_state.current_lap = 1

# ── Page Config ──
st.set_page_config(
    page_title="Apex Analytics — F1 2026",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── BigQuery Client ──
@st.cache_resource
def get_bq_client():
    return bigquery.Client()

# ── Load available races from BigQuery ──
@st.cache_data
def get_races():
    client = get_bq_client()
    query = """
        SELECT DISTINCT 
            race_name, 
            round_number,
            year
        FROM `f1-analytics-491120.transformed.fact_laps`
        ORDER BY round_number
    """
    return client.query(query).to_dataframe()

# ── Load drivers for selected race from BigQuery ──
@st.cache_data
def get_drivers(race_name):
    client = get_bq_client()
    query = f"""
        SELECT DISTINCT driver_code, team
        FROM `f1-analytics-491120.transformed.fact_laps`
        WHERE race_name = '{race_name}'
        ORDER BY driver_code
    """
    return client.query(query).to_dataframe()

# ── Load telemetry from FastF1 ──
@st.cache_data
def get_telemetry(year, round_number, driver_code):
        try:
            session = fastf1.get_session(year, round_number, 'R')
            session.load(telemetry=True, laps=True)
            driver_laps = session.laps.pick_driver(driver_code)
        
            if driver_laps.empty:
                return None
            
            fastest_lap = driver_laps.pick_fastest()
        
            if fastest_lap is None:
                return None
            
            telemetry = fastest_lap.get_telemetry()
        
            # Check required columns exist
            if 'X' not in telemetry.columns or 'Y' not in telemetry.columns:
                return None
            
            return telemetry[['X', 'Y', 'Speed']]
        
        except Exception:
            return None

# ── Team Colors ──
TEAM_COLORS = {
    'Mercedes': '#00D2BE',
    'Ferrari': '#E8002D',
    'Red Bull Racing': '#3671C6',
    'McLaren': '#FF8000',
    'Aston Martin': '#358C75',
    'Alpine': '#FF87BC',
    'Williams': '#64C4FF',
    'Haas F1 Team': '#B6BABD',
    'Kick Sauber': '#52E252',
    'Audi': '#C9003C',
    'Cadillac': '#00CFFF',
}

def get_color(team):
    return TEAM_COLORS.get(team, '#FFFFFF')

def fingerprinting_page():
    st.title("🧬 Driver Style Fingerprinting")
    st.markdown("K-Means clustering of F1 drivers based on 2026 telemetry and lap data")
    st.markdown("---")

    # Load and process data
    with st.spinner("Loading driver data from BigQuery..."):
        df = get_driver_features()
        X_scaled, feature_cols, scaler = prepare_features(df)
        clusters, kmeans = run_clustering(X_scaled)
        df['cluster'] = clusters
        X_pca, pca = run_pca(X_scaled)
        df['pca_x'] = X_pca[:, 0]
        df['pca_y'] = X_pca[:, 1]

    # Cluster labels
    cluster_names = {
        0: "Struggling",
        1: "Midfield",
        2: "Front Runner",
        3: "Outlier"
    }
    cluster_colors = {
        0: "#FF6B6B",   # Red
        1: "#FFD93D",   # Yellow
        2: "#6BCB77",   # Green
        3: "#4D96FF",   # Blue
    }
    df['cluster_name'] = df['cluster'].map(cluster_names)

    # ── Driver Selector (must be before scatter plot) ──
    driver_list = df['driver_code'].tolist()
    selected_driver = st.selectbox("Select Driver", driver_list)

    # ── Row 1: Scatter Plot + Similarity Table ──
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🗺️ Driver Clusters")

        fig_scatter = go.Figure()

        for cluster_id, cluster_name in cluster_names.items():
            cluster_df = df[df['cluster'] == cluster_id]
            color = cluster_colors[cluster_id]

            for _, row in cluster_df.iterrows():
                is_selected = row['driver_code'] == selected_driver
                fig_scatter.add_trace(go.Scatter(
                    x=cluster_df['pca_x'],
                    y=cluster_df['pca_y'],
                    mode='markers+text',
                    name=row['driver_code'] if is_selected else cluster_name,
                    text=[row['driver_code']],
                    textposition='middle right',
                    textfont=dict(
                        color='white' if not is_selected else '#FFD700',
                        size=11 if not is_selected else 13,
                        family='Arial Bold' if is_selected else 'Arial'
                    ),
                    marker=dict(
                        color=color,
                        size=12 if is_selected else 9,
                        symbol='square' if is_selected else 'circle',
                        line=dict(
                            color='#FFD700' if is_selected else '#1A1A2E',
                            width=2.5 if is_selected else 1
                        )
                    ),
                    showlegend=is_selected,
                    hovertemplate=(
                        f"<b>{row['driver_code']}</b><br>" +
                        f"Cluster: {cluster_name}<br>" +
                        "<extra></extra>"
                    )
                ))

        fig_scatter.update_layout(
            paper_bgcolor='#1A1A2E',
            plot_bgcolor='#1A1A2E',
            font=dict(color='white'),
            xaxis=dict(
                title='PCA Component 1',
                showgrid=True,
                gridcolor='#2D2D2D',
                zeroline=False
            ),
            yaxis=dict(
                title='PCA Component 2',
                showgrid=True,
                gridcolor='#2D2D2D',
                zeroline=False
            ),
            legend=dict(
                bgcolor='#16213E',
                bordercolor='#C8102E',
                borderwidth=1
            ),
            height=500,
        )

        st.plotly_chart(fig_scatter, use_container_width=True)

    with col2:
        st.subheader("👥 Similar Drivers")

        similar = get_similar_drivers(df, X_scaled, selected_driver)

        for _, row in similar.iterrows():
            team = row['team']
            color = get_color(team)
            score_pct = int(row['similarity_score'] * 100)
            st.markdown(f"""
                <div style='
                    background-color: #16213E;
                    border-left: 4px solid {color};
                    padding: 10px;
                    margin-bottom: 8px;
                    border-radius: 4px;
                '>
                    <b style='color:{color}'>{row['driver_code']}</b>
                    <span style='color:#AAAAAA; font-size:12px'> — {team}</span><br>
                    <span style='color:white; font-size:12px'>Similarity: {score_pct}%</span>
                </div>
            """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Row 2: Radar Chart ──
    st.subheader(f"🎯 {selected_driver} — Driving Style Profile")

    radar_data = get_radar_data(df, selected_driver)
    categories = list(radar_data.keys())
    values = list(radar_data.values())
    values_closed = values + [values[0]]
    categories_closed = categories + [categories[0]]

    fig_radar = go.Figure()

    driver_team = df[df['driver_code'] == selected_driver]['team'].values[0]
    driver_color = get_color(driver_team)

    fig_radar.add_trace(go.Scatterpolar(
        r=values_closed,
        theta=categories_closed,
        fill='toself',
        fillcolor='rgba({},{},{},0.2)'.format(
            int(driver_color[1:3], 16),
        int(driver_color[3:5], 16),
            int(driver_color[5:7], 16)
        ),
        line=dict(color=driver_color, width=2),
        name=selected_driver
    ))

    fig_radar.update_layout(
        polar=dict(
            bgcolor='#16213E',
            radialaxis=dict(
                visible=True,
                range=[0, 100],
                showticklabels=False,
                gridcolor='#2D2D2D'
            ),
            angularaxis=dict(
                gridcolor='#2D2D2D',
                tickfont=dict(color='white', size=13)
            )
        ),
        paper_bgcolor='#1A1A2E',
        font=dict(color='white'),
        showlegend=False,
        height=400,
    )

    col_r1, col_r2, col_r3 = st.columns([1, 2, 1])
    with col_r2:
        st.plotly_chart(fig_radar, use_container_width=True)

    # ── Row 3: Driver Stats Table ──
    st.subheader("📊 All Driver Stats")
    display_df = df[[
        'driver_code', 'team', 'cluster_name',
        'avg_lap_time', 'lap_consistency',
        'top_speed', 'soft_preference'
    ]].copy()
    display_df.columns = [
        'Driver', 'Team', 'Cluster',
        'Avg Lap (s)', 'Consistency (σ)',
        'Top Speed (km/h)', 'Soft %'
    ]
    display_df['Soft %'] = (display_df['Soft %'] * 100).round(1)
    display_df['Avg Lap (s)'] = display_df['Avg Lap (s)'].round(3)
    display_df['Consistency (σ)'] = display_df['Consistency (σ)'].round(3)
    display_df['Top Speed (km/h)'] = display_df['Top Speed (km/h)'].round(1)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

# ── Main App ──
def main():
    # Navigation
    st.sidebar.image(
        "https://upload.wikimedia.org/wikipedia/commons/3/33/F1.svg",
        width=100
    )
    page = st.sidebar.radio(
        "Navigation",
        ["🏎️ Circuit Replay", "🧬 Driver Fingerprinting"],
    )

    if page == "🏎️ Circuit Replay":
        st.title("🏎️ Apex Analytics — F1 2026 Circuit Replay")
        st.markdown("---")

        # Sidebar
        with st.sidebar:
            st.header("🎛️ Controls")

            # Race selector
            races_df = get_races()
            race_options = races_df['race_name'].tolist()
            selected_race = st.selectbox("Select Race", race_options)

            # Get race details
            race_row = races_df[races_df['race_name'] == selected_race].iloc[0]
            year = int(race_row['year'])
            round_number = int(race_row['round_number'])

            # Driver selector
            drivers_df = get_drivers(selected_race)
            driver_options = drivers_df['driver_code'].tolist()
            selected_drivers = st.multiselect(
                "Select Drivers (min 1, max 5)",
                driver_options,
                default=[driver_options[0]] if driver_options else []
            )

            # Validation
            if len(selected_drivers) == 0:
                st.error("Please select at least 1 driver")
                st.stop()
            if len(selected_drivers) > 5:
                st.error("Maximum 5 drivers allowed")
                st.stop()

            st.markdown("---")
            st.caption(f"Round {round_number} — {year}")

        # Main content
        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader(f"🗺️ {selected_race} — Track Map")

            # Build track map
            fig = go.Figure()

            for driver in selected_drivers:
                team = drivers_df[drivers_df['driver_code'] == driver]['team'].values[0]
                color = get_color(team)

                telemetry = get_telemetry(year, round_number, driver)

                if telemetry is None:
                    st.warning(f"⚠️ Telemetry unavailable for {driver} at this circuit")
                    continue

                # Draw racing line
                fig.add_trace(go.Scatter(
                    x=telemetry['X'],
                    y=telemetry['Y'],
                    mode='lines',
                    line=dict(color=color, width=3),
                    name=driver,
                    hovertemplate=f"<b>{driver}</b><br>Speed: %{{customdata}} km/h",
                    customdata=telemetry['Speed']
                ))

                # Draw driver dot (starting position)
                fig.add_trace(go.Scatter(
                    x=[telemetry['X'].iloc[0]],
                    y=[telemetry['Y'].iloc[0]],
                    mode='markers+text',
                    marker=dict(color=color, size=14, symbol='circle'),
                    text=[driver],
                    textposition='top center',
                    textfont=dict(color=color, size=11),
                    showlegend=False,
                    hovertemplate=f"<b>{driver}</b>"
                ))

            fig.update_layout(
                paper_bgcolor='#1A1A2E',
                plot_bgcolor='#1A1A2E',
                font=dict(color='white'),
                showlegend=True,
                legend=dict(
                    bgcolor='#16213E',
                    bordercolor='#C8102E',
                    borderwidth=1
                ),
                xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
                yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, scaleanchor='x'),
                margin=dict(l=0, r=0, t=0, b=0),
                height=500,
            )

            st.plotly_chart(fig, use_container_width=True)

            # Animation buttons
            btn_col1, btn_col2, btn_col3 = st.columns([1, 1, 4])
            with btn_col1:
                if st.button("▶ Start", type="primary"):
                    st.session_state.animating = True
                    st.session_state.current_lap = 1
            with btn_col2:
                if st.button("⏹ Stop"):
                    st.session_state.animating = False

            # Animation placeholder
            anim_placeholder = st.empty()
            stats_placeholder = col2.empty()

            if st.session_state.animating:
                # Load all telemetry for selected drivers
                all_telemetry = {}
                for driver in selected_drivers:
                    tel = get_telemetry(year, round_number, driver)
                    if tel is not None:
                        all_telemetry[driver] = tel

                # Get max laps from BigQuery
                client = get_bq_client()
                max_lap_query = f"""
                    SELECT MAX(lap_number) as max_lap
                    FROM `f1-analytics-491120.transformed.fact_laps`
                    WHERE race_name = '{selected_race}'
                """
                max_lap = int(
                    client.query(max_lap_query)
                    .to_dataframe()['max_lap']
                    .iloc[0]
                )

                # Animate lap by lap
                import time
                for lap in range(1, max_lap + 1):
                    if not st.session_state.animating:
                        break

                    st.session_state.current_lap = lap

                    # Build animated frame
                    anim_fig = go.Figure()

                    for driver in selected_drivers:
                        if driver not in all_telemetry:
                            continue

                        tel = all_telemetry[driver]
                        team = drivers_df[
                            drivers_df['driver_code'] == driver
                        ]['team'].values[0]
                        color = get_color(team)

                        # Draw full racing line (faded)
                        anim_fig.add_trace(go.Scatter(
                            x=tel['X'],
                            y=tel['Y'],
                            mode='lines',
                            line=dict(color=color, width=2, dash='dot'),
                            opacity=0.3,
                            name=f"{driver} line",
                            showlegend=False
                        ))

                        # Calculate position on track
                        total_points = len(tel)
                        position_index = (lap * 50) % total_points

                        # Draw driver dot at current position
                        anim_fig.add_trace(go.Scatter(
                            x=[tel['X'].iloc[position_index]],
                            y=[tel['Y'].iloc[position_index]],
                            mode='markers+text',
                            marker=dict(
                                color=color,
                                size=16,
                                symbol='circle',
                                line=dict(color='white', width=2)
                            ),
                            text=[driver],
                            textposition='top center',
                            textfont=dict(color=color, size=11),
                            name=driver,
                            hovertemplate=f"<b>{driver}</b><br>Lap: {lap}"
                        ))

                    anim_fig.update_layout(
                        paper_bgcolor='#1A1A2E',
                        plot_bgcolor='#1A1A2E',
                        font=dict(color='white'),
                        showlegend=True,
                        legend=dict(
                            bgcolor='#16213E',
                            bordercolor='#C8102E',
                            borderwidth=1
                        ),
                        xaxis=dict(
                            showgrid=False,
                            showticklabels=False,
                            zeroline=False
                        ),
                        yaxis=dict(
                            showgrid=False,
                            showticklabels=False,
                            zeroline=False,
                            scaleanchor='x'
                        ),
                        margin=dict(l=0, r=0, t=30, b=0),
                        height=500,
                        title=dict(
                            text=f"Lap {lap} / {max_lap}",
                            font=dict(color='white', size=16)
                        )
                    )

                    # Update chart
                    anim_placeholder.plotly_chart(
                        anim_fig,
                        use_container_width=True
                    )

                    # Update driver stats
                    with stats_placeholder.container():
                        for driver in selected_drivers:
                            team = drivers_df[
                                drivers_df['driver_code'] == driver
                            ]['team'].values[0]
                            color = get_color(team)
                            st.markdown(f"""
                                <div style='
                                    background-color: #16213E;
                                    border-left: 4px solid {color};
                                    padding: 12px;
                                    margin-bottom: 12px;
                                    border-radius: 4px;
                                '>
                                    <b style='color:{color}; font-size:18px'>{driver}</b><br>
                                    <span style='color:#AAAAAA; font-size:13px'>{team}</span><br>
                                    <span style='color:white; font-size:13px'>Lap: {lap}</span>
                                </div>
                            """, unsafe_allow_html=True)

                    time.sleep(0.5)

                st.session_state.animating = False

        with col2:
            st.subheader("📊 Driver Stats")
            for driver in selected_drivers:
                team = drivers_df[drivers_df['driver_code'] == driver]['team'].values[0]
                color = get_color(team)
                st.markdown(f"""
                    <div style='
                        background-color: #16213E;
                        border-left: 4px solid {color};
                        padding: 12px;
                        margin-bottom: 12px;
                        border-radius: 4px;
                    '>
                        <b style='color:{color}; font-size:18px'>{driver}</b><br>
                        <span style='color:#AAAAAA; font-size:13px'>{team}</span>
                    </div>
                """, unsafe_allow_html=True)

    elif page == "🧬 Driver Fingerprinting":
        fingerprinting_page()


if __name__ == "__main__":
    main()