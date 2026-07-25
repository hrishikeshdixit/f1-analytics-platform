import streamlit as st
import plotly.graph_objects as go
from dashboards.utils.bigquery import get_driver_features
from dashboards.components.driver_cards import similarity_card
from dashboards.utils.styles import (
    get_color, hex_to_rgba, 
    DARK_LAYOUT, LEGEND_TOP_RIGHT, get_driver_photo)
import ml.driver_fingerprints

'''
CLUSTER_NAMES = {
    0: "Outlier",
    1: "Midfield",
    2: "Struggling",
    3: "Front Runner"
}
'''

CLUSTER_COLORS = {
    'Front Runner': "#9B59B6",   # Purple
    'Midfield': "#6BCB77",       # Green
    'Struggling': "#FFD93D",     # Yellow
    'Back Marker': "#FF6B6B",    # Red
    'Outlier': "#000000",        # Purple
}

def show():
    st.title("🧬 Driver Style Fingerprinting")
    st.markdown("Gaussian Mixture Model clustering of F1 drivers based on 2026 race and sprint data")
    st.markdown("---")

    # ── Load and Process Data ──
    with st.spinner("Loading driver data from BigQuery..."):
        df = get_driver_features()
        #st.write("Columns in df:", df.columns.tolist())
        X_scaled, feature_cols, scaler = ml.driver_fingerprints.prepare_features(df)
        clusters, gmm, probabilities = ml.driver_fingerprints.run_clustering(X_scaled)
        df['cluster'] = clusters
        X_pca, pca = ml.driver_fingerprints.run_pca(X_scaled)
        df['pca_x'] = X_pca[:, 0]
        df['pca_y'] = X_pca[:, 1]
        CLUSTER_NAMES = ml.driver_fingerprints.assign_cluster_labels(df, clusters)
        df['cluster_name'] = df['cluster'].map(CLUSTER_NAMES)

    # ── Driver Selector ──
    driver_display = dict(zip(df['full_name'], df['driver_code']))
    selected_full_name = st.selectbox(
        "Select Driver",
        list(driver_display.keys()),
        key="fingerprint_driver_select"
    )
    selected_driver = driver_display[selected_full_name]

    # ── Row 1: Scatter + Similar Drivers ──
    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("🗺️ Driver Clusters")
        fig_scatter = _build_scatter(df, selected_driver)
        st.plotly_chart(fig_scatter, width='stretch')

    with col2:
        # ── Driver Photo ──
        photo = get_driver_photo(selected_driver)
        st.image(photo, width=400)
        st.markdown("---")

        st.subheader("👥 Similar Drivers")
        similar = ml.driver_fingerprints.get_similar_drivers(df, X_scaled, selected_driver)
        for _, row in similar.iterrows():
            st.markdown(
                similarity_card(row['driver_code'], row['team'], row['similarity_score']),
                unsafe_allow_html=True
            )

    st.markdown("---")

    # ── Row 2: Radar Chart ──
    st.subheader(f"🎯 {selected_driver} — Driving Style Profile")
    fig_radar = _build_radar(df, selected_driver)
    col_r1, col_r2, col_r3 = st.columns([1, 2, 1])
    with col_r2:
        st.plotly_chart(fig_radar, width='stretch')
    st.markdown("---")

    # ── Row 3: Stats Table ──
    st.subheader("📊 Driver Stats")
        
    display_df = df[df['driver_code'] == selected_driver][[
        'full_name', 'team', 'cluster_name',
        'avg_norm_lap_time', 'lap_consistency',
        'top_speed', 'soft_preference'
    ]].copy()

    display_df.columns = [
        'Driver', 'Team', 'Cluster',
        'Avg Gap to Fastest (%)', 'Consistency (σ)',
        'Top Speed (km/h)', 'Soft %'
    ]

    display_df['Soft %'] = (display_df['Soft %'] * 100).round(1)
    display_df['Avg Gap to Fastest (%)'] = display_df['Avg Gap to Fastest (%)'].round(3)
    display_df['Consistency (σ)'] = display_df['Consistency (σ)'].round(3)
    display_df['Top Speed (km/h)'] = display_df['Top Speed (km/h)'].round(1)

    st.dataframe(display_df, use_container_width=True, hide_index=True)

'''    # ── GMM Probability Table ──
    st.markdown("---")
    st.subheader("🎲 Cluster Membership Probabilities (%)")
    st.caption("Each driver's probability of belonging to each cluster — the key advantage of GMM over K-Means")
    
    from ml.driver_fingerprints import get_probability_summary
    prob_df = get_probability_summary(df, probabilities, CLUSTER_NAMES)
    st.dataframe(prob_df, use_container_width=True)
'''



def _build_scatter(df, selected_driver):
    """Build the PCA scatter plot with highlighted selected driver."""
    fig = go.Figure()
    
    # Track which cluster names already have a legend entry
    legend_added = set()

    for cluster_name in df['cluster_name'].unique():
        cluster_df = df[df['cluster_name'] == cluster_name]
        color = CLUSTER_COLORS.get(cluster_name, "#FFFFFF")

        # Plot all non-selected cluster members as one cluster trace
        non_selected = cluster_df[cluster_df['driver_code'] != selected_driver]
        if not non_selected.empty:
            fig.add_trace(go.Scatter(
                x=non_selected['pca_x'],
                y=non_selected['pca_y'],
                mode='markers+text',
                name=cluster_name,
                text=non_selected['full_name'],
                textposition='middle right',
                textfont=dict(color='white', size=10, family='Arial'),
                marker=dict(
                    color=color,
                    size=9,
                    symbol='circle',
                    line=dict(color='#1A1A2E', width=1)
                ),
                legendgroup=cluster_name,
                showlegend=True,
                hovertemplate=(
                    '<b>%{text}</b><br>'
                    'Driver: %{customdata[0]}<br>'
                    'Team: %{customdata[1]}<br>'
                    f'Cluster: {cluster_name}<br>'
                    '<extra></extra>'
                ),
                customdata=non_selected[['driver_code', 'team']].values
            ))
        else:
            # Ensure cluster appears in the legend even if selected driver is the only member
            fig.add_trace(go.Scatter(
                x=[None],
                y=[None],
                mode='markers',
                name=cluster_name,
                marker=dict(color=color, size=9, symbol='circle'),
                legendgroup=cluster_name,
                showlegend=True,
                hoverinfo='skip'
            ))

    selected = df[df['driver_code'] == selected_driver].iloc[0]
    fig.add_trace(go.Scatter(
        x=[selected['pca_x']],
        y=[selected['pca_y']],
        mode='markers+text',
        name='Selected Driver',
        text=[selected['full_name']],
        textposition='middle right',
        textfont=dict(color='#FFD700', size=13, family='Arial Black'),
        marker=dict(
            color='#FFD700',
            size=14,
            symbol='square',
            line=dict(color='white', width=2.5)
        ),
        legendgroup='selected',
        showlegend=True,
        hovertemplate=(
            f"<b>{selected['full_name']}</b><br>"
            f"Driver: {selected['driver_code']}<br>"
            f"Team: {selected['team']}<br>"
            f"Cluster: {selected['cluster_name']}<br>"
            "<extra></extra>"
        )
    ))

    fig.update_layout(
        **DARK_LAYOUT,
        legend=dict(
            bgcolor='#16213E',
            bordercolor='#C8102E',
            borderwidth=1,
            title=dict(text='Cluster', font=dict(color='white', size=12)),
            yanchor="top",
            y=1.25,
            xanchor="right",
            x=0.99,
            itemclick=False,
            itemdoubleclick=False,
        ),
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
        height=500,
    )
    return fig


def _build_radar(df, selected_driver):
    """Build the radar chart for a selected driver."""
    radar_data = ml.driver_fingerprints.get_radar_data(df, selected_driver)
    categories = list(radar_data.keys())
    values = list(radar_data.values())
    values_closed = values + [values[0]]
    categories_closed = categories + [categories[0]]

    driver_team = df[df['driver_code'] == selected_driver]['team'].values[0]
    driver_color = get_color(driver_team)

    fig = go.Figure()
    fig.add_trace(go.Scatterpolar(
        r=values_closed,
        theta=categories_closed,
        fill='toself',
        fillcolor=hex_to_rgba(driver_color, 0.2),
        line=dict(color=driver_color, width=2),
        name=selected_driver
    ))

    fig.update_layout(
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
    return fig