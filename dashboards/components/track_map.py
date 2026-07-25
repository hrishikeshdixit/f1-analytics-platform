import numpy as np
import plotly.graph_objects as go
from dashboards.utils.styles import get_color, DARK_LAYOUT, LEGEND_BOTTOM_RIGHT

def build_track_map(selected_drivers, drivers_df, telemetry_dict):
    """
    Build the static track map figure showing racing lines
    and driver starting positions.
    """
    fig = go.Figure()

    for driver in selected_drivers:
        team = drivers_df[
            drivers_df['driver_code'] == driver
        ]['team'].values[0]
        color = get_color(team)
        tel = telemetry_dict.get(driver)

        if tel is None:
            continue

        '''
        # Racing line
        fig.add_trace(go.Scatter(
            x=tel['X'],
            y=tel['Y'],
            mode='lines',
            line=dict(color=color, width=3),
            name=driver,
            hovertemplate=f"<b>{driver}</b><br>Speed: %{{customdata}} km/h",
            customdata=tel['Speed']
        ))
        '''

        # Starting position dot
        fig.add_trace(go.Scatter(
            x=[tel['X'].iloc[0]],
            y=[tel['Y'].iloc[0]],
            mode='markers+text',
            marker=dict(color=color, size=14, symbol='circle'),
            text=[driver],
            textposition='top center',
            textfont=dict(color=color, size=11),
            showlegend=False,
            hovertemplate=f"<b>{driver}</b>"
        ))

    fig.update_layout(
        **DARK_LAYOUT,
        legend = LEGEND_BOTTOM_RIGHT,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, scaleanchor='x'),
        margin=dict(l=0, r=0, t=0, b=0),
        height=500,
    )

    return fig


def build_animation_frame(lap, selected_drivers, drivers_df, telemetry_dict, max_lap):
    """
    Build a single animation frame for a given lap number.
    """
    fig = go.Figure()

    for driver in selected_drivers:
        if driver not in telemetry_dict:
            continue

        tel = telemetry_dict[driver]
        team = drivers_df[
            drivers_df['driver_code'] == driver
        ]['team'].values[0]
        color = get_color(team)

        # Faded racing line
        fig.add_trace(go.Scatter(
            x=tel['X'],
            y=tel['Y'],
            mode='lines',
            line=dict(color=color, width=2, dash='dot'),
            opacity=0.3,
            showlegend=False
        ))

        # Driver dot at current position
        total_points = len(tel)
        position_index = (lap * 50) % total_points

        fig.add_trace(go.Scatter(
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

    fig.update_layout(
        **DARK_LAYOUT,
        legend = LEGEND_BOTTOM_RIGHT,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, scaleanchor='x'),
        margin=dict(l=0, r=0, t=30, b=0),
        height=500,
        title=dict(
            text=f"Lap {lap} / {max_lap}",
            font=dict(color='white', size=16)
        )
    )

    return fig

def build_sector_track_map(telemetry_dict, pit_exit = None):
    """
    Draw track map colored by sector.
    S1 = Red, S2 = Yellow, S3 = Sapphire Blue.
    Uses first available driver's telemetry for track layout.
    """
    fig = go.Figure()

    if not telemetry_dict:
        return fig

    # Use first driver's telemetry for track shape
    first_driver = list(telemetry_dict.keys())[0]
    tel = telemetry_dict[first_driver]

    if tel is None or 'Distance' not in tel.columns:
        return fig

    total_distance = tel['Distance'].max()
    s1_end = total_distance * 0.33
    s2_end = total_distance * 0.66

    sector_config = [
        ('Sector 1', tel[tel['Distance'] <= s1_end], '#E8002D'),
        ('Sector 2', tel[(tel['Distance'] > s1_end) & (tel['Distance'] <= s2_end)], '#0F52BA'),
        ('Sector 3', tel[tel['Distance'] > s2_end], '#FFD700'),
    ]

    for name, sector_tel, color in sector_config:
        if sector_tel.empty:
            continue
        fig.add_trace(go.Scatter(
            x=sector_tel['X'],
            y=sector_tel['Y'],
            mode='lines',
            line=dict(color=color, width=4),
            name=name,
            hoverinfo='name'
        ))


    '''

    # ── Turn Numbers ──
    # Sample every ~200m along the track to find turn points
    # A turn is where direction changes significantly
    tel_full = list(telemetry_dict.values())[0]
    
    if tel_full is not None and 'Distance' in tel_full.columns:
        # Calculate heading change to detect corners
        tel_full = tel_full.copy()
        tel_full['dx'] = tel_full['X'].diff()
        tel_full['dy'] = tel_full['Y'].diff()
        tel_full['heading'] = np.arctan2(tel_full['dy'], tel_full['dx'])
        tel_full['heading_change'] = tel_full['heading'].diff().abs()

        # Smooth out noise
        tel_full['heading_change'] = tel_full['heading_change'].rolling(
            window=10, center=True
        ).mean().fillna(0)

        # Find corners — significant heading changes
        threshold = 0.05
        min_distance_between = 120  # minimum meters between turns

        corners = []
        last_distance = -min_distance_between

        for _, row in tel_full.iterrows():
            if (row['heading_change'] > threshold and
                    row['Distance'] - last_distance > min_distance_between):
                corners.append(row)
                last_distance = row['Distance']

        # Add turn number annotations
        for i, corner in enumerate(corners):
            fig.add_trace(go.Scatter(
                x=[corner['X']],
                y=[corner['Y']],
                mode='text',
                text=[f"T{i+1}"],
                textfont=dict(
                    color='white',
                    size=9,
                    family='Arial'
                ),
                showlegend=False,
                hoverinfo='skip'
            ))
        '''

    # ── Start/Finish Flag + Direction Arrow ──
    if tel is not None and not tel.empty:
        start_x = float(tel['X'].iloc[0])
        start_y = float(tel['Y'].iloc[0])
        arrow_x = float(tel['X'].iloc[20])
        arrow_y = float(tel['Y'].iloc[20])

        # Checkered flag marker at start/finish
        fig.add_trace(go.Scatter(
            x=[start_x],
            y=[start_y],
            mode='markers+text',
            marker=dict(
                symbol='square',
                size=14,
                color='white',
                line=dict(color='black', width=2)
            ),
            text=['🏁'],
            textposition='top center',
            textfont=dict(size=16),
            name='Start/Finish',
            hovertemplate="<b>Start / Finish Line</b><extra></extra>"
        ))

        # Direction arrow
        fig.add_annotation(
            x=arrow_x,
            y=arrow_y,
            ax=start_x,
            ay=start_y,
            xref='x', yref='y',
            axref='x', ayref='y',
            showarrow=True,
            arrowhead=2,
            arrowsize=1.0,
            arrowwidth=1.5,
            arrowcolor='white',
            opacity=0.8
        )

    fig.update_layout(
        **DARK_LAYOUT,
        legend=LEGEND_BOTTOM_RIGHT,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, scaleanchor='x'),
        margin=dict(l=0, r=0, t=0, b=0),
        height=400,
    )

    return fig

#3d TRACK MAP FUNCTION
def build_3d_sector_track_map(telemetry_3d):
    """
    Build a 3D track map colored by sector.
    S1=Red, S2=Yellow, S3=Purple.
    Z axis = elevation above sea level.
    """
    if telemetry_3d is None or telemetry_3d.empty:
        return go.Figure()

    total_distance = telemetry_3d['Distance'].max()
    s1_end = total_distance * 0.33
    s2_end = total_distance * 0.66

    sector_config = [
        ('Sector 1', telemetry_3d[telemetry_3d['Distance'] <= s1_end], '#E8002D'),
        ('Sector 2', telemetry_3d[(telemetry_3d['Distance'] > s1_end) & (telemetry_3d['Distance'] <= s2_end)], '#FFD700'),
        ('Sector 3', telemetry_3d[telemetry_3d['Distance'] > s2_end], '#9B59B6'),
    ]

    fig = go.Figure()

    for name, sector_tel, color in sector_config:
        if sector_tel.empty:
            continue

        fig.add_trace(go.Scatter3d(
            x=sector_tel['X'],
            y=sector_tel['Y'],
            z=sector_tel['Z'],
            mode='lines',
            line=dict(color=color, width=6),
            name=name,
            hovertemplate=(
                f"<b>{name}</b><br>"
                "Speed: %{customdata} km/h<br>"
                "Elevation: %{z:.0f}m<br>"
                "<extra></extra>"
            ),
            customdata=sector_tel['Speed']
        ))

    fig.update_layout(
        paper_bgcolor='#1A1A2E',
        font=dict(color='white'),
        legend=dict(
            bgcolor='#16213E',
            bordercolor='#C8102E',
            borderwidth=1,
            yanchor="bottom",
            y=0.01,
            xanchor="right",
            x=0.99
        ),
        scene=dict(
            bgcolor='#1A1A2E',
            xaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                title='',
                showbackground=False
            ),
            yaxis=dict(
                showgrid=False,
                showticklabels=False,
                zeroline=False,
                title='',
                showbackground=False
            ),
            zaxis=dict(
                showgrid=True,
                gridcolor='#2D2D2D',
                showticklabels=True,
                zeroline=False,
                title='Elevation (m)',
                color='white'
            ),
            camera=dict(
                eye=dict(x=1.5, y=1.5, z=0.8)
            )
        ),
        height=500,
        margin=dict(l=0, r=0, t=0, b=0)
    )

    return fig