import plotly.graph_objects as go
import numpy as np
import pandas as pd
from dashboards.utils.styles import get_color, DARK_LAYOUT, LEGEND_BOTTOM_RIGHT


def build_sector_track_map(telemetry_dict, pit_exit=None, circuit_info=None):
    """
    Build track map using official FastF1 circuit geometry if available,
    falling back to telemetry GPS.
    """
    fig = go.Figure()

    if circuit_info is not None and telemetry_dict:
        try:
            tel = list(telemetry_dict.values())[0]

            if tel is not None and 'Distance' in tel.columns:
                angle = circuit_info.rotation / 180 * np.pi

                x = tel['X'] * np.cos(angle) - tel['Y'] * np.sin(angle)
                y = tel['X'] * np.sin(angle) + tel['Y'] * np.cos(angle)

                total_distance = tel['Distance'].max()
                s1_end = total_distance * 0.33
                s2_end = total_distance * 0.66

                sector_config = [
                    ('Sector 1', tel[tel['Distance'] <= s1_end], '#E8002D'),
                    ('Sector 2', tel[(tel['Distance'] > s1_end) & (tel['Distance'] <= s2_end)], '#FFD700'),
                    ('Sector 3', tel[tel['Distance'] > s2_end], '#9B59B6'),
                ]

                for name, sector_tel, color in sector_config:
                    if sector_tel.empty:
                        continue
                    idx = sector_tel.index
                    fig.add_trace(go.Scatter(
                        x=x[idx],
                        y=y[idx],
                        mode='lines',
                        line=dict(color=color, width=4),
                        name=name,
                        hoverinfo='name'
                    ))

                # Start/finish flag
                start_x = float(x.iloc[0])
                start_y = float(y.iloc[0])
                arrow_x = float(x.iloc[20])
                arrow_y = float(y.iloc[20])

                fig.add_trace(go.Scatter(
                    x=[start_x], y=[start_y],
                    mode='markers+text',
                    marker=dict(symbol='square', size=14, color='white',
                                line=dict(color='black', width=2)),
                    text=['🏁'],
                    textposition='top center',
                    textfont=dict(size=16),
                    name='Start/Finish',
                    hovertemplate="<b>Start / Finish Line</b><extra></extra>"
                ))

                fig.add_annotation(
                    x=arrow_x, y=arrow_y,
                    ax=start_x, ay=start_y,
                    xref='x', yref='y',
                    axref='x', ayref='y',
                    showarrow=True,
                    arrowhead=3,
                    arrowsize=1.5,
                    arrowwidth=2,
                    arrowcolor='white',
                    opacity=0.8
                )

                # Pit exit
                if pit_exit is not None:
                    pit_x = pit_exit['x'] * np.cos(angle) - pit_exit['y'] * np.sin(angle)
                    pit_y = pit_exit['x'] * np.sin(angle) + pit_exit['y'] * np.cos(angle)
                    fig.add_trace(go.Scatter(
                        x=[pit_x], y=[pit_y],
                        mode='markers+text',
                        marker=dict(symbol='triangle-up', size=12,
                                    color='#00FF00',
                                    line=dict(color='white', width=1)),
                        text=['PIT'],
                        textposition='top center',
                        textfont=dict(color='#00FF00', size=9),
                        name='Pit Exit',
                        hovertemplate="<b>Pit Exit</b><extra></extra>"
                    ))

        except Exception as e:
            print(f"Circuit info rendering failed: {e}")
            return _build_standard_sector_map(fig, telemetry_dict, pit_exit)

    else:
        return _build_standard_sector_map(fig, telemetry_dict, pit_exit)

    fig.update_layout(
        **DARK_LAYOUT,
        legend=LEGEND_BOTTOM_RIGHT,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, scaleanchor='x'),
        margin=dict(l=0, r=0, t=0, b=0),
        height=500,
    )

    return fig


def _build_standard_sector_map(fig, telemetry_dict, pit_exit=None):
    """Standard telemetry-based sector map — fallback."""
    if not telemetry_dict:
        return fig

    tel = list(telemetry_dict.values())[0]

    if tel is None or 'Distance' not in tel.columns:
        return fig

    total_distance = tel['Distance'].max()
    s1_end = total_distance * 0.33
    s2_end = total_distance * 0.66

    sector_config = [
        ('Sector 1', tel[tel['Distance'] <= s1_end], '#E8002D'),
        ('Sector 2', tel[(tel['Distance'] > s1_end) & (tel['Distance'] <= s2_end)], '#FFD700'),
        ('Sector 3', tel[tel['Distance'] > s2_end], '#9B59B6'),
    ]

    for name, sector_tel, color in sector_config:
        if sector_tel.empty:
            continue

        has_throttle = 'Throttle' in sector_tel.columns
        has_gear = 'Gear' in sector_tel.columns
        has_brake = 'Brake' in sector_tel.columns

        customdata = pd.DataFrame({
            'Speed': sector_tel['Speed'].round(0),
            'Distance': sector_tel['Distance'].round(0),
            'Gear': sector_tel['Gear'] if has_gear else 'N/A',
            'Throttle': sector_tel['Throttle'].round(0) if has_throttle else 'N/A',
            'Brake': sector_tel['Brake'].astype(bool) if has_brake else 'N/A',
        })

        fig.add_trace(go.Scatter(
            x=sector_tel['X'],
            y=sector_tel['Y'],
            mode='lines',
            line=dict(color=color, width=4),
            name=name,
            customdata=customdata.values,
            hovertemplate=(
                f"<b>{name}</b><br>"
                "─────────────<br>"
                "🚀 Speed: %{customdata[0]:.0f} km/h<br>"
                "📏 Distance: %{customdata[1]:.0f} m<br>"
                "⚙️ Gear: %{customdata[2]}<br>"
                "🦶 Throttle: %{customdata[3]:.0f}%<br>"
                "🛑 Brake: %{customdata[4]}<br>"
                "<extra></extra>"
            )
        ))

    # Start/finish flag
    if not tel.empty:
        start_x = float(tel['X'].iloc[0])
        start_y = float(tel['Y'].iloc[0])
        arrow_x = float(tel['X'].iloc[20])
        arrow_y = float(tel['Y'].iloc[20])

        fig.add_trace(go.Scatter(
            x=[start_x], y=[start_y],
            mode='markers+text',
            marker=dict(symbol='square', size=14, color='white',
                        line=dict(color='black', width=2)),
            text=['🏁'],
            textposition='top center',
            textfont=dict(size=16),
            name='Start/Finish',
            hovertemplate="<b>Start / Finish Line</b><extra></extra>"
        ))

        fig.add_annotation(
            x=arrow_x, y=arrow_y,
            ax=start_x, ay=start_y,
            xref='x', yref='y',
            axref='x', ayref='y',
            showarrow=True,
            arrowhead=3,
            arrowsize=1.5,
            arrowwidth=2,
            arrowcolor='white',
            opacity=0.8
        )

    # Pit exit
    if pit_exit is not None:
        fig.add_trace(go.Scatter(
            x=[pit_exit['x']], y=[pit_exit['y']],
            mode='markers+text',
            marker=dict(symbol='triangle-up', size=12,
                        color='#00FF00',
                        line=dict(color='white', width=1)),
            text=['PIT'],
            textposition='top center',
            textfont=dict(color='#00FF00', size=9),
            name='Pit Exit',
            hovertemplate="<b>Pit Exit</b><extra></extra>"
        ))

    fig.update_layout(
        **DARK_LAYOUT,
        legend=LEGEND_BOTTOM_RIGHT,
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, scaleanchor='x'),
        margin=dict(l=0, r=0, t=0, b=0),
        height=500,
    )

    return fig


def build_animation_frame(lap, selected_drivers, drivers_df, telemetry_dict, max_lap):
    """Build a single animation frame for a given lap number."""
    fig = go.Figure()

    for driver in selected_drivers:
        if driver not in telemetry_dict:
            continue

        tel = telemetry_dict[driver]
        team = drivers_df[
            drivers_df['driver_code'] == driver
        ]['team'].values[0]
        color = get_color(team)

        fig.add_trace(go.Scatter(
            x=tel['X'],
            y=tel['Y'],
            mode='lines',
            line=dict(color=color, width=2, dash='dot'),
            opacity=0.3,
            showlegend=False
        ))

        total_points = len(tel)
        position_index = (lap * 50) % total_points

        fig.add_trace(go.Scatter(
            x=[tel['X'].iloc[position_index]],
            y=[tel['Y'].iloc[position_index]],
            mode='markers+text',
            marker=dict(color=color, size=16, symbol='circle',
                        line=dict(color='white', width=2)),
            text=[driver],
            textposition='top center',
            textfont=dict(color=color, size=11),
            name=driver,
            hovertemplate=f"<b>{driver}</b><br>Lap: {lap}"
        ))

    fig.update_layout(
        **DARK_LAYOUT,
        legend=LEGEND_BOTTOM_RIGHT,
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