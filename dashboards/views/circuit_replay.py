import streamlit as st
#import time
from dashboards.utils.bigquery import get_races, get_drivers, get_max_laps, get_available_sessions
from dashboards.utils.telemetry import get_telemetry, get_lap_telemetry
from dashboards.utils.styles import get_color, set_background_theme, get_driver_photo, get_driver_line_style
from dashboards.components.driver_cards import driver_card
from dashboards.components.track_map import build_track_map, build_animation_frame, build_sector_track_map, build_3d_sector_track_map


def show():
    st.title("🏎️ Apex Analytics — F1 2026 Circuit Replay")
    st.markdown("---")

    st.subheader("🎛️ Controls")
    col1, col2, col3 = st.columns([1, 1, 1])

    # ── Left Column — Race Selection ──

    with col1:
        st.subheader("Select Race")
        races_df = get_races()
        race_options = races_df['race_name'].tolist()
        selected_race = st.selectbox("Select Race", race_options)
        race_row = races_df[races_df['race_name'] == selected_race].iloc[0]
        year = int(race_row['year'])
        round_number = int(race_row['round_number'])

    # ── Middle Column — Session Selection ──
    with col2:
        st.subheader("Select Session")

        # Get only sessions available for this race weekend
        available_sessions = get_available_sessions(selected_race)

        # Define display order
        session_order = [
            'Practice 1', 'Practice 2', 'Practice 3',
            'Sprint Qualifying', 'Sprint',
            'Qualifying', 'Race'
        ]

        # Filter and sort by preferred order
        session_options = [
            s for s in session_order
            if s in available_sessions
        ]

        selected_session = st.selectbox(
            "Select Session",
            session_options,
            key="session_select"
        )
    
    SESSION_MAP = {
        'Race': 'R',
        'Qualifying': 'Q',
        'Sprint': 'Sprint',
        'Sprint Qualifying': 'Sprint Qualifying',
        'Practice 1': 'FP1',
        'Practice 2': 'FP2',
        'Practice 3': 'FP3'
    }
    session_identifier = SESSION_MAP[selected_session]
        
        
    # ── Right Column — Driver Selection ──
    with col3:
        st.subheader("Select Driver")
        drivers_df = get_drivers(selected_race, selected_session)
        driver_options = drivers_df['driver_code'].tolist()
        selected_drivers = st.multiselect(
            "Select Drivers (min 1, max 5)",
            driver_options,
            default=[driver_options[0]] if driver_options else []
        )

        if len(selected_drivers) == 0:
            st.error("Please select at least 1 driver")
            st.stop()
        if len(selected_drivers) > 5:
            st.error("Maximum 5 drivers allowed")
            st.stop()

    st.caption(f"Round {round_number} — {year}")

    st.markdown("---")
    
    # ── Background theming for single driver ──
    if len(selected_drivers) == 1:
        team = drivers_df[
            drivers_df['driver_code'] == selected_drivers[0]
        ]['team'].values[0]
        color = get_color(team)
        set_background_theme(color)

    # ── Load Telemetry ──
    telemetry_dict = {}
    for driver in selected_drivers:
        tel = get_telemetry(year, round_number, driver, session_identifier)
        if tel is not None:
            telemetry_dict[driver] = tel

    # ── Main Layout ──
    col4, col5 = st.columns([4, 3])

    # ── Right Column — Driver Photos and Cards ──

    with col5:
        st.subheader("👤 Driver Info")

        for driver in selected_drivers:
            team = drivers_df[
                drivers_df['driver_code'] == driver
            ]['team'].values[0]
            st.markdown(
                driver_card(driver, team),
                unsafe_allow_html=True
            )
            photo = get_driver_photo(driver)
            st.image(photo, width=450)
            #st.markdown("---")

    # ── Left Column — Track Map and Animation ──
    with col4:
        st.subheader(f"🗺️ {selected_race} — Track Map")

        # Warnings for missing telemetry
        for driver in selected_drivers:
            if driver not in telemetry_dict:
                st.warning(
                    f"⚠️ Telemetry unavailable for {driver} at this circuit"
                )

        # Static track map

        # 3D Elevation Track Map
        # Use first available driver's telemetry for track shape
        
        first_driver = selected_drivers[0]
        '''
        telemetry_3d = get_telemetry_3d(year, round_number, first_driver)
        
        
        if telemetry_3d is not None:
            fig_3d = build_3d_sector_track_map(telemetry_3d)
            st.plotly_chart(fig_3d, use_container_width=True)
            st.caption(
                "🖱️ Click and drag to rotate · Scroll to zoom · "
                "Double-click to reset view"
            )
        else:
            st.warning("⚠️ 3D elevation data unavailable for this circuit")
        '''
        
        sector_fig = build_sector_track_map(telemetry_dict)
        st.plotly_chart(sector_fig, width='stretch', config={
        'scrollZoom': True,
        'displayModeBar': True,
        'modeBarButtonsToRemove': ['toImage'],
        'displaylogo': False
    })
        
        st.caption(
                "Scroll to zoom · Click and drag to pan · Double-click to reset"
            )

    st.markdown("---")

    st.subheader("📡 Telemetry Analysis")

    # Lap selector
    max_lap_num = get_max_laps(selected_race, selected_session)

        # Session context label
    session_context = {
        'Race': f"Lap 1 to {max_lap_num} — Full Race Distance",
        'Qualifying': f"Lap 1 to {max_lap_num} — Q1/Q2/Q3 Flying Laps",
        'Sprint': f"Lap 1 to {max_lap_num} — Sprint Race",
        'Sprint Qualifying': f"Lap 1 to {max_lap_num} — SQ Flying Laps",
        'Practice 1': f"Lap 1 to {max_lap_num} — FP1 Laps",
        'Practice 2': f"Lap 1 to {max_lap_num} — FP2 Laps",
        'Practice 3': f"Lap 1 to {max_lap_num} — FP3 Laps",
    }
    #st.caption(session_context.get(selected_session, ''))

    selected_lap = st.slider(
        "Select Lap",
        min_value=1,
        max_value=int(max_lap_num),
        value=1,
        step=1
    )

    # Load telemetry for selected lap
    lap_telemetry = {}
    for driver in selected_drivers:
        tel = get_lap_telemetry(year, round_number, driver, selected_lap, session_identifier)
        if tel is not None:
            lap_telemetry[driver] = tel
        else:
            st.warning(f"⚠️ No telemetry for {driver} on lap {selected_lap}")

    if lap_telemetry:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        fig_tel = make_subplots(
            rows=4, cols=1,
            shared_xaxes=True,
            subplot_titles=('Speed (km/h)', 'Throttle (%)', 'Brake (%)', 'Gear'),
            vertical_spacing=0.08
        )

        for driver in selected_drivers:
            if driver not in lap_telemetry:
                continue

            tel = lap_telemetry[driver]
            team = drivers_df[
                drivers_df['driver_code'] == driver
            ]['team'].values[0]
            color = get_color(team)

            # Get lap type from telemetry
            lap_type_val = tel['lap_type'].iloc[0] \
                if 'lap_type' in tel.columns else 'flying'

            # Apply color logic for ALL sessions
            if lap_type_val == 'flying':
                trace_color = color
                trace_opacity = 1.0
                line_width = 3
                lap_label = "Flying Lap"
            elif lap_type_val == 'out_lap':
                trace_color = "#87CEEB"  # Sky blue
                trace_opacity = 0.7
                line_width = 1.5
                lap_label = "Out Lap"
            else:  # cool_down
                trace_color = '#FFFFFF'
                trace_opacity = 0.4
                line_width = 1.5
                lap_label = "Cool Down Lap"

            # Speed
            fig_tel.add_trace(go.Scatter(
                x=tel['Distance'],
                y=tel['Speed'],
                mode='lines',
                name=f"{driver} {lap_label}",
                line=dict(
                    color=trace_color,
                    width=line_width,
                    dash=get_driver_line_style(driver, selected_drivers, drivers_df)
                ),
                opacity=trace_opacity,
                legendgroup=driver,
                showlegend=True
            ), row=1, col=1)

            # Throttle
            fig_tel.add_trace(go.Scatter(
                x=tel['Distance'],
                y=tel['Throttle'],
                mode='lines',
                name=driver,
                line=dict(
                    color=trace_color,
                    width=line_width,
                    dash=get_driver_line_style(driver, selected_drivers, drivers_df)
                ),
                opacity=trace_opacity,
                legendgroup=driver,
                showlegend=False
            ), row=2, col=1)

            # Brake
            fig_tel.add_trace(go.Scatter(
                x=tel['Distance'],
                y=tel['Brake'],
                mode='lines',
                name=driver,
                line=dict(
                    color=trace_color,
                    width=line_width,
                    dash=get_driver_line_style(driver, selected_drivers, drivers_df)
                ),
                opacity=trace_opacity,
                legendgroup=driver,
                showlegend=False
            ), row=3, col=1)

            # Gear
            fig_tel.add_trace(go.Scatter(
                x=tel['Distance'],
                y=tel['Gear'],
                mode='lines',
                name=driver,
                line=dict(
                    color=trace_color,
                    width=line_width,
                    dash=get_driver_line_style(driver, selected_drivers, drivers_df)
                ),
                opacity=trace_opacity,
                legendgroup=driver,
                showlegend=False
            ), row=4, col=1)
        
        if selected_session in ['Practice 1', 'Practice 2', 'Practice 3', 
                            'Qualifying', 'Sprint Qualifying']:
        
            for driver, tel in lap_telemetry.items():
                is_flying = tel['is_flying'].iloc[0] if 'is_flying' in tel.columns else 'N/A'
                st.write(f"{driver} — Lap {selected_lap} — Flying: {is_flying}")

        st.caption(session_context.get(selected_session, ''))
        st.markdown("""
        <small style='color:#888888'>
        Flying lap — Team Colour<br>
        Cool down lap — White<br>
        Out lap — Sky Blue
        </small>
    """, unsafe_allow_html=True)

        fig_tel.update_layout(
            paper_bgcolor='#1A1A2E',
            plot_bgcolor='#1A1A2E',
            font=dict(color='white'),
            height=700,
            legend=dict(
                bgcolor='#16213E',
                bordercolor='#C8102E',
                borderwidth=1
            ),
            xaxis4=dict(
                title='Distance (m)',
                color='white',
                gridcolor='#2D2D2D'
            ),
            margin=dict(l=0, r=0, t=40, b=0)
        )

        # Style all subplot axes
        for i in range(1, 5):
            fig_tel.update_xaxes(
                gridcolor='#2D2D2D',
                zeroline=False,
                row=i, col=1
            )
            fig_tel.update_yaxes(
                gridcolor='#2D2D2D',
                zeroline=False,
                color='white',
                row=i, col=1
            )

        st.plotly_chart(fig_tel, width='stretch')