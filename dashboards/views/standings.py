import pandas as pd
import streamlit as st
import plotly.graph_objects as go
from dashboards.utils.telemetry import get_championship_standings, get_driver_race_results
from dashboards.utils.styles import get_color, get_driver_photo, DARK_LAYOUT, LEGEND_TOP_RIGHT

def show_driver_drilldown(driver_code, drivers_df, year):
    """Show detailed driver championship stats."""
    
    driver_row = drivers_df[drivers_df['driver_code'] == driver_code].iloc[0]
    team = driver_row['team']
    color = get_color(team)
    points = int(driver_row['points'])
    position = int(driver_row['position'])
    full_name = driver_row['full_name']

    # Back button
    if st.button("← Back to Standings"):
        st.session_state.selected_championship_driver = None
        st.rerun()

    st.markdown("---")

    # ── Hero Card ──
    col1, col2 = st.columns([1, 3])

    with col1:
        photo = get_driver_photo(driver_code)
        st.image(photo, width='stretch')

    with col2:
        st.markdown(f"""
            <div style='
                background: linear-gradient(135deg, #1A1A2E, {color}33);
                border: 2px solid {color};
                border-radius: 12px;
                padding: 24px;
            '>
                <p style='color:#AAAAAA; font-size:13px; margin:0'>
                    P{position} — {team}
                </p>
                <p style='color:{color}; font-size:36px; 
                    font-weight:bold; margin:8px 0'>
                    {full_name}
                </p>
                <p style='color:white; font-size:28px; 
                    font-weight:bold; margin:0'>
                    {points} PTS
                </p>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # ── Race by Race Results ──
    st.subheader("📋 Race by Race Results")

    with st.spinner("Loading race results..."):
        race_results = get_driver_race_results(driver_code, year)

    if race_results is not None and not race_results.empty:
        # Points progression chart
        race_results['cumulative_points'] = race_results['total_points'].cumsum()

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=race_results['race_name'],
            y=race_results['cumulative_points'],
            mode='lines+markers',
            line=dict(color=color, width=3),
            marker=dict(color=color, size=8),
            name='Cumulative Points',
            hovertemplate="<b>%{x}</b><br>Points: %{y}<extra></extra>"
        ))
        fig.update_layout(
            paper_bgcolor='#1A1A2E',
            plot_bgcolor='#1A1A2E',
            font=dict(color='white'),
            xaxis=dict(
                showgrid=False,
                tickangle=-45,
                color='white'
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='#2D2D2D',
                color='white',
                title='Cumulative Points'
            ),
            height=350,
            margin=dict(l=0, r=0, t=20, b=100)
        )
        st.plotly_chart(fig, use_container_width=True)

        # Season summary stats
        wins = len(race_results[race_results['position'] == 1])
        podiums = len(race_results[race_results['position'] <= 3])
        dnfs = len(race_results[race_results['position'].isna()])
        best_finish = int(race_results['position'].min()) \
            if not race_results['position'].isna().all() else 'N/A'

        s1, s2, s3, s4 = st.columns(4)
        with s1:
            st.metric("🏆 Wins", wins)
        with s2:
            st.metric("🥇 Podiums", podiums)
        with s3:
            st.metric("Best Finish", f"P{best_finish}")
        with s4:
            st.metric("DNFs", dnfs)

        st.markdown("---")

        # Race by race table
        display_results = race_results[[
            'round_number', 'race_name', 'position', 
            'points', 'sprint_points', 'total_points'
        ]].copy()
        display_results.columns = [
            'Round', 'Race', 'Position',
            'Race Pts', 'Sprint Pts', 'Total Pts'
        ]
        display_results['Position'] = display_results['Position'].apply(
            lambda x: f"P{int(x)}" if pd.notna(x) else "DNF"
        )
        display_results['Total Pts'] = display_results['Total Pts'].astype(int)
        st.dataframe(display_results, use_container_width=True, hide_index=True)
    else:
        st.warning("No race results available for this driver.")

def show():

    # ── Check if drill down is active ──
    if st.session_state.get('selected_championship_driver'):
        with st.spinner("Loading driver details..."):
            drivers_df, constructors_df = get_championship_standings(2026)
        show_driver_drilldown(
            st.session_state.selected_championship_driver,
            drivers_df,
            2026
        )
        return

    st.title("🏆 2026 Championship Standings")
    st.markdown("Live WDC and WCC standings")
    st.markdown("---")

    with st.spinner("Loading championship data..."):
        drivers_df, constructors_df = get_championship_standings(2026)

    if drivers_df is None or constructors_df is None:
        st.error("Could not load championship data. Please try again.")
        return

    # ── Top Cards ──
    col1, col2 = st.columns(2)

    with col1:
        leader = drivers_df.iloc[0]
        leader_color = get_color(leader['team'])
        st.markdown(f"""
            <div style='
                background: linear-gradient(135deg, #1A1A2E, {leader_color}33);
                border: 2px solid {leader_color};
                border-radius: 12px;
                padding: 20px;
                text-align: center;
            '>
                <p style='color:#AAAAAA; font-size:18px; margin:0'>
                    WDC LEADER
                </p>
                <p style='color:{leader_color}; font-size:32px; 
                    font-weight:bold; margin:8px 0'>
                    {leader['full_name']}
                </p>
                <p style='color:{leader_color}; font-size:28px; 
                    font-weight:bold; margin:8px 0'>
                    {int(leader['points'])} PTS
                </p>
            </div>
        """, unsafe_allow_html=True)

        # Driver photo
        #photo = get_driver_photo(leader['driver_code'])
        #st.image(photo, width=500)

    with col2:
        leader_team = constructors_df.iloc[0]
        team_color = get_color(leader_team['team'])
        st.markdown(f"""
            <div style='
                background: linear-gradient(135deg, #1A1A2E, {team_color}33);
                border: 2px solid {team_color};
                border-radius: 12px;
                padding: 20px;
                text-align: center;
            '>
                <p style='color:#AAAAAA; font-size:18px; margin:0'>
                    WCC LEADER
                </p>
                <p style='color:{team_color}; font-size:32px; 
                    font-weight:bold; margin:8px 0'>
                    {leader_team['team'].upper()}
                </p>
                <p style='color:{team_color}; font-size:28px; 
                    font-weight:bold; margin:8px 0'>
                    {int(leader_team['points'])} PTS
                </p>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # ── WDC Chart + Table ──
    st.subheader("🏎️ Drivers Championship")
    col3, col4 = st.columns([2, 1])

    with col3:
        fig_wdc = go.Figure()

        fig_wdc.add_trace(go.Bar(
            x=drivers_df['driver_code'],
            y=drivers_df['points'],
            marker_color=[
                get_color(team) for team in drivers_df['team']
            ],
            text=drivers_df['points'].astype(int),
            textposition='outside',
            textfont=dict(color='white', size=11),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Points: %{y}<br>"
                "<extra></extra>"
            )
        ))

        fig_wdc.update_layout(
            **DARK_LAYOUT,
            legend=LEGEND_TOP_RIGHT,
            xaxis=dict(
                showgrid=False,
                color='white'
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='#2D2D2D',
                color='white',
                title='Points'
            ),
            height=400,
            margin=dict(l=0, r=0, t=20, b=0)
        )

        st.plotly_chart(fig_wdc, width='stretch')

    with col4:
        # WDC Table
        display_wdc = drivers_df[[
            'position', 'driver_code', 'team', 'points'
        ]].copy()
        display_wdc.columns = ['Pos', 'Driver', 'Team', 'Pts']
        display_wdc['Pts'] = display_wdc['Pts'].astype(int)
        st.caption("Double click the checkbox to see driver details")
        selected_rows = st.dataframe(
            display_wdc,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="single-row"
        )

        # Check if a row was selected
        if selected_rows.selection.rows:
            selected_idx = selected_rows.selection.rows[0]
            selected_driver_code = drivers_df.iloc[selected_idx]['driver_code']
            st.session_state.selected_championship_driver = selected_driver_code

    st.markdown("---")

    # ── WCC Chart + Table ──
    st.subheader("🏗️ Constructors Championship")
    col5, col6 = st.columns([2, 1])

    with col5:
        fig_wcc = go.Figure()

        fig_wcc.add_trace(go.Bar(
            x=constructors_df['team'],
            y=constructors_df['points'],
            marker_color=[
                get_color(team) for team in constructors_df['team']
            ],
            text=constructors_df['points'].astype(int),
            textposition='outside',
            textfont=dict(color='white', size=11),
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Points: %{y}<br>"
                "<extra></extra>"
            )
        ))

        fig_wcc.update_layout(
            **DARK_LAYOUT,
            legend=LEGEND_TOP_RIGHT,
            xaxis=dict(
                showgrid=False,
                color='white',
                tickangle=-20
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='#2D2D2D',
                color='white',
                title='Points'
            ),
            height=400,
            margin=dict(l=0, r=0, t=20, b=0)
        )

        st.plotly_chart(fig_wcc, width='stretch')

    with col6:
        display_wcc = constructors_df[[
            'position', 'team', 'points'
        ]].copy()
        display_wcc.columns = ['Pos', 'Team', 'Pts']
        display_wcc['Pts'] = display_wcc['Pts'].astype(int)
        st.dataframe(
            display_wcc,
            width='stretch',
            hide_index=True
        )