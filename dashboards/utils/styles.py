import os
import streamlit as st

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
    """Return hex color for a given team name."""
    return TEAM_COLORS.get(team, '#FFFFFF')

def hex_to_rgba(hex_color, opacity=0.2):
    """Convert hex color to rgba string for Plotly."""
    r = int(hex_color[1:3], 16)
    g = int(hex_color[3:5], 16)
    b = int(hex_color[5:7], 16)
    return f'rgba({r},{g},{b},{opacity})'

def get_driver_photo(driver_code):
    path = f"C:/Users/Asus/Desktop/UTA/Projects/f1-analytics-platform/dashboards/assets/drivers/{driver_code}.jpg"
    if os.path.exists(path):
        return path
    return f"C:/Users/Asus/Desktop/UTA/Projects/f1-analytics-platform/dashboards/assets/drivers/default.jpg"

def set_background_theme(team_color):
    r = int(team_color[1:3], 16)
    g = int(team_color[3:5], 16)
    b = int(team_color[5:7], 16)
    st.markdown(f"""
        <style>
        .stApp {{
            background: linear-gradient(
                135deg,
                #0a0a0a 0%,
                rgba({r},{g},{b},0.15) 100%
            ) !important;
        }}
        </style>
    """, unsafe_allow_html=True)

def get_driver_line_style(driver_code, selected_drivers, drivers_df):
    """
    Returns line style for a driver.
    Same team = same color but different line style.
    First driver from team = solid, second = dash.
    """
    team = drivers_df[
        drivers_df['driver_code'] == driver_code
    ]['team'].values[0]

    # Count how many drivers from same team appear before this one
    team_driver_count = 0
    for d in selected_drivers:
        if d == driver_code:
            break
        d_team = drivers_df[
            drivers_df['driver_code'] == d
        ]['team'].values[0]
        if d_team == team:
            team_driver_count += 1

    line_styles = ['solid', 'dash', 'dot', 'dashdot']
    return line_styles[team_driver_count % len(line_styles)]

# ── Plotly Layout Defaults ──
DARK_LAYOUT = dict(
    paper_bgcolor='#1A1A2E',
    plot_bgcolor='#1A1A2E',
    font=dict(color='white'),
)
LEGEND_BOTTOM_RIGHT = dict(
    bgcolor='#16213E',
    bordercolor='#C8102E',
    borderwidth=1,
    yanchor="bottom",
    y=0.01,
    xanchor="right",
    x=0.99
)

LEGEND_TOP_RIGHT = dict(
    bgcolor='#16213E',
    bordercolor='#C8102E',
    borderwidth=1,
    yanchor="top",
    y=0.99,
    xanchor="right",
    x=0.99
)

def apply_sidebar_styles():
    st.markdown("""
        <style>
        /* Sidebar background */
        [data-testid="stSidebar"] {
            background-color: #0D0D0D !important;
            border-right: 1px solid #C8102E !important;
        }

        /* Sidebar content area */
        [data-testid="stSidebar"] > div:first-child {
            background-color: #0D0D0D !important;
        }

        /* Radio button container */
        [data-testid="stSidebar"] [data-testid="stRadio"] > div {
            gap: 0px !important;
            flex-direction: column !important;
        }

        /* Each radio label */
        [data-testid="stSidebar"] [data-testid="stRadio"] label {
            display: flex !important;
            align-items: center !important;
            padding: 12px 16px !important;
            margin: 2px 0 !important;
            border-radius: 6px !important;
            border-left: 3px solid transparent !important;
            color: #888888 !important;
            font-size: 14px !important;
            font-weight: 500 !important;
            cursor: pointer !important;
            width: 100% !important;
            transition: all 0.2s !important;
        }

        /* Hover */
        [data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
            background-color: #1A1A2E !important;
            color: white !important;
            border-left: 3px solid #C8102E !important;
        }

        /* Hide radio circle */
        [data-testid="stSidebar"] [data-testid="stRadio"] [data-testid="stMarkdownContainer"] p {
            color: inherit !important;
            font-size: 14px !important;
        }

        /* Hide the actual circle input */
        [data-testid="stSidebar"] [data-testid="stRadio"] input[type="radio"] {
            display: none !important;
        }

        /* Selected item */
        [data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
            background-color: #1A1A2E !important;
            color: white !important;
            border-left: 3px solid #C8102E !important;
        }

        /* Navigation header text */
        [data-testid="stSidebar"] [data-testid="stRadio"] > label:first-child {
            color: #444444 !important;
            font-size: 10px !important;
            text-transform: uppercase !important;
            letter-spacing: 1.5px !important;
            padding: 0 16px 8px 16px !important;
            border-left: none !important;
        }

        /* Sidebar caption */
        [data-testid="stSidebar"] [data-testid="stCaptionContainer"] {
            color: #444444 !important;
        }

        /* Sidebar headers */
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: white !important;
        }

        /* Divider */
        [data-testid="stSidebar"] hr {
            border-color: #1A1A2E !important;
            margin: 8px 0 !important;
        }
        </style>
    """, unsafe_allow_html=True)