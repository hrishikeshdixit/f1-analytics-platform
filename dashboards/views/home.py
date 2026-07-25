import streamlit as st
#import time
from dashboards.utils.bigquery import get_races, get_drivers, get_max_laps
from dashboards.utils.telemetry import get_telemetry, get_lap_telemetry, get_pit_exit
from dashboards.utils.styles import get_color, set_background_theme, get_driver_photo, get_driver_line_style
from dashboards.components.driver_cards import driver_card
from dashboards.components.track_map import build_track_map, build_animation_frame, build_sector_track_map, build_3d_sector_track_map


def show():
    st.markdown("--- Yet to build this page. Please Stay Tuned! ---")