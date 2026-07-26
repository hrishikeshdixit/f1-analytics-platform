import streamlit as st
import fastf1
from dotenv import load_dotenv
import sys
import os

# Add root project folder to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── Setup ──
load_dotenv()
cache_dir = '/tmp/fastf1_cache/' if os.environ.get('STREAMLIT_SHARING_MODE') else 'cache/'
fastf1.Cache.enable_cache(cache_dir)

# ── Page Config ──
st.set_page_config(
    page_title="Apex Analytics — F1 2026",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Session State for Navigation ──

if 'active_page' not in st.session_state:
    st.session_state.active_page = 'Home'

if 'animating' not in st.session_state:
    st.session_state.animating = False
if 'current_lap' not in st.session_state:
    st.session_state.current_lap = 1
if 'selected_championship_driver' not in st.session_state:
    st.session_state.selected_championship_driver = None

# ── Navigation ──
with st.sidebar:
    # F1 Logo
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/3/33/F1.svg",
        width=80
    )
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # Navigation items
    nav_items = {
        #"Home": "Home",
        "Circuit Replay": "Circuit Replay",
        "Driver Fingerprinting": "Driver Fingerprinting",
        "Championship": "Championship",
    }

    st.markdown("""
        <style>
        [data-testid="stSidebar"] {
            background-color: #0D0D0D !important;
            border-right: 1px solid #1A1A2E !important;
        }
        [data-testid="stSidebar"] > div:first-child {
            background-color: #0D0D0D !important;
        }
        .nav-section-header {
            color: #444444;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1.5px;
            padding: 16px 8px 6px 8px;
            font-family: Arial;
        }
        .nav-divider {
            border: none;
            border-top: 1px solid #1A1A2E;
            margin: 8px 0;
        }
        </style>
    """, unsafe_allow_html=True)

    # Render nav buttons
    st.markdown('<div class="nav-section-header">Navigation</div>',
                unsafe_allow_html=True)

    for label, page in nav_items.items():
        is_active = st.session_state.active_page == page
        if st.button(
            label,
            key=f"nav_{page}",
            use_container_width=True,
            type="primary" if is_active else "secondary"
        ):
            st.session_state.active_page = page
            st.rerun()

    st.markdown('<hr class="nav-divider">', unsafe_allow_html=True)
    st.markdown(
        "<div style='color:#333; font-size:11px; padding:8px'>Apex Analytics © 2026</div>",
        unsafe_allow_html=True
    )


# ── Route to Pages ──
active = st.session_state.active_page

if active == "Home":
    from dashboards.views.home import show
    show()

elif active == "Circuit Replay":
    from dashboards.views.circuit_replay import show
    show()

elif active == "Driver Fingerprinting":
    from dashboards.views.fingerprinting import show
    show()

elif active == "Championship":
    from dashboards.views.standings import show
    show()