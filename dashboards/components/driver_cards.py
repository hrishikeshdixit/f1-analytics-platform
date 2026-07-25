from dashboards.utils.styles import get_color

# ── Driver Card HTML ──
def driver_card(driver_code, team, extra_info=''):
    color = get_color(team)
    return f"""
        <div style='
            background-color: #16213E;
            border-left: 4px solid {color};
            padding: 12px;
            margin-bottom: 12px;
            border-radius: 4px;
        '>
            <b style='color:{color}; font-size:18px'>{driver_code}</b><br>
            <span style='color:#AAAAAA; font-size:13px'>{team}</span>
            {f"<br><span style='color:white; font-size:13px'>{extra_info}</span>" if extra_info else ''}
        </div>
    """

# ── Similarity Card HTML ──
def similarity_card(driver_code, team, similarity_score):
    color = get_color(team)
    score_pct = int(similarity_score * 100)
    return f"""
        <div style='
            background-color: #16213E;
            border-left: 4px solid {color};
            padding: 10px;
            margin-bottom: 8px;
            border-radius: 4px;
        '>
            <b style='color:{color}'>{driver_code}</b>
            <span style='color:#AAAAAA; font-size:12px'> — {team}</span><br>
            <span style='color:white; font-size:12px'>Similarity: {score_pct}%</span>
        </div>
    """