"""
app.py — Sénégal Environmental Monitoring Dashboard v2
Dashboard professionnel : 14 régions, double source, cartes, graphiques avancés.
"""

import json, time, random
from datetime import datetime, timezone
from collections import deque

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sénégal Weather Intelligence",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Theme CSS ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  /* ── Reset & base ── */
  html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
  }

  /* ── App background ── */
  .stApp {
    background: #040D18;
    background-image:
      radial-gradient(ellipse at 20% 10%, rgba(2,195,154,0.06) 0%, transparent 50%),
      radial-gradient(ellipse at 80% 80%, rgba(6,90,130,0.08) 0%, transparent 50%);
  }

  /* ── Sidebar ── */
  section[data-testid="stSidebar"] {
    background: #070F1C !important;
    border-right: 1px solid #0F2034 !important;
  }
  section[data-testid="stSidebar"] * {
    color: #CBD5E1 !important;
  }
  section[data-testid="stSidebar"] .stButton > button {
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    transition: all 0.2s ease !important;
  }
  section[data-testid="stSidebar"] .stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #02C39A, #065A82) !important;
    border: none !important;
    color: #fff !important;
  }
  section[data-testid="stSidebar"] .stButton > button:not([kind="primary"]) {
    background: #0F2034 !important;
    border: 1px solid #1E3A52 !important;
    color: #94A3B8 !important;
  }
  section[data-testid="stSidebar"] .stButton > button:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 12px rgba(2,195,154,0.2) !important;
  }
  section[data-testid="stSidebar"] hr {
    border-color: #0F2034 !important;
  }

  /* ── Hero banner ── */
  .hero {
    background: linear-gradient(135deg, #040D18 0%, #061A2E 40%, #073B5A 70%, #065A82 100%);
    border: 1px solid #0F2A42;
    padding: 2rem 2.4rem;
    border-radius: 20px;
    margin-bottom: 1.4rem;
    position: relative;
    overflow: hidden;
  }
  .hero::before {
    content: "";
    position: absolute; inset: 0;
    background: radial-gradient(ellipse at 90% 50%, rgba(2,195,154,0.12) 0%, transparent 60%);
    pointer-events: none;
  }
  .hero::after {
    content: "";
    position: absolute; top: -50%; right: -10%;
    width: 300px; height: 300px;
    background: radial-gradient(circle, rgba(2,195,154,0.07) 0%, transparent 70%);
    border-radius: 50%;
    pointer-events: none;
  }
  .hero h1 {
    color: #F1F5F9;
    font-size: 1.85rem;
    font-weight: 700;
    margin: 0 0 0.4rem;
    letter-spacing: -0.02em;
  }
  .hero h1 span {
    background: linear-gradient(90deg, #02C39A, #38BDF8);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
  }
  .hero p {
    color: rgba(148,163,184,0.85);
    margin: 0;
    font-size: 0.875rem;
    letter-spacing: 0.01em;
  }
  .hero-pills {
    display: flex; gap: 8px; margin-top: 1rem; flex-wrap: wrap;
  }
  .hero-pill {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 99px;
    padding: 3px 12px;
    font-size: 0.72rem;
    font-weight: 500;
    color: #94A3B8;
    letter-spacing: 0.03em;
  }

  /* ── KPI cards ── */
  .kpi-card {
    background: linear-gradient(145deg, #080F1C, #0A1625);
    border: 1px solid #1E2A3A;
    border-radius: 16px;
    padding: 1.2rem 1.4rem;
    transition: all 0.25s ease;
    position: relative;
    overflow: hidden;
  }
  .kpi-card::before {
    content: "";
    position: absolute; inset: 0;
    background: radial-gradient(ellipse at top left, rgba(2,195,154,0.04) 0%, transparent 60%);
    pointer-events: none;
  }
  .kpi-card:hover {
    border-color: rgba(2,195,154,0.35);
    box-shadow: 0 8px 32px rgba(2,195,154,0.1), 0 0 0 1px rgba(2,195,154,0.08);
    transform: translateY(-2px);
  }
  .kpi-label {
    font-size: 0.7rem;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    margin-bottom: 0.3rem;
    font-weight: 600;
  }
  .kpi-value {
    font-size: 1.95rem;
    font-weight: 700;
    color: #F1F5F9;
    line-height: 1;
    font-family: 'JetBrains Mono', monospace;
  }
  .kpi-unit {
    font-size: 0.85rem;
    color: #64748B;
    margin-left: 4px;
    font-family: 'Inter', sans-serif;
    font-weight: 400;
  }
  .kpi-delta-up   { color: #F87171; font-size: 0.75rem; margin-top: 0.3rem; font-weight: 500; }
  .kpi-delta-down { color: #34D399; font-size: 0.75rem; margin-top: 0.3rem; font-weight: 500; }

  /* ── Source badges ── */
  .badge-owm  {
    background: linear-gradient(135deg, #D97706, #F59E0B);
    color: #000; padding: 3px 10px; border-radius: 99px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.05em;
  }
  .badge-om   {
    background: linear-gradient(135deg, #059669, #02C39A);
    color: #000; padding: 3px 10px; border-radius: 99px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.05em;
  }
  .badge-demo {
    background: linear-gradient(135deg, #6D28D9, #7C3AED);
    color: #fff; padding: 3px 10px; border-radius: 99px;
    font-size: 0.65rem; font-weight: 700; letter-spacing: 0.05em;
  }

  /* ── Section titles ── */
  .section-title {
    font-size: 0.75rem;
    font-weight: 700;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin: 1.6rem 0 0.8rem;
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .section-title::after {
    content: "";
    flex: 1;
    height: 1px;
    background: linear-gradient(90deg, #1E2A3A, transparent);
  }

  /* ── Alert banners ── */
  .alert-hot {
    background: linear-gradient(135deg, rgba(127,29,29,0.2), rgba(153,27,27,0.1));
    border: 1px solid rgba(239,68,68,0.3);
    border-left: 3px solid #EF4444;
    padding: 0.7rem 1rem;
    border-radius: 10px;
    color: #FCA5A5;
    margin-bottom: 0.5rem;
    font-size: 0.875rem;
  }
  .alert-ok {
    background: linear-gradient(135deg, rgba(6,67,40,0.2), rgba(5,150,105,0.08));
    border: 1px solid rgba(34,197,94,0.2);
    border-left: 3px solid #22C55E;
    padding: 0.7rem 1rem;
    border-radius: 10px;
    color: #86EFAC;
    font-size: 0.875rem;
  }

  /* ── Streamlit metric cards ── */
  div[data-testid="metric-container"] {
    background: linear-gradient(145deg, #080F1C, #0A1625) !important;
    border: 1px solid #1E2A3A !important;
    border-radius: 14px !important;
    padding: 1rem 1.2rem !important;
    transition: all 0.2s ease !important;
  }
  div[data-testid="metric-container"]:hover {
    border-color: rgba(2,195,154,0.25) !important;
    box-shadow: 0 4px 20px rgba(2,195,154,0.08) !important;
  }
  div[data-testid="metric-container"] label {
    color: #475569 !important;
    font-size: 0.7rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    font-weight: 600 !important;
  }
  div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #F1F5F9 !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-weight: 700 !important;
  }
  div[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-size: 0.75rem !important;
  }

  /* ── Dividers ── */
  hr {
    border: none !important;
    border-top: 1px solid #0F2034 !important;
    margin: 1.2rem 0 !important;
  }

  /* ── Selectbox / Multiselect / Slider ── */
  .stSelectbox > div > div,
  .stMultiSelect > div > div {
    background: #080F1C !important;
    border: 1px solid #1E2A3A !important;
    border-radius: 10px !important;
    color: #CBD5E1 !important;
  }
  .stSlider > div > div > div > div {
    background: linear-gradient(90deg, #02C39A, #065A82) !important;
  }

  /* ── Expander ── */
  .streamlit-expanderHeader {
    background: #080F1C !important;
    border: 1px solid #1E2A3A !important;
    border-radius: 12px !important;
    color: #94A3B8 !important;
    font-weight: 600 !important;
  }
  .streamlit-expanderContent {
    background: #040D18 !important;
    border: 1px solid #1E2A3A !important;
    border-top: none !important;
    border-radius: 0 0 12px 12px !important;
  }

  /* ── Download button ── */
  .stDownloadButton > button {
    background: linear-gradient(135deg, #065A82, #02C39A) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.8rem !important;
    padding: 0.4rem 1rem !important;
    transition: all 0.2s !important;
  }
  .stDownloadButton > button:hover {
    box-shadow: 0 4px 15px rgba(2,195,154,0.3) !important;
    transform: translateY(-1px) !important;
  }

  /* ── Info / Warning ── */
  .stAlert {
    background: #080F1C !important;
    border: 1px solid #1E2A3A !important;
    border-radius: 12px !important;
  }

  /* ── Dataframe ── */
  .stDataFrame {
    border: 1px solid #1E2A3A !important;
    border-radius: 12px !important;
    overflow: hidden !important;
  }

  /* ── Number input ── */
  .stNumberInput > div > div > input {
    background: #080F1C !important;
    border: 1px solid #1E2A3A !important;
    border-radius: 8px !important;
    color: #F1F5F9 !important;
  }

  /* ── Success / Warning badges in sidebar ── */
  .stSuccess, .stWarning {
    background: #080F1C !important;
    border-radius: 10px !important;
    font-size: 0.8rem !important;
  }

  /* ── Plotly chart containers ── */
  .js-plotly-plot {
    border-radius: 14px !important;
    overflow: hidden !important;
  }

  /* ── Scrollbar ── */
  ::-webkit-scrollbar { width: 5px; height: 5px; }
  ::-webkit-scrollbar-track { background: #040D18; }
  ::-webkit-scrollbar-thumb { background: #1E2A3A; border-radius: 99px; }
  ::-webkit-scrollbar-thumb:hover { background: #02C39A44; }

  /* ── Hide Streamlit chrome ── */
  footer { visibility: hidden; }
  #MainMenu { visibility: hidden; }
  header[data-testid="stHeader"] { background: transparent !important; }
  .block-container { padding-top: 1.5rem !important; }
</style>
""", unsafe_allow_html=True)

# ── Constantes ─────────────────────────────────────────────────────────────────
KAFKA_BROKERS = ["kafka1:9092", "kafka2:9092", "kafka3:9092"]
KAFKA_TOPIC   = "senegal-meteo"
HISTORY_LEN   = 500

REGIONS_LIST = [
    "Dakar","Thiès","Diourbel","Fatick","Kaolack","Kaffrine",
    "Tambacounda","Kédougou","Kolda","Sédhiou","Ziguinchor",
    "Saint-Louis","Louga","Matam"
]
REGION_COORDS = {
    "Dakar":{"lat":14.6928,"lon":-17.4467}, "Thiès":{"lat":14.7886,"lon":-16.9255},
    "Diourbel":{"lat":14.6554,"lon":-16.2322}, "Fatick":{"lat":14.3390,"lon":-16.4114},
    "Kaolack":{"lat":14.1652,"lon":-16.0726}, "Kaffrine":{"lat":14.1059,"lon":-15.5503},
    "Tambacounda":{"lat":13.7707,"lon":-13.6673}, "Kédougou":{"lat":12.5559,"lon":-12.1747},
    "Kolda":{"lat":12.8989,"lon":-14.9413}, "Sédhiou":{"lat":12.7080,"lon":-15.5569},
    "Ziguinchor":{"lat":12.5681,"lon":-16.2719}, "Saint-Louis":{"lat":16.0179,"lon":-16.4896},
    "Louga":{"lat":15.6179,"lon":-16.2241}, "Matam":{"lat":15.6559,"lon":-13.2557},
}
COLORS = {
    "open-meteo":    "#02C39A",
    "openweathermap":"#F59E0B",
    "demo":          "#A78BFA",
}

# ── Session state ──────────────────────────────────────────────────────────────
for key, default in [("history", deque(maxlen=HISTORY_LEN)),
                     ("running", False), ("consumer", None)]:
    if key not in st.session_state:
        st.session_state[key] = default


# ── Kafka ──────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_consumer():
    try:
        c = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKERS,
            group_id="dashboard-v2",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            consumer_timeout_ms=2500,
        )
        return c, True
    except NoBrokersAvailable:
        return None, False
    except Exception:
        return None, False

# Polling Kafka pour récupérer les nouveaux messages
def poll(consumer, n=30):
    msgs = []
    try:
        for m in consumer:
            d = m.value
            if "time" in d and "temp" in d and "region" in d:
                msgs.append(d)
            if len(msgs) >= n:
                break
    except Exception:
        pass
    return msgs

#── Mode démo (génération de données aléatoires) ───────────────────────────────
def demo_point(region):
    base_temps = {
        "Dakar":26,"Thiès":28,"Diourbel":30,"Fatick":29,"Kaolack":31,
        "Kaffrine":32,"Tambacounda":34,"Kédougou":33,"Kolda":30,
        "Sédhiou":29,"Ziguinchor":28,"Saint-Louis":27,"Louga":29,"Matam":35
    }
    base = base_temps.get(region, 30)
    return {
        "time": datetime.now(timezone.utc).isoformat(),
        "region": region, "region_code": region[:2].upper(),
        "source": "demo",
        "temp":       round(base + random.uniform(-2, 3), 1),
        "temp_feels": round(base + random.uniform(-3, 4), 1),
        "hum":        round(random.uniform(40, 85), 1),
        "pressure":   round(random.uniform(1008, 1018), 1),
        "wind_speed": round(random.uniform(2, 18), 1),
        "wind_dir":   random.randint(0, 359),
    }



# SIDEBAR

with st.sidebar:
    st.markdown("## ⚙️ Contrôle")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶ Démarrer", use_container_width=True, type="primary"):
            st.session_state.running = True
    with col2:
        if st.button("⏹ Arrêter", use_container_width=True):
            st.session_state.running = False

    st.divider()
    st.markdown("### 🗺️ Filtres")
    selected_regions = st.multiselect(
        "Régions à afficher",
        REGIONS_LIST,
        default=REGIONS_LIST,
        help="Sélectionner une ou plusieurs régions"
    )
    selected_sources = st.multiselect(
        "Sources de données",
        ["open-meteo", "openweathermap", "demo"],
        default=["open-meteo", "openweathermap", "demo"],
    )

    st.divider()
    st.markdown("### ⏱️ Paramètres")
    refresh_rate = st.slider("Refresh (s)", 5, 120, 30)
    history_pts  = st.slider("Points historique", 20, 500, 100)

    st.divider()
    st.markdown("### 🚨 Alertes")
    alert_temp = st.number_input("Seuil temp. (°C)", value=38.0, step=0.5)
    alert_hum  = st.number_input("Seuil humidité (%)", value=85.0, step=1.0)
    alert_wind = st.number_input("Seuil vent (km/h)", value=50.0, step=1.0)

    st.divider()
    consumer, kafka_ok = get_consumer()
    if kafka_ok:
        st.success("● Kafka connecté", icon="✅")
    else:
        st.warning("● Mode démo (Kafka absent)", icon="⚠️")
    st.caption(f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC")


# POLLING veut dire : à chaque exécution du script (à chaque interaction ou refresh), 
# on récupère les nouveaux messages Kafka (ou on génère des points de données aléatoires en mode démo) 
# et on les ajoute à l'historique stocké dans session_state. Ensuite, on construit un DataFrame à partir de cet historique pour l'affichage.

if st.session_state.running:
    if kafka_ok and consumer:
        new_msgs = poll(consumer)
    else:
        new_msgs = [demo_point(r) for r in REGIONS_LIST]
    for m in new_msgs:
        st.session_state.history.append(m)

history = list(st.session_state.history)
df_all = pd.DataFrame(history) if history else pd.DataFrame()
if not df_all.empty:
    df_all["time"] = pd.to_datetime(df_all["time"])
    if selected_regions:
        df_all = df_all[df_all["region"].isin(selected_regions)]
    if selected_sources:
        df_all = df_all[df_all["source"].isin(selected_sources)]

# HERO HEADER

st.markdown("""
<div class="hero">
  <h1>🌍 Sénégal <span>Weather Intelligence</span></h1>
  <p>Monitoring environnemental temps réel · 14 régions · Open-Meteo + OpenWeatherMap · Apache Kafka · MinIO</p>
  <div class="hero-pills">
    <span class="hero-pill">⚡ Temps réel</span>
    <span class="hero-pill">🗺️ 14 régions</span>
    <span class="hero-pill">📡 Kafka cluster</span>
    <span class="hero-pill">🗄️ MinIO Parquet</span>
    <span class="hero-pill">🌐 Open-Meteo · OWM</span>
  </div>
</div>
""", unsafe_allow_html=True)

# Status bar
status_cols = st.columns([1,1,1,1,1,1])
n_records   = len(df_all) if not df_all.empty else 0
n_regions   = df_all["region"].nunique() if not df_all.empty else 0
n_sources   = df_all["source"].nunique() if not df_all.empty else 0

with status_cols[0]: st.metric("📦 Messages", f"{n_records:,}")
with status_cols[1]: st.metric("🗺️ Régions actives", n_regions)
with status_cols[2]: st.metric("🔌 Sources", n_sources)
with status_cols[3]:
    state_label = "🟢 En cours" if st.session_state.running else "⏸ Arrêté"
    st.metric("Statut", state_label)
with status_cols[4]:
    src_badge = "Kafka" if kafka_ok else "Démo"
    st.metric("Mode", src_badge)
with status_cols[5]:
    last_t = df_all["time"].max().strftime("%H:%M:%S") if not df_all.empty else "—"
    st.metric("Dernière MAJ", last_t)

st.divider()

# KPIs : dernières valeurs par région sélectionnée

st.markdown('<p class="section-title">📊 Métriques en temps réel par région</p>', unsafe_allow_html=True)

if not df_all.empty:
    latest = df_all.sort_values("time").groupby("region").last().reset_index()
    rows = [latest.iloc[i:i+4] for i in range(0, len(latest), 4)]
    for row_df in rows:
        cols = st.columns(4)
        for col_idx, (_, r) in enumerate(row_df.iterrows()):
            with cols[col_idx]:
                temp = r.get("temp", None)
                hum  = r.get("hum",  None)
                src  = r.get("source", "—")
                badge_cls = "badge-owm" if src == "openweathermap" else ("badge-demo" if src == "demo" else "badge-om")
                badge_lbl = "OWM" if src == "openweathermap" else ("DEMO" if src == "demo" else "OM")
                is_hot = temp is not None and temp > alert_temp
                border_color = "#EF4444" if is_hot else "#1E2A3A"
                st.markdown(f"""
<div class="kpi-card" style="border-color:{border_color}">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:0.4rem">
    <span style="font-weight:600;color:#E2E8F0;font-size:0.9rem">{r['region']}</span>
    <span class="{badge_cls}">{badge_lbl}</span>
  </div>
  <div class="kpi-value">{"—" if temp is None else f"{temp:.1f}"}<span class="kpi-unit">°C</span></div>
  <div style="color:#64748B;font-size:0.78rem;margin-top:0.3rem">
    💧 {"—" if hum is None else f"{hum:.0f}%"}  &nbsp;|&nbsp;
    💨 {"—" if r.get("wind_speed") is None else f"{r.get('wind_speed'):.1f} km/h"}
  </div>
  {"<div class='kpi-delta-up'>🔥 ALERTE CHALEUR</div>" if is_hot else ""}
</div>
""", unsafe_allow_html=True)
else:
    st.info("⏳ En attente de données — cliquez sur **▶ Démarrer**.")

st.divider()

# CARTE GÉOGRAPHIQUE
st.markdown('<p class="section-title">🗺️ Carte thermique des régions</p>', unsafe_allow_html=True)

if not df_all.empty:
    map_data = df_all.sort_values("time").groupby("region").last().reset_index()
    map_data["lat"] = map_data["region"].map(lambda r: REGION_COORDS.get(r, {}).get("lat", 14))
    map_data["lon"] = map_data["region"].map(lambda r: REGION_COORDS.get(r, {}).get("lon", -15))
    map_data["temp_display"] = map_data["temp"].fillna(0)
    
    fig_map = px.scatter_mapbox(
        map_data, lat="lat", lon="lon",
        color="temp_display", size="temp_display",
        hover_name="region", 
        color_continuous_scale="RdYlGn_r",
        size_max=30, zoom=5.5,
        center={"lat": 14.3, "lon": -14.7},
        # MODIFICATION : Style clair
        mapbox_style="carto-positron", 
        labels={"temp_display": "Température (°C)"},
    )
    
    fig_map.update_traces(
        hovertemplate="<b>%{hovertext}</b><br>Temp: %{marker.color:.1f}°C<extra></extra>"
    )
    
    fig_map.update_layout(
        height=420, margin={"r":0,"t":0,"l":0,"b":0},
        paper_bgcolor="white", # Fond du conteneur blanc
        # MODIFICATION : Police sombre pour la légende
        coloraxis_colorbar=dict(
            title="°C", 
            tickfont=dict(color="#1E293B"), 
            title_font=dict(color="#1E293B")
        ),
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("Carte disponible après réception des données.")

st.divider()


# GRAPHIQUES D'ÉVOLUTION

if not df_all.empty and len(df_all) > 2:
    st.markdown('<p class="section-title">📈 Évolution temporelle</p>', unsafe_allow_html=True)

    # Sélecteur de région pour les graphiques
    focus_region = st.selectbox(
        "Région à analyser en détail",
        selected_regions if selected_regions else REGIONS_LIST,
        index=0,
    )
    df_focus = df_all[df_all["region"] == focus_region].sort_values("time")

    # ── 4 graphiques en grille 2×2 ────────────────────────────────────────────
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=("🌡️ Température (°C)", "💧 Humidité (%)",
                        "🔽 Pression (hPa)", "💨 Vitesse du vent (km/h)"),
        vertical_spacing=0.18, horizontal_spacing=0.1,
    )

    metrics = [
        ("temp",       "#02C39A", 1, 1, alert_temp),
        ("hum",        "#065A82", 1, 2, alert_hum),
        ("pressure",   "#F59E0B", 2, 1, None),
        ("wind_speed", "#A78BFA", 2, 2, alert_wind),
    ]

    def hex_to_rgba(hex_color: str, alpha: float = 0.08) -> str:
        h = hex_color.lstrip("#")
        if len(h) == 3:
            h = "".join(c*2 for c in h)
        r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return f"rgba({r},{g},{b},{alpha})"

    for source in df_focus["source"].unique():
        df_s = df_focus[df_focus["source"] == source]
        color = COLORS.get(source, "#888888")
        name  = source.upper()[:3]
        fill_color = hex_to_rgba(color, 0.08)
        for col_name, _color, row, col, threshold in metrics:
            if col_name in df_s.columns and df_s[col_name].notna().any():
                fig.add_trace(go.Scatter(
                    x=df_s["time"], y=df_s[col_name],
                    mode="lines+markers",
                    marker=dict(size=4, color=color),
                    line=dict(color=color, width=1.8),
                    name=name, legendgroup=source,
                    showlegend=(col_name == "temp"),
                    fill="tozeroy",
                    fillcolor=fill_color,
                ), row=row, col=col)
            if threshold is not None and col_name == "temp":
                fig.add_hline(y=threshold, line_dash="dash",
                              line_color="#EF4444", line_width=1,
                              annotation_text=f"Seuil {threshold}°C",
                              annotation_font_color="#EF4444", row=row, col=col)

    fig.update_layout(
        height=520, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=40, r=20, t=60, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, font=dict(size=11)),
        font=dict(color="#94A3B8"),
    )
    for ann in fig.layout.annotations:
        ann.font.color = "#CBD5E1"
        ann.font.size  = 12

    st.plotly_chart(fig, use_container_width=True)

    # ── Comparaison toutes régions ─────────────────────────────────────────────
    st.markdown('<p class="section-title">🌡️ Comparaison des températures par région</p>', unsafe_allow_html=True)

    latest_all = df_all.sort_values("time").groupby(["region", "source"]).last().reset_index()
    fig_bar = px.bar(
        latest_all.sort_values("temp", ascending=False),
        x="region", y="temp", color="source",
        color_discrete_map=COLORS,
        barmode="group",
        labels={"temp": "Température (°C)", "region": "Région", "source": "Source"},
        text="temp",
    )
    fig_bar.add_hline(y=alert_temp, line_dash="dash", line_color="#EF4444",
                      annotation_text=f"Seuil alerte {alert_temp}°C",
                      annotation_font_color="#EF4444")
    fig_bar.update_traces(texttemplate="%{text:.1f}°C", textposition="outside", textfont_size=10)
    fig_bar.update_layout(
        height=380, template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=10, b=40), font=dict(color="#94A3B8"),
        legend=dict(title="Source"),
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    # ── Rose des vents ─────────────────────────────────────────────────────────
    wind_col, heatmap_col = st.columns(2)

    with wind_col:
        st.markdown('<p class="section-title">🧭 Rose des vents</p>', unsafe_allow_html=True)
        df_wind = df_focus.dropna(subset=["wind_dir", "wind_speed"])
        if not df_wind.empty:
            fig_wind = go.Figure(go.Barpolar(
                r=df_wind["wind_speed"].tolist(),
                theta=df_wind["wind_dir"].tolist(),
                marker_color=df_wind["wind_speed"].tolist(),
                marker_colorscale="Teal",
                opacity=0.8,
            ))
            fig_wind.update_layout(
                height=320, template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                polar=dict(bgcolor="rgba(0,0,0,0)"),
                margin=dict(l=20, r=20, t=10, b=10),
                font=dict(color="#94A3B8"),
            )
            st.plotly_chart(fig_wind, use_container_width=True)
        else:
            st.info("Données de vent insuffisantes.")

    with heatmap_col:
        st.markdown('<p class="section-title">🔥 Carte chaleur régions × source</p>', unsafe_allow_html=True)
        if not df_all.empty and len(df_all) > 4:
            pivot = df_all.groupby(["region", "source"])["temp"].mean().reset_index()
            pivot_wide = pivot.pivot(index="region", columns="source", values="temp").fillna(0)
            fig_heat = px.imshow(
                pivot_wide,
                color_continuous_scale="RdYlGn_r",
                labels={"color": "Temp moy (°C)"},
                aspect="auto",
            )
            fig_heat.update_layout(
                height=320, template="plotly_dark",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=10, r=10, t=10, b=10),
                font=dict(color="#94A3B8"),
                coloraxis_colorbar=dict(tickfont=dict(color="#94A3B8")),
            )
            st.plotly_chart(fig_heat, use_container_width=True)

    # ── Alertes actives ────────────────────────────────────────────────────────
    st.divider()
    st.markdown('<p class="section-title">🚨 Alertes actives</p>', unsafe_allow_html=True)
    latest_all2 = df_all.sort_values("time").groupby("region").last().reset_index()
    alerts = []
    for _, r in latest_all2.iterrows():
        if r.get("temp", 0) and r["temp"] > alert_temp:
            alerts.append(f"🔥 **{r['region']}** — Température {r['temp']:.1f}°C > seuil {alert_temp}°C")
        if r.get("hum", 0) and r["hum"] > alert_hum:
            alerts.append(f"💧 **{r['region']}** — Humidité {r['hum']:.0f}% > seuil {alert_hum}%")
        if r.get("wind_speed", 0) and r["wind_speed"] > alert_wind:
            alerts.append(f"💨 **{r['region']}** — Vent {r['wind_speed']:.1f} km/h > seuil {alert_wind} km/h")

    if alerts:
        for a in alerts:
            st.markdown(f'<div class="alert-hot">{a}</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div class="alert-ok">✅ Tous les paramètres sont dans les seuils normaux.</div>',
                    unsafe_allow_html=True)

# TABLE

st.divider()
with st.expander("📋 Tableau complet des données", expanded=False):
    if not df_all.empty:
        disp = df_all[["time","region","source","temp","temp_feels","hum","pressure","wind_speed"]].copy()
        disp.columns = ["Heure","Région","Source","Temp °C","Ressenti °C","Humidité %","Pression hPa","Vent km/h"]
        disp = disp.sort_values("Heure", ascending=False).head(100)
        st.dataframe(disp, use_container_width=True, hide_index=True)
        csv = disp.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Télécharger CSV", csv, "senegal_meteo.csv", "text/csv")
    else:
        st.info("Aucune donnée disponible.")

# ── Auto-refresh ───────────────────────────────────────────────────────────────
if st.session_state.running:
    time.sleep(refresh_rate)
    st.rerun()
