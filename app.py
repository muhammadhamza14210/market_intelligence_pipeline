"""
STEP 5: Market Intelligence Dashboard + Azure OpenAI Assistant
----------------------------------------------------------------
Builds an interactive Streamlit dashboard to visualize market signals and sentiment analysis.
Integrates Azure OpenAI to provide a natural language interface for querying the data and generating insights.
"""

import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import os
from openai import AzureOpenAI
from dotenv import load_dotenv

load_dotenv()

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Market Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .stApp {
        background-color: #0E1117;
        color: #FFFFFF;
    }
    
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1a1f2e 0%, #151922 100%);
        border: 1px solid #2a3040;
        border-radius: 12px;
        padding: 16px 20px;
    }
    [data-testid="stMetricValue"] {
        font-size: 24px;
        font-weight: 700;
        color: #FFFFFF !important;
    }
    [data-testid="stMetricLabel"] {
        font-size: 13px;
        color: #c8d0e0 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    /* Chat messages */
    [data-testid="stChatMessage"] {
        background: #1a1f2e !important;
        border: 1px solid #2a3040 !important;
        border-radius: 12px !important;
        color: #FFFFFF !important;
    }
    [data-testid="stChatMessage"] p {
        color: #FFFFFF !important;
    }
    [data-testid="stChatMessage"] li {
        color: #FFFFFF !important;
    }
    
    /* Chat input box */
    [data-testid="stChatInput"] textarea {
        color: #FFFFFF !important;
        background: #1a1f2e !important;
    }
    
    [data-testid="stButton"] button {
        background: #1a1f2e !important;
        color: #FFFFFF !important;
        border: 1px solid #2a3040 !important;
    }
    [data-testid="stButton"] button:hover {
        background: #252b3d !important;
        border-color: #4a90d9 !important;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: #151922;
        border-radius: 12px;
        padding: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 500;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
    background: red;
    color: white;
    }

    .section-header {
        color: #c8d0e0;
        font-size: 18px;
        font-weight: 600;
        margin: 20px 0 10px 0;
        padding-bottom: 8px;
        border-bottom: 1px solid #1e2535;
    }
    
    /* Chat message headings and markdown */
    [data-testid="stChatMessage"] h1,
    [data-testid="stChatMessage"] h2,
    [data-testid="stChatMessage"] h3,
    [data-testid="stChatMessage"] h4,
    [data-testid="stChatMessage"] strong {
        color: #FFFFFF !important;
    }
    
    /* Text input styling */
    [data-testid="stTextInput"] input {
        background: #1a1f2e !important;
        color: #FFFFFF !important;
        border: 1px solid #2a3040 !important;
        border-radius: 8px !important;
    }
    [data-testid="stTextInput"] input::placeholder {
        color: #8892a4 !important;
    }
    
    /* Tab text color - white by default */
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        padding: 8px 20px;
        font-weight: 500;
        color: #FFFFFF !important;
    }
    .stTabs [data-baseweb="tab"]:hover {
        color: #FFFFFF !important;
    }
    .stTabs [aria-selected="true"] {
        color: #FFFFFF !important;
    }
    
    hr { border-color: #1e2535; }
    
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)


# --- DATABASE ---
@st.cache_resource
def get_connection():
    return duckdb.connect("data/market_intelligence.duckdb", read_only=True)

con = get_connection()


# --- AZURE OPENAI ---
@st.cache_resource
def get_openai_client():
    endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
    key = os.getenv("AZURE_OPENAI_KEY")
    if not endpoint or not key:
        return None
    if not endpoint.startswith("https://"):
        endpoint = f"https://{endpoint}"
    return AzureOpenAI(
        azure_endpoint=endpoint,
        api_key=key,
        api_version="2024-12-01-preview",
    )


def query(sql: str) -> pd.DataFrame:
    return con.execute(sql).df()


PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color="#FFFFFF", size=12),
    margin=dict(l=40, r=20, t=40, b=40),
    xaxis=dict(gridcolor="#1e2535", zerolinecolor="#1e2535"),
    yaxis=dict(gridcolor="#1e2535", zerolinecolor="#1e2535"),
    legend=dict(font=dict(color="#FFFFFF")),
    colorway=["#4a90d9", "#50c878", "#ff6b6b", "#ffa726", "#ab47bc", "#26c6da"],
)
def styled_plotly(fig, height=350):
    fig.update_layout(**PLOT_LAYOUT, height=height)
    return fig


def get_data_context() -> str:
    overview = query("SELECT count(distinct ticker) as t, min(date) as s, max(date) as e, count(*) as r FROM analytics.fct_market_signals")
    signals = query("SELECT market_signal, count(*) as count FROM analytics.fct_market_signals GROUP BY market_signal ORDER BY count DESC")
    top = query("SELECT ticker, daily_return, close, market_signal FROM analytics.fct_market_signals WHERE date=(SELECT max(date) FROM analytics.fct_market_signals) ORDER BY daily_return DESC LIMIT 5")
    bottom = query("SELECT ticker, daily_return, close, market_signal FROM analytics.fct_market_signals WHERE date=(SELECT max(date) FROM analytics.fct_market_signals) ORDER BY daily_return ASC LIMIT 5")
    sens = query("SELECT ticker, sentiment_sensitivity, avg_return_bullish_news, avg_return_bearish_news FROM gold.fct_sentiment_sensitivity ORDER BY sentiment_sensitivity DESC LIMIT 10")
    vix = query("SELECT vix_volatility_index FROM analytics.fct_market_signals WHERE vix_volatility_index IS NOT NULL ORDER BY date DESC LIMIT 1")
    div = query("SELECT date, ticker, daily_return, sentiment_ratio, sentiment_price_alignment FROM analytics.fct_market_signals WHERE sentiment_price_alignment != 'aligned' ORDER BY date DESC LIMIT 10")
    
    return f"""You are a market intelligence assistant. Be concise, analytical, use bullet points and numbers.
DATA: {overview['t'].iloc[0]} tickers, {overview['s'].iloc[0]} to {overview['e'].iloc[0]}, {overview['r'].iloc[0]} records.
SIGNALS: {signals.to_string(index=False)}
GAINERS: {top.to_string(index=False)}
LOSERS: {bottom.to_string(index=False)}
SENSITIVE: {sens.to_string(index=False)}
DIVERGENCES: {div.to_string(index=False)}
VIX: {vix['vix_volatility_index'].iloc[0] if len(vix)>0 else 'N/A'}
Do NOT give financial advice."""


def chat_with_azure(message: str, context: str, history: list) -> str:
    client = get_openai_client()
    if not client:
        return "⚠️ Azure OpenAI not configured. Add credentials to .env"
    deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o-mini")
    messages = [{"role": "system", "content": context}]
    for h in history[-6:]:
        messages.append(h)
    messages.append({"role": "user", "content": message})
    try:
        response = client.chat.completions.create(model=deployment, messages=messages, temperature=0.7, max_tokens=800)
        return response.choices[0].message.content
    except Exception as e:
        return f"⚠️ Error: {str(e)}"


# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.markdown("### 📊 Market Intelligence")
    st.caption("Real-time analytics powered by AI")
    st.divider()
    
    tickers = query("SELECT DISTINCT ticker FROM analytics.fct_market_signals ORDER BY ticker")
    selected_ticker = st.selectbox("🔍 Select ticker", tickers["ticker"].tolist(), index=0)
    
    client = get_openai_client()
    if client:
        st.success("✓ Azure OpenAI connected", icon="🤖")
    else:
        st.error("✗ Azure OpenAI not configured", icon="⚠️")
    
    st.divider()
    st.markdown("### 💡 Quick questions")
    
    suggestion_map = {
        "📈 Market trend summary": "Summarize the overall market trend this month",
        "🏆 Top gainers & losers": "What were the top performing and worst performing stocks?",
        "🧠 Sentiment-sensitive stocks": "Which tickers are most sensitive to news sentiment and why?",
        "⚡ Price-sentiment divergences": "Were there any days where stocks moved opposite to sentiment?",
        "📊 VIX & fear level": "What does the current VIX level tell us about market conditions?",
        "📋 Full market briefing": "Give me a comprehensive market intelligence briefing.",
    }
    
    for label, prompt in suggestion_map.items():
        if st.button(label, key=f"sb_{label}", use_container_width=True):
            if "messages" not in st.session_state:
                st.session_state.messages = []
            st.session_state.messages.append({"role": "user", "content": prompt})
            st.session_state.pending_response = True
            st.rerun()
    
    st.divider()
    st.caption("Data: Yahoo Finance · GNews · FRED")
    st.caption("NLP: FinBERT · Azure OpenAI")
    st.caption("Stack: Azure Data Lake · PySpark · dbt · DuckDB")


# ============================================================
# TABS
# ============================================================
st.markdown("## 📊 Market Intelligence Dashboard")

tab_overview, tab_ticker, tab_sentiment, tab_chat = st.tabs([
    "  Market Overview  ", "  Ticker Deep-Dive  ", 
    "  Sentiment Analysis  ", "  AI Assistant  "
])

# ============================================================
# TAB 1: OVERVIEW
# ============================================================
with tab_overview:
    latest_date = query("SELECT max(date) as d FROM analytics.fct_market_signals")["d"].iloc[0]
    stats = query(f"""
        SELECT round(avg(daily_return),2) as avg_ret,
               sum(case when daily_return>0 then 1 else 0 end) as up,
               sum(case when daily_return<0 then 1 else 0 end) as down,
               round(avg(vix_volatility_index),1) as vix
        FROM analytics.fct_market_signals WHERE date='{latest_date}'
    """)
    
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("📅 Latest", str(latest_date))
    c2.metric("📊 Avg Return", f"{stats['avg_ret'].iloc[0]}%")
    c3.metric("📈 Up / Down", f"{int(stats['up'].iloc[0])} / {int(stats['down'].iloc[0])}")
    c4.metric("🌡️ VIX", f"{stats['vix'].iloc[0]}" if pd.notna(stats['vix'].iloc[0]) else "—")
    
    st.markdown("")
    cl, cr = st.columns(2)
    
    with cl:
        st.markdown('<p class="section-header">Market sentiment breakdown</p>', unsafe_allow_html=True)
        sig = query("""
            SELECT 
                CASE 
                    WHEN market_signal IN ('strong_bullish', 'bullish', 'slightly_bullish') THEN 'Bullish'
                    WHEN market_signal IN ('strong_bearish', 'bearish', 'slightly_bearish') THEN 'Bearish'
                    ELSE 'Neutral'
                END as direction,
                count(*) as count
            FROM analytics.fct_market_signals
            GROUP BY direction
        """)
        fig = px.pie(sig, values="count", names="direction", 
                     color="direction",
                     color_discrete_map={"Bullish": "#50c878", "Bearish": "#ff6b6b", "Neutral": "#78909C"},
                     hole=0.4)
        fig.update_traces(textinfo="percent+label", textfont_color="#FFFFFF")
        st.plotly_chart(styled_plotly(fig), use_container_width=True)
    
    with cr:
        st.markdown('<p class="section-header">VIX volatility index</p>', unsafe_allow_html=True)
        vdf = query("SELECT date, vix_volatility_index FROM analytics.fct_market_signals WHERE vix_volatility_index IS NOT NULL AND ticker='AAPL' ORDER BY date")
        if not vdf.empty:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=vdf["date"], y=vdf["vix_volatility_index"],
                fillcolor="rgba(74,144,217,0.1)", line=dict(color="#4a90d9", width=2)))
            fig.add_hline(y=20, line_dash="dash", line_color="#ffa726", annotation_text="Elevated", annotation_font_color="#ffa726")
            fig.add_hline(y=30, line_dash="dash", line_color="#ff6b6b", annotation_text="High Fear", annotation_font_color="#ff6b6b")
            st.plotly_chart(styled_plotly(fig), use_container_width=True)
    
    st.markdown('<p class="section-header">Daily returns heatmap</p>', unsafe_allow_html=True)
    hm = query(f"SELECT date,ticker,daily_return FROM analytics.fct_market_signals WHERE date>=(SELECT max(date)-INTERVAL 14 DAY FROM analytics.fct_market_signals) ORDER BY date,ticker")
    if not hm.empty:
        piv = hm.pivot(index="ticker", columns="date", values="daily_return")
        fig = px.imshow(piv, color_continuous_scale="RdYlGn", color_continuous_midpoint=0, aspect="auto")
        st.plotly_chart(styled_plotly(fig, 600), use_container_width=True)

# ============================================================
# TAB 2: TICKER
# ============================================================
with tab_ticker:
    td = query(f"""
        SELECT date,open,high,low,close,volume,daily_return,sentiment_ratio,
               news_article_count,market_signal,rolling_7d_avg_close,volume_spike_ratio
        FROM analytics.fct_market_signals WHERE ticker='{selected_ticker}' ORDER BY date
    """)
    
    if not td.empty:
        lat = td.iloc[-1]
        c1,c2,c3,c4 = st.columns(4)
        c1.metric(f"💰 {selected_ticker}", f"${lat['close']:.2f}", f"{lat['daily_return']:.2f}%")
        c2.metric("📦 Volume", f"{lat['volume']:,.0f}")
        c3.metric("🎯 Signal", lat['market_signal'])
        c4.metric("🧠 Sentiment", f"{lat['sentiment_ratio']:.2f}" if lat['sentiment_ratio']!=0 else "—")
        
        st.markdown('<p class="section-header">Price + 7-day average</p>', unsafe_allow_html=True)
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=td["date"],y=td["close"],name="Close",
            line=dict(color="#4a90d9",width=2.5),fillcolor="rgba(74,144,217,0.1)"))
        fig.add_trace(go.Scatter(x=td["date"],y=td["rolling_7d_avg_close"],name="7d Avg",
            line=dict(color="#ffa726",width=1.5,dash="dash")))
        st.plotly_chart(styled_plotly(fig,400), use_container_width=True)
        
        cl,cr = st.columns(2)
        with cl:
            st.markdown('<p class="section-header">Volume</p>', unsafe_allow_html=True)
            fig = go.Figure(go.Bar(x=td["date"],y=td["volume"],marker_color="#4a90d9",marker_opacity=0.7))
            st.plotly_chart(styled_plotly(fig,280), use_container_width=True)
        with cr:
            st.markdown('<p class="section-header">Daily returns</p>', unsafe_allow_html=True)
            cols = ['#50c878' if x>0 else '#ff6b6b' for x in td["daily_return"]]
            fig = go.Figure(go.Bar(x=td["date"],y=td["daily_return"],marker_color=cols))
            st.plotly_chart(styled_plotly(fig,280), use_container_width=True)
        
        sd = td[td["sentiment_ratio"]!=0]
        if not sd.empty:
            st.markdown('<p class="section-header">Days with sentiment data</p>', unsafe_allow_html=True)
            st.dataframe(sd[["date","close","daily_return","sentiment_ratio","news_article_count","market_signal"]].reset_index(drop=True),
                use_container_width=True, hide_index=True)

# ============================================================
# TAB 3: SENTIMENT
# ============================================================
with tab_sentiment:
    sens = query("SELECT ticker,avg_return_bullish_news,avg_return_bearish_news,sentiment_sensitivity,avg_abs_return,worst_day,best_day FROM gold.fct_sentiment_sensitivity ORDER BY sentiment_sensitivity DESC")
    
    if not sens.empty:
        st.markdown('<p class="section-header">Sentiment sensitivity by ticker</p>', unsafe_allow_html=True)
        st.caption("Higher = stock reacts more to bullish vs bearish news")
        fig = px.bar(sens, x="ticker", y="sentiment_sensitivity", color="sentiment_sensitivity",
                     color_continuous_scale=["#ff6b6b","#78909C","#50c878"], color_continuous_midpoint=0)
        st.plotly_chart(styled_plotly(fig,400), use_container_width=True)
        
        cl,cr = st.columns(2)
        with cl:
            st.markdown('<p class="section-header">Returns on bullish news</p>', unsafe_allow_html=True)
            b = sens.dropna(subset=["avg_return_bullish_news"]).head(20)
            if not b.empty:
                fig = go.Figure(go.Bar(x=b["ticker"],y=b["avg_return_bullish_news"],marker_color="#50c878",marker_opacity=0.8))
                st.plotly_chart(styled_plotly(fig,320), use_container_width=True)
        with cr:
            st.markdown('<p class="section-header">Returns on bearish news</p>', unsafe_allow_html=True)
            br = sens.dropna(subset=["avg_return_bearish_news"]).head(20)
            if not br.empty:
                fig = go.Figure(go.Bar(x=br["ticker"],y=br["avg_return_bearish_news"],marker_color="#ff6b6b",marker_opacity=0.8))
                st.plotly_chart(styled_plotly(fig,320), use_container_width=True)
        
        st.markdown('<p class="section-header">Full data</p>', unsafe_allow_html=True)
        st.dataframe(sens, use_container_width=True, hide_index=True)
    
    st.markdown('<p class="section-header">Sentiment-price divergence</p>', unsafe_allow_html=True)
    dv = query("SELECT date,ticker,daily_return,sentiment_ratio,market_signal,sentiment_price_alignment FROM analytics.fct_market_signals WHERE sentiment_price_alignment!='aligned' ORDER BY date DESC,abs(daily_return) DESC")
    if not dv.empty:
        st.dataframe(dv, use_container_width=True, hide_index=True)
    else:
        st.info("No divergence events found.")

# ============================================================
# TAB 4: AI CHAT
# ============================================================
with tab_chat:
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "pending_response" not in st.session_state:
        st.session_state.pending_response = False
    
    data_context = get_data_context()
    
    c1, c2 = st.columns([3, 1])
    with c1:
        st.markdown("### 🤖 Market Intelligence Assistant")
        st.caption("Use the quick questions in the sidebar or type below.")
    with c2:
        if st.button("🗑️ Clear chat", use_container_width=True):
            st.session_state.messages = []
            st.rerun()
    
    # Scrollable chat area
    chat_area = st.container(height=450)
    
    with chat_area:
        if not st.session_state.messages:
            pass
        
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])
        
        if st.session_state.pending_response and st.session_state.messages:
            last = st.session_state.messages[-1]
            if last["role"] == "user":
                with st.chat_message("assistant"):
                    with st.spinner("Analyzing..."):
                        resp = chat_with_azure(last["content"], data_context, st.session_state.messages[:-1])
                    st.markdown(resp)
                st.session_state.messages.append({"role": "assistant", "content": resp})
                st.session_state.pending_response = False
                st.rerun()
    
    # Text input BELOW the chat area (not st.chat_input)
    col_input, col_send = st.columns([6, 1])
    with col_input:
        user_input = st.text_input("Ask about your market data...", key="chat_input", placeholder="Ask about your market data...", label_visibility="collapsed")
    with col_send:
        send_clicked = st.button("Send ➤", use_container_width=True)
    
    if send_clicked and user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})
        st.session_state.pending_response = True
        st.rerun()