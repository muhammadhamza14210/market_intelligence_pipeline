"""
STEP 5: Market Intelligence Dashboard + LLM Assistant
-------------------------------------------------------
Streamlit app with:
  - Market overview charts (price, sentiment, signals)
  - Ticker deep-dive
  - Sentiment sensitivity analysis
  - Ollama-powered chatbot that answers questions about your data
"""

import streamlit as st
import duckdb
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import requests
import json

# --- PAGE CONFIG ---
st.set_page_config(
    page_title="Market Intelligence",
    page_icon="📊",
    layout="wide",
)

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_connection():
    return duckdb.connect("data/market_intelligence.duckdb", read_only=True)

con = get_connection()


# --- HELPER: query to dataframe ---
def query(sql: str) -> pd.DataFrame:
    return con.execute(sql).df()


# --- HELPER: get data context for LLM ---
def get_data_context() -> str:
    """Build a context string from the data for the LLM."""
    
    # Market overview
    overview = query("""
        SELECT 
            count(distinct ticker) as total_tickers,
            min(date) as start_date,
            max(date) as end_date,
            count(*) as total_records
        FROM analytics.fct_market_signals
    """)
    
    # Signal distribution
    signals = query("""
        SELECT market_signal, count(*) as count
        FROM analytics.fct_market_signals
        GROUP BY market_signal ORDER BY count DESC
    """)
    
    # Top movers today (latest date)
    top_movers = query("""
        SELECT ticker, daily_return, close, sentiment_ratio, market_signal
        FROM analytics.fct_market_signals
        WHERE date = (SELECT max(date) FROM analytics.fct_market_signals)
        ORDER BY daily_return DESC
        LIMIT 5
    """)
    
    bottom_movers = query("""
        SELECT ticker, daily_return, close, sentiment_ratio, market_signal
        FROM analytics.fct_market_signals
        WHERE date = (SELECT max(date) FROM analytics.fct_market_signals)
        ORDER BY daily_return ASC
        LIMIT 5
    """)
    
    # Sentiment sensitivity
    sensitivity = query("""
        SELECT ticker, sentiment_sensitivity, avg_return_bullish_news, avg_return_bearish_news
        FROM gold.fct_sentiment_sensitivity
        ORDER BY sentiment_sensitivity DESC
        LIMIT 10
    """)
    
    # VIX level
    vix = query("""
        SELECT date, vix_volatility_index
        FROM analytics.fct_market_signals
        WHERE vix_volatility_index IS NOT NULL
        ORDER BY date DESC
        LIMIT 1
    """)
    
    context = f"""
You are a market intelligence assistant analyzing stock market data.

DATA OVERVIEW:
- Tracking {overview['total_tickers'].iloc[0]} tickers from {overview['start_date'].iloc[0]} to {overview['end_date'].iloc[0]}
- {overview['total_records'].iloc[0]} total daily records

SIGNAL DISTRIBUTION:
{signals.to_string(index=False)}

LATEST DAY TOP GAINERS:
{top_movers.to_string(index=False)}

LATEST DAY TOP LOSERS:
{bottom_movers.to_string(index=False)}

MOST SENTIMENT-SENSITIVE TICKERS (react most to news):
{sensitivity.to_string(index=False)}

LATEST VIX: {vix['vix_volatility_index'].iloc[0] if len(vix) > 0 else 'N/A'}

Answer questions about this market data. Be specific with numbers. If asked about a ticker, 
query your knowledge of the data above. Keep responses concise and analytical.
Do NOT give financial advice. Say "based on the data" not "you should buy/sell".
"""
    return context


# --- HELPER: Chat with Ollama ---
def chat_with_ollama(message: str, context: str, history: list) -> str:
    """Send a message to Ollama and get a response."""
    
    messages = [{"role": "system", "content": context}]
    
    # Add conversation history
    for h in history[-6:]:  # Keep last 6 messages for context
        messages.append(h)
    
    messages.append({"role": "user", "content": message})
    
    try:
        response = requests.post(
            "http://localhost:11434/api/chat",
            json={
                "model": st.session_state.get("ollama_model", "llama3.2"),
                "messages": messages,
                "stream": False,
            },
            timeout=60,
        )
        response.raise_for_status()
        return response.json()["message"]["content"]
    except requests.exceptions.ConnectionError:
        return "⚠️ Cannot connect to Ollama. Make sure it's running: `ollama serve`"
    except Exception as e:
        return f"⚠️ Error: {str(e)}"


# ============================================================
# SIDEBAR
# ============================================================
with st.sidebar:
    st.title("⚙️ Settings")
    
    # Ticker selector
    tickers = query("SELECT DISTINCT ticker FROM analytics.fct_market_signals ORDER BY ticker")
    selected_ticker = st.selectbox("Select ticker", tickers["ticker"].tolist(), index=0)
    
    # Ollama model
    ollama_model = st.text_input("Ollama model", value="llama3.2")
    st.session_state["ollama_model"] = ollama_model
    
    st.divider()
    st.caption("Market Intelligence Pipeline")
    st.caption("Data: Yahoo Finance · GNews · FRED")
    st.caption("NLP: FinBERT · Ollama")


# ============================================================
# MAIN DASHBOARD
# ============================================================
st.title("📊 Market Intelligence Dashboard")

# --- TAB LAYOUT ---
tab_overview, tab_ticker, tab_sentiment, tab_chat = st.tabs([
    "🏠 Market Overview", 
    "🔍 Ticker Deep-Dive", 
    "🧠 Sentiment Analysis",
    "🤖 AI Assistant"
])


# ============================================================
# TAB 1: MARKET OVERVIEW
# ============================================================
with tab_overview:
    
    # KPI row
    col1, col2, col3, col4 = st.columns(4)
    
    latest_date = query("SELECT max(date) as d FROM analytics.fct_market_signals")["d"].iloc[0]
    
    latest_stats = query(f"""
        SELECT 
            round(avg(daily_return), 2) as avg_return,
            sum(case when daily_return > 0 then 1 else 0 end) as up_count,
            sum(case when daily_return < 0 then 1 else 0 end) as down_count,
            round(avg(vix_volatility_index), 1) as vix
        FROM analytics.fct_market_signals
        WHERE date = '{latest_date}'
    """)
    
    col1.metric("Latest Date", str(latest_date))
    col2.metric("Avg Return", f"{latest_stats['avg_return'].iloc[0]}%")
    col3.metric("Up / Down", f"{int(latest_stats['up_count'].iloc[0])} / {int(latest_stats['down_count'].iloc[0])}")
    col4.metric("VIX", f"{latest_stats['vix'].iloc[0]}" if latest_stats['vix'].iloc[0] else "N/A")
    
    st.divider()
    
    # Signal distribution
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.subheader("Market signal distribution")
        signals_df = query("""
            SELECT market_signal, count(*) as count
            FROM analytics.fct_market_signals
            GROUP BY market_signal ORDER BY count DESC
        """)
        color_map = {
            'strong_bullish': '#00C853',
            'bullish': '#4CAF50',
            'slightly_bullish': '#81C784',
            'neutral': '#9E9E9E',
            'slightly_bearish': '#E57373',
            'bearish': '#F44336',
            'strong_bearish': '#B71C1C',
        }
        fig = px.bar(signals_df, x="market_signal", y="count", 
                     color="market_signal", color_discrete_map=color_map)
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    
    with col_right:
        st.subheader("VIX risk level over time")
        vix_df = query("""
            SELECT date, vix_volatility_index, vix_risk_level
            FROM analytics.fct_market_signals
            WHERE vix_volatility_index IS NOT NULL AND ticker = 'AAPL'
            ORDER BY date
        """)
        if not vix_df.empty:
            fig = px.line(vix_df, x="date", y="vix_volatility_index")
            fig.add_hline(y=20, line_dash="dash", line_color="orange", annotation_text="Elevated")
            fig.add_hline(y=30, line_dash="dash", line_color="red", annotation_text="High Fear")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
    
    # Heatmap: daily returns by ticker
    st.subheader("Daily returns heatmap (last 10 days)")
    heatmap_df = query(f"""
        SELECT date, ticker, daily_return
        FROM analytics.fct_market_signals
        WHERE date >= (SELECT max(date) - INTERVAL 14 DAY FROM analytics.fct_market_signals)
        ORDER BY date, ticker
    """)
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot(index="ticker", columns="date", values="daily_return")
        fig = px.imshow(pivot, color_continuous_scale="RdYlGn", 
                        color_continuous_midpoint=0, aspect="auto")
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)


# ============================================================
# TAB 2: TICKER DEEP-DIVE
# ============================================================
with tab_ticker:
    
    st.subheader(f"{selected_ticker} — Deep Dive")
    
    ticker_data = query(f"""
        SELECT date, open, high, low, close, volume, daily_return,
               sentiment_ratio, news_article_count, market_signal,
               rolling_7d_avg_close, volume_spike_ratio
        FROM analytics.fct_market_signals
        WHERE ticker = '{selected_ticker}'
        ORDER BY date
    """)
    
    if not ticker_data.empty:
        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        latest = ticker_data.iloc[-1]
        prev = ticker_data.iloc[-2] if len(ticker_data) > 1 else latest
        
        col1.metric("Close", f"${latest['close']:.2f}", 
                     f"{latest['daily_return']:.2f}%")
        col2.metric("Volume", f"{latest['volume']:,.0f}")
        col3.metric("Signal", latest['market_signal'])
        col4.metric("Sentiment", f"{latest['sentiment_ratio']:.2f}" if latest['sentiment_ratio'] != 0 else "N/A")
        
        # Price chart with 7d moving average
        st.subheader("Price + 7-day moving average")
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=ticker_data["date"], y=ticker_data["close"],
                                  name="Close", line=dict(color="#2196F3", width=2)))
        fig.add_trace(go.Scatter(x=ticker_data["date"], y=ticker_data["rolling_7d_avg_close"],
                                  name="7d Avg", line=dict(color="#FF9800", width=1, dash="dash")))
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Volume + sentiment
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("Volume")
            fig = px.bar(ticker_data, x="date", y="volume")
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        with col_right:
            st.subheader("Daily returns")
            fig = px.bar(ticker_data, x="date", y="daily_return",
                        color=ticker_data["daily_return"].apply(lambda x: "green" if x > 0 else "red"))
            fig.update_layout(showlegend=False, height=300)
            st.plotly_chart(fig, use_container_width=True)
        
        # Signal history table
        st.subheader("Signal history (days with sentiment)")
        sentiment_days = ticker_data[ticker_data["sentiment_ratio"] != 0]
        if not sentiment_days.empty:
            st.dataframe(
                sentiment_days[["date", "close", "daily_return", "sentiment_ratio", 
                               "news_article_count", "market_signal"]],
                use_container_width=True
            )


# ============================================================
# TAB 3: SENTIMENT ANALYSIS
# ============================================================
with tab_sentiment:
    
    st.subheader("Sentiment sensitivity by ticker")
    st.caption("How much does each stock react to positive vs negative news?")
    
    sensitivity_df = query("""
        SELECT ticker, avg_return_bullish_news, avg_return_bearish_news,
               sentiment_sensitivity, avg_abs_return, worst_day, best_day
        FROM gold.fct_sentiment_sensitivity
        ORDER BY sentiment_sensitivity DESC
    """)
    
    if not sensitivity_df.empty:
        # Bar chart: sensitivity score
        fig = px.bar(sensitivity_df, x="ticker", y="sentiment_sensitivity",
                     color="sentiment_sensitivity",
                     color_continuous_scale="RdYlGn",
                     color_continuous_midpoint=0)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Comparison: bullish vs bearish day returns
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("Avg return on bullish news days")
            fig = px.bar(sensitivity_df.head(20), x="ticker", y="avg_return_bullish_news",
                        color="avg_return_bullish_news", color_continuous_scale="Greens")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        with col_right:
            st.subheader("Avg return on bearish news days")
            fig = px.bar(sensitivity_df.head(20), x="ticker", y="avg_return_bearish_news",
                        color="avg_return_bearish_news", color_continuous_scale="Reds_r")
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)
        
        # Full table
        st.subheader("Full sensitivity data")
        st.dataframe(sensitivity_df, use_container_width=True)
    
    # Divergence events
    st.divider()
    st.subheader("Sentiment-price divergence events")
    st.caption("Days where news sentiment and price action disagreed")
    
    divergence_df = query("""
        SELECT date, ticker, daily_return, sentiment_ratio, 
               market_signal, sentiment_price_alignment
        FROM analytics.fct_market_signals
        WHERE sentiment_price_alignment != 'aligned'
        ORDER BY date, ticker
    """)
    
    if not divergence_df.empty:
        st.dataframe(divergence_df, use_container_width=True)
    else:
        st.info("No divergence events found in the data.")


# ============================================================
# TAB 4: AI ASSISTANT (OLLAMA)
# ============================================================
with tab_chat:
    
    st.subheader("🤖 Market Intelligence Assistant")
    st.caption("Ask questions about your market data. Powered by Ollama (local LLM).")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Build data context for LLM
    data_context = get_data_context()
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Suggested questions
    if not st.session_state.messages:
        st.markdown("**Try asking:**")
        suggestions = [
            "What were the top performing stocks on the latest day?",
            "Which tickers are most sensitive to news sentiment?",
            "Summarize the overall market trend this month",
            "What does the VIX level tell us about market fear?",
            "Were there any days where stocks dropped despite positive news?",
            "Give me a brief market intelligence report",
        ]
        for s in suggestions:
            if st.button(s, key=s):
                st.session_state.messages.append({"role": "user", "content": s})
                with st.chat_message("user"):
                    st.markdown(s)
                with st.chat_message("assistant"):
                    with st.spinner("Analyzing..."):
                        response = chat_with_ollama(s, data_context, st.session_state.messages)
                    st.markdown(response)
                st.session_state.messages.append({"role": "assistant", "content": response})
                st.rerun()
    
    # Chat input
    if prompt := st.chat_input("Ask about your market data..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                response = chat_with_ollama(prompt, data_context, st.session_state.messages)
            st.markdown(response)
        
        st.session_state.messages.append({"role": "assistant", "content": response})