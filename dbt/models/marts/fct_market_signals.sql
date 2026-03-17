/*
  MART: fct_market_signals
  -------------------------
  Adds business logic on top of staging:
  - Market signal classification (bullish/bearish/neutral)
  - Unusual activity flags
  - Risk indicators
  - Sentiment-price divergence detection

  Each CASE statement is a business decision you can explain in interviews.
*/

with daily as (
    select * from {{ ref('stg_daily_market') }}
),

with_signals as (
    select
        date,
        ticker,
        high,
        low,
        open,
        close,
        daily_return,
        volume,
        sentiment_ratio,
        has_sentiment_data,
        news_article_count,
        positive_news_count,
        negative_news_count,
        federal_funds_rate,
        vix_volatility_index,
        rolling_7d_avg_close,
        volume_spike_ratio,
        price_vs_7d_avg_pct,
        overnight_gap_pct,

        -- === MARKET SIGNAL ===
        -- Combines price action + sentiment into a single classification
        -- sentiment_ratio: -1 (all negative) to +1 (all positive), 0 = neutral/no data
        case
            when has_sentiment_data = true
                 and daily_return > 1.0
                 and sentiment_ratio > 0
                then 'strong_bullish'
            when has_sentiment_data = true
                 and daily_return < -1.0
                 and sentiment_ratio < 0
                then 'strong_bearish'
            when daily_return > 1.5
                then 'bullish'
            when daily_return < -1.5
                then 'bearish'
            when daily_return > 0.5
                then 'slightly_bullish'
            when daily_return < -0.5
                then 'slightly_bearish'
            else 'neutral'
        end as market_signal,

        -- === UNUSUAL VOLUME FLAG ===
        case
            when volume_spike_ratio > 2.0 then 'high_volume_spike'
            when volume_spike_ratio > 1.5 then 'elevated_volume'
            when volume_spike_ratio < 0.5 then 'low_volume'
            else 'normal_volume'
        end as volume_signal,

        -- === TREND POSITION ===
        case
            when price_vs_7d_avg_pct > 3.0 then 'well_above_trend'
            when price_vs_7d_avg_pct > 1.0 then 'above_trend'
            when price_vs_7d_avg_pct < -3.0 then 'well_below_trend'
            when price_vs_7d_avg_pct < -1.0 then 'below_trend'
            else 'at_trend'
        end as trend_position,

        -- === VIX RISK LEVEL ===
        case
            when vix_volatility_index > 30 then 'high_fear'
            when vix_volatility_index > 20 then 'elevated_anxiety'
            when vix_volatility_index > 15 then 'normal'
            when vix_volatility_index is not null then 'low_fear'
            else 'unknown'
        end as vix_risk_level,

        -- === SENTIMENT-PRICE DIVERGENCE ===
        -- When news sentiment and price action disagree
        case
            when has_sentiment_data = true
                 and sentiment_ratio > 0
                 and daily_return < -1.0
                then 'negative_divergence'  -- bullish news but price drops
            when has_sentiment_data = true
                 and sentiment_ratio < 0
                 and daily_return > 1.0
                then 'positive_divergence'  -- bearish news but price rises
            else 'aligned'
        end as sentiment_price_alignment,

        -- === NEWS INTENSITY ===
        case
            when news_article_count >= 20 then 'high_coverage'
            when news_article_count >= 5 then 'moderate_coverage'
            when news_article_count >= 1 then 'low_coverage'
            else 'no_coverage'
        end as news_intensity,

        -- === OVERNIGHT GAP SIGNAL ===
        case
            when overnight_gap_pct > 2.0 then 'gap_up'
            when overnight_gap_pct < -2.0 then 'gap_down'
            else 'no_gap'
        end as gap_signal

    from daily
)

select * from with_signals