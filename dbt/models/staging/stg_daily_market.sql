/*
  STAGING: stg_daily_market
  --------------------------
  Clean read from Gold layer with:
  - NULL sentiment defaulted to 0 (neutral)
  - Flag for days with real sentiment data
  - Consistent column naming
*/

with source as (
    select * from {{ source('gold', 'fct_daily_market_summary') }}
),

cleaned as (
    select
        date,
        ticker,

        -- Price data (always present)
        open,
        high,
        low,
        close,
        volume,
        daily_return,
        price_range,

        -- Sentiment (default to 0 = neutral when no news data)
        coalesce(news_article_count, 0) as news_article_count,
        coalesce(sentiment_ratio, 0.0) as sentiment_ratio,
        coalesce(positive_news_count, 0) as positive_news_count,
        coalesce(negative_news_count, 0) as negative_news_count,
        coalesce(neutral_news_count, 0) as neutral_news_count,
        coalesce(avg_positive_strength, 0.0) as avg_positive_strength,
        coalesce(avg_negative_strength, 0.0) as avg_negative_strength,

        -- Flag: do we have real sentiment data for this day?
        case
            when sentiment_ratio is not null then true
            else false
        end as has_sentiment_data,

        -- Macro indicators
        federal_funds_rate,
        vix_volatility_index,
        treasury_yield_spread,
        consumer_price_index,
        unemployment_rate,

        -- Derived features
        rolling_7d_avg_close,
        rolling_7d_avg_volume,
        overnight_gap_pct,
        price_vs_7d_avg_pct,
        volume_spike_ratio,

        -- Metadata
        source,
        ingested_at

    from source
)

select * from cleaned