/*
  MART: fct_weekly_summary
  -------------------------
  Aggregates daily data into weekly summaries per ticker.
  Useful for dashboard charts and trend analysis.
*/

with daily as (
    select * from {{ ref('fct_market_signals') }}
),

weekly as (
    select
        ticker,
        date_trunc('week', date) as week_start,

        -- Price summary
        min(low) as week_low,
        max(high) as week_high,
        (array_agg(open order by date))[1] as week_open,
        (array_agg(close order by date desc))[1] as week_close,

        -- Returns
        round(avg(daily_return), 4) as avg_daily_return,
        round(sum(daily_return), 4) as cumulative_return,
        count(*) as trading_days,

        -- Volume
        round(avg(volume), 0)::bigint as avg_volume,
        max(volume) as max_volume,

        -- Sentiment (only from days with real data)
        round(avg(case when has_sentiment_data then sentiment_ratio end), 4) as avg_sentiment_ratio,
        sum(case when has_sentiment_data then news_article_count else 0 end) as total_articles,
        sum(case when has_sentiment_data then 1 else 0 end) as days_with_sentiment,

        -- Signal counts
        sum(case when market_signal in ('bullish', 'strong_bullish') then 1 else 0 end) as bullish_days,
        sum(case when market_signal in ('bearish', 'strong_bearish') then 1 else 0 end) as bearish_days,
        sum(case when volume_signal = 'high_volume_spike' then 1 else 0 end) as volume_spike_days,
        sum(case when sentiment_price_alignment != 'aligned' then 1 else 0 end) as divergence_days,

        -- Risk
        round(avg(vix_volatility_index), 2) as avg_vix

    from daily
    group by ticker, date_trunc('week', date)
)

select
    *,
    round((week_close - week_open) / week_open * 100, 4) as weekly_return_pct,
    case
        when (week_close - week_open) / week_open * 100 > 2.0 then 'strong_up_week'
        when (week_close - week_open) / week_open * 100 > 0.5 then 'up_week'
        when (week_close - week_open) / week_open * 100 < -2.0 then 'strong_down_week'
        when (week_close - week_open) / week_open * 100 < -0.5 then 'down_week'
        else 'flat_week'
    end as week_classification
from weekly