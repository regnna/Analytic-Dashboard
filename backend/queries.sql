-- ==========================================
-- QUERY SET: 10+ COMPLEX ANALYTICAL QUERIES
-- All queries use window functions, CTEs, and advanced PostgreSQL features
-- ==========================================

-- 1. ROLLING 7-DAY REVENUE WITH TREND ANALYSIS
-- Uses window functions for moving averages and trend detection
WITH daily_revenue AS (
    SELECT 
        DATE_TRUNC('day', created_at) as date,
        SUM(amount) as revenue,
        COUNT(*) as orders,
        COUNT(DISTINCT user_id) as unique_customers
    FROM orders
    WHERE status = 'completed'
    AND created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY 1
)
SELECT 
    date,
    revenue,
    orders,
    unique_customers,
    -- Rolling 7-day average (window function)
    ROUND(AVG(revenue) OVER (
        ORDER BY date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) as rolling_7d_avg,
    -- Day-over-day growth using LAG
    ROUND(100.0 * (revenue - LAG(revenue, 1) OVER (ORDER BY date)) 
        / NULLIF(LAG(revenue, 1) OVER (ORDER BY date), 0), 2) as daily_growth_pct,
    -- Cumulative revenue
    SUM(revenue) OVER (ORDER BY date) as cumulative_revenue,
    -- Z-score for anomaly detection (standard deviations from mean)
    (revenue - AVG(revenue) OVER ()) / NULLIF(STDDEV(revenue) OVER (), 0) as z_score
FROM daily_revenue
ORDER BY date DESC;

-- 2. COHORT RETENTION ANALYSIS WITH REVENUE METRICS
-- Analyzes user behavior by acquisition cohort over time
WITH cohorts AS (
    SELECT 
        user_id,
        DATE_TRUNC('week', first_seen_at) as cohort_week,
        acquisition_source
    FROM users
    WHERE first_seen_at >= CURRENT_DATE - INTERVAL '12 weeks'
),
user_activity AS (
    SELECT 
        c.user_id,
        c.cohort_week,
        c.acquisition_source,
        DATE_TRUNC('week', o.created_at) as activity_week,
        (DATE_TRUNC('week', o.created_at) - c.cohort_week) / 7 as weeks_since_signup,
        SUM(o.amount) as revenue
    FROM cohorts c
    LEFT JOIN orders o ON c.user_id = o.user_id 
        AND o.status = 'completed'
        AND o.created_at >= c.cohort_week
    GROUP BY 1, 2, 3, 4
)
SELECT 
    cohort_week,
    acquisition_source,
    weeks_since_signup,
    COUNT(DISTINCT user_id) as active_users,
    SUM(revenue) as total_revenue,
    ROUND(AVG(revenue), 2) as arpu, -- Average Revenue Per User
    -- Retention calculation: users active this week / total users in cohort
    ROUND(100.0 * COUNT(DISTINCT user_id) / 
        MAX(COUNT(DISTINCT user_id)) OVER (
            PARTITION BY cohort_week, acquisition_source 
            ORDER BY weeks_since_signup 
            ROWS UNBOUNDED PRECEDING
        ), 2) as retention_pct,
    -- LTV (Lifetime Value) cumulative
    SUM(SUM(revenue)) OVER (
        PARTITION BY cohort_week, acquisition_source 
        ORDER BY weeks_since_signup
    ) as cumulative_ltv
FROM user_activity
WHERE weeks_since_signup <= 12
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- 3. FUNNEL ANALYSIS WITH DROP-OFF RECOVERY
-- Multi-touch attribution with time between steps
WITH user_funnel AS (
    SELECT 
        user_id,
        session_id,
        created_at as event_time,
        event_type,
        -- Assign step numbers using CASE
        CASE event_type
            WHEN 'page_view' THEN 1
            WHEN 'add_to_cart' THEN 2
            WHEN 'checkout_start' THEN 3
            WHEN 'purchase_complete' THEN 4
        END as step_number,
        -- Lead function to see next step (forward-looking window)
        LEAD(event_type) OVER (
            PARTITION BY session_id 
            ORDER BY created_at
        ) as next_step,
        LEAD(created_at) OVER (
            PARTITION BY session_id 
            ORDER BY created_at
        ) as next_step_time
    FROM events
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
    AND event_type IN ('page_view', 'add_to_cart', 'checkout_start', 'purchase_complete')
),
funnel_stats AS (
    SELECT 
        step_number,
        COUNT(*) as total_entries,
        COUNT(next_step) as progressed,
        AVG(EXTRACT(EPOCH FROM (next_step_time - event_time))/60) as avg_time_minutes,
        -- Calculate drop-off rate
        ROUND(100.0 * (COUNT(*) - COUNT(next_step)) / NULLIF(COUNT(*), 0), 2) as drop_off_pct
    FROM user_funnel
    GROUP BY step_number
)
SELECT 
    step_number,
    CASE step_number
        WHEN 1 THEN 'Page View'
        WHEN 2 THEN 'Add to Cart'
        WHEN 3 THEN 'Checkout Start'
        WHEN 4 THEN 'Purchase Complete'
    END as step_name,
    total_entries,
    progressed,
    avg_time_minutes,
    drop_off_pct,
    -- Conversion rate from previous step
    ROUND(100.0 * progressed / NULLIF(LAG(total_entries) OVER (ORDER BY step_number), 0), 2) as step_conversion_pct
FROM funnel_stats
ORDER BY step_number;

-- 4. RFM ANALYSIS (Recency, Frequency, Monetary)
-- Customer segmentation for marketing targeting
WITH customer_stats AS (
    SELECT 
        user_id,
        MAX(created_at) as last_order_date,
        COUNT(*) as frequency,
        SUM(amount) as monetary,
        CURRENT_DATE - MAX(created_at)::date as recency_days
    FROM orders
    WHERE status = 'completed'
    AND created_at >= CURRENT_DATE - INTERVAL '1 year'
    GROUP BY user_id
),
rfm_scores AS (
    SELECT 
        user_id,
        recency_days,
        frequency,
        monetary,
        -- NTILE creates quintiles (1-5), 5 being best
        NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
        NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
        NTILE(5) OVER (ORDER BY monetary ASC) as m_score
    FROM customer_stats
)
SELECT 
    user_id,
    recency_days,
    frequency,
    ROUND(monetary, 2) as monetary,
    r_score,
    f_score,
    m_score,
    -- Combined RFM score
    r_score + f_score + m_score as rfm_total,
    -- Segment classification
    CASE 
        WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'New Customers'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3 THEN 'Cannot Lose Them'
        ELSE 'Others'
    END as segment
FROM rfm_scores
ORDER BY rfm_total DESC
LIMIT 1000;

-- 5. SESSIONIZATION WITH ATTRIBUTION
-- Groups events into sessions with smart timeout logic (30 min inactivity)
WITH sessionized_events AS (
    SELECT 
        user_id,
        event_type,
        created_at,
        page_path,
        metadata->>'referrer' as referrer,
        metadata->>'utm_source' as utm_source,
        -- Detect new session (30 min gap or no previous event)
        CASE 
            WHEN created_at - LAG(created_at) OVER (
                PARTITION BY user_id ORDER BY created_at
            ) > INTERVAL '30 minutes' THEN 1
            WHEN LAG(created_at) OVER (
                PARTITION BY user_id ORDER BY created_at
            ) IS NULL THEN 1
            ELSE 0
        END as is_new_session
    FROM events
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
),
sessions AS (
    SELECT 
        user_id,
        event_type,
        created_at,
        page_path,
        referrer,
        utm_source,
        -- Running sum to create session IDs
        SUM(is_new_session) OVER (
            PARTITION BY user_id 
            ORDER BY created_at 
            ROWS UNBOUNDED PRECEDING
        ) as session_number
    FROM sessionized_events
),
session_metrics AS (
    SELECT 
        user_id,
        session_number,
        MIN(created_at) as session_start,
        MAX(created_at) as session_end,
        EXTRACT(EPOCH FROM (MAX(created_at) - MIN(created_at)))/60 as session_duration_min,
        COUNT(*) as events_count,
        COUNT(DISTINCT page_path) as unique_pages,
        BOOL_OR(event_type = 'purchase_complete') as converted,
        FIRST_VALUE(utm_source) OVER (
            PARTITION BY user_id, session_number 
            ORDER BY created_at
        ) as session_source
    FROM sessions
    GROUP BY user_id, session_number
)
SELECT 
    DATE_TRUNC('day', session_start) as day,
    session_source,
    COUNT(*) as total_sessions,
    AVG(session_duration_min) as avg_duration,
    SUM(CASE WHEN converted THEN 1 ELSE 0 END) as conversions,
    ROUND(100.0 * SUM(CASE WHEN converted THEN 1 ELSE 0 END) / COUNT(*), 2) as conversion_rate
FROM session_metrics
GROUP BY 1, 2
ORDER BY 1 DESC, conversions DESC;

-- 6. REAL-TIME ANOMALY DETECTION
-- Z-score calculation for detecting traffic spikes/drops
-- Uses window functions for moving statistics
WITH hourly_stats AS (
    SELECT 
        time_bucket('1 hour', created_at) as hour,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users
    FROM events
    WHERE created_at >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY 1
),
stats_with_avg AS (
    SELECT 
        hour,
        event_count,
        unique_users,
        AVG(event_count) OVER (
            ORDER BY hour 
            ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING -- 7 days rolling window (excluding current)
        ) as avg_7d,
        STDDEV(event_count) OVER (
            ORDER BY hour 
            ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
        ) as stddev_7d
    FROM hourly_stats
)
SELECT 
    hour,
    event_count,
    avg_7d,
    stddev_7d,
    (event_count - avg_7d) / NULLIF(stddev_7d, 0) as z_score,
    CASE 
        WHEN ABS((event_count - avg_7d) / NULLIF(stddev_7d, 0)) > 3 THEN 'ANOMALY'
        WHEN ABS((event_count - avg_7d) / NULLIF(stddev_7d, 0)) > 2 THEN 'WARNING'
        ELSE 'NORMAL'
    END as status
FROM stats_with_avg
WHERE hour >= CURRENT_DATE - INTERVAL '2 days'
ORDER BY hour DESC;

-- 7. PRODUCT AFFINITY ANALYSIS (Market Basket Analysis)
-- Finds products frequently bought together using array operations
WITH order_items AS (
    SELECT 
        o.id as order_id,
        o.user_id,
        jsonb_array_elements_text(o.metadata->'items') as item_id
    FROM orders o
    WHERE o.status = 'completed'
    AND o.created_at >= CURRENT_DATE - INTERVAL '30 days'
),
item_pairs AS (
    SELECT 
        a.item_id as item_a,
        b.item_id as item_b,
        COUNT(*) as co_occurrence
    FROM order_items a
    JOIN order_items b ON a.order_id = b.order_id AND a.item_id < b.item_id
    GROUP BY 1, 2
    HAVING COUNT(*) >= 5 -- Filter rare combinations
),
totals AS (
    SELECT item_id, COUNT(DISTINCT order_id) as total_orders
    FROM order_items
    GROUP BY 1
)
SELECT 
    p.item_a,
    p.item_b,
    p.co_occurrence,
    t1.total_orders as item_a_count,
    t2.total_orders as item_b_count,
    -- Lift calculation: P(A&B) / (P(A) * P(B))
    ROUND(100.0 * p.co_occurrence / (t1.total_orders + t2.total_orders - p.co_occurrence), 2) as jaccard_similarity,
    -- Confidence: P(B|A) = P(A&B) / P(A)
    ROUND(100.0 * p.co_occurrence / t1.total_orders, 2) as confidence_a_to_b
FROM item_pairs p
JOIN totals t1 ON p.item_a = t1.item_id
JOIN totals t2 ON p.item_b = t2.item_id
ORDER BY confidence_a_to_b DESC
LIMIT 50;

-- 8. PREDICTIVE CHURN SCORE
-- Identifies users likely to churn based on recency and behavior drop-off
WITH user_behavior AS (
    SELECT 
        user_id,
        MAX(created_at) as last_activity,
        COUNT(DISTINCT DATE_TRUNC('day', created_at)) as active_days_last_30,
        COUNT(*) as total_events_last_30,
        COUNT(DISTINCT session_id) as total_sessions_last_30
    FROM events
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY user_id
),
purchase_history AS (
    SELECT 
        user_id,
        MAX(created_at) as last_purchase,
        COUNT(*) as total_orders,
        SUM(amount) as total_spent
    FROM orders
    WHERE status = 'completed'
    GROUP BY user_id
)
SELECT 
    u.user_id,
    CURRENT_DATE - u.last_activity::date as days_since_last_activity,
    u.active_days_last_30,
    u.total_events_last_30,
    u.total_sessions_last_30,
    p.total_orders,
    p.total_spent,
    -- Churn risk score (0-100, higher = more likely to churn)
    CASE 
        WHEN CURRENT_DATE - u.last_activity::date > 30 THEN 100
        WHEN u.active_days_last_30 = 0 THEN 90
        WHEN p.total_orders IS NULL AND u.total_events_last_30 < 5 THEN 80
        ELSE GREATEST(0, 100 - (u.active_days_last_30 * 3) - (COALESCE(p.total_orders, 0) * 10))
    END as churn_risk_score,
    CASE 
        WHEN CURRENT_DATE - u.last_activity::date > 30 THEN 'Churned'
        WHEN CURRENT_DATE - u.last_activity::date > 14 THEN 'High Risk'
        WHEN u.active_days_last_30 < 3 THEN 'Medium Risk'
        ELSE 'Active'
    END as churn_segment
FROM user_behavior u
LEFT JOIN purchase_history p ON u.user_id = p.user_id
ORDER BY churn_risk_score DESC
LIMIT 1000;

-- 9. YEAR-OVER-YEAR GROWTH COMPARISON
-- Compares current period vs same period last year with proper alignment
WITH daily_current AS (
    SELECT 
        EXTRACT(DOY FROM created_at) as day_of_year,
        DATE_TRUNC('day', created_at) as date,
        SUM(amount) as revenue,
        COUNT(*) as orders
    FROM orders
    WHERE status = 'completed'
    AND created_at >= DATE_TRUNC('year', CURRENT_DATE)
    GROUP BY 1, 2
),
daily_previous AS (
    SELECT 
        EXTRACT(DOY FROM created_at) as day_of_year,
        SUM(amount) as revenue,
        COUNT(*) as orders
    FROM orders
    WHERE status = 'completed'
    AND created_at >= DATE_TRUNC('year', CURRENT_DATE - INTERVAL '1 year')
    AND created_at < DATE_TRUNC('year', CURRENT_DATE)
    GROUP BY 1
)
SELECT 
    c.date,
    c.revenue as current_revenue,
    p.revenue as previous_revenue,
    c.orders as current_orders,
    p.orders as previous_orders,
    -- YoY calculations
    ROUND(100.0 * (c.revenue - p.revenue) / NULLIF(p.revenue, 0), 2) as revenue_yoy_pct,
    ROUND(100.0 * (c.orders - p.orders) / NULLIF(p.orders, 0), 2) as orders_yoy_pct,
    -- Running totals using window functions
    SUM(c.revenue) OVER (ORDER BY c.date) as ytd_revenue_current,
    SUM(p.revenue) OVER (ORDER BY c.date) as ytd_revenue_previous
FROM daily_current c
LEFT JOIN daily_previous p ON c.day_of_year = p.day_of_year
ORDER BY c.date DESC;

-- 10. GEOGRAPHIC PERFORMANCE WITH RANKING
-- Window functions for ranking countries by performance metrics
WITH country_stats AS (
    SELECT 
        u.country_code,
        DATE_TRUNC('month', o.created_at) as month,
        SUM(o.amount) as revenue,
        COUNT(*) as orders,
        COUNT(DISTINCT o.user_id) as unique_customers
    FROM orders o
    JOIN users u ON o.user_id = u.id
    WHERE o.status = 'completed'
    AND u.country_code IS NOT NULL
    GROUP BY 1, 2
)
SELECT 
    month,
    country_code,
    revenue,
    orders,
    unique_customers,
    RANK() OVER (PARTITION BY month ORDER BY revenue DESC) as revenue_rank,
    RANK() OVER (PARTITION BY month ORDER BY orders DESC) as orders_rank,
    -- Market share within month
    ROUND(100.0 * revenue / SUM(revenue) OVER (PARTITION BY month), 2) as pct_of_monthly_revenue,
    -- Growth from previous month using LAG
    revenue - LAG(revenue) OVER (PARTITION BY country_code ORDER BY month) as mom_revenue_change,
    ROUND(100.0 * (revenue - LAG(revenue) OVER (PARTITION BY country_code ORDER BY month)) 
        / NULLIF(LAG(revenue) OVER (PARTITION BY country_code ORDER BY month), 0), 2) as mom_growth_pct
FROM country_stats
WHERE month >= CURRENT_DATE - INTERVAL '6 months'
ORDER BY month DESC, revenue_rank;