-- Simplified Schema for Analytics Dashboard
-- Removes complex table partitioning that requires composite keys

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
-- Note: TimescaleDB extension requires special installation, skipping for now

-- ==========================================
-- CORE TABLES (No partitioning for simplicity)
-- ==========================================

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    acquisition_source VARCHAR(100),
    country_code CHAR(2),
    device_type VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_users_acquisition ON users(acquisition_source, created_at DESC);

CREATE TABLE IF NOT EXISTS events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    session_id UUID NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    page_path TEXT,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for events
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_user_time ON events(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_metadata ON events USING GIN (metadata jsonb_path_ops);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    order_number VARCHAR(50) UNIQUE NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    amount DECIMAL(10,2) NOT NULL,
    currency VARCHAR(3) DEFAULT 'USD',
    items_count INTEGER DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);

CREATE INDEX IF NOT EXISTS idx_orders_user_time ON orders(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_status_time ON orders(status, created_at DESC) WHERE status = 'completed';
CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at DESC);

-- ==========================================
-- MATERIALIZED VIEWS (Simplified without TimescaleDB functions)
-- ==========================================

DROP MATERIALIZED VIEW IF EXISTS mv_hourly_metrics CASCADE;
CREATE MATERIALIZED VIEW mv_hourly_metrics AS
WITH hourly_events AS (
    SELECT 
        date_trunc('hour', created_at) as hour,
        event_type,
        COUNT(*)::int as event_count,
        COUNT(DISTINCT user_id)::int as unique_users,
        COUNT(DISTINCT session_id)::int as unique_sessions
    FROM events
    WHERE created_at >= NOW() - INTERVAL '7 days'
    GROUP BY 1, 2
),
hourly_revenue AS (
    SELECT 
        date_trunc('hour', created_at) as hour,
        COALESCE(SUM(amount), 0)::numeric as revenue,
        COUNT(*)::int as order_count,
        COALESCE(AVG(amount), 0)::float as avg_order_value
    FROM orders
    WHERE status = 'completed' 
    AND created_at >= NOW() - INTERVAL '7 days'
    GROUP BY 1
)
SELECT 
    h.hour,
    h.event_type,
    h.event_count,
    h.unique_users,
    h.unique_sessions,
    COALESCE(r.revenue, 0)::numeric as revenue,
    COALESCE(r.order_count, 0)::int as order_count,
    COALESCE(r.avg_order_value, 0)::float as avg_order_value,
    AVG(h.event_count) OVER (
        PARTITION BY h.event_type 
        ORDER BY h.hour 
        ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
    )::float as rolling_24h_avg,
    LAG(h.event_count, 24) OVER (PARTITION BY h.event_type ORDER BY h.hour)::int as prev_day_same_hour
FROM hourly_events h
LEFT JOIN hourly_revenue r ON h.hour = r.hour
ORDER BY h.hour DESC, h.event_type;

CREATE UNIQUE INDEX idx_mv_hourly_metrics_unique ON mv_hourly_metrics(hour, event_type);

DROP MATERIALIZED VIEW IF EXISTS mv_cohort_retention CASCADE;
CREATE MATERIALIZED VIEW mv_cohort_retention AS
WITH user_cohorts AS (
    SELECT 
        user_id,
        date_trunc('day', first_seen_at) as cohort_date,
        acquisition_source
    FROM users
    WHERE first_seen_at >= NOW() - INTERVAL '90 days'
),
activity AS (
    SELECT 
        u.user_id,
        u.cohort_date,
        u.acquisition_source,
        date_trunc('day', e.created_at) as activity_date,
        (date_trunc('day', e.created_at) - u.cohort_date) as day_diff
    FROM user_cohorts u
    JOIN events e ON u.user_id = e.user_id
    GROUP BY 1, 2, 3, 4
)
SELECT 
    cohort_date,
    acquisition_source,
    EXTRACT(DAY FROM day_diff)::int as day_diff,
    COUNT(DISTINCT user_id)::int as active_users,
    ROUND(100.0 * COUNT(DISTINCT user_id) / 
        NULLIF(FIRST_VALUE(COUNT(DISTINCT user_id)) OVER (
            PARTITION BY cohort_date, acquisition_source 
            ORDER BY day_diff 
            ROWS UNBOUNDED PRECEDING
        ), 0), 2) as retention_pct
FROM activity
WHERE day_diff <= INTERVAL '30 days'
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

CREATE UNIQUE INDEX idx_mv_cohort_unique ON mv_cohort_retention(cohort_date, acquisition_source, day_diff);

DROP MATERIALIZED VIEW IF EXISTS mv_funnel_daily CASCADE;
CREATE MATERIALIZED VIEW mv_funnel_daily AS
WITH session_events AS (
    SELECT 
        session_id,
        user_id,
        date_trunc('day', created_at) as day,
        MIN(CASE WHEN event_type = 'page_view' THEN created_at END) as step_1,
        MIN(CASE WHEN event_type = 'add_to_cart' THEN created_at END) as step_2,
        MIN(CASE WHEN event_type = 'checkout_start' THEN created_at END) as step_3,
        MIN(CASE WHEN event_type = 'purchase_complete' THEN created_at END) as step_4
    FROM events
    WHERE created_at >= NOW() - INTERVAL '30 days'
    AND event_type IN ('page_view', 'add_to_cart', 'checkout_start', 'purchase_complete')
    GROUP BY 1, 2, 3
)
SELECT 
    day,
    COUNT(*)::int as total_sessions,
    COUNT(step_1)::int as step_1_count,
    COUNT(step_2)::int as step_2_count,
    COUNT(step_3)::int as step_3_count,
    COUNT(step_4)::int as step_4_count,
    ROUND(100.0 * COUNT(step_2) / NULLIF(COUNT(step_1), 0), 2) as step_1_2_rate,
    ROUND(100.0 * COUNT(step_3) / NULLIF(COUNT(step_2), 0), 2) as step_2_3_rate,
    ROUND(100.0 * COUNT(step_4) / NULLIF(COUNT(step_3), 0), 2) as step_3_4_rate,
    AVG(EXTRACT(EPOCH FROM (step_2 - step_1))/60)::float as avg_time_1_2,
    AVG(EXTRACT(EPOCH FROM (step_3 - step_2))/60)::float as avg_time_2_3
FROM session_events
GROUP BY 1
ORDER BY 1 DESC;

CREATE UNIQUE INDEX idx_mv_funnel_day ON mv_funnel_daily(day);

-- Refresh function for materialized views
CREATE OR REPLACE FUNCTION refresh_dashboard_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_hourly_metrics;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_cohort_retention;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_funnel_daily;
END;
$$ LANGUAGE plpgsql;