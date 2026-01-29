from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Any, Optional
from database import execute_with_timeout
from redis_cache import cache
import logging
import time

logger = logging.getLogger(__name__)

class AnalyticsService:
    def __init__(self, db: AsyncSession):
        self.db = db
    
    async def get_dashboard_metrics(
        self, 
        hours: int = 24, 
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """Get hourly metrics for dashboard with caching"""
        cache_key = f"dashboard_metrics:{hours}"
        
        if use_cache:
            cached = await cache.get(cache_key)
            if cached:
                return cached
        
        # Query materialized view for performance
        query = """
        SELECT 
            hour, event_type, event_count, unique_users, 
            revenue, order_count, avg_order_value,
            rolling_24h_avg, prev_day_same_hour
        FROM mv_hourly_metrics 
        WHERE hour >= NOW() - (INTERVAL '1 hour' * :hours)
/*        WHERE hour >= NOW() - INTERVAL ':hours hours' */
        ORDER BY hour DESC, event_type
        """
        
        start_time = time.time()
        result = await execute_with_timeout(self.db, query, {"hours": hours})
        execution_time = (time.time() - start_time) * 1000
        
        logger.info(f"Dashboard metrics query took {execution_time:.2f}ms")
        
        # Convert to dict and cache
        data = [dict(row) for row in result]
        await cache.set(cache_key, data, ttl=300)  # 5 min cache
        
        return data
    
    async def get_cohort_analysis(
        self, 
        weeks: int = 12,
        source: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get cohort retention analysis"""
        cache_key = f"cohort_analysis:{weeks}:{source or 'all'}"
        
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        if source:
            # Query from materialized view with filter
            query = """
            SELECT * FROM mv_cohort_retention 
            WHERE cohort_date >= NOW() - (INTERVAL '1 week' * :weeks)
            /*WHERE cohort_date >= NOW() - INTERVAL ':weeks weeks' */
            AND acquisition_source = :source
            ORDER BY cohort_date DESC, day_diff
            """
            params = {"weeks": weeks, "source": source}
        else:
            query = """
            SELECT * FROM mv_cohort_retention 
            WHERE cohort_date >= NOW() - (INTERVAL '1 week' * :weeks)
/*            WHERE cohort_date >= NOW() - INTERVAL ':weeks weeks'*/
            ORDER BY cohort_date DESC, acquisition_source, day_diff
            """
            params = {"weeks": weeks}
        
        result = await execute_with_timeout(self.db, query, params, timeout=10)
        data = [dict(row) for row in result]
        await cache.set(cache_key, data, ttl=600)  # 10 min cache
        
        return data
    
    async def get_funnel_analysis(self, days: int = 7) -> List[Dict[str, Any]]:
        """Get funnel analysis with step-by-step conversion"""
        cache_key = f"funnel_analysis:{days}"
        
        cached = await cache.get(cache_key)
        if cached:
            return cached
        
        # Use the complex funnel CTE query from queries.sql
        query = """
        WITH user_funnel AS (
            SELECT 
                user_id,
                session_id,
                created_at as event_time,
                event_type,
                CASE event_type
                    WHEN 'page_view' THEN 1
                    WHEN 'add_to_cart' THEN 2
                    WHEN 'checkout_start' THEN 3
                    WHEN 'purchase_complete' THEN 4
                END as step_number,
                LEAD(event_type) OVER (PARTITION BY session_id ORDER BY created_at) as next_step,
                LEAD(created_at) OVER (PARTITION BY session_id ORDER BY created_at) as next_step_time
            FROM events
            WHERE created_at >= NOW() - (INTERVAL '1 day' * :days)
/*            WHERE created_at >= NOW() - INTERVAL ':days days'*/
            AND event_type IN ('page_view', 'add_to_cart', 'checkout_start', 'purchase_complete')
        ),
        funnel_stats AS (
            SELECT 
                step_number,
                COUNT(*) as total_entries,
                COUNT(next_step) as progressed,
                AVG(EXTRACT(EPOCH FROM (next_step_time - event_time))/60) as avg_time_minutes,
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
            ROUND(100.0 * progressed / NULLIF(LAG(total_entries) OVER (ORDER BY step_number), 0), 2) as step_conversion_pct
        FROM funnel_stats
        ORDER BY step_number
        """
        
        result = await execute_with_timeout(self.db, query, {"days": days})
        data = [dict(row) for row in result]
        await cache.set(cache_key, data, ttl=300)
        
        return data
    
    async def get_rolling_revenue(self, days: int = 30) -> List[Dict[str, Any]]:
        """Get daily revenue with 7-day rolling average and growth metrics"""
        query = """
        WITH daily_revenue AS (
            SELECT 
                DATE_TRUNC('day', created_at) as date,
                SUM(amount) as revenue,
                COUNT(*) as orders,
                COUNT(DISTINCT user_id) as unique_customers
            FROM orders
            WHERE status = 'completed'
            AND created_at >= CURRENT_DATE - (INTERVAL '1 day' *:days)
            GROUP BY 1
        )
        SELECT 
            date,
            revenue,
            orders,
            unique_customers,
            ROUND(AVG(revenue) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) as rolling_7d_avg,
            ROUND(100.0 * (revenue - LAG(revenue, 1) OVER (ORDER BY date)) 
                / NULLIF(LAG(revenue, 1) OVER (ORDER BY date), 0), 2) as daily_growth_pct,
            SUM(revenue) OVER (ORDER BY date) as cumulative_revenue
        FROM daily_revenue
        ORDER BY date DESC
        """
        
        result = await execute_with_timeout(self.db, query, {"days": days})
        return [dict(row) for row in result]
    
    async def get_rfm_analysis(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Get RFM segmentation analysis"""
        query = """
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
                NTILE(5) OVER (ORDER BY recency_days DESC) as r_score,
                NTILE(5) OVER (ORDER BY frequency ASC) as f_score,
                NTILE(5) OVER (ORDER BY monetary ASC) as m_score
            FROM customer_stats
        )
        SELECT 
            user_id,
            recency_days,
            frequency,
            ROUND(monetary, 2) as monetary_value,
            r_score,
            f_score,
            m_score,
            (r_score + f_score + m_score) as rfm_total,
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
        LIMIT :limit
        """
        
        result = await execute_with_timeout(self.db, query, {"limit": limit}, timeout=15)
        return [dict(row) for row in result]
    
    async def execute_custom_query(
        self, 
        query_type: str, 
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Execute predefined complex queries safely (prevent SQL injection)
        Maps query_type to predefined safe queries
        """
        allowed_queries = {
            "anomaly_detection": {
                "sql": """
                    WITH hourly_stats AS (
                        SELECT time_bucket('1 hour', created_at) as hour,
                               COUNT(*) as event_count
                        FROM events
                        WHERE created_at >= NOW() - (INTERVAL '1 day' * :days)
                        /* WHERE created_at >= NOW() - INTERVAL '7 days' */
                        GROUP BY 1
                    )
                    SELECT hour, event_count,
                           AVG(event_count) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND 1 PRECEDING) as avg_24h,
                           STDDEV(event_count) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND 1 PRECEDING) as stddev,
                           (event_count - AVG(event_count) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND 1 PRECEDING)) 
                           / NULLIF(STDDEV(event_count) OVER (ORDER BY hour ROWS BETWEEN 23 PRECEDING AND 1 PRECEDING), 0) as z_score
                    FROM hourly_stats
                    ORDER BY hour DESC
                    LIMIT 48
                """,
                "timeout": 10
            },
            "top_products": {
                "sql": """
                    SELECT 
                    metadata->>'product_id'
                    as product_id,
                           COUNT(*) as times_purchased,
                           SUM(amount) as total_revenue
                    FROM orders
                    WHERE status = 'completed'
                    AND created_at >= NOW() - INTERVAL '30 days'
                    GROUP BY 1
                    ORDER BY total_revenue DESC
                    LIMIT 20
                """,
                "timeout": 10
            }
        }
        
        if query_type not in allowed_queries:
            raise ValueError(f"Unknown query type: {query_type}")
        
        query_config = allowed_queries[query_type]
        start = time.time()
        
        result = await execute_with_timeout(
            self.db, 
            query_config["sql"], 
            params,
            timeout=query_config["timeout"]
        )
        
        execution_time = (time.time() - start) * 1000
        
        return {
            "query_type": query_type,
            "execution_time_ms": round(execution_time, 2),
            "rows_count": len(result),
            "data": [dict(row) for row in result]
        }