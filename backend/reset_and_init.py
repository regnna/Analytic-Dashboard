import asyncio
import asyncpg
import random
import json
from datetime import datetime, timedelta
from uuid import uuid4

DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/analytics"

async def reset_database():
    """Completely reset and recreate database"""
    print("Connecting to PostgreSQL...")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("Make sure Docker is running: docker-compose up -d postgres redis")
        return
    
    print("⚠️  Resetting database (dropping all tables)...")
    
    # Drop everything in correct order (views first, then tables)
    drops = [
        "DROP MATERIALIZED VIEW IF EXISTS mv_funnel_daily CASCADE",
        "DROP MATERIALIZED VIEW IF EXISTS mv_cohort_retention CASCADE", 
        "DROP MATERIALIZED VIEW IF EXISTS mv_hourly_metrics CASCADE",
        "DROP FUNCTION IF EXISTS refresh_dashboard_views() CASCADE",
        "DROP TABLE IF EXISTS events CASCADE",
        "DROP TABLE IF EXISTS orders CASCADE",
        "DROP TABLE IF EXISTS users CASCADE"
    ]
    
    for drop in drops:
        try:
            await conn.execute(drop)
            print(f"  ✓ {drop.split('IF EXISTS')[1].split('CASCADE')[0].strip()}")
        except Exception as e:
            print(f"  ⚠️  {e}")
    
    print("\n🏗️  Creating tables...")
    
    # Create tables one by one with explicit error handling
    
    # Users table
    try:
        await conn.execute("""
            CREATE TABLE users (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                email VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                first_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                acquisition_source VARCHAR(100),
                country_code CHAR(2),
                device_type VARCHAR(50)
            )
        """)
        await conn.execute("CREATE INDEX idx_users_created_at ON users(created_at DESC)")
        await conn.execute("CREATE INDEX idx_users_acquisition ON users(acquisition_source, created_at DESC)")
        print("  ✓ users table")
    except Exception as e:
        print(f"  ❌ users table failed: {e}")
        return
    
    # Events table
    try:
        await conn.execute("""
            CREATE TABLE events (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                user_id UUID REFERENCES users(id),
                session_id UUID NOT NULL,
                event_type VARCHAR(50) NOT NULL,
                page_path TEXT,
                metadata JSONB DEFAULT '{}',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await conn.execute("CREATE INDEX idx_events_created_at ON events(created_at DESC)")
        await conn.execute("CREATE INDEX idx_events_user_time ON events(user_id, created_at DESC)")
        await conn.execute("CREATE INDEX idx_events_session ON events(session_id, created_at)")
        await conn.execute("CREATE INDEX idx_events_type_time ON events(event_type, created_at DESC)")
        await conn.execute("CREATE INDEX idx_events_metadata ON events USING GIN (metadata jsonb_path_ops)")
        print("  ✓ events table + indexes")
    except Exception as e:
        print(f"  ❌ events table failed: {e}")
        return
    
    # Orders table
    try:
        await conn.execute("""
            CREATE TABLE orders (
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
            )
        """)
        await conn.execute("CREATE INDEX idx_orders_user_time ON orders(user_id, created_at DESC)")
        await conn.execute("CREATE INDEX idx_orders_status_time ON orders(status, created_at DESC) WHERE status = 'completed'")
        await conn.execute("CREATE INDEX idx_orders_created_at ON orders(created_at DESC)")
        print("  ✓ orders table + indexes")
    except Exception as e:
        print(f"  ❌ orders table failed: {e}")
        return
    
    # Materialized views
    print("\n📊 Creating materialized views...")
    
    try:
        # Hourly metrics
        # await conn.execute("""
        #     CREATE MATERIALIZED VIEW mv_hourly_metrics AS
        #     WITH hourly_events AS (
        #         SELECT 
        #             date_trunc('hour', created_at) as hour,
        #             event_type,
        #             COUNT(*)::int as event_count,
        #             COUNT(DISTINCT user_id)::int as unique_users,
        #             COUNT(DISTINCT session_id)::int as unique_sessions
        #         FROM events
        #         WHERE created_at >= NOW() - INTERVAL '7 days'
        #         GROUP BY 1, 2
        #     ),
        #     hourly_revenue AS (
        #         SELECT 
        #             date_trunc('hour', created_at) as hour,
        #             COALESCE(SUM(amount), 0)::numeric as revenue,
        #             COUNT(*)::int as order_count,
        #             COALESCE(AVG(amount), 0)::numeric as avg_order_value
        #         FROM orders
        #         WHERE status = 'completed' 
        #         AND created_at >= NOW() - INTERVAL '7 days'
        #         GROUP BY 1
        #     )
        #     SELECT 
        #         h.hour,
        #         h.event_type,
        #         h.event_count,
        #         h.unique_users,
        #         h.unique_sessions,
        #         COALESCE(r.revenue, 0)::numeric as revenue,
        #         COALESCE(r.order_count, 0)::int as order_count,
        #         COALESCE(r.avg_order_value, 0)::numeric as avg_order_value
        #     FROM hourly_events h
        #     LEFT JOIN hourly_revenue r ON h.hour = r.hour
        #     ORDER BY h.hour DESC, h.event_type
        # """)
                # Hourly metrics WITH window functions
        await conn.execute("""
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
                    COALESCE(AVG(amount), 0)::numeric as avg_order_value
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
                COALESCE(r.avg_order_value, 0)::numeric as avg_order_value,
                -- ADD THESE TWO COLUMNS:
                AVG(h.event_count) OVER (
                    PARTITION BY h.event_type 
                    ORDER BY h.hour 
                    ROWS BETWEEN 23 PRECEDING AND CURRENT ROW
                )::float as rolling_24h_avg,
                LAG(h.event_count, 24) OVER (
                    PARTITION BY h.event_type 
                    ORDER BY h.hour
                )::int as prev_day_same_hour
            FROM hourly_events h
            LEFT JOIN hourly_revenue r ON h.hour = r.hour
            ORDER BY h.hour DESC, h.event_type
        """)
        await conn.execute("CREATE UNIQUE INDEX idx_mv_hourly_unique ON mv_hourly_metrics(hour, event_type)")
        print("  ✓ mv_hourly_metrics")
       
        
        # Cohort retention (simplified)
        await conn.execute("""
            CREATE MATERIALIZED VIEW mv_cohort_retention AS
            SELECT 
                date_trunc('day', created_at) as cohort_date,
                acquisition_source,
                0 as day_diff,
                COUNT(*)::int as active_users,
                100.0 as retention_pct
            FROM users
            WHERE created_at >= NOW() - INTERVAL '90 days'
            GROUP BY 1, 2
            ORDER BY 1 DESC
        """)
        await conn.execute("CREATE UNIQUE INDEX idx_mv_cohort_unique ON mv_cohort_retention(cohort_date, acquisition_source, day_diff)")
        print("  ✓ mv_cohort_retention")
        
        # Funnel daily (simplified)
        await conn.execute("""
            CREATE MATERIALIZED VIEW mv_funnel_daily AS
            SELECT 
                date_trunc('day', created_at) as day,
                COUNT(DISTINCT session_id)::int as total_sessions,
                COUNT(*) FILTER (WHERE event_type = 'page_view')::int as step_1_count,
                COUNT(*) FILTER (WHERE event_type = 'add_to_cart')::int as step_2_count,
                COUNT(*) FILTER (WHERE event_type = 'checkout_start')::int as step_3_count,
                COUNT(*) FILTER (WHERE event_type = 'purchase_complete')::int as step_4_count
            FROM events
            WHERE created_at >= NOW() - INTERVAL '30 days'
            GROUP BY 1
            ORDER BY 1 DESC
        """)
        await conn.execute("CREATE UNIQUE INDEX idx_mv_funnel_day ON mv_funnel_daily(day)")
        print("  ✓ mv_funnel_daily")
        
        # Refresh function
        await conn.execute("""
            CREATE OR REPLACE FUNCTION refresh_dashboard_views()
            RETURNS void AS $$
            BEGIN
                REFRESH MATERIALIZED VIEW mv_hourly_metrics;
                REFRESH MATERIALIZED VIEW mv_cohort_retention;
                REFRESH MATERIALIZED VIEW mv_funnel_daily;
            END;
            $$ LANGUAGE plpgsql
        """)
        print("  ✓ refresh function")
        
    except Exception as e:
        print(f"  ❌ Materialized views failed: {e}")
        return
    
    # Generate sample data
    print("\n🎲 Generating sample data...")
    try:
        await generate_sample_data(conn)
    except Exception as e:
        print(f"  ❌ Sample data failed: {e}")
        import traceback
        traceback.print_exc()
        return
    
    await conn.close()
    print("\n✅ Database initialization complete!")
    print("   Test the API at: http://localhost:8000/docs")

async def generate_sample_data(conn):
    """Generate realistic sample data"""
    
    # Generate 50 users
    users = []
    sources = ["organic", "paid_search", "social", "referral", "email"]
    devices = ["desktop", "mobile", "tablet"]
    countries = ["US", "CA", "GB", "DE", "FR"]
    
    for i in range(50):
        user_id = await conn.fetchval("""
            INSERT INTO users (email, acquisition_source, country_code, device_type, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (email) DO NOTHING
            RETURNING id
        """, 
        f"user_{i}_{uuid4().hex[:4]}@example.com", 
        random.choice(sources),
        random.choice(countries),
        random.choice(devices),
        datetime.now() - timedelta(days=random.randint(1, 30))
        )
        if user_id:
            users.append(user_id)
    
    if not users:
        # If all existed, fetch existing
        users = await conn.fetch("SELECT id FROM users LIMIT 50")
        users = [u['id'] for u in users]
    
    print(f"  ✓ Created/found {len(users)} users")
    
    # Generate events
    total_events = 0
    total_orders = 0
    event_types = ["page_view", "click", "add_to_cart", "checkout_start", "purchase_complete"]
    pages = ["/", "/products", "/cart", "/checkout", "/about"]
    
    for user_id in users:
        for _ in range(random.randint(2, 4)):  # 2-4 sessions per user
            session_id = uuid4()
            session_start = datetime.now() - timedelta(days=random.randint(0, 7), hours=random.randint(0, 23))
            
            purchased = False
            session_value = 0
            
            # Create 5-15 events per session
            for i in range(random.randint(5, 15)):
                if i == 0:
                    event_type = "page_view"
                elif i == 1:
                    event_type = random.choice(["click", "page_view"])
                elif i > 10 and random.random() > 0.8 and not purchased:
                    event_type = "purchase_complete"
                    purchased = True
                    session_value = random.randint(20, 500)
                else:
                    event_type = random.choice(event_types[:-1])  # Exclude purchase for most
                
                page = random.choice(pages)
                event_time = session_start + timedelta(minutes=i*random.randint(1, 5))
                
                await conn.execute("""
                    INSERT INTO events (user_id, session_id, event_type, page_path, metadata, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """, user_id, session_id, event_type, page, 
                    json.dumps({"product_id": f"prod_{random.randint(1, 50)}"}), event_time)
                total_events += 1
            
            # Create order if purchased
            if purchased and session_value > 0:
                await conn.execute("""
                    INSERT INTO orders (user_id, order_number, status, amount, currency, items_count, metadata, created_at)
                    VALUES ($1, $2, 'completed', $3, 'USD', $4, $5, $6)
                """, 
                user_id, 
                f"ORD-{uuid4().hex[:8].upper()}", 
                session_value, 
                random.randint(1, 5),
                json.dumps({"products": [f"prod_{random.randint(1, 50)}" for _ in range(3)]}),
                event_time + timedelta(minutes=1)
                )
                total_orders += 1
    
    print(f"  ✓ Created {total_events} events")
    print(f"  ✓ Created {total_orders} orders")
    
    # Refresh materialized views
    
    await conn.execute("REFRESH MATERIALIZED VIEW mv_hourly_metrics")
    await conn.execute("REFRESH MATERIALIZED VIEW mv_cohort_retention")
    await conn.execute("REFRESH MATERIALIZED VIEW mv_funnel_daily")
    print("  ✓ Materialized views refreshed")

if __name__ == "__main__":
    print("🚀 Analytics Dashboard Database Setup\n")
    asyncio.run(reset_database())