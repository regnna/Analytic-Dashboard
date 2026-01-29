from fastapi import FastAPI, Depends, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from contextlib import asynccontextmanager
import logging
import asyncio
import json
from datetime import datetime
from typing import List, Optional

from database import get_db, engine, Base, refresh_materialized_views
from models import User, Event, Order
from schemas import (
    DashboardMetrics, DateRangeFilter, EventCreate, OrderCreate, 
    CohortRetention, FunnelStep, RealTimeMetrics, QueryPerformance
)
from analytics_service import AnalyticsService
from redis_cache import cache

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting up Analytics Dashboard API")
    
    # Create tables (in production, use Alembic migrations instead)
    async with engine.begin() as conn:
        # await conn.run_sync(Base.metadata.create_all)
        pass
    
    # Start background task for refreshing materialized views
    asyncio.create_task(periodic_refresh_task())
    
    yield
    
    # Shutdown
    logger.info("Shutting down")
    await cache.close()
    await engine.dispose()

app = FastAPI(
    title="Real-Time Analytics Dashboard API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Background task: Refresh materialized views every 5 minutes
async def periodic_refresh_task():
    while True:
        try:
            await asyncio.sleep(300)  # 5 minutes
            await refresh_materialized_views()
            # Invalidate cache after refresh
            await cache.delete("dashboard_metrics:24")
            logger.info("Periodic refresh completed")
        except Exception as e:
            logger.error(f"Error in periodic refresh: {e}")

# WebSocket connection manager for real-time updates
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
    
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# =============================================================================
# API ENDPOINTS
# =============================================================================

@app.get("/")
async def root():
    return {
        "message": "Real-Time Analytics Dashboard API",
        "docs": "/docs",
        "version": "1.0.0"
    }

@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    """Health check endpoint with DB connectivity test"""
    try:
        result = await db.execute(text("SELECT 1"))
        await cache.client.ping()
        return {"status": "healthy", "database": "connected", "cache": "connected"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# -----------------------------------------------------------------------------
# Ingestion Endpoints
# -----------------------------------------------------------------------------

@app.post("/events", status_code=201)
async def create_event(
    event: EventCreate,
    db: AsyncSession = Depends(get_db)
):
    """Ingest a new event - auto-creates user if doesn't exist"""
    try:
        # Auto-create user if user_id provided but doesn't exist
        if event.user_id:
            await db.execute(
                text("""
                    INSERT INTO users (id, email, created_at) 
                    VALUES (:id, :email, NOW())
                    ON CONFLICT (id) DO NOTHING
                """),
                {"id": event.user_id, "email": f"user_{event.user_id}@example.com"}
            )
        
        # SQL uses 'metadata' (actual column name), bind param is 'meta_data'
        query = """
        INSERT INTO events (user_id, session_id, event_type, page_path, metadata, created_at)
        VALUES (:user_id, :session_id, :event_type, :page_path, :meta_data, NOW())
        RETURNING id, created_at
        """
        result = await db.execute(
            text(query),
            {
                "user_id": event.user_id,
                "session_id": event.session_id,
                "event_type": event.event_type,
                "page_path": event.page_path,
                "meta_data": json.dumps(event.metadata)  # Bind param name
            }
        )
        await db.commit()
        row = result.mappings().first()
        
        return {"id": str(row["id"]), "created_at": row["created_at"]}
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders", status_code=201)
async def create_order(
    order: OrderCreate,
    db: AsyncSession = Depends(get_db)
):
    """Ingest a new order"""
    try:
        query = """
        INSERT INTO orders (user_id, order_number, status, amount, currency, items_count, metadata, created_at)
        VALUES (:user_id, :order_number, 'completed', :amount, :currency, :items_count, :meta_data, NOW())
        RETURNING id, created_at
        """
        result = await db.execute(
            text(query),
            {
                "user_id": order.user_id,
                "order_number": order.order_number,
                "amount": order.amount,
                "currency": order.currency,
                "items_count": order.items_count,
                "meta_data": json.dumps(order.metadata)  # Bind param name
            }
        )
        await db.commit()
        row = result.mappings().first()
        
        return {"id": str(row["id"]), "created_at": row["created_at"]}
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
# -----------------------------------------------------------------------------
# Analytics Endpoints
# -----------------------------------------------------------------------------

@app.get("/analytics/dashboard", response_model=List[DashboardMetrics])
async def get_dashboard_metrics(
    hours: int = Query(default=24, ge=1, le=168),
    db: AsyncSession = Depends(get_db)
):
    """Get dashboard metrics with caching"""
    service = AnalyticsService(db)
    return await service.get_dashboard_metrics(hours)

@app.get("/analytics/cohorts", response_model=List[CohortRetention])
async def get_cohort_analysis(
    weeks: int = Query(default=12, ge=1, le=52),
    source: Optional[str] = None,
    db: AsyncSession = Depends(get_db)
):
    """Get cohort retention analysis"""
    service = AnalyticsService(db)
    return await service.get_cohort_analysis(weeks, source)

@app.get("/analytics/funnel", response_model=List[FunnelStep])
async def get_funnel_analysis(
    days: int = Query(default=7, ge=1, le=30),
    db: AsyncSession = Depends(get_db)
):
    """Get conversion funnel analysis"""
    service = AnalyticsService(db)
    return await service.get_funnel_analysis(days)

@app.get("/analytics/revenue")
async def get_revenue_analysis(
    days: int = Query(default=30, ge=7, le=365),
    db: AsyncSession = Depends(get_db)
):
    """Get revenue with rolling averages"""
    service = AnalyticsService(db)
    return await service.get_rolling_revenue(days)

@app.get("/analytics/rfm")
async def get_rfm_segmentation(
    limit: int = Query(default=1000, ge=10, le=10000),
    db: AsyncSession = Depends(get_db)
):
    """Get RFM customer segmentation"""
    service = AnalyticsService(db)
    return await service.get_rfm_analysis(limit)

@app.get("/analytics/realtime", response_model=RealTimeMetrics)
async def get_realtime_metrics():
    """Get real-time metrics from Redis cache"""
    try:
        # Get from Redis counters
        orders_hour = await cache.get("orders:last_hour") or 0
        revenue_hour = await cache.get("revenue:last_hour") or 0
        active_users = await cache.get("active_users:now") or 0
        
        # Calculate events per second from recent data
        events_ps = await cache.get("events:per_second") or 0.0
        
        return RealTimeMetrics(
            active_users_now=active_users,
            orders_last_hour=orders_hour,
            revenue_last_hour=revenue_hour,
            events_per_second=events_ps
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/analytics/custom-query")
async def execute_custom_query(
    query_type: str,
    params: dict = {},
    db: AsyncSession = Depends(get_db)
):
    """Execute predefined safe analytical queries"""
    try:
        service = AnalyticsService(db)
        return await service.execute_custom_query(query_type, params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/query/performance/{query_name}")
async def get_query_performance(query_name: str, db: AsyncSession = Depends(get_db)):
    """Get EXPLAIN ANALYZE output for query optimization debugging"""
    allowed_queries = {
        "hourly_metrics": "SELECT * FROM mv_hourly_metrics LIMIT 100",
        "funnel": "SELECT * FROM mv_funnel_daily LIMIT 10",
        "cohort": "SELECT * FROM mv_cohort_retention LIMIT 50"
    }
    
    if query_name not in allowed_queries:
        raise HTTPException(status_code=404, detail="Query not found")
    
    try:
        # Get execution plan
        explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {allowed_queries[query_name]}"
        result = await db.execute(text(explain_query))
        plan = result.scalar()
        
        return {
            "query": query_name,
            "execution_plan": plan,
            "optimization_tips": "Check for Seq Scan operations and consider indexes"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# WebSocket for Real-time Updates
# -----------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time dashboard updates"""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and send periodic updates
            data = await websocket.receive_text()
            # Echo back or process commands
            await websocket.send_json({
                "type": "ping", 
                "timestamp": datetime.utcnow().isoformat()
            })
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# -----------------------------------------------------------------------------
# Utility Endpoints
# -----------------------------------------------------------------------------

@app.post("/admin/refresh-views")
async def manual_refresh_views():
    """Manually trigger materialized view refresh"""
    try:
        await refresh_materialized_views()
        return {"message": "Materialized views refreshed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)