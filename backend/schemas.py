from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal
from uuid import UUID

# Request schemas
class DateRangeFilter(BaseModel):
    start_date: datetime
    end_date: datetime
    acquisition_source: Optional[str] = None

class EventCreate(BaseModel):
    user_id: Optional[UUID] = None
    session_id: UUID
    event_type: str
    page_path: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class OrderCreate(BaseModel):
    user_id: Optional[UUID] = None
    order_number: str
    amount: Decimal = Field(..., gt=0)
    currency: str = "USD"
    items_count: int = 0
    metadata: Dict[str, Any] = Field(default_factory=dict)

# Response schemas
class DashboardMetrics(BaseModel):
    hour: datetime
    event_type: str
    event_count: int
    unique_users: int
    revenue: Decimal
    order_count: int
    avg_order_value: float
    rolling_24h_avg: Optional[float]
    prev_day_same_hour: Optional[int]

class CohortRetention(BaseModel):
    cohort_date: datetime
    acquisition_source: Optional[str]
    weeks_since_signup: int
    active_users: int
    retention_pct: float

class FunnelStep(BaseModel):
    step_number: int
    step_name: str
    total_entries: int
    progressed: int
    avg_time_minutes: Optional[float]
    drop_off_pct: float
    step_conversion_pct: Optional[float]

class RealTimeMetrics(BaseModel):
    active_users_now: int
    orders_last_hour: int
    revenue_last_hour: Decimal
    events_per_second: float

class QueryPerformance(BaseModel):
    query_name: str
    execution_time_ms: float
    rows_returned: int
    cached: bool = False