from sqlalchemy import Column, String, DateTime, Numeric, Integer, ForeignKey, JSON, UUID, Index
from sqlalchemy.sql import func
import uuid
from database import Base

class User(Base):
    __tablename__ = "users"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    first_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    last_seen_at = Column(DateTime(timezone=True), server_default=func.now())
    acquisition_source = Column(String(100))
    country_code = Column(String(2))
    device_type = Column(String(50))

class Event(Base):
    __tablename__ = "events"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    session_id = Column(UUID(as_uuid=True), nullable=False, index=True)
    event_type = Column(String(50), nullable=False, index=True)
    page_path = Column(String(500))
    # Column name in DB is 'metadata', Python attribute is 'meta_data'
    meta_data = Column("metadata", JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True)

class Order(Base):
    __tablename__ = "orders"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.id"), index=True)
    order_number = Column(String(50), unique=True, nullable=False)
    status = Column(String(50), default='pending')
    amount = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(3), default='USD')
    items_count = Column(Integer, default=0)
    # Column name in DB is 'metadata', Python attribute is 'meta_data'
    meta_data = Column("metadata", JSON, default={})
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    completed_at = Column(DateTime(timezone=True))