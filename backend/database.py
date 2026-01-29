from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
from config import settings
import logging

logger = logging.getLogger(__name__)

# Create async engine with proper pool settings for high concurrency
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,  # Set to True for debugging SQL
    pool_size=settings.DB_POOL_SIZE,
    max_overflow=settings.DB_MAX_OVERFLOW,
    pool_pre_ping=True,  # Verify connections before using them
    pool_recycle=3600,   # Recycle connections after 1 hour
)

# Session factory
AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False
)

Base = declarative_base()

async def get_db() -> AsyncSession:
    """Dependency for FastAPI to get DB session"""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            raise e
        finally:
            await session.close()

async def execute_with_timeout(session: AsyncSession, query: str, params: dict = None, timeout: int = None):
    """Execute query with statement timeout to prevent runaway queries"""
    timeout = timeout or settings.QUERY_TIMEOUT_SECONDS
    await session.execute(text(f"SET LOCAL statement_timeout = '{timeout}s'"))
    result = await session.execute(text(query), params or {})
    return result.mappings().all()

async def refresh_materialized_views():
    """Background task to refresh materialized views"""
    async with AsyncSessionLocal() as session:
        try:
            await session.execute(text("SELECT refresh_dashboard_views()"))
            await session.commit()
            logger.info("Materialized views refreshed successfully")
        except Exception as e:
            await session.rollback()
            logger.error(f"Failed to refresh materialized views: {e}")
            raise