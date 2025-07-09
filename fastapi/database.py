from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Index, CheckConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import NUMERIC
from datetime import datetime, timezone
import os

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://airflow:airflow@postgres:5432/finance_app")

# Create async engine
engine = create_async_engine(DATABASE_URL, echo=False, pool_pre_ping=True)

# Create async session factory
async_session_maker = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

def get_current_timestamp():
    """Get current timestamp with timezone for database defaults"""
    return datetime.now(timezone.utc)

class Transaction(Base):
    __tablename__ = "transactions"
    
    # Add table-level constraints and indexes
    __table_args__ = (
        # Composite index for common query patterns
        Index('idx_user_id_date', 'user_id', 'transaction_date'),
        Index('idx_transaction_date', 'transaction_date'),
        Index('idx_amount', 'amount'),
        # Check constraints for data integrity
        CheckConstraint('user_id > 0', name='chk_user_id_positive'),
        CheckConstraint("transaction_id ~ '^[A-Z0-9]{3,50}$'", name='chk_transaction_id_format'),
    )
    
    # Primary key
    id = Column(Integer, primary_key=True, index=True)
    
    # Business fields with proper constraints
    transaction_id = Column(String(50), unique=True, nullable=False, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    amount = Column(NUMERIC(15, 2), nullable=False)  # Changed from DECIMAL to NUMERIC
    transaction_date = Column(Date, nullable=False, index=True)
    
    # Audit fields for tracking with proper timezone handling
    created_at = Column(DateTime, default=get_current_timestamp, nullable=False)
    updated_at = Column(DateTime, default=get_current_timestamp, onupdate=get_current_timestamp, nullable=False)

# Dependency to get database session
async def get_db():
    async with async_session_maker() as session:
        try:
            yield session
        except Exception as e:
            await session.rollback()
            raise
        finally:
            await session.close()

# Database initialization function
async def init_db():
    """Initialize database tables"""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Database health check
async def check_db_health():
    """Check database connectivity and health"""
    try:
        async with engine.begin() as conn:
            await conn.execute("SELECT 1")
        return True
    except Exception:
        return False