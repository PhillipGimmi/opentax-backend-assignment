from fastapi import FastAPI, Depends, HTTPException, Query, Path, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import func, select, text
from database import get_db, Transaction
from pydantic import BaseModel, Field
from typing import Optional
from decimal import Decimal
import structlog
import time
from datetime import date
import os

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Create FastAPI app with proper configuration
app = FastAPI(
    title="Financial Transactions API",
    version="1.0.0",
    description="Production-ready financial transaction processing API",
    docs_url="/docs" if os.getenv("ENVIRONMENT") != "production" else None,
    redoc_url="/redoc" if os.getenv("ENVIRONMENT") != "production" else None,
)

# Add security middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://yourdomain.com"],  # Configure appropriately for production
    allow_credentials=False,  # Changed from True to prevent security vulnerability
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["localhost", "yourdomain.com"]  # Configure appropriately for production
)

# Pydantic models with proper validation
class TransactionSummary(BaseModel):
    user_id: int = Field(..., description="User ID")
    total_transactions: int = Field(..., description="Total number of transactions")
    total_amount: Decimal = Field(..., description="Sum of all transaction amounts")
    average_transaction_amount: Decimal = Field(..., description="Average transaction amount")

class ErrorResponse(BaseModel):
    detail: str = Field(..., description="Error message")
    error_code: str = Field(..., description="Error code")

# Request/Response logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Log request
    logger.info(
        "Request started",
        method=request.method,
        url=str(request.url),
        client_ip=request.client.host if request.client else None
    )
    
    response = await call_next(request)
    
    # Log response
    process_time = time.time() - start_time
    logger.info(
        "Request completed",
        method=request.method,
        url=str(request.url),
        status_code=response.status_code,
        process_time=process_time
    )
    
    return response

@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Welcome to the Financial Transactions API", "version": "1.0.0"}

@app.get("/health")
async def health_check():
    """Comprehensive health check endpoint"""
    try:
        # Test database connectivity
        from database import engine
        async with engine.begin() as conn:
            await conn.execute(text("SELECT 1"))
        
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise HTTPException(status_code=503, detail="Service unhealthy")

@app.get(
    "/transactions/{user_id}/summary", 
    response_model=TransactionSummary,
    responses={
        404: {"model": ErrorResponse, "description": "User not found"},
        422: {"model": ErrorResponse, "description": "Validation error"},
        500: {"model": ErrorResponse, "description": "Internal server error"}
    }
)
async def get_transaction_summary(
    user_id: int = Path(..., gt=0, description="User ID must be positive"),
    start_date: Optional[date] = Query(None, description="Filter start date (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Filter end date (YYYY-MM-DD)"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get transaction summary for a specific user with optional date filtering.
    
    - **user_id**: Positive integer user ID
    - **start_date**: Optional start date filter (YYYY-MM-DD)
    - **end_date**: Optional end date filter (YYYY-MM-DD)
    """
    try:
        # Validate date range if provided
        if start_date and end_date and start_date > end_date:
            raise HTTPException(
                status_code=422, 
                detail="start_date must be before or equal to end_date"
            )
        
        # Build query with proper filtering
        query = select(
            Transaction.user_id,
            func.count(Transaction.id).label('total_transactions'),
            func.sum(Transaction.amount).label('total_amount'),
            func.avg(Transaction.amount).label('average_transaction_amount')
        ).where(Transaction.user_id == user_id)
        
        # Add date filters if provided
        if start_date:
            query = query.where(Transaction.transaction_date >= start_date)
        if end_date:
            query = query.where(Transaction.transaction_date <= end_date)
        
        query = query.group_by(Transaction.user_id)
        
        # Execute query with timeout
        result = await db.execute(query)
        row = result.first()
        
        if not row:
            logger.warning("No transactions found", user_id=user_id)
            raise HTTPException(
                status_code=404, 
                detail=f"No transactions found for user_id: {user_id}"
            )
        
        # Handle null values safely
        total_amount = row.total_amount or Decimal(0)
        avg_amount = row.average_transaction_amount or Decimal(0)
        
        response = TransactionSummary(
            user_id=row.user_id,
            total_transactions=row.total_transactions,
            total_amount=round(total_amount, 2),
            average_transaction_amount=round(avg_amount, 2)
        )
        
        logger.info(
            "Transaction summary retrieved",
            user_id=user_id,
            total_transactions=row.total_transactions,
            total_amount=total_amount
        )
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrieving transaction summary", user_id=user_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")

@app.get("/transactions/{user_id}/transactions")
async def get_user_transactions(
    user_id: int = Path(..., gt=0, description="User ID must be positive"),
    limit: int = Query(100, ge=1, le=1000, description="Number of transactions to return"),
    offset: int = Query(0, ge=0, description="Number of transactions to skip"),
    db: AsyncSession = Depends(get_db)
):
    """
    Get paginated list of transactions for a specific user.
    
    - **user_id**: Positive integer user ID
    - **limit**: Number of transactions to return (1-1000)
    - **offset**: Number of transactions to skip
    """
    try:
        # Check if user has any transactions
        count_query = select(func.count(Transaction.id)).where(Transaction.user_id == user_id)
        total_count = await db.scalar(count_query)
        
        if total_count == 0:
            raise HTTPException(
                status_code=404, 
                detail=f"No transactions found for user_id: {user_id}"
            )
        
        # Get paginated transactions
        query = select(Transaction).where(
            Transaction.user_id == user_id
        ).order_by(Transaction.transaction_date.desc()).limit(limit).offset(offset)
        
        result = await db.execute(query)
        transactions = result.scalars().all()
        
        return {
            "user_id": user_id,
            "transactions": [
                {
                    "transaction_id": t.transaction_id,
                    "amount": float(t.amount),
                    "transaction_date": t.transaction_date.isoformat()
                }
                for t in transactions
            ],
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "has_more": offset + limit < total_count
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Error retrieving user transactions", user_id=user_id, error=str(e))
        raise HTTPException(status_code=500, detail="Internal server error")