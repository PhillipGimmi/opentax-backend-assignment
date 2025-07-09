"""
Security configuration for FastAPI application
"""
from fastapi import HTTPException, Request
from fastapi.security import HTTPBearer
from typing import Optional, Dict, Any
import time
import re
from decimal import Decimal
import logging

# Rate limiting configuration
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_REQUESTS = 100  # requests per window

# In-memory rate limiting (use Redis in production)
request_counts: Dict[str, Dict[str, Any]] = {}

def validate_user_id(user_id: int) -> bool:
    """Validate user ID format and range"""
    return isinstance(user_id, int) and 1 <= user_id <= 999999

def validate_amount(amount: Decimal) -> bool:
    """Validate financial amount"""
    return isinstance(amount, Decimal) and -999999999.99 <= amount <= 999999999.99

def validate_date_range(start_date: Optional[str], end_date: Optional[str]) -> bool:
    """Validate date range parameters"""
    if start_date and end_date:
        try:
            from datetime import datetime
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            return start <= end
        except ValueError:
            return False
    return True

def sanitize_input(input_str: str) -> str:
    """Sanitize user input to prevent injection attacks"""
    # Remove potentially dangerous characters
    sanitized = re.sub(r'[<>"\']', '', input_str)
    return sanitized.strip()

def rate_limit_check(client_ip: str) -> bool:
    """Check if client has exceeded rate limit"""
    current_time = time.time()
    
    if client_ip not in request_counts:
        request_counts[client_ip] = {"count": 1, "window_start": current_time}
        return True
    
    client_data = request_counts[client_ip]
    
    # Reset window if expired
    if current_time - client_data["window_start"] > RATE_LIMIT_WINDOW:
        client_data["count"] = 1
        client_data["window_start"] = current_time
        return True
    
    # Check if limit exceeded
    if client_data["count"] >= RATE_LIMIT_MAX_REQUESTS:
        return False
    
    client_data["count"] += 1
    return True

def get_client_ip(request: Request) -> str:
    """Get client IP address considering proxy headers"""
    # Check for forwarded headers first
    forwarded_for = request.headers.get("X-Forwarded-For")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    
    # Fall back to direct connection
    if request.client:
        return request.client.host
    
    return "unknown"

def validate_request_headers(request: Request) -> None:
    """Validate request headers for security"""
    # Check for suspicious headers
    suspicious_headers = ["X-Forwarded-Host", "X-Original-URL"]
    for header in suspicious_headers:
        if header in request.headers:
            raise HTTPException(status_code=400, detail="Invalid request headers")

def log_security_event(event_type: str, details: Dict[str, Any]) -> None:
    """Log security events without sensitive data"""
    # Use standard logging if structlog is not available
    try:
        import structlog
        logger = structlog.get_logger()
    except ImportError:
        logger = logging.getLogger(__name__)
    
    # Remove sensitive data before logging
    safe_details = {k: v for k, v in details.items() 
                   if k not in ["password", "token", "secret"]}
    
    logger.warning(
        f"Security event: {event_type}",
        extra=safe_details
    )

class SecurityMiddleware:
    """Security middleware for request validation"""
    
    def __init__(self, app):
        self.app = app
    
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            request = Request(scope, receive)
            
            # Validate headers
            try:
                validate_request_headers(request)
            except HTTPException as e:
                # Properly send HTTP response for header validation errors
                response = {
                    "type": "http.response.start",
                    "status": e.status_code,
                    "headers": [
                        (b"content-type", b"application/json"),
                    ],
                }
                await send(response)
                
                response_body = {
                    "detail": e.detail,
                    "status_code": e.status_code
                }
                await send({
                    "type": "http.response.body",
                    "body": str(response_body).encode(),
                })
                return
            
            # Rate limiting
            client_ip = get_client_ip(request)
            if not rate_limit_check(client_ip):
                log_security_event("rate_limit_exceeded", {"client_ip": client_ip})
                
                # Properly send HTTP response for rate limiting
                response = {
                    "type": "http.response.start",
                    "status": 429,
                    "headers": [
                        (b"content-type", b"application/json"),
                    ],
                }
                await send(response)
                
                response_body = {
                    "detail": "Rate limit exceeded",
                    "status_code": 429
                }
                await send({
                    "type": "http.response.body",
                    "body": str(response_body).encode(),
                })
                return
        
        await self.app(scope, receive, send) 