from pydantic import BaseSettings, Field
from typing import Optional
import os

class Settings(BaseSettings):
    """Application settings with environment variable support"""
    
    # Database settings
    database_url: str = Field(
        default="postgresql+asyncpg://airflow:airflow@postgres:5432/finance_app",
        description="Database connection URL"
    )
    
    # API settings
    api_host: str = Field(default="0.0.0.0", description="API host")
    api_port: int = Field(default=8000, description="API port")
    api_reload: bool = Field(default=True, description="Enable auto-reload")
    
    # Security settings
    cors_origins: list = Field(
        default=["*"], 
        description="Allowed CORS origins"
    )
    trusted_hosts: list = Field(
        default=["*"], 
        description="Trusted host headers"
    )
    
    # Logging settings
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(
        default="json", 
        description="Log format (json or text)"
    )
    
    # Performance settings
    db_pool_size: int = Field(default=10, description="Database pool size")
    db_max_overflow: int = Field(default=20, description="Database max overflow")
    db_pool_recycle: int = Field(default=3600, description="Database pool recycle time")
    
    # Rate limiting
    rate_limit_per_minute: int = Field(default=100, description="Rate limit per minute")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

# Create global settings instance
settings = Settings()

# Environment-specific overrides
if os.getenv("ENVIRONMENT") == "production":
    settings.api_reload = False
    settings.log_level = "WARNING"
    settings.cors_origins = ["https://yourdomain.com"]  # Configure appropriately
    settings.trusted_hosts = ["yourdomain.com"]  # Configure appropriately 