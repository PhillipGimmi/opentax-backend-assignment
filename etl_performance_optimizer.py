"""
Performance Configuration and Monitoring Script for High-Volume ETL
Run this before executing your ETL pipeline for optimal performance
"""

import psycopg2
import time
import logging
from typing import Dict, Any
import os
from contextlib import contextmanager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/finance_app_optimized")

def get_connection_params():
    """Extract connection parameters from DATABASE_URL"""
    import urllib.parse as urlparse
    url = urlparse.urlparse(DATABASE_URL)
    return {
        'host': url.hostname,
        'port': url.port,
        'database': url.path[1:],
        'user': url.username,
        'password': url.password
    }

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn_params = get_connection_params()
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        yield conn
    finally:
        if conn:
            conn.close()

class ETLPerformanceOptimizer:
    """Handles database optimization for high-volume ETL operations"""
    
    def __init__(self):
        self.conn_params = get_connection_params()
        self.original_settings = {}
    
    def check_system_resources(self) -> Dict[str, Any]:
        """Check available system resources"""
        try:
            import psutil
            
            # Get system info
            cpu_count = psutil.cpu_count()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            resources = {
                'cpu_cores': cpu_count,
                'total_memory_gb': round(memory.total / (1024**3), 2),
                'available_memory_gb': round(memory.available / (1024**3), 2),
                'memory_percent': memory.percent,
                'disk_free_gb': round(disk.free / (1024**3), 2),
                'disk_percent': round((disk.used / disk.total) * 100, 1)
            }
            
            logger.info(f"System Resources: {resources}")
            return resources
            
        except ImportError:
            logger.warning("psutil not available - cannot check system resources")
            return {}
    
    def get_recommended_settings(self) -> Dict[str, str]:
        """Get recommended PostgreSQL settings based on system resources"""
        resources = self.check_system_resources()
        
        # Default settings for moderate hardware
        settings = {
            'shared_buffers': '1GB',
            'effective_cache_size': '3GB',
            'work_mem': '256MB',
            'maintenance_work_mem': '1GB',
            'wal_buffers': '32MB',
            'max_worker_processes': '4',
            'max_parallel_workers': '3',
            'max_parallel_workers_per_gather': '2',
            'effective_io_concurrency': '100'
        }
        
        # Adjust based on available memory
        if 'total_memory_gb' in resources:
            memory_gb = resources['total_memory_gb']
            
            if memory_gb >= 8:
                settings.update({
                    'shared_buffers': f'{int(memory_gb * 0.25)}GB',
                    'effective_cache_size': f'{int(memory_gb * 0.75)}GB',
                    'work_mem': '512MB',
                    'maintenance_work_mem': '2GB',
                    'wal_buffers': '64MB'
                })
            
            if memory_gb >= 16:
                settings.update({
                    'work_mem': '1GB',
                    'maintenance_work_mem': '4GB',
                    'max_worker_processes': '8',
                    'max_parallel_workers': '6',
                    'max_parallel_workers_per_gather': '4'
                })
        
        # Adjust based on CPU cores
        if 'cpu_cores' in resources:
            cpu_cores = resources['cpu_cores']
            if cpu_cores >= 8:
                settings.update({
                    'max_worker_processes': str(cpu_cores),
                    'max_parallel_workers': str(max(1, cpu_cores - 2)),
                    'max_parallel_workers_per_gather': str(max(1, cpu_cores // 2))
                })
        
        return settings
    
    def backup_current_settings(self):
        """Backup current PostgreSQL settings"""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    settings_to_backup = [
                        'shared_buffers', 'effective_cache_size', 'work_mem',
                        'maintenance_work_mem', 'wal_buffers', 'synchronous_commit',
                        'full_page_writes', 'checkpoint_completion_target'
                    ]
                    
                    for setting in settings_to_backup:
                        cur.execute(f"SHOW {setting}")
                        value = cur.fetchone()[0]
                        self.original_settings[setting] = value
                    
                    logger.info(f"Backed up current settings: {self.original_settings}")
        
        except Exception as e:
            logger.error(f"Error backing up settings: {e}")
    
    def apply_bulk_load_settings(self):
        """Apply optimized settings for bulk loading"""
        try:
            recommended = self.get_recommended_settings()
            
            with get_db_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # Apply memory settings
                    cur.execute(f"SET shared_buffers = '{recommended['shared_buffers']}'")
                    cur.execute(f"SET effective_cache_size = '{recommended['effective_cache_size']}'")
                    cur.execute(f"SET work_mem = '{recommended['work_mem']}'")
                    cur.execute(f"SET maintenance_work_mem = '{recommended['maintenance_work_mem']}'")
                    cur.execute(f"SET wal_buffers = '{recommended['wal_buffers']}'")
                    
                    # Apply bulk load optimizations
                    cur.execute("SET synchronous_commit = OFF")
                    cur.execute("SET full_page_writes = OFF")
                    cur.execute("SET checkpoint_completion_target = 0.9")
                    cur.execute("SET fsync = OFF")  # DANGER: Only for bulk loads!
                    
                    # Apply parallel settings
                    cur.execute(f"SET max_worker_processes = {recommended['max_worker_processes']}")
                    cur.execute(f"SET max_parallel_workers = {recommended['max_parallel_workers']}")
                    cur.execute(f"SET max_parallel_workers_per_gather = {recommended['max_parallel_workers_per_gather']}")
                    cur.execute(f"SET effective_io_concurrency = {recommended['effective_io_concurrency']}")
                    
                    logger.info("Applied bulk load optimization settings")
        
        except Exception as e:
            logger.error(f"Error applying bulk load settings: {e}")
            raise
    
    def restore_original_settings(self):
        """Restore original PostgreSQL settings"""
        try:
            with get_db_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # Restore critical safety settings first
                    cur.execute("SET synchronous_commit = ON")
                    cur.execute("SET full_page_writes = ON")
                    cur.execute("SET fsync = ON")
                    
                    # Restore other settings if we have them
                    for setting, value in self.original_settings.items():
                        try:
                            cur.execute(f"SET {setting} = '{value}'")
                        except Exception as e:
                            logger.warning(f"Could not restore {setting}: {e}")
                    
                    logger.info("Restored original database settings")
        
        except Exception as e:
            logger.error(f"Error restoring settings: {e}")
    
    def drop_indexes_for_bulk_load(self, table_name: str):
        """Drop indexes to speed up bulk loading"""
        try:
            with get_db_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # Get all indexes for the table
                    cur.execute("""
                        SELECT indexname 
                        FROM pg_indexes 
                        WHERE tablename = %s 
                        AND indexname NOT LIKE '%%_pkey'
                    """, (table_name,))
                    
                    indexes = cur.fetchall()
                    
                    for (index_name,) in indexes:
                        cur.execute(f"DROP INDEX IF EXISTS {index_name}")
                        logger.info(f"Dropped index: {index_name}")
        
        except Exception as e:
            logger.error(f"Error dropping indexes: {e}")
    
    def recreate_indexes(self, table_name: str):
        """Recreate indexes after bulk loading"""
        try:
            with get_db_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    # Standard indexes for transactions table
                    if table_name == 'transactions':
                        index_commands = [
                            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_user_id ON transactions(user_id)",
                            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_date ON transactions(transaction_date)",
                            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_user_date ON transactions(user_id, transaction_date)",
                            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_amount ON transactions(amount)",
                            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_created_at ON transactions(created_at)"
                        ]
                        
                        for cmd in index_commands:
                            try:
                                cur.execute(cmd)
                                logger.info(f"Created index with command: {cmd}")
                            except Exception as e:
                                logger.warning(f"Index creation failed: {e}")
                    
                    # Update table statistics
                    cur.execute(f"ANALYZE {table_name}")
                    logger.info(f"Updated statistics for table: {table_name}")
        
        except Exception as e:
            logger.error(f"Error recreating indexes: {e}")

class ETLPerformanceMonitor:
    """Monitor ETL performance and provide insights"""
    
    def __init__(self):
        self.conn_params = get_connection_params()
        self.start_time = None
    
    def start_monitoring(self):
        """Start performance monitoring"""
        self.start_time = time.time()
        logger.info("Started performance monitoring")
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get current database performance statistics"""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    # Get connection count
                    cur.execute("SELECT count(*) FROM pg_stat_activity")
                    active_connections = cur.fetchone()[0]
                    
                    # Get database size
                    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                    db_size = cur.fetchone()[0]
                    
                    # Get transaction count
                    cur.execute("SELECT COUNT(*) FROM transactions")
                    transaction_count = cur.fetchone()[0]
                    
                    # Get slow queries
                    cur.execute("""
                        SELECT COUNT(*) 
                        FROM pg_stat_statements 
                        WHERE mean_time > 1000
                    """)
                    slow_queries = cur.fetchone()[0] if cur.fetchone() else 0
                    
                    return {
                        'active_connections': active_connections,
                        'database_size': db_size,
                        'transaction_count': transaction_count,
                        'slow_queries': slow_queries,
                        'monitoring_duration': time.time() - self.start_time if self.start_time else 0
                    }
        
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            return {}
    
    def log_performance_summary(self):
        """Log performance summary"""
        stats = self.get_database_stats()
        if stats:
            logger.info(f"Performance Summary: {stats}")

def main():
    """Main function to run performance optimization"""
    logger.info("Starting ETL Performance Optimization")
    
    # Initialize optimizer and monitor
    optimizer = ETLPerformanceOptimizer()
    monitor = ETLPerformanceMonitor()
    
    try:
        # Check system resources
        optimizer.check_system_resources()
        
        # Backup current settings
        optimizer.backup_current_settings()
        
        # Apply optimizations
        optimizer.apply_bulk_load_settings()
        
        # Start monitoring
        monitor.start_monitoring()
        
        logger.info("ETL environment optimized and ready for bulk operations")
        logger.info("Run your ETL pipeline now, then call restore_settings()")
        
        # Example: Wait for ETL to complete (in real usage, this would be your ETL pipeline)
        input("Press Enter after your ETL pipeline completes...")
        
    except Exception as e:
        logger.error(f"Error during optimization: {e}")
    
    finally:
        # Always restore settings
        try:
            optimizer.restore_original_settings()
            monitor.log_performance_summary()
            logger.info("Performance optimization complete")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

if __name__ == "__main__":
    main() 