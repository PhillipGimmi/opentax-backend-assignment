from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from typing import Dict, Any
import os
import csv
from io import StringIO
import multiprocessing as mp
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from functools import partial

# Configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/finance_app")
CSV_PATH = os.getenv("CSV_PATH", "/opt/airflow/data/financial_transactions.csv")
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "50000"))  # Process in 50K chunks
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))  # Parallel processing threads

# Define default arguments
default_args = {
    'owner': 'candidate',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

# Define the DAG
dag = DAG(
    'etl_transactions_optimized',
    default_args=default_args,
    description='High-performance ETL pipeline for 1M+ financial transactions',
    schedule_interval='0 0 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
)

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

def optimize_database_for_bulk_load(**context):
    """Optimize database settings for bulk operations"""
    try:
        conn_params = get_connection_params()
        
        with psycopg2.connect(**conn_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Only set session-level parameters that can be changed at runtime
                try:
                    cur.execute("SET maintenance_work_mem = '1GB'")
                    logging.info("Set maintenance_work_mem = 1GB")
                except Exception as e:
                    logging.warning(f"Could not set maintenance_work_mem: {e}")
                
                try:
                    cur.execute("SET work_mem = '512MB'")
                    logging.info("Set work_mem = 512MB")
                except Exception as e:
                    logging.warning(f"Could not set work_mem: {e}")
                
                try:
                    cur.execute("SET synchronous_commit = OFF")
                    logging.info("Set synchronous_commit = OFF")
                except Exception as e:
                    logging.warning(f"Could not set synchronous_commit: {e}")
                
                try:
                    cur.execute("SET commit_delay = 100000")  # 100ms delay for better batching
                    logging.info("Set commit_delay = 100000")
                except Exception as e:
                    logging.warning(f"Could not set commit_delay: {e}")
                
                # Note: checkpoint_completion_target, wal_buffers, full_page_writes 
                # require server-level configuration, not session-level
                
                # Drop indexes temporarily for faster loading
                try:
                    cur.execute("""
                        DROP INDEX IF EXISTS idx_transactions_user_id;
                        DROP INDEX IF EXISTS idx_transactions_date;
                        DROP INDEX IF EXISTS idx_transactions_user_date;
                        DROP INDEX IF EXISTS idx_transactions_amount;
                        DROP INDEX IF EXISTS idx_transactions_created_at;
                    """)
                    logging.info("Dropped existing indexes for bulk load")
                except Exception as e:
                    logging.warning(f"Could not drop some indexes: {e}")
                
                # Clean and create staging tables
                cur.execute("DROP TABLE IF EXISTS transactions_staging_bulk CASCADE")
                cur.execute("""
                    CREATE UNLOGGED TABLE transactions_staging_bulk (
                        transaction_id TEXT,
                        user_id TEXT,
                        amount TEXT,
                        transaction_date TEXT
                    ) WITH (fillfactor=100, autovacuum_enabled=false)
                """)
                logging.info("Created staging table for bulk operations")
        
        logging.info("Database optimized for bulk operations")
        return {'status': 'success'}
    except Exception as e:
        logging.error(f"Error optimizing database: {str(e)}")
        raise

def process_csv_chunk(chunk_data, chunk_index):
    """Process a single chunk of CSV data"""
    try:
        processed_rows = []
        
        for row in chunk_data:
            # Clean and validate data
            transaction_id = str(row[0]).strip() if row[0] else None
            user_id = str(row[1]).strip() if row[1] else None
            amount = str(row[2]).strip() if row[2] else None
            transaction_date = str(row[3]).strip() if row[3] else None
            
            # Basic validation
            if not all([transaction_id, user_id, amount, transaction_date]):
                continue
                
            # Clean amount (remove $, commas, spaces)
            amount_cleaned = amount.replace('$', '').replace(',', '').replace(' ', '')
            
            processed_rows.append((transaction_id, user_id, amount_cleaned, transaction_date))
        
        return processed_rows
    except Exception as e:
        logging.error(f"Error processing chunk {chunk_index}: {str(e)}")
        return []

def bulk_load_chunk_to_staging(chunk_data, conn_params):
    """Load a chunk of data to staging table using COPY"""
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Prepare data for COPY
                csv_buffer = StringIO()
                csv_writer = csv.writer(csv_buffer, delimiter='\t')
                csv_writer.writerows(chunk_data)
                csv_buffer.seek(0)
                
                # Use COPY for maximum performance
                cur.copy_from(
                    csv_buffer,
                    'transactions_staging_bulk',
                    columns=('transaction_id', 'user_id', 'amount', 'transaction_date'),
                    sep='\t'
                )
                conn.commit()
                
        return len(chunk_data)
    except Exception as e:
        logging.error(f"Error loading chunk to staging: {str(e)}")
        return 0

def extract_transactions_optimized(**context) -> Dict[str, Any]:
    """Extract transactions using streaming and parallel processing"""
    try:
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
        
        conn_params = get_connection_params()
        total_processed = 0
        
        # Read and process CSV in chunks with parallel processing
        with open(CSV_PATH, 'r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)
            header = next(csv_reader)  # Skip header
            
            chunk_data = []
            chunk_index = 0
            
            # Process file in chunks
            for row in csv_reader:
                chunk_data.append(row)
                
                if len(chunk_data) >= CHUNK_SIZE:
                    # Process chunk in parallel
                    processed_chunk = process_csv_chunk(chunk_data, chunk_index)
                    
                    if processed_chunk:
                        # Load to staging table
                        loaded_count = bulk_load_chunk_to_staging(processed_chunk, conn_params)
                        total_processed += loaded_count
                        
                        logging.info(f"Processed chunk {chunk_index}: {loaded_count} records")
                    
                    chunk_data = []
                    chunk_index += 1
            
            # Process remaining data
            if chunk_data:
                processed_chunk = process_csv_chunk(chunk_data, chunk_index)
                if processed_chunk:
                    loaded_count = bulk_load_chunk_to_staging(processed_chunk, conn_params)
                    total_processed += loaded_count
        
        logging.info(f"Total extracted: {total_processed} transactions")
        
        context['task_instance'].xcom_push(key='extracted_count', value=total_processed)
        return {'status': 'success', 'count': total_processed}
    
    except Exception as e:
        logging.error(f"Error in extract: {str(e)}")
        raise

def transform_transactions_optimized(**context) -> Dict[str, Any]:
    """Transform using optimized SQL with minimal complexity"""
    try:
        conn_params = get_connection_params()
        
        with psycopg2.connect(**conn_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Single optimized transformation query
                transform_sql = """
                -- Create optimized cleaned table
                DROP TABLE IF EXISTS transactions_cleaned_bulk CASCADE;
                CREATE TABLE transactions_cleaned_bulk (
                    transaction_id VARCHAR(50),
                    user_id INTEGER,
                    amount NUMERIC(15,2),
                    transaction_date DATE
                ) WITH (fillfactor=100);
                
                -- Insert with streamlined transformations
                INSERT INTO transactions_cleaned_bulk
                SELECT DISTINCT
                    transaction_id,
                    CAST(user_id AS INTEGER),
                    CAST(amount AS NUMERIC(15,2)),
                    CASE 
                        -- ISO format YYYY-MM-DD
                        WHEN transaction_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' 
                        THEN transaction_date::DATE
                        -- Handle MM/DD/YYYY and DD/MM/YYYY
                        WHEN transaction_date ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$'
                        THEN (
                            CASE 
                                WHEN SPLIT_PART(transaction_date, '/', 1)::INTEGER > 12 
                                THEN TO_DATE(transaction_date, 'DD/MM/YYYY')
                                ELSE TO_DATE(transaction_date, 'MM/DD/YYYY')
                            END
                        )
                        ELSE NULL
                    END
                FROM transactions_staging_bulk
                WHERE transaction_id IS NOT NULL 
                AND user_id ~ '^[0-9]+$'
                AND amount ~ '^[0-9]+\.?[0-9]*$'
                AND transaction_date IS NOT NULL;
                """
                
                cur.execute(transform_sql)
                
                # Get count
                cur.execute("SELECT COUNT(*) FROM transactions_cleaned_bulk")
                count = cur.fetchone()[0]
        
        logging.info(f"Transformed {count} transactions")
        
        context['task_instance'].xcom_push(key='transformed_count', value=count)
        return {'status': 'success', 'count': count}
    
    except Exception as e:
        logging.error(f"Error in transform: {str(e)}")
        raise

def load_transactions_optimized(**context) -> Dict[str, Any]:
    """Load using bulk operations with conflict handling"""
    try:
        conn_params = get_connection_params()
        
        with psycopg2.connect(**conn_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Bulk upsert using optimized query
                load_sql = """
                -- Use efficient bulk insert with conflict handling
                INSERT INTO transactions (transaction_id, user_id, amount, transaction_date)
                SELECT transaction_id, user_id, amount, transaction_date
                FROM transactions_cleaned_bulk
                ON CONFLICT (transaction_id) DO NOTHING;
                """
                
                cur.execute(load_sql)
                
                # Get final count
                cur.execute("SELECT COUNT(*) FROM transactions")
                total_count = cur.fetchone()[0]
                
                # Clean up staging tables
                cur.execute("DROP TABLE IF EXISTS transactions_staging_bulk CASCADE")
                cur.execute("DROP TABLE IF EXISTS transactions_cleaned_bulk CASCADE")
        
        logging.info(f"Loaded transactions. Total in table: {total_count}")
        return {'status': 'success', 'total_count': total_count}
    
    except Exception as e:
        logging.error(f"Error in load: {str(e)}")
        raise

def restore_database_settings(**context):
    """Restore database settings and recreate indexes"""
    try:
        conn_params = get_connection_params()
        
        with psycopg2.connect(**conn_params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # Restore session-level settings to defaults
                try:
                    cur.execute("SET synchronous_commit = ON")
                    logging.info("Restored synchronous_commit = ON")
                except Exception as e:
                    logging.warning(f"Could not restore synchronous_commit: {e}")
                
                try:
                    cur.execute("SET work_mem = DEFAULT")
                    logging.info("Restored work_mem to default")
                except Exception as e:
                    logging.warning(f"Could not restore work_mem: {e}")
                
                try:
                    cur.execute("SET maintenance_work_mem = DEFAULT")
                    logging.info("Restored maintenance_work_mem to default")
                except Exception as e:
                    logging.warning(f"Could not restore maintenance_work_mem: {e}")
                
                try:
                    cur.execute("SET commit_delay = DEFAULT")
                    logging.info("Restored commit_delay to default")
                except Exception as e:
                    logging.warning(f"Could not restore commit_delay: {e}")
                
                # Recreate indexes for optimal query performance
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
                        logging.info(f"Created index: {cmd}")
                    except Exception as e:
                        logging.warning(f"Index creation failed: {e}")
                
                # Update table statistics
                cur.execute("ANALYZE transactions")
                logging.info("Updated table statistics")
        
        logging.info("Database settings restored and indexes recreated")
        return {'status': 'success'}
    
    except Exception as e:
        logging.error(f"Error restoring database: {str(e)}")
        raise

# Define optimized tasks
optimize_db_task = PythonOperator(
    task_id='optimize_database',
    python_callable=optimize_database_for_bulk_load,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_optimized',
    python_callable=extract_transactions_optimized,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_optimized',
    python_callable=transform_transactions_optimized,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_optimized',
    python_callable=load_transactions_optimized,
    dag=dag,
)

restore_db_task = PythonOperator(
    task_id='restore_database',
    python_callable=restore_database_settings,
    dag=dag,
)

# Define optimized task dependencies
optimize_db_task >> extract_task >> transform_task >> load_task >> restore_db_task