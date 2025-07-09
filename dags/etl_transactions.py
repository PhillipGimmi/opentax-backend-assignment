from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from typing import Dict, Any
import re
import os

# Configuration - should be moved to environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/finance_app")
CSV_PATH = os.getenv("CSV_PATH", "/opt/airflow/data/financial_transactions.csv")

# Removed dynamic table name functions - using fixed table names for security

# Define default arguments for the DAG
default_args = {
    'owner': 'candidate',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_transactions',
    default_args=default_args,
    description='ETL pipeline for financial transactions',
    schedule_interval='0 0 * * *',  # Run daily at midnight
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
)

def extract_transactions(**context) -> Dict[str, Any]:
    """Extract transactions from CSV file to staging table"""
    try:
        # Validate CSV file exists
        if not os.path.exists(CSV_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")
        
        # Use fixed staging table name instead of dynamic names
        staging_table = "transactions_staging_temp"
        
        # Read CSV file with validation
        df = pd.read_csv(CSV_PATH)
        if df.empty:
            raise ValueError("CSV file is empty")
        
        logging.info(f"Extracted {len(df)} transactions from CSV")
        
        # Create engine and load to staging table
        engine = create_engine(DATABASE_URL)
        
        # Clean staging table first
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS transactions_staging_temp"))
        
        # Load raw data to staging table with safe table name
        df.to_sql(staging_table, engine, if_exists='replace', index=False, method='multi')
        
        logging.info(f"Loaded {len(df)} transactions to staging table: {staging_table}")
        
        # Store staging table name in XCom (small string, not large data)
        context['task_instance'].xcom_push(key='staging_table', value=staging_table)
        return {'status': 'success', 'staging_table': staging_table, 'count': len(df)}
    except Exception as e:
        logging.error(f"Error in extract: {str(e)}")
        raise

def transform_transactions(**context) -> Dict[str, Any]:
    """Transform transactions data using SQL for better performance"""
    try:
        # Use fixed table names - no dynamic names to prevent SQL injection
        staging_table = "transactions_staging_temp"
        cleaned_table = "transactions_cleaned"
        
        engine = create_engine(DATABASE_URL)
        
        # Use parameterized queries with fixed table names
        transform_sql = text("""
        DROP TABLE IF EXISTS transactions_cleaned;
        CREATE TABLE transactions_cleaned AS
        SELECT DISTINCT ON (transaction_id)
            transaction_id,
            CAST(user_id AS INTEGER) as user_id,
            CASE 
                WHEN amount IS NULL OR amount = '' THEN NULL
                ELSE CAST(
                    REPLACE(REPLACE(REPLACE(amount, '$', ''), ',', ''), ' ', '') AS NUMERIC(15,2)
                )
            END as amount,
            CASE 
                WHEN transaction_date IS NULL OR transaction_date = '' THEN NULL
                -- Try multiple date formats
                WHEN transaction_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TO_DATE(transaction_date, 'YYYY-MM-DD')
                WHEN transaction_date ~ '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN TO_DATE(transaction_date, 'MM/DD/YYYY')
                WHEN transaction_date ~ '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN TO_DATE(transaction_date, 'YYYY/MM/DD')
                WHEN transaction_date ~ '^[0-9]{2}-[0-9]{2}-[0-9]{4}$' THEN TO_DATE(transaction_date, 'DD-MM-YYYY')
                ELSE NULL
            END as transaction_date
        FROM transactions_staging_temp
        WHERE transaction_id IS NOT NULL 
        AND user_id IS NOT NULL 
        AND amount IS NOT NULL 
        AND amount != ''
        AND transaction_date IS NOT NULL 
        AND transaction_date != ''
        ORDER BY transaction_id, user_id
        """)
        
        with engine.begin() as conn:
            conn.execute(transform_sql)
        
        # Get count of cleaned records
        count_sql = text("SELECT COUNT(*) as count FROM transactions_cleaned")
        with engine.begin() as conn:
            result = conn.execute(count_sql)
            count = result.fetchone()[0]
        
        logging.info(f"Transformed {count} transactions")
        
        # Store cleaned table name in XCom
        context['task_instance'].xcom_push(key='cleaned_table', value=cleaned_table)
        return {'status': 'success', 'cleaned_table': cleaned_table, 'count': count}
    except Exception as e:
        logging.error(f"Error in transform: {str(e)}")
        raise

def load_transactions(**context) -> Dict[str, Any]:
    """Load transactions into final table with proper transaction management"""
    try:
        # Use fixed table names
        cleaned_table = "transactions_cleaned"
        staging_table = "transactions_staging_temp"
        
        engine = create_engine(DATABASE_URL)
        
        # Use proper transaction management with fixed table names
        with engine.begin() as conn:
            # Insert with simple conflict handling - just ignore duplicates to avoid cardinality issues
            insert_sql = text("""
            INSERT INTO transactions (transaction_id, user_id, amount, transaction_date)
            SELECT transaction_id, user_id, amount, transaction_date
            FROM transactions_cleaned
            ON CONFLICT (transaction_id) DO NOTHING
            """)
            conn.execute(insert_sql)
            
            # Get count of inserted records
            count_sql = text("SELECT COUNT(*) FROM transactions")
            result = conn.execute(count_sql)
            count = result.fetchone()[0]
        
        # Clean up staging and cleaned tables
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS transactions_cleaned"))
            conn.execute(text("DROP TABLE IF EXISTS transactions_staging_temp"))
        
        logging.info(f"Loaded {count} transactions into final table")
        return {'status': 'success', 'count': count}
    except Exception as e:
        logging.error(f"Error in load: {str(e)}")
        raise

# Define tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_transactions,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_transactions,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_transactions,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task