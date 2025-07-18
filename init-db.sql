-- Optimized PostgreSQL configuration for high-volume ETL
-- Add these to postgresql.conf for maximum performance

-- Memory settings for large datasets
shared_buffers = '2GB'                    -- 25% of total RAM
effective_cache_size = '6GB'              -- 75% of total RAM
work_mem = '512MB'                        -- Per operation memory
maintenance_work_mem = '2GB'              -- For bulk operations
temp_buffers = '32MB'                     -- Temporary table memory

-- WAL and checkpoint settings for bulk operations
wal_buffers = '64MB'                      -- WAL buffer size
checkpoint_completion_target = 0.9        -- Spread checkpoints
max_wal_size = '4GB'                      -- Maximum WAL size
min_wal_size = '2GB'                      -- Minimum WAL size
checkpoint_timeout = '15min'              -- Checkpoint frequency

-- Connection and parallel settings
max_connections = 200                     -- Connection limit
max_worker_processes = 8                  -- Background workers
max_parallel_workers = 6                  -- Parallel query workers
max_parallel_workers_per_gather = 4       -- Workers per query
max_parallel_maintenance_workers = 4      -- Maintenance workers

-- Query planner settings
random_page_cost = 1.1                    -- SSD optimization
effective_io_concurrency = 200            -- Concurrent I/O operations
default_statistics_target = 500           -- Statistics for better plans

-- Logging for performance monitoring
log_min_duration_statement = 1000         -- Log slow queries (1 second)
log_statement = 'mod'                     -- Log data modifications
log_checkpoints = on                      -- Log checkpoint activity
log_connections = on                      -- Log connections

-- Create optimized database for finance application
CREATE DATABASE finance_app_optimized 
WITH 
    ENCODING = 'UTF8'
    LC_COLLATE = 'C'
    LC_CTYPE = 'C'
    TEMPLATE = template0;

\c finance_app_optimized;

-- Create optimized transactions table with partitioning for scale
CREATE TABLE transactions (
    id BIGSERIAL,
    transaction_id VARCHAR(50) NOT NULL,
    user_id INTEGER NOT NULL,
    amount NUMERIC(15,2) NOT NULL,
    transaction_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    CONSTRAINT pk_transactions PRIMARY KEY (id, transaction_date),
    CONSTRAINT uk_transactions_transaction_id UNIQUE (transaction_id)
) PARTITION BY RANGE (transaction_date);

-- Create monthly partitions for better performance (example for 2024-2025)
CREATE TABLE transactions_2024_01 PARTITION OF transactions 
    FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
CREATE TABLE transactions_2024_02 PARTITION OF transactions 
    FOR VALUES FROM ('2024-02-01') TO ('2024-03-01');
CREATE TABLE transactions_2024_03 PARTITION OF transactions 
    FOR VALUES FROM ('2024-03-01') TO ('2024-04-01');
CREATE TABLE transactions_2024_04 PARTITION OF transactions 
    FOR VALUES FROM ('2024-04-01') TO ('2024-05-01');
CREATE TABLE transactions_2024_05 PARTITION OF transactions 
    FOR VALUES FROM ('2024-05-01') TO ('2024-06-01');
CREATE TABLE transactions_2024_06 PARTITION OF transactions 
    FOR VALUES FROM ('2024-06-01') TO ('2024-07-01');
CREATE TABLE transactions_2024_07 PARTITION OF transactions 
    FOR VALUES FROM ('2024-07-01') TO ('2024-08-01');
CREATE TABLE transactions_2024_08 PARTITION OF transactions 
    FOR VALUES FROM ('2024-08-01') TO ('2024-09-01');
CREATE TABLE transactions_2024_09 PARTITION OF transactions 
    FOR VALUES FROM ('2024-09-01') TO ('2024-10-01');
CREATE TABLE transactions_2024_10 PARTITION OF transactions 
    FOR VALUES FROM ('2024-10-01') TO ('2024-11-01');
CREATE TABLE transactions_2024_11 PARTITION OF transactions 
    FOR VALUES FROM ('2024-11-01') TO ('2024-12-01');
CREATE TABLE transactions_2024_12 PARTITION OF transactions 
    FOR VALUES FROM ('2024-12-01') TO ('2025-01-01');

CREATE TABLE transactions_2025_01 PARTITION OF transactions 
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
CREATE TABLE transactions_2025_02 PARTITION OF transactions 
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');
CREATE TABLE transactions_2025_03 PARTITION OF transactions 
    FOR VALUES FROM ('2025-03-01') TO ('2025-04-01');
CREATE TABLE transactions_2025_04 PARTITION OF transactions 
    FOR VALUES FROM ('2025-04-01') TO ('2025-05-01');
CREATE TABLE transactions_2025_05 PARTITION OF transactions 
    FOR VALUES FROM ('2025-05-01') TO ('2025-06-01');
CREATE TABLE transactions_2025_06 PARTITION OF transactions 
    FOR VALUES FROM ('2025-06-01') TO ('2025-07-01');
CREATE TABLE transactions_2025_07 PARTITION OF transactions 
    FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');
CREATE TABLE transactions_2025_08 PARTITION OF transactions 
    FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');
CREATE TABLE transactions_2025_09 PARTITION OF transactions 
    FOR VALUES FROM ('2025-09-01') TO ('2025-10-01');
CREATE TABLE transactions_2025_10 PARTITION OF transactions 
    FOR VALUES FROM ('2025-10-01') TO ('2025-11-01');
CREATE TABLE transactions_2025_11 PARTITION OF transactions 
    FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE transactions_2025_12 PARTITION OF transactions 
    FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');

-- Create indexes on partitioned table (will be inherited by partitions)
CREATE INDEX idx_transactions_user_id ON transactions USING BTREE (user_id);
CREATE INDEX idx_transactions_amount ON transactions USING BTREE (amount);
CREATE INDEX idx_transactions_created_at ON transactions USING BTREE (created_at);
CREATE INDEX idx_transactions_user_date ON transactions USING BTREE (user_id, transaction_date);

-- Add constraints for data integrity
ALTER TABLE transactions ADD CONSTRAINT chk_user_id_positive CHECK (user_id > 0);
ALTER TABLE transactions ADD CONSTRAINT chk_amount_positive CHECK (amount > 0);
ALTER TABLE transactions ADD CONSTRAINT chk_transaction_id_format 
    CHECK (transaction_id ~ '^[A-Z0-9]{3,50}$');
ALTER TABLE transactions ADD CONSTRAINT chk_valid_date 
    CHECK (transaction_date >= '1900-01-01' AND transaction_date <= CURRENT_DATE + INTERVAL '1 year');

-- Create function for automatic partition creation
CREATE OR REPLACE FUNCTION create_monthly_partition(partition_date DATE)
RETURNS VOID AS $$
DECLARE
    start_date DATE := DATE_TRUNC('month', partition_date);
    end_date DATE := start_date + INTERVAL '1 month';
    partition_name TEXT := 'transactions_' || TO_CHAR(start_date, 'YYYY_MM');
BEGIN
    EXECUTE format('CREATE TABLE IF NOT EXISTS %I PARTITION OF transactions 
                   FOR VALUES FROM (%L) TO (%L)',
                   partition_name, start_date, end_date);
END;
$$ LANGUAGE plpgsql;

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger for updated_at (works on partitioned tables)
CREATE TRIGGER update_transactions_updated_at 
    BEFORE UPDATE ON transactions 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create materialized view for common aggregations
CREATE MATERIALIZED VIEW daily_transaction_summary AS
SELECT 
    transaction_date,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM transactions
GROUP BY transaction_date
ORDER BY transaction_date;

-- Create index on materialized view
CREATE UNIQUE INDEX idx_daily_summary_date ON daily_transaction_summary (transaction_date);

-- Create user summary materialized view
CREATE MATERIALIZED VIEW user_transaction_summary AS
SELECT 
    user_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(transaction_date) as first_transaction,
    MAX(transaction_date) as last_transaction
FROM transactions
GROUP BY user_id;

-- Create index on user summary
CREATE UNIQUE INDEX idx_user_summary_user_id ON user_transaction_summary (user_id);

-- Create function to refresh materialized views
CREATE OR REPLACE FUNCTION refresh_transaction_summaries()
RETURNS VOID AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY daily_transaction_summary;
    REFRESH MATERIALIZED VIEW CONCURRENTLY user_transaction_summary;
END;
$$ LANGUAGE plpgsql;

-- Optimize table storage
ALTER TABLE transactions SET (fillfactor = 90);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON transactions TO airflow;
GRANT USAGE, SELECT ON SEQUENCE transactions_id_seq TO airflow;
GRANT SELECT ON daily_transaction_summary TO airflow;
GRANT SELECT ON user_transaction_summary TO airflow;

-- Create temporary staging tables with optimized settings
CREATE UNLOGGED TABLE transactions_staging_bulk (
    transaction_id TEXT,
    user_id TEXT,
    amount TEXT,
    transaction_date TEXT
) WITH (fillfactor = 100, autovacuum_enabled = false);

CREATE UNLOGGED TABLE transactions_cleaned_bulk (
    transaction_id VARCHAR(50),
    user_id INTEGER,
    amount NUMERIC(15,2),
    transaction_date DATE
) WITH (fillfactor = 100, autovacuum_enabled = false);

-- Performance monitoring queries
-- Query to check partition sizes
CREATE VIEW partition_sizes AS
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables 
WHERE tablename LIKE 'transactions_%'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Query to check index usage
CREATE VIEW index_usage AS
SELECT 
    t.relname as table_name,
    indexrelname as index_name,
    c.reltuples as num_rows,
    pg_size_pretty(pg_relation_size(indexrelname::regclass)) as index_size,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched
FROM pg_stat_user_indexes 
JOIN pg_class c ON c.oid = pg_stat_user_indexes.relid
JOIN pg_class t ON t.oid = pg_stat_user_indexes.relid
WHERE t.relname = 'transactions'
ORDER BY index_scans DESC;