-- Create a separate database for application data
CREATE DATABASE finance_app;

-- Connect to the finance_app database
\c finance_app;

-- Create transactions table with improved schema
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    user_id INT NOT NULL,
    amount NUMERIC(15,2) NOT NULL,
    transaction_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Create performance indexes
CREATE INDEX idx_transactions_user_id ON transactions(user_id);
CREATE INDEX idx_transactions_date ON transactions(transaction_date);
CREATE INDEX idx_transactions_user_date ON transactions(user_id, transaction_date);
CREATE INDEX idx_transactions_amount ON transactions(amount);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);

-- Add data integrity constraints
ALTER TABLE transactions ADD CONSTRAINT chk_user_id_positive CHECK (user_id > 0);
ALTER TABLE transactions ADD CONSTRAINT chk_amount_not_null CHECK (amount IS NOT NULL);
ALTER TABLE transactions ADD CONSTRAINT chk_transaction_id_format CHECK (transaction_id ~ '^[A-Z0-9]{3,50}$');

-- Create function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_transactions_updated_at 
    BEFORE UPDATE ON transactions 
    FOR EACH ROW 
    EXECUTE FUNCTION update_updated_at_column();

-- Create staging table for ETL process
CREATE TABLE IF NOT EXISTS transactions_staging (
    transaction_id VARCHAR(50),
    user_id VARCHAR(50),  -- Changed to VARCHAR to handle potential bad data
    amount VARCHAR(50),
    transaction_date VARCHAR(50)
);

-- Create temporary staging table for ETL process
CREATE TABLE IF NOT EXISTS transactions_staging_temp (
    transaction_id VARCHAR(50),
    user_id VARCHAR(50),
    amount VARCHAR(50),
    transaction_date VARCHAR(50)
);

-- Create cleaned transactions table for ETL process
CREATE TABLE IF NOT EXISTS transactions_cleaned (
    transaction_id VARCHAR(50),
    user_id INT,
    amount NUMERIC(15,2),
    transaction_date DATE
);

-- Switch back to airflow database for Airflow metadata
\c airflow;