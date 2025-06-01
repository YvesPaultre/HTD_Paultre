USE master
GO
IF NOT EXISTS (
    SELECT [name]
        FROM sys.databases
        WHERE [name] = N'BankingDW'
)
CREATE DATABASE BankingDW
GO

USE BankingDW;
GO

-- Drop existing tables if they already exist

-- Start with fact table, otherwise dependencies fail
IF OBJECT_ID('[dbo].[fact_transactions]', 'U') IS NOT NULL
DROP TABLE [dbo].[fact_transactions]
GO

IF OBJECT_ID('[dbo].[dim_account_holder]', 'U') IS NOT NULL
DROP TABLE [dbo].[dim_account_holder]
GO

IF OBJECT_ID('[dbo].[dim_banking_product]', 'U') IS NOT NULL
DROP TABLE [dbo].[dim_banking_product]
GO

IF OBJECT_ID('[dbo].[dim_date]', 'U') IS NOT NULL
DROP TABLE [dbo].[dim_date]
GO

IF OBJECT_ID('[dbo].[validation_results]', 'U') IS NOT NULL
DROP TABLE [dbo].[validation_results]
GO

-- Create dimensions BEFORE fact

---------------------------------------------------------------------------------
-- TABLE: dim_account_holder
-- SCD TYPE: 2
-- BUSINESS KEY: account_holder_id
-- TRACKED ATTRIBUTES: account_holder_name, address, relationship_tier
---------------------------------------------------------------------------------
CREATE TABLE dim_account_holder (
    account_holder_key INT IDENTITY(1,1) PRIMARY KEY,
    account_holder_id VARCHAR(20) NOT NULL,
    account_holder_name VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    relationship_tier VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    updated_date DATETIME NULL
);
GO

---------------------------------------------------------------------------------
-- TABLE: dim_banking_product
-- SCD TYPE: 2 supported, 1 used
-- BUSINESS KEY: banking_product_id
-- TRACKED ATTRIBUTES: N/A (product_name, product_category supported)
---------------------------------------------------------------------------------
CREATE TABLE dim_banking_product (
    banking_product_key INT IDENTITY(1,1) PRIMARY KEY,
    banking_product_id VARCHAR(20) NOT NULL UNIQUE,
    product_name VARCHAR(100) NOT NULL,
    product_category VARCHAR(50) NOT NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE()
);
GO

---------------------------------------------------------------------------------
-- TABLE: dim_date
-- SCD TYPE: 1
-- BUSINESS KEY: full_date
-- PURPOSE: Identify a date by its key and provide supplementary information about that date.
---------------------------------------------------------------------------------
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    day_number INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter_number INT NOT NULL,
    is_weekend BIT NOT NULL,
    is_business_day BIT NOT NULL
);
GO

---------------------------------------------------------------------------------
-- TABLE: fact_transactions
-- GRAIN STATEMENT:
--      transaction_key: Primary key. Uniquely identifies a row in the table.
--      account_holder_key: Foreign key. Identifies an account holder instance in dim_account_holder.
--      banking_product_key: Foreign key. Identifies a banking product instance in dim_banking_product.
-- MEASURES:
--      transaction_id: Natural key. Uniquely identifies a transaction in a form meaningful to humans.
--      account_number: Supplementary data. Provides additional context on an account holder.
--      transaction_amount: Supplementary data. Notates the amount handled by the transaction.
--      transaction_type: Supplementary data. Generally one of: Fee, Payment, Withdrawal, Transfer, or Deposit.
---------------------------------------------------------------------------------
CREATE TABLE fact_transactions (
    transaction_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    account_holder_key INT NOT NULL,
    banking_product_key INT NOT NULL,
    date_key INT NOT NULL,
    transaction_id VARCHAR(50) NOT NULL UNIQUE,
    account_number VARCHAR(20) NOT NULL,
    transaction_amount DECIMAL(12,2) NOT NULL,
    transaction_type VARCHAR(30) NOT NULL,
    load_date DATETIME NOT NULL DEFAULT GETDATE(),
    FOREIGN KEY (account_holder_key) REFERENCES dim_account_holder(account_holder_key),
    FOREIGN KEY (banking_product_key) REFERENCES dim_banking_product(banking_product_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
);
GO

---------------------------------------------------------------------------------
-- TABLE: validation_results
-- PURPOSE: Store results of various validation processes, used for data parity
-- KEY ATTRIBUTES: rule_name, table_name, records_checked, records_failed, failure_percentage, check_date, error_details
---------------------------------------------------------------------------------
CREATE TABLE validation_results (
    validation_id INT IDENTITY(1,1) PRIMARY KEY,
    rule_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    records_checked INT NOT NULL,
    records_failed INT NOT NULL,
    validation_status VARCHAR(20) NOT NULL,
    failure_percentage DECIMAL(5,2) NOT NULL,
    check_date DATETIME NOT NULL DEFAULT GETDATE(),
    error_details NVARCHAR(500) NULL
);
GO