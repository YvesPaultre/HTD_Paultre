# Community Bank Data Warehouse

## Setup Instructions

### Prerequisites
- A SQL Server Database
- Azure Data Studio or equivalent environment

### Step-by-Step Setup
1. Launch your SQL Server instance.
2. Connect to your SQL Server instance with Azure Data Studio.
3. Run the `schema_ddl.sql` file.
4. Use the Import Wizard to import your csv file as `stg_banking_transactions`.
5. Run the `etl_pipeline.sql` file.
6. Run the `validation_checks.sql` file.
7. Run the `window_functions.sql` file.

### Expected Results
- Insertion results:
    - `dim_account_holder`: 178 records (150 unique `customer_id`s)
    - `dim_banking_product`: 16 records
    - `dim_date`: 366 records (2024 was a leap year)
    - `fact_transactions`: 1000 records

## Schema Design

### Star Schema Overview
Dimensional model provided as:
```
---------------------------------------------------------------------------------
-- TABLE: dim_account_holder
-- SCD TYPE: 2
-- BUSINESS KEY: account_holder_id
-- TRACKED ATTRIBUTES: account_holder_name, address, relationship_tier
---------------------------------------------------------------------------------

---------------------------------------------------------------------------------
-- TABLE: dim_banking_product
-- SCD TYPE: 2 supported, 1 used
-- BUSINESS KEY: banking_product_id
-- TRACKED ATTRIBUTES: N/A (product_name, product_category supported)
---------------------------------------------------------------------------------

---------------------------------------------------------------------------------
-- TABLE: dim_date
-- SCD TYPE: 1
-- BUSINESS KEY: full_date
-- PURPOSE: Identify a date by its key and provide supplementary information about that date.
---------------------------------------------------------------------------------

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
```

### Business Rules Applied
- Key Business Rules:
    - Apply specific variants of 'Premium' customer tier to customers in different locales
    - Exclude transactions outside of logical bounds (> 50000.00 or < -50000.00)
    - Exclude transactions listed as occurring at a future date

## Key Assumptions

### Data Quality Assumptions
- Assumes data is in a consistent CSV format
- Assumes `account_holder_id` is an 8-9 character alphanumeric string (based on dataset analysis)

### Business Logic Assumptions  
- Assume that all transactions logged have been externally verified

### Technical Assumptions
- Explicitly handled customer versioning for batch inserts to avoid future data conflicts.

## Validation Results

### Summary by Category
|validation_id|rule_name|table_name|records_checked|records_failed|validation_status|failure_percentage|check_date|error_details|
|---|---|---|---|---|---|---|---|---|
|1|Account Holder Foreign Key Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.593|Orphaned Records Found|
|2|Banking Product Foreign Key Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.613|Orphaned Records Found|
|3|Date Foreign Key Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.623|Orphaned Records Found|
|4|Transaction Value Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.633|Transactions Outside Constraints Found|
|5|Future Date Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.640|Future Dates Found|
|6|Account Number Format Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.643|Invalid Account Numbers Found|
|7|Expiration Continuity Check|dim_account_holder|28|0|PASSED|0.00|2025-06-01 16:31:27.653|Invalid Expiration Dates Found|
|8|Current Not Expired Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.657|Records Found Marked CURRENT with EXPIRATION_DATE Listed|
|9|One Current Record Per Account Check|dim_account_holder|178|0|PASSED|0.00|2025-06-01 16:31:27.670|Account With Multiple CURRENT Records Found|
|10|One Transaction Per ID Check|fact_transactions|1000|0|PASSED|0.00|2025-06-01 16:31:27.673|Duplicate transaction_id Found|

### Data Quality Score
All constraints validated