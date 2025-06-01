USE BankingDW;
GO

-- Validate foreign key references
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Account Holder Foreign Key Check',
    'fact_transactions',
    COUNT(*),
    SUM(CASE WHEN ah.account_holder_key IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN ah.account_holder_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN ah.account_holder_key IS NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Orphaned Records Found'
FROM fact_transactions ft LEFT JOIN dim_account_holder ah ON ft.account_holder_key = ah.account_holder_key
GO

INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Banking Product Foreign Key Check',
    'fact_transactions',
    COUNT(*),
    SUM(CASE WHEN bp.banking_product_key IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN bp.banking_product_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN bp.banking_product_key IS NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Orphaned Records Found'
FROM fact_transactions ft LEFT JOIN dim_banking_product bp ON ft.banking_product_key = bp.banking_product_key
GO

INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Date Foreign Key Check',
    'fact_transactions',
    COUNT(*),
    SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN dd.date_key IS NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Orphaned Records Found'
FROM fact_transactions ft LEFT JOIN dim_date dd ON ft.date_key = dd.date_key
GO

-- Validate Values Within Range Constraints

-- Transaction Values Between -50000 and 50000
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Transaction Value Check',
    'fact_transactions',
    COUNT(*),
    SUM(CASE WHEN ft.transaction_amount > 50000.00 OR ft.transaction_amount < -50000.00 THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN ft.transaction_amount > 50000.00 OR ft.transaction_amount < -50000.00 THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN ft.transaction_amount > 50000.00 OR ft.transaction_amount < -50000.00 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Transactions Outside Constraints Found'
FROM fact_transactions ft
GO

-- No Future Dates
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Future Date Check',
    'fact_transactions',
    COUNT(*),
    SUM(CASE WHEN dd.full_date > GETDATE() THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN dd.full_date > GETDATE() THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN dd.full_date > GETDATE() THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Future Dates Found'
FROM fact_transactions ft LEFT JOIN dim_date dd ON ft.date_key = dd.date_key
GO

-- Account Number Matches Format
-- FORMAT: 8-10 Alphanumeric Characters
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Account Number Format Check',
    'fact_transactions',
    COUNT(*),
    SUM(CASE WHEN ft.account_number NOT LIKE '________%' THEN 1 ELSE 0 END), -- PATTERN: 8 characters minimum, 9th optional
    CASE WHEN SUM(CASE WHEN ft.account_number NOT LIKE '________%' THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN ft.account_number NOT LIKE '________%' THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Invalid Account Numbers Found'
FROM fact_transactions ft
GO

-- Expired Record Cannot Expire Before Effective
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Expiration Continuity Check',
    'dim_account_holder',
    COUNT(*),
    SUM(CASE WHEN ah.effective_date > ah.expiration_date THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN ah.effective_date > ah.expiration_date THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN ah.effective_date > ah.expiration_date THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Invalid Expiration Dates Found'
FROM dim_account_holder ah
WHERE ah.expiration_date IS NOT NULL
GO

-- Current Record Has No Expiration Date
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'Current Not Expired Check',
    'fact_transactions',
    COUNT(*),
    SUM(CASE WHEN ah.is_current = 1 AND ah.expiration_date IS NOT NULL THEN 1 ELSE 0 END),
    CASE WHEN SUM(CASE WHEN ah.is_current = 1 AND ah.expiration_date IS NOT NULL THEN 1 ELSE 0 END) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(CASE WHEN ah.is_current = 1 AND ah.expiration_date IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Records Found Marked CURRENT with EXPIRATION_DATE Listed'
FROM fact_transactions ft LEFT JOIN dim_account_holder ah ON ft.account_holder_key = ah.account_holder_key
GO

-- Validate Calculation Consistency
WITH multiple_active AS(
    SELECT COUNT(*) - 1 AS conflicts_found
    FROM dim_account_holder ah
    GROUP BY ah.account_holder_id, ah.is_current
)
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'One Current Record Per Account Check',
    'dim_account_holder',
    COUNT(*),
    SUM(ma.conflicts_found),
    CASE WHEN SUM(ma.conflicts_found) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(ma.conflicts_found) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Account With Multiple CURRENT Records Found'
FROM multiple_active ma
GO

WITH multiple_active AS(
    SELECT COUNT(*) - 1 AS conflicts_found
    FROM fact_transactions
    GROUP BY transaction_id
)
INSERT INTO validation_results(rule_name, table_name, records_checked, records_failed, validation_status, failure_percentage, check_date, error_details)
SELECT
    'One Transaction Per ID Check',
    'fact_transactions',
    COUNT(*),
    SUM(ma.conflicts_found),
    CASE WHEN SUM(ma.conflicts_found) = 0 THEN 'PASSED' ELSE 'FAILED' END,
    CAST(SUM(ma.conflicts_found) AS FLOAT) / COUNT(*),
    GETDATE(),
    'Duplicate transaction_id Found'
FROM multiple_active ma
GO

-- Collate And Display Validation Results
SELECT *
FROM validation_results
ORDER BY validation_status, check_date