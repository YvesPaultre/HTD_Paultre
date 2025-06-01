-- ETL Pipeline
USE BankingDW;
GO
-- Begin with dimension tables

-- Create temp table for dim_account_holder
IF OBJECT_ID('[tempdb]..[#account_holder_staging]', 'U') IS NOT NULL
DROP TABLE #account_holder_staging
GO
CREATE TABLE #account_holder_staging(
    account_holder_key INT IDENTITY(1,1) PRIMARY KEY,
    account_holder_id VARCHAR(20) NOT NULL,
    account_holder_name VARCHAR(100) NOT NULL,
    address VARCHAR(200) NOT NULL,
    relationship_tier VARCHAR(20) NOT NULL,
    effective_date DATE NOT NULL,
    expiration_date DATE NULL,
    is_current BIT NOT NULL DEFAULT 1,
    created_date DATETIME NOT NULL DEFAULT GETDATE(),
    updated_date DATETIME NULL,
    change_type VARCHAR(20) NOT NULL
);
GO

-- Begin account_holder ETL process
WITH name_cleanup AS(
    SELECT
        account_holder_id,
        account_holder_name,
        (
        SELECT STRING_AGG(UPPER(SUBSTRING(value, 1, 1)) + LOWER(SUBSTRING(value, 2, LEN(value))), ' ')
        FROM STRING_SPLIT(TRIM(account_holder_name), ' ')
        ) as proper_case_name
    FROM stg_banking_transactions
    WHERE account_holder_name IS NOT NULL
    GROUP BY account_holder_id, account_holder_name
),
address_cleanup AS(
    SELECT
        account_holder_id,
        address,
        (
        SELECT STRING_AGG(UPPER(SUBSTRING(value, 1, 1)) + LOWER(SUBSTRING(value, 2, LEN(value))), ' ')
        FROM STRING_SPLIT(TRIM(address), ' ')
        ) as proper_case_address
    FROM stg_banking_transactions
    WHERE address IS NOT NULL
    GROUP BY account_holder_id, address
),
data_cleanup AS (
    SELECT DISTINCT
        TRIM(UPPER(stg.account_holder_id)) AS clean_id,
        REPLACE(REPLACE(REPLACE(nc.proper_case_name, ' ', '<>'), '><', ''), '<>', ' ') AS clean_name,
        REPLACE(REPLACE(REPLACE(ac.proper_case_address, ' ', '<>'), '><', ''), '<>', ' ') AS clean_address,
        CASE
            WHEN stg.relationship_tier IN('PREM', 'premium', 'Premium', 'PREMIUM') THEN 'Premium'
            WHEN stg.relationship_tier IN('PREF', 'preferred', 'Preferred', 'PREFERRED') THEN 'Preferred'
            WHEN stg.relationship_tier IN('STD', 'standard', 'Standard', 'STANDARD') THEN 'Standard'
            WHEN stg.relationship_tier IN('BASIC', 'basic', 'Basic') THEN 'Basic'
            ELSE 'Standard'
        END AS clean_tier,
        stg.transaction_date -- Passthrough for version control
    FROM stg_banking_transactions stg
        LEFT JOIN name_cleanup nc ON stg.account_holder_id = nc.account_holder_id AND stg.account_holder_name = nc.account_holder_name
        LEFT JOIN address_cleanup ac ON stg.account_holder_id = ac.account_holder_id AND stg.address = ac.address
    WHERE
        stg.account_holder_id IS NOT NULL AND
        (stg.account_holder_name IS NOT NULL AND stg.account_holder_name != '') AND
        stg.transaction_amount BETWEEN -50000 AND 50000
),
business_rules AS (
    SELECT dc.clean_id, dc.clean_name, dc.clean_address, dc.transaction_date,
        CASE
            WHEN dc.clean_tier = 'Premium' AND (dc.clean_address LIKE '%New York%' OR dc.clean_address LIKE '%Los Angeles%' OR dc.clean_address LIKE '%Chicago%') THEN 'Premium Metro'
            WHEN dc.clean_tier = 'Premium' THEN 'Premium Standard'
            ELSE dc.clean_tier
        END AS final_tier
    FROM data_cleanup dc
),
version_control AS(
    SELECT br.*,
        CASE
            WHEN br.transaction_date != MAX(br.transaction_date) OVER(PARTITION BY br.clean_id) THEN 0
            ELSE 1
        END AS is_current,
        CAST('2024-01-01' AS DATE) AS created_date,
        CASE
            WHEN br.transaction_date != MAX(br.transaction_date) OVER(PARTITION BY br.clean_id) THEN GETDATE()
            ELSE NULL
        END AS expiration_date,
        -- Change detection
        CASE
            WHEN ah.account_holder_key IS NULL THEN 'NEW' -- If no matching record then NEW
            WHEN ah.account_holder_name != br.clean_name OR ah.address != br.clean_address OR ah.relationship_tier != br.final_tier THEN 'UPDATED'
            ELSE 'UNCHANGED'
        END as change_type
    FROM business_rules br LEFT JOIN dim_account_holder ah ON br.clean_id = ah.account_holder_id AND ah.is_current = 1 -- Only compare to "current" records
),
final_staging AS (
    -- Search dim_account_holder for customer with matching account_holder_id 
    SELECT DISTINCT vc.clean_id, vc.clean_name, vc.clean_address, vc.final_tier, MAX(vc.is_current) AS is_current, vc.created_date, vc.change_type --MAX(is_current) ensures 1 returned if row is current, omitting false-expired duplicates
    FROM version_control vc
    GROUP BY vc.clean_id, vc.clean_name, vc.clean_address, vc.final_tier, vc.created_date, vc.change_type
)
INSERT INTO #account_holder_staging(account_holder_id, account_holder_name, address, relationship_tier, is_current, effective_date, created_date, expiration_date, change_type)
SELECT DISTINCT fs.clean_id, fs.clean_name, fs.clean_address, fs.final_tier, fs.is_current, GETDATE(), fs.created_date, CASE WHEN fs.is_current = 0 THEN GETDATE() ELSE NULL END, fs.change_type
FROM final_staging fs
WHERE fs.change_type != 'UNCHANGED' -- pre-filters out records not registered as new or updated
GO

-- Match and expire existing records for customers
UPDATE dim_account_holder SET
is_current = 0
WHERE account_holder_id IN(
    SELECT account_holder_id
    FROM #account_holder_staging
    WHERE change_type = 'UPDATED'
)
GO

INSERT INTO dim_account_holder(account_holder_id, account_holder_name, address, relationship_tier, is_current, effective_date, expiration_date, created_date)
SELECT account_holder_id, account_holder_name, address, relationship_tier, is_current, effective_date, expiration_date, created_date
FROM #account_holder_staging
GO

INSERT INTO dim_banking_product
SELECT DISTINCT banking_product_id, product_name, product_category, 1, GETDATE()
FROM stg_banking_transactions
GO

DELETE FROM dim_date WHERE year_number = 2024;

WITH date_generator AS (
    SELECT CAST('2024-01-01' AS DATE) AS date_value
    UNION ALL
    SELECT DATEADD(DAY, 1, date_value)
    FROM date_generator
    WHERE date_value < '2024-12-31'
)
INSERT INTO dim_date (
    date_key, 
    full_date, 
    year_number, 
    month_number, 
    day_number, 
    day_name, 
    month_name, 
    quarter_number, 
    is_weekend, 
    is_business_day
)
SELECT 
    CAST(FORMAT(date_value, 'yyyyMMdd') AS INT) AS date_key,
    date_value,
    YEAR(date_value),
    MONTH(date_value),
    DAY(date_value),
    DATENAME(WEEKDAY, date_value),
    DATENAME(MONTH, date_value),
    CASE 
        WHEN MONTH(date_value) IN (1,2,3) THEN 1
        WHEN MONTH(date_value) IN (4,5,6) THEN 2
        WHEN MONTH(date_value) IN (7,8,9) THEN 3
        ELSE 4
    END,
    CASE WHEN DATENAME(WEEKDAY, date_value) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END,
    CASE WHEN DATENAME(WEEKDAY, date_value) NOT IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END
FROM date_generator
OPTION (MAXRECURSION 400);
GO

-- CTEs needed for pre- vs. post-cleanup comparisons
WITH name_cleanup AS(
    SELECT
        account_holder_id,
        account_holder_name,
        (
        SELECT STRING_AGG(UPPER(SUBSTRING(value, 1, 1)) + LOWER(SUBSTRING(value, 2, LEN(value))), ' ')
        FROM STRING_SPLIT(TRIM(account_holder_name), ' ')
        ) as proper_case_name
    FROM stg_banking_transactions
    WHERE account_holder_name IS NOT NULL
    GROUP BY account_holder_id, account_holder_name
),
address_cleanup AS(
    SELECT
        account_holder_id,
        address,
        (
        SELECT STRING_AGG(UPPER(SUBSTRING(value, 1, 1)) + LOWER(SUBSTRING(value, 2, LEN(value))), ' ')
        FROM STRING_SPLIT(TRIM(address), ' ')
        ) as proper_case_address
    FROM stg_banking_transactions
    WHERE address IS NOT NULL
    GROUP BY account_holder_id, address
),
data_cleanup AS (
    SELECT DISTINCT
        TRIM(UPPER(stg.account_holder_id)) AS clean_id,
        nc.account_holder_name AS original_name,
        REPLACE(REPLACE(REPLACE(nc.proper_case_name, ' ', '<>'), '><', ''), '<>', ' ') AS clean_name,
        ac.address AS original_address,
        REPLACE(REPLACE(REPLACE(ac.proper_case_address, ' ', '<>'), '><', ''), '<>', ' ') AS clean_address,
        stg.relationship_tier AS original_tier,
        CASE
            WHEN stg.relationship_tier IN('PREM', 'premium', 'Premium', 'PREMIUM') THEN 'Premium'
            WHEN stg.relationship_tier IN('PREF', 'preferred', 'Preferred', 'PREFERRED') THEN 'Preferred'
            WHEN stg.relationship_tier IN('STD', 'standard', 'Standard', 'STANDARD') THEN 'Standard'
            WHEN stg.relationship_tier IN('BASIC', 'basic', 'Basic') THEN 'Basic'
            ELSE 'Standard'
        END AS clean_tier,
        stg.transaction_date -- Passthrough for version control
    FROM stg_banking_transactions stg
        LEFT JOIN name_cleanup nc ON stg.account_holder_id = nc.account_holder_id AND stg.account_holder_name = nc.account_holder_name
        LEFT JOIN address_cleanup ac ON stg.account_holder_id = ac.account_holder_id AND stg.address = ac.address
    WHERE
        stg.account_holder_id IS NOT NULL AND
        (stg.account_holder_name IS NOT NULL AND stg.account_holder_name != '') AND
        stg.transaction_amount BETWEEN -50000 AND 50000
),
business_rules AS (
    SELECT dc.clean_id, dc.original_name, dc.clean_name, dc.original_address, dc.clean_address, dc.transaction_date, dc.original_tier,
        CASE
            WHEN dc.clean_tier = 'Premium' AND (dc.clean_address LIKE '%New York%' OR dc.clean_address LIKE '%Los Angeles%' OR dc.clean_address LIKE '%Chicago%') THEN 'Premium Metro'
            WHEN dc.clean_tier = 'Premium' THEN 'Premium Standard'
            ELSE dc.clean_tier
        END AS final_tier
    FROM data_cleanup dc
)
INSERT INTO fact_transactions(
    account_holder_key,
    banking_product_key,
    date_key,
    transaction_id,
    account_number,
    transaction_amount,
    transaction_type,
    load_date
)
SELECT DISTINCT
    ah.account_holder_key,
    bp.banking_product_key,
    dd.date_key,
    stg.transaction_id,
    stg.account_number,
    stg.transaction_amount,
    stg.transaction_type,
    GETDATE()
FROM stg_banking_transactions stg
    LEFT JOIN business_rules br ON TRIM(UPPER(stg.account_holder_id)) = br.clean_id AND stg.account_holder_name = br.original_name AND stg.address = original_address AND stg.relationship_tier = br.original_tier AND stg.transaction_date = br.transaction_date -- Merge with business_rules on ALL matching columns
    LEFT JOIN dim_account_holder ah ON stg.account_holder_id = ah.account_holder_id AND br.clean_name = ah.account_holder_name AND br.clean_address = ah.address AND br.final_tier = ah.relationship_tier -- Must uniquely identify ONE customer instance
    LEFT JOIN dim_banking_product bp ON stg.banking_product_id = bp.banking_product_id
    LEFT JOIN dim_date dd ON CAST(stg.transaction_date AS DATE) = dd.full_date
WHERE -- Replicate constraints from account_holder process
    stg.account_holder_id IS NOT NULL AND
    (stg.account_holder_name IS NOT NULL AND stg.account_holder_name != '') AND
    stg.transaction_amount BETWEEN -50000 AND 50000 AND
    NOT EXISTS(
        SELECT 1 FROM fact_transactions ft
        WHERE ft.transaction_id = stg.transaction_id
        )-- Additional duplicate protection
ORDER BY stg.transaction_id;
GO

