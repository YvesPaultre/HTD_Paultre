USE BankingDW;
GO

-------------------------------------------------------------------------
-- Window Function: Moving Average
-- Business Purpose:
--      Evaluate per-customer action trends over a short window
--      Evaluate deposit/withdrawal trends to see how a customer account fluctuates over time
--      Unusual patterns might indicate large expenditures
-------------------------------------------------------------------------
SELECT
    ah.account_holder_id,
    ah.account_holder_name,
    dd.full_date,
    AVG(ft.transaction_amount) OVER(PARTITION BY ah.account_holder_key ORDER BY dd.full_date) AS moving_3_transaction_avg
FROM dim_account_holder ah LEFT JOIN fact_transactions ft ON ah.account_holder_key = ft.account_holder_key
    LEFT JOIN dim_date dd ON ft.date_key = dd.date_key
GO

-------------------------------------------------------------------------
-- Window Function: Running Total
-- Business Purpose:
--      Evaluate account net gain/loss over time
--      Negative totals indicate account deficit/overdrawn
--      Enables analysis of customer 'net value'
-------------------------------------------------------------------------
SELECT
    ah.account_holder_id,
    ah.account_holder_name,
    dd.full_date,
    SUM(ft.transaction_amount) OVER(PARTITION BY ah.account_holder_key ORDER BY dd.full_date ROWS UNBOUNDED PRECEDING) AS account_running_balance
FROM dim_account_holder ah LEFT JOIN fact_transactions ft ON ah.account_holder_key = ft.account_holder_key
    LEFT JOIN dim_date dd ON ft.date_key = dd.date_key
GO

-------------------------------------------------------------------------
-- Window Function: Ranking
-- Business Purpose:
--      Evaluate value of individual transactions per customer.
--      Rank 1 transactions represent the largest deposit.
--      Relationships can be evaluated based on transaction volume.
-------------------------------------------------------------------------
SELECT
    ah.account_holder_id,
    ah.account_holder_name,
    dd.full_date,
    ft.transaction_amount,
    ft.transaction_type,
    DENSE_RANK() OVER(PARTITION BY ah.account_holder_key ORDER BY ft.transaction_amount DESC) AS account_transaction_rank
FROM dim_account_holder ah LEFT JOIN fact_transactions ft ON ah.account_holder_key = ft.account_holder_key
    LEFT JOIN dim_date dd ON ft.date_key = dd.date_key
GO