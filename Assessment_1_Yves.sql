-- =====================================================
-- Assessment # 1: Retail Sales Database Analysis
--
-- Submitted By: Yves Paultre
--
-- =====================================================

-- Initial database creation

USE master
GO
IF NOT EXISTS (
    SELECT [name]
        FROM sys.databases
        WHERE [name] = N'RetailSales'
)
CREATE DATABASE RetailSales
GO

USE RetailSales;
GO

-- Drop existing tables (except Staging)

IF OBJECT_ID('[dbo].[Orders]', 'U') IS NOT NULL
DROP TABLE [dbo].[Orders]
GO

IF OBJECT_ID('[dbo].[Products]', 'U') IS NOT NULL
DROP TABLE [dbo].[Products]
GO

IF OBJECT_ID('[dbo].[Categories]', 'U') IS NOT NULL
DROP TABLE [dbo].[Categories]
GO

IF OBJECT_ID('[dbo].[Customers]', 'U') IS NOT NULL
DROP TABLE [dbo].[Customers]
GO

IF OBJECT_ID('[dbo].[Stores]', 'U') IS NOT NULL
DROP TABLE [dbo].[Stores]
GO

-- (re)Create tables

CREATE TABLE [dbo].[Stores]
(
    store_id INT PRIMARY KEY,
    store_name VARCHAR(50) NOT NULL,
    store_city VARCHAR(50) NOT NULL,
    store_state VARCHAR(2) NOT NULL
);
GO

CREATE TABLE [dbo].[Categories]
(
    category_id INT IDENTITY(1,1) PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL
);
GO

CREATE TABLE [dbo].[Products]
(
    product_id INT PRIMARY KEY,
    category_id INT NOT NULL,
    product_name VARCHAR(50) NOT NULL,
    price DECIMAL(10,2) NOT NULL,

    CONSTRAINT fk_product_category FOREIGN KEY (category_id) REFERENCES Categories(category_id)
);
GO

CREATE TABLE [dbo].[Customers]
(
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(255),
    customer_city VARCHAR(50) NOT NULL,
    customer_state VARCHAR(2) NOT NULL,
    loyalty BIT NOT NULL
);
GO

CREATE TABLE [dbo].[Orders]
(
    order_id INT PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    store_id INT NOT NULL,
    sale_date DATE NOT NULL,
    quantity INT NOT NULL,
    total_amt DECIMAL(12,2) NOT NULL,

    CONSTRAINT fk_order_customer FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    CONSTRAINT fk_order_product FOREIGN KEY (product_id) REFERENCES Products(product_id),
    CONSTRAINT fk_order_store FOREIGN KEY (store_id) REFERENCES Stores(store_id),
    CONSTRAINT quantity_pos CHECK (quantity > 0),
    CONSTRAINT total_pos CHECK (total_amt > 0)
);
GO

-- Populate normalized tables from staging table

INSERT INTO Stores(store_id, store_name, store_city, store_state)
    SELECT DISTINCT StoreId, StoreName, StoreCity, StoreState
    FROM Staging_RetailSales;
GO

INSERT INTO Categories(category_name)
    SELECT DISTINCT Category FROM Staging_RetailSales;
GO

INSERT INTO Products(product_id, category_id, product_name, price)
    SELECT DISTINCT st.ProductID, c.category_id, st.ProductName, st.UnitPrice
    FROM Staging_RetailSales st LEFT JOIN Categories c ON st.Category = c.category_name;
GO

INSERT INTO Customers(customer_id, first_name, last_name, email, customer_city, customer_state, loyalty)
    SELECT DISTINCT CustomerID, FirstName, LastName, Email, CustomerCity, CustomerState,
        CASE
            WHEN LoyaltyMember = '1' THEN 1
            WHEN LoyaltyMember = '0' THEN 0
            ELSE 0
        END
    FROM Staging_RetailSales;
GO

INSERT INTO Orders(order_id, customer_id, product_id, store_id, sale_date, quantity, total_amt)
    SELECT SaleID, CustomerID, ProductID, StoreID, SaleDate, Quantity, TotalAmount
    FROM Staging_RetailSales;
GO

-- =====================================================
-- Task 1: Count the number of sales by product category
-- =====================================================

SELECT c.category_name, COUNT(*) AS num_orders
FROM Orders o LEFT JOIN Products p ON o.product_id = p.product_id
    LEFT JOIN  Categories c ON p.category_id = c.category_id
GROUP BY c.category_name;
GO

-- =====================================================
-- Task 2: Alter the Sales table to add a Discount column.
-- =====================================================

ALTER TABLE Orders
ADD discount DECIMAL(10,2);
GO

UPDATE Orders SET discount = 0.0;
GO

SELECT TOP 5 * FROM Orders;
GO

-- =====================================================
-- Task 3: Update the LoyaltyMember status for high-spending customers.
-- =====================================================

UPDATE Customers SET loyalty = 1
WHERE customer_id IN (
    SELECT customer_id
    FROM (
        SELECT DISTINCT c.customer_id, SUM(total_amt) AS customer_total
        FROM Customers c LEFT JOIN Orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
    ) AS CustomerExpenses
    WHERE customer_total > 1000.00
);
GO

CREATE TABLE #UpdatedCustomers(
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(255),
    customer_city VARCHAR(50) NOT NULL,
    customer_state VARCHAR(2) NOT NULL,
    loyalty BIT NOT NULL
);
GO

INSERT INTO #UpdatedCustomers
SELECT * FROM Customers WHERE loyalty = 1;
GO

SELECT COUNT(*) AS record_count FROM #UpdatedCustomers;

SELECT TOP 5 * FROM #UpdatedCustomers;
GO

-- =====================================================
-- Task 4: Find the total sales amount for each store.
-- =====================================================

SELECT s.store_name, SUM(o.total_amt) AS total_sales
FROM Stores s LEFT JOIN Orders o ON s.store_id = o.store_id
GROUP BY s.store_name;
GO

-- =====================================================
-- Task 5: List the top 5 customers by total purchase amount.
-- =====================================================

SELECT TOP 5 c.first_name, c.last_name, SUM(o.total_amt) AS total_purchases
FROM Customers c LEFT JOIN Orders o ON c.customer_id = o.customer_id
GROUP BY c.first_name, c.last_name
ORDER BY total_purchases DESC;
GO

-- =====================================================
-- Task 6: Find the average sale amount for loyalty members vs. non-members.
-- =====================================================

SELECT 
    CASE
        WHEN c.loyalty = 1 THEN 'Loyalty Member'
        WHEN c.loyalty = 0 THEN 'Non-Loyalty Member'
    END AS loyalty_status,
    AVG(total_amt) AS avg_sale_amount
FROM Customers c LEFT JOIN Orders o ON c.customer_id = o.customer_id
GROUP BY c.loyalty;
GO

-- =====================================================
-- Task 7: Create a nonclustered index on the Sales table.
-- =====================================================

CREATE INDEX IX_Orders_SaleDate ON [RetailSales].[dbo].[Orders] ([sale_date] DESC);
GO

SELECT i.name AS index_name, o.name AS object_name, c.name AS column_name 
FROM sys.indexes i INNER JOIN sys.objects o ON i.[object_id] = o.[object_id]
    INNER JOIN sys.columns c ON i.[object_id] = c.[object_id]
WHERE i.name = 'IX_Orders_SaleDate';
GO

-- =====================================================
-- Task 8: Find the store with the highest average sale amount.
-- =====================================================

SELECT s.store_name, AVG(o.total_amt) AS avg_sale_amount
FROM Stores s LEFT JOIN Orders o ON s.store_id = o.store_id
GROUP BY s.store_name;
GO

-- =====================================================
-- Task 9: List sales where the total amount exceeds $500.
-- =====================================================

SELECT o.order_id, o.customer_id, p.product_name, s.store_name, o.total_amt
FROM Orders o LEFT JOIN Products p ON o.product_id = p.product_id
    LEFT JOIN Stores s ON o.store_id = s.store_id
WHERE o.total_amt > 500.00;
GO

-- =====================================================
-- Task 10: Calculate the total quantity sold by state.
-- =====================================================

SELECT s.store_state, SUM(o.quantity) AS total_quantity
FROM Stores s LEFT JOIN Orders o ON s.store_id = o.store_id
GROUP BY s.store_state;
GO

-- =====================================================
-- Task 11: Find the top 3 product categories by total sales amount using a CTE.
-- =====================================================

WITH ProductCategorySales AS (
    SELECT
        c.category_name,
        o.total_amt
    FROM
        Orders o LEFT JOIN Products p ON o.product_id = p.product_id
        LEFT JOIN Categories c ON p.category_id = c.category_id
)
SELECT TOP 3 category_name, SUM(total_amt) AS category_sales
FROM ProductCategorySales
GROUP BY category_name;
GO

-- =====================================================
-- Task 12: Find customers who made purchases in multiple states.
-- =====================================================

SELECT DISTINCT c.customer_id, c.first_name, c.last_name, COUNT(DISTINCT s.store_state) AS order_states
FROM Customers c LEFT JOIN Orders o ON c.customer_id = o.customer_id
    LEFT JOIN Stores s ON o.store_id = s.store_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(DISTINCT s.store_state) >= 2;
GO

-- =====================================================
-- Task 13: Analyze sales trends by month using a temporary table.
-- =====================================================

CREATE TABLE #MonthlyTotals(
    year VARCHAR(4) NOT NULL,
    month VARCHAR(2) NOT NULL,
    monthly_total DECIMAL(12,2) NOT NULL,

    CONSTRAINT pk_month_year PRIMARY KEY(year, month)
);
GO

INSERT INTO #MonthlyTotals(year, month, monthly_total)
    SELECT YEAR(sale_date), MONTH(sale_date), SUM(total_amt)
    FROM Orders
    GROUP BY YEAR(sale_date), MONTH(sale_date);
GO

SELECT TOP 1 * FROM #MonthlyTotals
ORDER BY monthly_total;
GO

-- =====================================================
-- Task 14: Create a view for customer purchase history.
-- =====================================================

CREATE VIEW vw_customer_purchases AS
    SELECT c.customer_id, c.first_name, c.last_name, c.email, c.customer_city, c.customer_state, c.loyalty,
        p.product_name, p.price, o.quantity, o.total_amt, o.sale_date
    FROM Customers c LEFT JOIN Orders o ON c.customer_id = o.customer_id
        LEFT JOIN Products p ON o.product_id = p.product_id;
GO

SELECT TOP 5 * FROM vw_customer_purchases;
GO

-- =====================================================
-- Task 15: Create a stored procedure to retrieve sales by date range.
-- =====================================================

IF EXISTS (
SELECT *
    FROM INFORMATION_SCHEMA.ROUTINES
WHERE SPECIFIC_SCHEMA = N'dbo'
    AND SPECIFIC_NAME = N'sp_SalesByDateRange'
    AND ROUTINE_TYPE = N'PROCEDURE'
)
DROP PROCEDURE dbo.sp_SalesByDateRange
GO
CREATE PROCEDURE dbo.sp_SalesByDateRange
    @start_date DATE,
    @end_date DATE
AS
BEGIN
    SELECT * FROM Orders WHERE sale_date BETWEEN @start_date AND @end_date
END
GO

EXECUTE dbo.sp_SalesByDateRange '2024-01-01' , '2024-12-31'
GO

-- =====================================================
-- Task 16: Generate a comprehensive sales report.
-- =====================================================

SELECT c.category_name, s.store_state, YEAR(o.sale_date) AS sale_year, SUM(o.total_amt) AS category_total, SUM(o.quantity) AS quantity_sold
FROM Orders o LEFT JOIN Stores s ON o.store_id = s.store_id
    LEFT JOIN Products p ON o.product_id = p.product_id
    LEFT JOIN Categories c ON p.category_id = c.category_id
GROUP BY c.category_name, s.store_state, YEAR(o.sale_date);
GO