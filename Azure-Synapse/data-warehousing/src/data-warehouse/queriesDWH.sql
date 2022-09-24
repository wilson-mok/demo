-- Clean up
-- DELETE FROM Demo2.DimDate;
-- DELETE FROM Demo2.DimStockItem;
-- DELETE FROM Demo2.FactTransaction;

-- DimDate
SELECT *
FROM Demo2.DimDate;

SELECT count(*)
FROM Demo2.DimDate;

--  Duplicate check
SELECT nkDateId, count(*)
FROM Demo2.DimDate
GROUP BY nkDateId
HAVING count(*) > 1;

-----------------------------------
-- DimStockItem
SELECT *
FROM [Demo2].[DimStockItem];

SELECT count(*)
FROM [Demo2].[DimStockItem];
-- Start with 227 
-- After expired records: 237

-- Active check
SELECT active, count(*)
FROM Demo2.DimStockItem
GROUP BY active;
-- Starts with 227 active
-- Ends with 10 inactive, 227 active


-------------------------------------
-- FactTransaction
SELECT TOP 10 *
FROM [Demo2].[FactTransaction];

SELECT *
FROM [Demo2].[DimStockItem]
WHERE nkStockItemId = 86
ORDER BY effectiveDate DESC;
-- We have 2 StockItems. One is active and one is expired.
-- We should expect the FactTransactions to map to the inactive record.

SELECT *
FROM [Demo2].[DimDate]
WHERE date = '2021-12-31';

-- Check StockItem mapping
SELECT distinct t.skStockItem
FROM [Demo2].[FactTransaction] t
JOIN [Demo2].[DimStockItem] si ON (si.skStockItem = t.skStockItem)
WHERE si.nkStockItemId = 86;
-- All the transactions took place before 2022-09-05. Because of this, the mapping should show the expired skStockItem

-- Complete query
SELECT d.month, sum(t.quantity) as 'totalQuantitySold', sum(profit) as 'totalProfit', sum(totalBeforeTax) as 'totalRevenueBeforeTax'
FROM [Demo2].[FactTransaction] t
JOIN [Demo2].[DimStockItem] si ON (si.skStockItem = t.skStockItem)
JOIN [Demo2].[DimDate] d ON (d.skDate = t.skInvoiceDate)
WHERE d.year = 2022
GROUP BY d.month
ORDER BY d.month;
