-- Invoice query
SELECT i.InvoiceDate, il.StockItemID, sum(il.UnitPrice*il.Quantity) as 'revenuePreTax', sum(il.LineProfit) as 'totalProfit'
FROM Demo2.InvoiceLines il
JOIN Demo2.Invoices i ON (i.InvoiceID = il.InvoiceID)
GROUP BY i.InvoiceDate, il.StockItemID
ORDER BY i.InvoiceDate DESC;

-- This table emulates the original state of StockItems. We do not have any changes or historical records.
SELECT TOP 10 *
FROM Demo2.StockItems;

-- This table emulates result of the UNION between the StockItems and StockItems_Archive.
--   For example, StockItemID = 77. The lead time is changed from 7 to 14 days starting on Sep 5, 2022
SELECT StockItemID, StockItemName, LeadTimeDays, ValidFrom, ValidTo
FROM Demo2.StockItemsChange
ORDER BY StockItemID, ValidFrom DESC;

SELECT count(*)
FROM Demo2.StockItemsChange;
-- 20

SELECT distinct StockItemID
FROM Demo2.StockItemsChange;
-- 77, 78, 80, 84, 86, 95, 98, 184, 193, 204
