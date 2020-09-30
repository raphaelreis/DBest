
-- Store_sales.data table queries -- 
-- Average price of sales
SELECT AVG("_c13") FROM store_sales;

-- Count the number of sales for ticket_number 1
SELECT COUNT(*) FROM store_sales
WHERE "_c10" BETWEEN 50 AND 100;

-- Net paid sum of all transactions grouped by day
SELECT "_c0", SUM("_c20")
FROM store_sales
GROUP BY "_c0"
