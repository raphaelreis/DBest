
-- Store_sales.data table queries -- 
-- 1) Count the number of sales for ticket_number 1
SELECT COUNT(*) FROM store_sales
WHERE "_c12" BETWEEN 50 AND 100;

-- 2) Average price of sales
SELECT AVG("_c20") FROM store_sales
WHERE "_c13" BETWEEN 5 AND 70;

-- 3) Average price of sales
SELECT SUM("_c20") FROM store_sales
WHERE "_c13" BETWEEN 5 AND 70;
