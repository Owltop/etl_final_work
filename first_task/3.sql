SELECT region_code, COUNT(*) AS cnt
FROM `transactions_v2`
GROUP BY region_code
ORDER BY cnt DESC;
