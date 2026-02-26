-- Monthly Revenue Summary

SELECT
    strftime('%Y-%m', OrderDate) AS SalesMonth,
    SUM(TotalOrderValue) AS MonthlyRevenue,
    COUNT(DISTINCT OrderID) AS TotalOrders,
    COUNT(DISTINCT CustomerID) AS UniqueCustomers
FROM sales_data
GROUP BY SalesMonth
ORDER BY SalesMonth;