SELECT 
    SalesOrderID,
    CustomerID,
    OrderDate,
    DueDate,
    ShipDate,
    SubTotal,
    TaxAmt,
    Freight,
    TotalDue,
    DATEDIFF(DAY, OrderDate, ShipDate) AS DaysToShip,
    CASE 
        WHEN TotalDue > 5000 THEN 'Large Order'
        WHEN TotalDue > 1000 THEN 'Medium Order'
        ELSE 'Small Order'
    END AS OrderSize
FROM Sales.SalesOrderHeader
WHERE OrderDate >= '2014-01-01'
    AND OnlineOrderFlag = 1
    AND TotalDue > 500
ORDER BY OrderDate DESC;