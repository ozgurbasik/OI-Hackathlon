SELECT 
    ProductID,
    Name AS ProductName,
    ProductNumber,
    ListPrice,
    StandardCost,
    Color,
    Size,
    Weight,
    DaysToManufacture,
    CASE 
        WHEN ListPrice > 1000 THEN 'Premium'
        WHEN ListPrice > 500 THEN 'Mid-Range'
        ELSE 'Standard'
    END AS PriceCategory
FROM Production.Product
WHERE ListPrice > 100 
    AND SellEndDate IS NULL
    AND Color IS NOT NULL
ORDER BY ListPrice DESC;