WITH EmployeeDetails AS (
    SELECT 
        e.BusinessEntityID,
        p.FirstName,
        p.LastName,
        e.JobTitle,
        e.HireDate,
        e.VacationHours,
        e.SickLeaveHours,
        e.SalariedFlag,
        dh.DepartmentID,
        d.Name AS DepartmentName,
        CASE 
            WHEN e.SalariedFlag = 1 THEN 'Salaried'
            ELSE 'Hourly'
        END AS PayType,
        CASE 
            WHEN YEAR(GETDATE()) - YEAR(e.HireDate) >= 10 THEN 'Senior'
            WHEN YEAR(GETDATE()) - YEAR(e.HireDate) >= 5 THEN 'Mid-Level'
            ELSE 'Junior'
        END AS ExperienceLevel
    FROM HumanResources.Employee e
    INNER JOIN Person.Person p ON e.BusinessEntityID = p.BusinessEntityID
    INNER JOIN HumanResources.EmployeeDepartmentHistory dh ON e.BusinessEntityID = dh.BusinessEntityID
    INNER JOIN HumanResources.Department d ON dh.DepartmentID = d.DepartmentID
    WHERE e.CurrentFlag = 1 
        AND dh.EndDate IS NULL
),
DepartmentStats AS (
    SELECT 
        DepartmentName,
        COUNT(*) AS TotalEmployees,
        COUNT(CASE WHEN PayType = 'Salaried' THEN 1 END) AS SalariedEmployees,
        COUNT(CASE WHEN PayType = 'Hourly' THEN 1 END) AS HourlyEmployees,
        COUNT(CASE WHEN ExperienceLevel = 'Senior' THEN 1 END) AS SeniorEmployees,
        COUNT(CASE WHEN ExperienceLevel = 'Mid-Level' THEN 1 END) AS MidLevelEmployees,
        COUNT(CASE WHEN ExperienceLevel = 'Junior' THEN 1 END) AS JuniorEmployees,
        AVG(VacationHours) AS AvgVacationHours,
        AVG(SickLeaveHours) AS AvgSickLeaveHours,
        MAX(VacationHours) AS MaxVacationHours,
        MIN(VacationHours) AS MinVacationHours
    FROM EmployeeDetails
    GROUP BY DepartmentName
),
PayTypeBreakdown AS (
    SELECT 
        DepartmentName,
        PayType,
        COUNT(*) AS CountByPayType,
        AVG(VacationHours) AS AvgVacationByPayType,
        AVG(SickLeaveHours) AS AvgSickLeaveByPayType
    FROM EmployeeDetails
    GROUP BY DepartmentName, PayType
)
SELECT 
    ed.DepartmentName,
    ed.FirstName,
    ed.LastName,
    ed.JobTitle,
    ed.HireDate,
    ed.PayType,
    ed.ExperienceLevel,
    ed.VacationHours,
    ed.SickLeaveHours,
    ds.TotalEmployees,
    ds.SalariedEmployees,
    ds.HourlyEmployees,
    ds.SeniorEmployees,
    ds.MidLevelEmployees,
    ds.JuniorEmployees,
    ROUND(ds.AvgVacationHours, 1) AS DeptAvgVacation,
    ROUND(ds.AvgSickLeaveHours, 1) AS DeptAvgSickLeave,
    CASE 
        WHEN ed.VacationHours > ds.AvgVacationHours THEN 'Above Average'
        WHEN ed.VacationHours = ds.AvgVacationHours THEN 'Average'
        ELSE 'Below Average'
    END AS VacationComparison,
    CASE 
        WHEN ds.TotalEmployees > 20 THEN 'Large Department'
        WHEN ds.TotalEmployees > 10 THEN 'Medium Department'
        ELSE 'Small Department'
    END AS DepartmentSize,
    ROUND((ds.SalariedEmployees * 100.0) / ds.TotalEmployees, 1) AS SalariedPercentage,
    ptb.CountByPayType,
    ROUND(ptb.AvgVacationByPayType, 1) AS PayTypeAvgVacation
FROM EmployeeDetails ed
INNER JOIN DepartmentStats ds ON ed.DepartmentName = ds.DepartmentName
INNER JOIN PayTypeBreakdown ptb ON ed.DepartmentName = ptb.DepartmentName 
    AND ed.PayType = ptb.PayType
ORDER BY ed.DepartmentName, ed.LastName, ed.FirstName;