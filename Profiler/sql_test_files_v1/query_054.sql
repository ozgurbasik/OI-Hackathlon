
WITH employee_metrics AS (
    SELECT 
        e.id,
        e.first_name,
        e.last_name,
        e.department_id,
        e.hire_date,
        e.salary,
        SUM(t.hours_worked) as total_hours,
        COUNT(DISTINCT t.project_id) as projects_count,
        AVG(t.hours_worked) as avg_daily_hours
    FROM employees e
    LEFT JOIN timesheets t ON e.id = t.employee_id
    WHERE t.date >= DATEADD(month, -6, GETDATE())
        AND e.active = 1
    GROUP BY e.id, e.first_name, e.last_name, e.department_id, e.hire_date, e.salary
),
department_stats AS (
    SELECT 
        d.id as department_id,
        d.name as department_name,
        COUNT(DISTINCT em.id) as employee_count,
        AVG(em.salary) as avg_department_salary,
        SUM(em.total_hours) as total_department_hours,
        AVG(em.total_hours) as avg_employee_hours
    FROM departments d
    LEFT JOIN employee_metrics em ON d.id = em.department_id
    WHERE d.active = 1
    GROUP BY d.id, d.name
),
productivity_rankings AS (
    SELECT 
        em.id,
        em.first_name,
        em.last_name,
        em.department_id,
        em.total_hours,
        em.projects_count,
        RANK() OVER (PARTITION BY em.department_id ORDER BY em.total_hours DESC) as hours_rank,
        CASE 
            WHEN em.total_hours > 160 THEN 'HIGH'
            WHEN em.total_hours > 120 THEN 'MEDIUM'
            ELSE 'LOW'
        END as performance_level
    FROM employee_metrics em
)
SELECT 
    e.id,
    e.first_name,
    e.last_name,
    ds.department_name,
    pr.total_hours,
    pr.projects_count,
    pr.hours_rank,
    pr.performance_level,
    e.salary,
    ds.avg_department_salary,
    DATEDIFF(year, e.hire_date, GETDATE()) as years_with_company
FROM employees e
JOIN department_stats ds ON e.department_id = ds.department_id
JOIN productivity_rankings pr ON e.id = pr.id
WHERE e.active = 1
ORDER BY ds.department_name, pr.hours_rank;

-- Department summary
WITH department_stats AS (
    SELECT 
        d.id as department_id,
        d.name as department_name,
        COUNT(DISTINCT e.id) as employee_count,
        AVG(e.salary) as avg_salary
    FROM departments d
    LEFT JOIN employees e ON d.id = e.department_id
    WHERE d.active = 1 AND e.active = 1
    GROUP BY d.id, d.name
)
SELECT 
    ds.department_name,
    ds.employee_count,
    ds.avg_salary
FROM department_stats ds
ORDER BY ds.avg_salary DESC;