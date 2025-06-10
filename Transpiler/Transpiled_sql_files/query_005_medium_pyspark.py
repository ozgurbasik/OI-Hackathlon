"""
PySpark code transpiled from query_005.sql
Classification: Medium
Generated on: 2025-06-10 19:28:04
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Define the paths to the Parquet files
employee_path = "/mnt/advdata/AdventureWorks/HumanResources_Employee.parquet"
person_path = "/mnt/advdata/AdventureWorks/Person_Person.parquet"
employee_department_history_path = "/mnt/advdata/AdventureWorks/HumanResources_EmployeeDepartmentHistory.parquet"
department_path = "/mnt/advdata/AdventureWorks/HumanResources_Department.parquet"

# Read the data from the Parquet files into DataFrames
employee_df = spark.read.parquet(employee_path)
person_df = spark.read.parquet(person_path)
employee_department_history_df = spark.read.parquet(employee_department_history_path)
department_df = spark.read.parquet(department_path)

# Create the EmployeeDetails CTE
employee_details_df = employee_df.alias("e").join(
    person_df.alias("p"),
    F.col("e.BusinessEntityID") == F.col("p.BusinessEntityID"),
    "inner"
).join(
    employee_department_history_df.alias("dh"),
    F.col("e.BusinessEntityID") == F.col("dh.BusinessEntityID"),
    "inner"
).join(
    department_df.alias("d"),
    F.col("dh.DepartmentID") == F.col("d.DepartmentID"),
    "inner"
).where(
    (F.col("e.CurrentFlag") == True) & (F.col("dh.EndDate").isNull())
).select(
    F.col("e.BusinessEntityID"),
    F.col("p.FirstName"),
    F.col("p.LastName"),
    F.col("e.JobTitle"),
    F.col("e.HireDate"),
    F.col("e.VacationHours"),
    F.col("e.SickLeaveHours"),
    F.col("e.SalariedFlag"),
    F.col("dh.DepartmentID"),
    F.col("d.Name").alias("DepartmentName"),
    F.when(F.col("e.SalariedFlag") == 1, "Salaried").otherwise("Hourly").alias("PayType"),
    F.when(F.year(F.current_date()) - F.year(F.col("e.HireDate")) >= 10, "Senior")
    .when(F.year(F.current_date()) - F.year(F.col("e.HireDate")) >= 5, "Mid-Level")
    .otherwise("Junior").alias("ExperienceLevel")
)

# Create the DepartmentStats CTE
department_stats_df = employee_details_df.groupBy("DepartmentName").agg(
    F.count("*").alias("TotalEmployees"),
    F.sum(F.when(F.col("PayType") == "Salaried", 1).otherwise(0)).alias("SalariedEmployees"),
    F.sum(F.when(F.col("PayType") == "Hourly", 1).otherwise(0)).alias("HourlyEmployees"),
    F.sum(F.when(F.col("ExperienceLevel") == "Senior", 1).otherwise(0)).alias("SeniorEmployees"),
    F.sum(F.when(F.col("ExperienceLevel") == "Mid-Level", 1).otherwise(0)).alias("MidLevelEmployees"),
    F.sum(F.when(F.col("ExperienceLevel") == "Junior", 1).otherwise(0)).alias("JuniorEmployees"),
    F.avg("VacationHours").alias("AvgVacationHours"),
    F.avg("SickLeaveHours").alias("AvgSickLeaveHours"),
    F.max("VacationHours").alias("MaxVacationHours"),
    F.min("VacationHours").alias("MinVacationHours")
)

# Create the PayTypeBreakdown CTE
pay_type_breakdown_df = employee_details_df.groupBy("DepartmentName", "PayType").agg(
    F.count("*").alias("CountByPayType"),
    F.avg("VacationHours").alias("AvgVacationByPayType"),
    F.avg("SickLeaveHours").alias("AvgSickLeaveByPayType")
)

# Final SELECT statement
final_df = employee_details_df.alias("ed").join(
    department_stats_df.alias("ds"),
    F.col("ed.DepartmentName") == F.col("ds.DepartmentName"),
    "inner"
).join(
    pay_type_breakdown_df.alias("ptb"),
    (F.col("ed.DepartmentName") == F.col("ptb.DepartmentName")) & (F.col("ed.PayType") == F.col("ptb.PayType")),
    "inner"
).select(
    F.col("ed.DepartmentName"),
    F.col("ed.FirstName"),
    F.col("ed.LastName"),
    F.col("ed.JobTitle"),
    F.col("ed.HireDate"),
    F.col("ed.PayType"),
    F.col("ed.ExperienceLevel"),
    F.col("ed.VacationHours"),
    F.col("ed.SickLeaveHours"),
    F.col("ds.TotalEmployees"),
    F.col("ds.SalariedEmployees"),
    F.col("ds.HourlyEmployees"),
    F.col("ds.SeniorEmployees"),
    F.col("ds.MidLevelEmployees"),
    F.col("ds.JuniorEmployees"),
    F.round(F.col("ds.AvgVacationHours"), 1).alias("DeptAvgVacation"),
    F.round(F.col("ds.AvgSickLeaveHours"), 1).alias("DeptAvgSickLeave"),
    F.when(F.col("ed.VacationHours") > F.col("ds.AvgVacationHours"), "Above Average")
    .when(F.col("ed.VacationHours") == F.col("ds.AvgVacationHours"), "Average")
    .otherwise("Below Average").alias("VacationComparison"),
    F.when(F.col("ds.TotalEmployees") > 20, "Large Department")
    .when(F.col("ds.TotalEmployees") > 10, "Medium Department")
    .otherwise("Small Department").alias("DepartmentSize"),
    F.round((F.col("ds.SalariedEmployees") * 100.0) / F.col("ds.TotalEmployees"), 1).alias("SalariedPercentage"),
    F.col("ptb.CountByPayType"),
    F.round(F.col("ptb.AvgVacationByPayType"), 1).alias("PayTypeAvgVacation")
).orderBy("DepartmentName", "LastName", "FirstName")

# Display the final DataFrame (optional, for verification)
# final_df.show()