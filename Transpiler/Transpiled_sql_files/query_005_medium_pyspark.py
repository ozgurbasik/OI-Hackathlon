"""
PySpark code transpiled from query_005.sql
Classification: Medium
Generated on: 2025-06-10 17:37:34
Transpiled using: Gemini AI (gemini-2.0-flash-exp)
"""

from pyspark.sql import SparkSession, functions as F, types as T, Window

# Assuming SparkSession is already created as 'spark'

# Define the schemas for the input DataFrames (if necessary, and replace with actual schema)
# from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, BooleanType
# employee_schema = StructType([
#     StructField("BusinessEntityID", IntegerType(), True),
#     StructField("JobTitle", StringType(), True),
#     StructField("HireDate", DateType(), True),
#     StructField("VacationHours", IntegerType(), True),
#     StructField("SickLeaveHours", IntegerType(), True),
#     StructField("SalariedFlag", BooleanType(), True),
#     StructField("CurrentFlag", BooleanType(), True)
# ])
# person_schema = StructType([
#     StructField("BusinessEntityID", IntegerType(), True),
#     StructField("FirstName", StringType(), True),
#     StructField("LastName", StringType(), True)
# ])
# employee_department_history_schema = StructType([
#     StructField("BusinessEntityID", IntegerType(), True),
#     StructField("DepartmentID", IntegerType(), True),
#     StructField("EndDate", DateType(), True)
# ])
# department_schema = StructType([
#     StructField("DepartmentID", IntegerType(), True),
#     StructField("Name", StringType(), True)
# ])

# Load the data (replace with your actual data loading)
employee_df = spark.table("HumanResources.Employee") # spark.read.csv("path/to/employee.csv", schema=employee_schema)
person_df = spark.table("Person.Person") # spark.read.csv("path/to/person.csv", schema=person_schema)
employee_department_history_df = spark.table("HumanResources.EmployeeDepartmentHistory") # spark.read.csv("path/to/employee_department_history.csv", schema=employee_department_history_schema)
department_df = spark.table("HumanResources.Department") # spark.read.csv("path/to/department.csv", schema=department_schema)

# Define the EmployeeDetails CTE
employee_details_df = employee_df.alias("e").join(person_df.alias("p"), employee_df.BusinessEntityID == person_df.BusinessEntityID) \
    .join(employee_department_history_df.alias("dh"), employee_df.BusinessEntityID == employee_department_history_df.BusinessEntityID) \
    .join(department_df.alias("d"), employee_department_history_df.DepartmentID == department_df.DepartmentID) \
    .where((employee_df.CurrentFlag == True) & (employee_department_history_df.EndDate.isNull())) \
    .select(
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
        F.when(F.year(F.current_date()) - F.year(F.col("e.HireDate")) >= 10, "Senior") \
            .when(F.year(F.current_date()) - F.year(F.col("e.HireDate")) >= 5, "Mid-Level") \
            .otherwise("Junior").alias("ExperienceLevel")
    )

# Define the DepartmentStats CTE
department_stats_df = employee_details_df.groupBy("DepartmentName") \
    .agg(
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

# Define the PayTypeBreakdown CTE
pay_type_breakdown_df = employee_details_df.groupBy("DepartmentName", "PayType") \
    .agg(
        F.count("*").alias("CountByPayType"),
        F.avg("VacationHours").alias("AvgVacationByPayType"),
        F.avg("SickLeaveHours").alias("AvgSickLeaveByPayType")
    )

# Final SELECT statement
final_df = employee_details_df.alias("ed").join(department_stats_df.alias("ds"), employee_details_df.DepartmentName == department_stats_df.DepartmentName) \
    .join(pay_type_breakdown_df.alias("ptb"), (employee_details_df.DepartmentName == pay_type_breakdown_df.DepartmentName) & (employee_details_df.PayType == pay_type_breakdown_df.PayType)) \
    .select(
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
        F.when(F.col("ed.VacationHours") > F.col("ds.AvgVacationHours"), "Above Average") \
            .when(F.col("ed.VacationHours") == F.col("ds.AvgVacationHours"), "Average") \
            .otherwise("Below Average").alias("VacationComparison"),
        F.when(F.col("ds.TotalEmployees") > 20, "Large Department") \
            .when(F.col("ds.TotalEmployees") > 10, "Medium Department") \
            .otherwise("Small Department").alias("DepartmentSize"),
        F.round((F.col("ds.SalariedEmployees") * 100.0) / F.col("ds.TotalEmployees"), 1).alias("SalariedPercentage"),
        F.col("ptb.CountByPayType"),
        F.round(F.col("ptb.AvgVacationByPayType"), 1).alias("PayTypeAvgVacation")
    ) \
    .orderBy("DepartmentName", "LastName", "FirstName")