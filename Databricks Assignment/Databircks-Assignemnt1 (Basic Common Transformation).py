# Databricks notebook source
# MAGIC %md
# MAGIC <span style="color:orange">
# MAGIC  <h2> Databircks-Assignemnt 1 (Basic Common Transformation)
# MAGIC </span>
# MAGIC   <h5>
# MAGIC     <span style="color:red">
# MAGIC <b>Author: Deepak Goyal <br>
# MAGIC    <a> adeus.azurelib.com </a><br>
# MAGIC    Email at: admin@azurelib.com
# MAGIC </span>

# COMMAND ----------

# MAGIC %md
# MAGIC <b>How many employees are in the input file?

# COMMAND ----------

# Write your code using the Spark Function
df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/utsavsingh62@gmail.com/EmployeeData.csv")
display(df)

# COMMAND ----------

df.createOrReplaceTempView("employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM employee

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC <b> What is the average salary of all the employees?

# COMMAND ----------

# Write your code using the Spark Function
from pyspark.sql.functions import *
average_salary_df = df.agg(avg("salary").alias("average_salary"))
display(average_salary_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(salary) AS AVG_SALARY FROM employee

# COMMAND ----------

# MAGIC %md
# MAGIC <b> How many male and female employees are there in the input file?

# COMMAND ----------

# Write your code using the Spark Function
df_gender = df.groupBy("gender").count()
display(df_gender)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*),gender FROM EMPLOYEE GROUP BY gender

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Who is the employee with the highest salary?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.agg(max("salary").alias("max_salary"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(salary) FROM EMPLOYEE

# COMMAND ----------

# MAGIC %md
# MAGIC <b> What is the total bonus amount paid to all the employees?

# COMMAND ----------

# Write your code using the Spark Function
df_bonus = df.agg(sum("bonus")).alias("Total Bonus")
display(df_bonus)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(bonus) AS TOTAL_BONUS FROM EMPLOYEE

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> How many employees were hired in the year 2020?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter(year("creationDate") == "2020").count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM EMPLOYEE WHERE YEAR(creationDate) = "2020"

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Who are the top 5 employees with the highest salaries?

# COMMAND ----------

# Write your code using the Spark Function
from pyspark.sql.functions import *
df3 = df.orderBy(desc("salary")).limit(5)
display(df3)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name,salary FROM EMPLOYEE GROUP BY first_name,salary ORDER BY salary desc LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the total salary paid to all male employees?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter(df["gender"]=="Male").agg(sum("salary"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(salary) FROM EMPLOYEE WHERE gender='Male'

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Who is the employee with the highest bonus amount?

# COMMAND ----------

# Write your code using the Spark Function# Write your code using the Spark Function
df2 = df.orderBy(desc("bonus")).limit(1)
display(df2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(bonus) FROM EMPLOYEE 

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the earliest creation date of any employee in the input file?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.agg(min("creationDate"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(creationDate) FROM EMPLOYEE

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> How many employees have a salary greater than 100,000?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter(df["salary"]>100000).count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM EMPLOYEE WHERE salary>100000

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Who is the female employee with the highest salary?

# COMMAND ----------

# Write your code using the Spark Function
df1=df.filter(df["gender"]=="Female").orderBy(desc("salary"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name,salary FROM EMPLOYEE WHERE gender='Female' GROUP BY first_name,salary ORDER BY salary desc

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the total bonus amount paid to female employees?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter(df["gender"]=="Female").agg(sum("bonus"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(bonus) FROM EMPLOYEE WHERE gender="Female"

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> How many employees have a salary between 50,000 and 75,000?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter((df["salary"]>=50000) & (df["salary"]<=750000)).count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM EMPLOYEE WHERE salary between 50000 AND 750000 

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Who are the top 10 employees with the highest bonuses?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.select("first_name","bonus").orderBy(desc("bonus"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name,bonus FROM EMPLOYEE ORDER BY bonus desc limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the latest creation date of any employee in the input file?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.agg(max("creationDate"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(creationDate) FROM EMPLOYEE

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> How many employees have a salary less than 50,000?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter(df["salary"]<500000).count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM EMPLOYEE WHERE salary < 500000

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Who are the top 3 employees with the lowest salaries?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.select("first_name","salary").orderBy(asc("salary")).limit(3)
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name,salary FROM EMPLOYEE ORDER BY salary ASC limit 3

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the average bonus amount paid to all employees?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.agg(avg("bonus"))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(bonus) FROM EMPLOYEE

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> How many employees have a bonus greater than their salary?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter(df["bonus"]> (df["salary"])).count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM EMPLOYEE WHERE bonus>salary;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc employee
