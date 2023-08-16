# Databricks notebook source
# MAGIC %md
# MAGIC <span style="color:orange">
# MAGIC  <h2> Databircks-Assignemnt 2 (Basic Data Cleaning Transformation)
# MAGIC </span>
# MAGIC   <h5>
# MAGIC     <span style="color:red">
# MAGIC <b>Author: Deepak Goyal <br>
# MAGIC    <a> adeus.azurelib.com </a><br>
# MAGIC    Email at: admin@azurelib.com
# MAGIC </span>

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Find the count of duplicate employee records in the input file (based on id)?

# COMMAND ----------

# Write your code using the Spark Function
df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/utsavsingh62@gmail.com/EmployeeData_1-1.csv")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
df1 = df.groupBy("id").count()
df2 = df1.filter(col("count") > 1)
display(df2)

# COMMAND ----------

df.createOrReplaceTempView("Employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(id),id FROM Employee GROUP BY id HAVING count(id)>1

# COMMAND ----------

# MAGIC %md
# MAGIC <b> Find out how many records have Gender value missing.

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.where(col("gender").isNull()).count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM Employee WHERE gender is NULL

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Are there any missing values in the "bonus" field? If so, filled them defualt bonus 100.

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.withColumn("bonus", when(col("bonus").isNull(), 100).otherwise(col("bonus")))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 100 AS bonus FROM Employee WHERE bonus IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Are there any employees with negative salary or bonus amounts in the input file? If so, how many?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter((col("Salary") < 0) | (col("Bonus") < 0))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM EMPLOYEE WHERE bonus < 0 or salary < 0

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Replace all the null/emtpy value in email column with admin@azurelib.com

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.withColumn("email", when(col("email").isNull(), 'admin@azurelib.com').otherwise(col("email")))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'admin@azurelib.com' AS email FROM Employee WHERE email IS NULL

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> Remove all the records where any record has any null values. Find out the total count of the records now.

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.dropna()
display(df1)

# COMMAND ----------

df1.createOrReplaceTempView("cleaned_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM cleaned_data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM EMPLOYEE

# COMMAND ----------


