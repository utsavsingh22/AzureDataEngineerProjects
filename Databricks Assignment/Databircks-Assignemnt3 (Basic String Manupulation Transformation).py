# Databricks notebook source
# MAGIC %md
# MAGIC <span style="color:orange">
# MAGIC  <h2> Databircks-Assignemnt 3 (Basic String Manupulation Transformation)
# MAGIC </span>
# MAGIC   <h5>
# MAGIC     <span style="color:red">
# MAGIC <b>Author: Deepak Goyal <br>
# MAGIC    <a> adeus.azurelib.com </a><br>
# MAGIC    Email at: admin@azurelib.com
# MAGIC </span>

# COMMAND ----------

# MAGIC %md
# MAGIC <b> How many employees have a first name that starts with the letter 'J'?

# COMMAND ----------

# Write your code using the Spark Function
from pyspark.sql.functions import *
df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/utsavsingh62@gmail.com/EmployeeData_3.csv")
display(df)
df.createOrReplaceTempView("Employee")

# COMMAND ----------

df1 = df.filter(col("first_name").startswith("J")).count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM EMPLOYEE WHERE first_name LIKE 'J%'

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> How many employees have a last name that ends with the letter 's'?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.filter(col("last_name").endswith("S")).count()
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM EMPLOYEE WHERE last_name LIKE '%S'

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the length of the longest first name?

# COMMAND ----------

# Write your code using the Spark Function
df1 = df.withColumn("first_name", length(col("first_name")))
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LEN(first_name),first_name FROM EMPLOYEE GROUP BY first_name ORDER BY LEN(first_name) DESC

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the length of the shortest last name?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT len(last_name),last_name FROM EMPLOYEE GROUP BY last_name ORDER BY last_name")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the email domain of the employee with the highest salary?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,max(salary) FROM EMPLOYEE GROUP BY email ORDER BY max(salary) DESC LIMIT 2")
display(df1)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT max(salary) FROM EMPLOYEE 

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the first name of the employee with the email address that contains the substring 'hr'?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email FROM EMPLOYEE WHERE email LIKE '%hr%'")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the last name of the employee with the email address that ends with the domain '.com'?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT last_name,email FROM EMPLOYEE WHERE email LIKE '%.com'")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> How many employees have an email address that contains their last name?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT COUNT(*) FROM EMPLOYEE WHERE email LIKE LOWER(CONCAT('%', last_name, '%'))")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the email of the employee with the first name that contains a space character?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,first_name FROM EMPLOYEE WHERE first_name LIKE '% %'")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the email of the employee with the last name that contains a hyphen character?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,last_name FROM EMPLOYEE WHERE last_name LIKE '%-%'")
display(df1)

# COMMAND ----------


