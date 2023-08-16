# Databricks notebook source
# MAGIC %md
# MAGIC <span style="color:orange">
# MAGIC  <h2> Databircks-Assignemnt 4: Advance String Manupulation Transformation)
# MAGIC </span>
# MAGIC   <h5>
# MAGIC     <span style="color:red">
# MAGIC <b>Author: Deepak Goyal <br>
# MAGIC    <a> adeus.azurelib.com </a><br>
# MAGIC    Email at: admin@azurelib.com
# MAGIC </span>

# COMMAND ----------

# MAGIC %md
# MAGIC <b> What is the abbreviated version of each employee's last name, with the first letter and the two following letters included?

# COMMAND ----------

# Write your code using the Spark Function
from pyspark.sql.functions import *
df = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/utsavsingh62@gmail.com/EmployeeData_4.csv")
display(df)
df.createOrReplaceTempView("Employee")

# COMMAND ----------

df1 = spark.sql("SELECT last_name,SUBSTRING(last_name,1,3) FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the domain name of each employee's email address, so that their emails can be filtered and routed to the correct department?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,substring(email, instr(EMAIL, '@')+1) AS Email_Domain FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> In an effort to protect their privacy, what is each employee's name spelled backwards?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT first_name,reverse(first_name) AS reversed_name FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> In order to make their email addresses harder to guess, what is each employee's email address spelled backwards?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,reverse(email) AS reversed_email FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is each employee's first name, repeated twice, to improve personalization of emails and messages?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT first_name,concat(first_name,' ',first_name) AS Modified_name FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the country code of each employee's email domain, which can help identify global market opportunities?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,REGEXP_SUBSTR(email, '[0-9]+') AS country_code FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the position of the first occurrence of a dot ('.') character in each employee's email address, which can be used to standardize email addresses in a database?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,instr(email,'.') AS STD_EMAIL FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the full name and email address of each employee, concatenated with a space separator, for use in email communication?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT email,concat(first_name,' ',last_name) AS FULL_NAME FROM EMPLOYEE")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC d
# MAGIC <b> What is the first letter of each employee's first name in uppercase, followed by the rest of the name in lowercase, to ensure consistency and proper capitalization?

# COMMAND ----------

# Write your code using the Spark Function
df1 = spark.sql("SELECT CONCAT(upper(SUBSTRING(first_name, 1, 1)), lower(SUBSTRING(first_name, 2))) AS Proper_Case_First_Name FROM employee")
display(df1)

# COMMAND ----------


