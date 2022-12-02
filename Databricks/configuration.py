# Databricks notebook source
# MAGIC 
# MAGIC %md
# MAGIC Define Data Paths.

# COMMAND ----------

# TODO
username = "Trang"

# COMMAND ----------

classicPipelinePath = f"/FileStore/Trang/classic1/"
landingPath = classicPipelinePath + "landing/"

rawPath = dataPipelinePath + "raw/"
bronzePath = dataPipelinePath + "bronze/"
silverPath = dataPipelinePath + "silver/"

# COMMAND ----------

# MAGIC %md
# MAGIC Configure Database

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS data_movies")
spark.sql(f"USE data_movies")

# COMMAND ----------

# MAGIC %md
# MAGIC Import Utility Functions

# COMMAND ----------

# MAGIC %run ./utilities

# COMMAND ----------


