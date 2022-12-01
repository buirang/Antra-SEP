# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )


# COMMAND ----------

def generate_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("device_id IS NOT NULL"),
        dataframe.filter("device_id IS NULL"),
    )


# COMMAND ----------

# TODO
def read_batch_bronze() -> DataFrame:
  return # FILL_THIS_IN

# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)


# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    kafka_schema = "value STRING"
    return spark.read.format("text").schema(kafka_schema).load(rawPath)


# COMMAND ----------

def transform_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

    json_schema = """
      time TIMESTAMP,
      name STRING,
      device_id STRING,
      steps INTEGER,
      day INTEGER,
      month INTEGER,
      hour INTEGER
  """

    bronzeAugmentedDF = bronze.withColumn(
        "nested_json", from_json(col("value"), json_schema)
    )

    data_movies = bronzeAugmentedDF.select("value", "nested_json.*")

    if not quarantine:
        data_movies = data_movies.select(
            "value",
            col("device_id").cast("integer").alias("device_id"),
            "steps",
            col("time").alias("eventtime"),
            "name",
            col("time").cast("date").alias("p_eventdate"),
        )
    else:
        data_movies = data_movies.select(
            "value",
            "device_id",
            "steps",
            col("time").alias("eventtime"),
            "name",
            col("time").cast("date").alias("p_eventdate"),
        )

    return data_movies


# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str, userTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
    health_tracker_user_df = spark.read.table(userTable).alias("user")
    repairDF = bronzeQuarTransDF.join(
        health_tracker_user_df,
        bronzeQuarTransDF.device_id == health_tracker_user_df.user_id,
    )
    silverCleanedDF = repairDF.select(
        col("quarantine.value").alias("value"),
        col("user.device_id").cast("INTEGER").alias("device_id"),
        col("quarantine.steps").alias("steps"),
        col("quarantine.eventtime").alias("eventtime"),
        col("quarantine.name").alias("name"),
        col("quarantine.eventtime").cast("date").alias("p_eventdate"),
    )
    return silverCleanedDF


# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
  
    return raw.select(
        lit("files.training.databricks.com").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        lit("new").alias("status"),
        "value",
        current_timestamp().cast("date").alias("p_ingestdate"),
    )


# COMMAND ----------

def bronze_to_silver(bronze: DataFrame) -> DataFrame:
    
    # First of all, create a schema
    json_schema = """
       BackdropUrl STRING,
       Budget DOUBLE,
       CreatedBy TIMESTAMP,
       CreatedDate STRING,
       Id LONG,
       ImdbUrl STRING,
       OriginalLanguage STRING,
       Overview STRING,
       PosterUrl STRING,
       Price DOUBLE,
       ReleaseDate TIMESTAMP,
       Revenue DOUBLE,
       RunTime LONG,
       Tagline STRING,
       Title STRING,
       TmdbUrl STRING,
       UpdatedBy TIMESTAMP,
       UpdatedDate TIMESTAMP,
       genres STRING
    """
    
    return(bronze.withColumn(
        "nested_json", from_json(col("movie"),
                                 json_schema))
          .select("movie", "nested_json.*")
          .select("movie",
                  "BackdropUrl",
                  "Budget",
                  "CreatedBy",
                  "CreatedDate",
                  "Id",
                  "ImdbUrl",
                  "OriginalLanguage",
                  "Overview",
                  "PosterUrl",
                  "Price",
                  col("ReleaseDate").alias("ReleaseTime"),
                  col("ReleaseDate").cast("date"),
                  trunc(col("ReleaseDate").cast("date"), "year").alias("ReleaseYear"),
                  "Revenue",
                  "RunTime",
                  "Tagline",
                  "Title",
                  "TmdbUrl",
                  "UpdatedBy",
                  "UpdatedDate",
                  "genres")
          )

  

# COMMAND ----------


