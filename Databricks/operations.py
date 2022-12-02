# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.types import *
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
from pyspark.sql.types import ArrayType, StructType, StructField
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
def read_batch_bronze(spark) -> DataFrame:
      return spark.read.table("movies_raw_bronze").filter("status = 'new'")

# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)


# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    json_schema = "movie ARRAY"
    movies = spark.read.format('json').option("multiline","true").load(rawPath)
    return df.select(explore(movies.movie))


# COMMAND ----------

def transform_bronze(bronze: DataFrame, quarantine: bool = False) -> DataFrame:

    json_schema = StructType(fields=[
       StructField('datasource', string(), true),
       StructField('ingesttime', timestamp(), true),
       StructField('movie', struct(), true),
       StructField('BackdropUrl',string(), true),
       StructField('Budget', double(), true),
       StructField('CreatedBy',string (), true),
       StructField('CreatedDate',string(), true),
       StructField('Id', long(), true),
       StructField('ImdbUrl', string(), true),
       StructField('OriginalLanguage', string(), true),
       StructField('Overview', string(), true),
       StructField('PosterUrl', string(), true),
       StructField('Price', double(), true),
       StructField('ReleaseDate', string(), true),
       StructField('Revenue', double(), true),
       StructField('RunTime', long (), true),
       StructField('Tagline', string(), true),
       StructField('Title', string(), true),
       StructField('TmdbUrl', string(), true),
       StructField('UpdatedBy', string(), true),
       StructField('UpdatedDate', string(), true),
       StructField('genres', arrayType(), true),
       StructField('element', struct(), containsNull = true),
       StructField('id', long(), true),
       StructField('name', string(), true),
       StructField('status', string (), true),
       StructField('p_ingestdate', date(), true),
    ])
    

    bronzeAugmentedDF = bronze.withColumn(
        "nested_json", from_json(col("movies"), json_schema)
    )

    data_movies = bronzeAugmentedDF.select("movies", "nested_json.*")

   # if not quarantine:
    #    data_movies = data_movies.select(
     #       "value",
      #      col("device_id").cast("integer").alias("device_id"),
      #      "steps",
       #     col("time").alias("eventtime"),
         #   "name",
         #   col("time").cast("date").alias("p_eventdate"),
       # )
    #else:
      #  data_movies = data_movies.select(
      #      "value",
       #     "device_id",
        #    "steps",
        #    col("time").alias("eventtime"),
         #   "name",
         #   col("time").cast("date").alias("p_eventdate"),
       # )

    return data_movies.select(
       'datasource', 
       'ingesttime', 
       'movie', 
       'BackdropUrl',
       'Budget', 
       'CreatedBy',
       'CreatedDate',
       'Id', 
       'ImdbUrl',
       'OriginalLanguage',
       'Overview', 
       'PosterUrl',
       'Price', 
       'ReleaseDate',
       'Revenue', 
       'RunTime', 
       'Tagline', 
       'Title', 
       'TmdbUrl', 
       'UpdatedBy', 
       'UpdatedDate', 
       'genres', 
       'element', 
       'id', 
       'name', 
       'status', 
       'p_ingestdate',
    )


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

def transform_raw(rawDF: DataFrame) -> DataFrame:
  
    return rawDF.select(
        col("col").alias("movies"),
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

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzePath)
    dataframeAugmented = dataframe.withColumn("status", lit(status))

    update_match = "bronze.value = dataframe.value"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True
  
    %md 
    

# COMMAND ----------


