// Databricks notebook source
// DBTITLE 1, Ingest results.json files
// MAGIC %run "../includes/common_functions"

// COMMAND ----------

// MAGIC %run "../includes/configuration"

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType, LongType,TimestampType}

val resultsSchema = StructType(Array(
    StructField("constructorId", IntegerType, true),
    StructField("driverId", IntegerType, true),
    StructField("fastestLap", IntegerType, true),
    StructField("fastestLapSpeed", DoubleType, true),
    StructField("fastestLapTime", TimestampType, true),
    StructField("grid", TimestampType, true),
    StructField("laps", IntegerType, true),
    StructField("milliseconds", LongType, true),
    StructField("number", IntegerType, true),
    StructField("points",DoubleType, true),
    StructField("position", IntegerType, true),
    StructField("positionOrder", IntegerType, true),
    StructField("positionText", StringType, true),
    StructField("raceId", IntegerType, true),
    StructField("rank", IntegerType, true),
    StructField("resultId", IntegerType, true),
    StructField("statusId", IntegerType, true),
    StructField("time", TimestampType, true)
  ))

// COMMAND ----------

var df = spark.read.schema(resultsSchema)
                   .json(s"$raw_dir/results.json")

// df.show(5)


// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp}

df = df.withColumnRenamed("constructorId", "constructor_id")
       .withColumnRenamed("driverId", "driver_d")
       .withColumnRenamed("fastestLap", "fastest_lap")
       .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
       .withColumnRenamed("fastestLapTime", "fastest_lap_time")
       .withColumnRenamed("positionOrder", "position_order")
       .withColumnRenamed("positionText", "position_text")
       .withColumnRenamed("raceId", "race_id")
       .withColumnRenamed("resultId", "result_id")
       .withColumnRenamed("statusId", "status_id")

df = addIngestionDate(df)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Repartition by year and Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").partitionBy("race_id").parquet(s"$processed_dir/results")

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
