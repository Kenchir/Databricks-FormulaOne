// Databricks notebook source
// DBTITLE 1, Ingest pit_stops.json files
display(dbutils.fs.mounts())

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType, LongType,TimestampType}

val pitStopsSchema = StructType(Array(
    StructField("driverId", IntegerType, false),
    StructField("duration", DoubleType, true),
    StructField("lap", IntegerType, true),
    StructField("milliseconds", LongType, true),
    StructField("raceId", IntegerType, true),
    StructField("stop", IntegerType, true),
    StructField("time", TimestampType, true)
  ))

// COMMAND ----------

// spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled",true)

var df = spark.read.schema(pitStopsSchema)
                   .option("multiLine", true)
                   .json("dbfs:/mnt/kchirchir/formulaone/raw/pit_stops.json")

df.show(10)


// COMMAND ----------

df.describe().show()

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
       .withColumn("ingestion_date", current_timestamp())


// COMMAND ----------

df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Repartition by year and Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").partitionBy("race_id").parquet("dbfs:/mnt/kchirchir/formulaone/processed/results")

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/kchirchir/formulaone/processed/results"))
