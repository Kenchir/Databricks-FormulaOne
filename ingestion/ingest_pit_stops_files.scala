// Databricks notebook source
// DBTITLE 1, Ingest pit_stops.json files
display(dbutils.fs.mounts())

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType, LongType,TimestampType}

val pitStopsSchema = StructType(Array(
    StructField("driverId", IntegerType, false),
    StructField("duration", StringType, true),
    StructField("lap", IntegerType, true),
    StructField("milliseconds", LongType, true),
    StructField("raceId", IntegerType, true),
    StructField("stop", IntegerType, true),
    StructField("time", StringType, true)
  ))

// COMMAND ----------

// spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled",true)

var df = spark.read.schema(pitStopsSchema)
                   .option("multiLine", true)
                   .json("dbfs:/mnt/kchirchir/formulaone/raw/pit_stops.json")

df.show(10)


// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp}

df = df.withColumnRenamed("driverId", "driver_d")
       .withColumnRenamed("raceId", "race_id")
       .withColumn("ingestion_date", current_timestamp())


// COMMAND ----------

df.show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/kchirchir/formulaone/processed/pit_stops")
