// Databricks notebook source
// DBTITLE 1, Ingest qualifying.json files
// MAGIC %run "../includes/common_functions"

// COMMAND ----------

// MAGIC %run "../includes/configuration"

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType, LongType,TimestampType}

val qualifySchema = StructType(Array(
    StructField("constructorId", IntegerType, false),
    StructField("driverId", IntegerType, true),
    StructField("number", IntegerType, true),
    StructField("position", IntegerType, true),
    StructField("q1", StringType, true),
    StructField("q2", StringType, true),
    StructField("q3", StringType, true),
    StructField("qualifyId", IntegerType, true),
    StructField("raceId", IntegerType, true)
  ))

// COMMAND ----------

// spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled",true)

var df = spark.read.schema(qualifySchema)
                   .option("MultiLine", true)
                   .option("recursiveFileLookup","true")
                   .json(s"$raw_dir/qualifying")

df.show(10)


// COMMAND ----------

df = df.withColumnRenamed("raceId", "race_id")
       .withColumnRenamed("driverId", "driver_id")
       .withColumnRenamed("constructorId", "constructor_id")
       .withColumnRenamed("qualifyId", "qualify_id")

df = addIngestionDate(df)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").parquet(s"$processed_dir/qualifying")

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
