// Databricks notebook source
// DBTITLE 1, Ingest lap_times.csv files
display(dbutils.fs.mounts())

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType, LongType,TimestampType}

val lapTimesSchema = StructType(Array(
    StructField("race_id", IntegerType, false),
    StructField("driver_id", IntegerType, true),
    StructField("lap", IntegerType, true),
    StructField("position", IntegerType, true),
    StructField("time", StringType, true),
    StructField("millisecond", IntegerType, true)
  ))

// COMMAND ----------

// spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled",true)

var df = spark.read.schema(lapTimesSchema)
                   .option("Header", false)
                   .option("recursiveFileLookup","true")
                   .csv("dbfs:/mnt/kchirchir/formulaone/raw/lap_times")

df.show(10)


// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp}

df = df.withColumn("ingestion_date", current_timestamp())


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/kchirchir/formulaone/processed/lap_times")
