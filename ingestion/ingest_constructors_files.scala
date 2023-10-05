// Databricks notebook source
// DBTITLE 1, Ingest circuits.csv files
// MAGIC %run "../includes/configuration"

// COMMAND ----------

// MAGIC %run "../includes/common_functions"

// COMMAND ----------

// MAGIC %run "../includes/common_functions"

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType}

val constructorsSchema = StructType(Array(
    StructField("constructorId",IntegerType,false),
    StructField("constructorRef", StringType, true),
    StructField("name", StringType, true),
    StructField("nationality", StringType, true),
    StructField("url", StringType, true)
  ))

// COMMAND ----------

var df = spark.read.schema(constructorsSchema).json(s"$raw_dir/constructors.json")

df.show(5)


// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp}

df = df.withColumnRenamed("constructorId", "constructor_id")
       .withColumnRenamed("constructorRef", "constructor_ref")
       .drop("url")
df = addIngestionDate(df)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").parquet(s"$processed_dir/constructors")

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
