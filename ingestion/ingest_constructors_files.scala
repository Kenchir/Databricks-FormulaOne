// Databricks notebook source
// DBTITLE 1, Ingest circuits.csv files
display(dbutils.fs.ls("/mnt/kchirchir/formulaone/raw"))

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

var df = spark.read.schema(constructorsSchema).json("dbfs:/mnt/kchirchir/formulaone/raw/constructors.json")

df.show(5)


// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp}

df = df.withColumnRenamed("constructorId", "constructor_id")
       .withColumnRenamed("constructorRef", "constructor_ref")
       .withColumn("ingestion_date", current_timestamp())
       .drop("url")


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/kchirchir/formulaone/processed/constructors")
