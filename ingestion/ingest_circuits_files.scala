// Databricks notebook source
// DBTITLE 1, Ingest circuits.csv files
display(dbutils.fs.mounts())

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType}

val circuitsSchema = StructType(Array(
    StructField("circuitId",IntegerType,false),
    StructField("circuitRef",StringType,true),
    StructField("name",StringType,true),
    StructField("location", StringType, true),
    StructField("country", StringType, true),
    StructField("lat", DoubleType, true),
    StructField("lng", DoubleType, true),
    StructField("alt", IntegerType, true),
    StructField("url", IntegerType, true)
  ))

// COMMAND ----------

var df = spark.read.option("header", "true")
                    .schema(circuitsSchema)
                    .csv("dbfs:/mnt/kchirchir/formulaone/raw/circuits.csv")

df.show(5)


// COMMAND ----------

df = df.drop("url")

// COMMAND ----------


df = df.withColumnRenamed("circuitId", "circuit_id")
       .withColumnRenamed("circuitref", "circuit_ref")
       .withColumnRenamed("lat", "latitude")
       .withColumnRenamed("lng", "longitude")
       .withColumnRenamed("alt", "altitude ")


// COMMAND ----------

import org.apache.spark.sql.functions.current_timestamp
df = df.withColumn("ingestion_date", current_timestamp())

df.show(5, false)

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/kchirchir/formulaone/processed/circuits")
