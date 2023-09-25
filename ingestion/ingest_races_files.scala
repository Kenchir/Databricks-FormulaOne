// Databricks notebook source
// DBTITLE 1, Ingest circuits.csv files
display(dbutils.fs.mounts())

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DoubleType}

val circuitsSchema = StructType(Array(
    StructField("raceId",IntegerType,false),
    StructField("year",IntegerType,true),
    StructField("round",IntegerType,true),
    StructField("circuitId",IntegerType,false),
    StructField("name", StringType, true),
    StructField("date", StringType, true),
    StructField("time", StringType, true),
    StructField("url", StringType, true)
  ))

// COMMAND ----------

var df = spark.read.option("header", "true")
                    .schema(circuitsSchema)
                    .csv("dbfs:/mnt/kchirchir/formulaone/raw/races.csv")

df.show(5)


// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp, concat, to_timestamp, lit, col}

df = df.withColumnRenamed("raceId", "race_id")
       .withColumnRenamed("circuitId", "circuit_id")
       .withColumnRenamed("year", "race_year")
       .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))
       .withColumn("ingestion_date", current_timestamp())
       .drop("date", "time", "url")


// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Repartition by year and Write to datalake as Parquet 

// COMMAND ----------

df.repartition(col("race_year")).write.mode("overwrite").partitionBy("race_year").parquet("dbfs:/mnt/kchirchir/formulaone/processed/races")

// COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/kchirchir/formulaone/processed/races", true)
