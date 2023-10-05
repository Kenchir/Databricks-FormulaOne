// Databricks notebook source
// DBTITLE 1, Ingest circuits.csv files
// MAGIC %run "../includes/common_functions"

// COMMAND ----------

// MAGIC %run "../includes/configuration"

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
                    .csv(s"$raw_dir/races.csv")

df.show(5)


// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp, concat, to_timestamp, lit, col}

df = df.withColumnRenamed("raceId", "race_id")
       .withColumnRenamed("circuitId", "circuit_id")
       .withColumnRenamed("year", "race_year")
       .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))
       .drop("date", "time", "url")

df = addIngestionDate(df)

// COMMAND ----------

df.describe().show()

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Repartition by year and Write to datalake as Parquet 

// COMMAND ----------

df.repartition(col("race_year")).write.mode("overwrite").partitionBy("race_year").parquet(s"$processed_dir/races")

// COMMAND ----------

dbutils.notebook.exit("SUCCESS")
