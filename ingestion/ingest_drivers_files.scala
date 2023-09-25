// Databricks notebook source
// DBTITLE 1, Ingest divers.csv files
display(dbutils.fs.mounts())

// COMMAND ----------

import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, DateType}
val nameSchema =StructType(
     fields=Seq(StructField("forename", StringType, true),
      StructField("surname", StringType, true))
  )

val driversSchema = StructType(Array(
    StructField("driverId",IntegerType,false),
    StructField("driverRef",StringType,true),
    StructField("number",IntegerType,true),
    StructField("code", StringType, true),
    StructField("name", nameSchema, true),
    StructField("dob", DateType, true),
    StructField("nationality", StringType, true),
    StructField("url", StringType, true) 
    ))

// COMMAND ----------

var df = spark.read.schema(driversSchema).json("dbfs:/mnt/kchirchir/formulaone/raw/drivers.json")

df.show(5)


// COMMAND ----------

// MAGIC %md
// MAGIC Rename driverid and driverRef <br>
// MAGIC Add ingestion date <br>
// MAGIC concat forename and surname to one value<br>
// MAGIC drop url column

// COMMAND ----------

import org.apache.spark.sql.functions.{current_timestamp, concat_ws, to_timestamp, lit, col}

df = df.withColumnRenamed("driverId", "driver_id")
       .withColumnRenamed("driverref", "driver_ref")
       .withColumn("name", concat_ws(" ",col("name.forename"),col("name.surname")))
       .withColumn("ingestion_date", current_timestamp)
       .drop("url")


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC #### Write to datalake as Parquet 

// COMMAND ----------

df.write.mode("overwrite").parquet("dbfs:/mnt/kchirchir/formulaone/processed/drivers")
