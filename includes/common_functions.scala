// Databricks notebook source
import org.apache.spark.sql.functions.current_timestamp
import org.apache.spark.sql.DataFrame

def addIngestionDate(df: DataFrame) : DataFrame ={
  df.withColumn("ingestion_date", current_timestamp())
}
