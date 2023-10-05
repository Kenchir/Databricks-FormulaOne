// Databricks notebook source
var results = dbutils.notebook.run("./ingestion/ingest_circuits_files", 0)

// COMMAND ----------

results = dbutils.notebook.run("./ingestion/ingest_constructors_files", 0)

// COMMAND ----------

results = dbutils.notebook.run("./ingestion/ingest_drivers_files", 0)

// COMMAND ----------

results = dbutils.notebook.run("./ingestion/ingest_laps_time_files", 0)

// COMMAND ----------

results = dbutils.notebook.run("./ingestion/ingest_pit_stops_files", 0)

// COMMAND ----------

results = dbutils.notebook.run("./ingestion/ingest_qualifying_files", 0)

// COMMAND ----------

results = dbutils.notebook.run("./ingestion/ingest_races_files", 0)

// COMMAND ----------

results = dbutils.notebook.run("./ingestion/ingest_results_files", 0)
