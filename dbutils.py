# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Mount Azure Data Lake Storage (ADLS Gen2) Containers to dbfs 
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC           "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="dbricks-learn", key="client-id"),
# MAGIC           "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dbricks-learn", key="service-principal-secret"),
# MAGIC           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get(scope='dbricks-learn', key='tenant-id')}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount raw directory

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.mount(
# MAGIC   source = "abfss://raw@kchirchirstorage.dfs.core.windows.net/",
# MAGIC   mount_point = "/mnt/kchirchir/formulaone/raw",
# MAGIC   extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount processed container

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://processed@kchirchirstorage.dfs.core.windows.net/",
  mount_point = "/mnt/kchirchir/formulaone/processed",
  extra_configs = configs)
