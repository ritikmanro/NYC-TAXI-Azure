# Databricks notebook source
# MAGIC %md
# MAGIC # Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxistorageritik.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistorageritik.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistorageritik.dfs.core.windows.net", "********************")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistorageritik.dfs.core.windows.net", "****************")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistorageritik.dfs.core.windows.net", "*************************n")

# COMMAND ----------

# MAGIC %md
# MAGIC # Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG adb_nyc_ritik
# MAGIC MANAGED LOCATION 'abfss://golddata@nyctaxistorageritik.dfs.core.windows.net/external_catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW CATALOGS
# MAGIC CREATE DATABASE gold

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA adb_nyc_ritik.golddata
# MAGIC MANAGED LOCATION 'abfss://golddata@nyctaxistorageritik.dfs.core.windows.net';

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Reading and Writing and Creating Delta tables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage variable

# COMMAND ----------

silver = 'abfss://silver@nyctaxistorageritik.dfs.core.windows.net'
golddata = 'abfss://golddata@nyctaxistorageritik.dfs.core.windows.net'

# COMMAND ----------

df_zone = spark.read.format('parquet')\
                        .option('inferSchema', True)\
                        .option('header', True)\
                        .load(f'{silver}/trip_zone')

# COMMAND ----------

df_zone.display()

# COMMAND ----------

df_zone.write.format('delta')\
            .mode('append')\
            .option('path', f'{golddata}/trip_zone')\
            .saveAsTable('golddata.trip_zone')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM golddata.trip_zone;

# COMMAND ----------

df_type = spark.read.format('parquet')\
                        .option('inferSchema', True)\
                        .option('header', True)\
                        .load(f'{silver}/trip_type')

# COMMAND ----------

df_type.write.format('delta')\
            .mode('append')\
            .option('path', f'{golddata}/trip_type')\
            .saveAsTable('golddata.trip_type')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trips Data

# COMMAND ----------

df_trip = spark.read.format('parquet')\
                        .option('inferSchema', True)\
                        .option('header', True)\
                        .load(f'{silver}/trip2023data')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('delta')\
            .mode('append')\
            .option('path', f'{golddata}/tripsdata')\
            .saveAsTable('golddata.trip_data')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake

# COMMAND ----------

# MAGIC %md
# MAGIC #### Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddata.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC update golddata.trip_zone set Borough = 'EMR' where LocationID = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddata.trip_zone where LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from golddata.trip_zone where LocationId = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddata.trip_zone where LocationID = 1

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Versioning**

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY golddata.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC #### Time travel 

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE golddata.trip_zone TO VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddata.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trip Type

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddata.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trip Zone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddata.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trips data 2023

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from golddata.trip_data

# COMMAND ----------

