# Databricks notebook source
# MAGIC %md
# MAGIC #### Data Access

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.nyctaxistorageritik.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.nyctaxistorageritik.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.nyctaxistorageritik.dfs.core.windows.net", "******************")
spark.conf.set("fs.azure.account.oauth2.client.secret.nyctaxistorageritik.dfs.core.windows.net", "*********")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.nyctaxistorageritik.dfs.core.windows.net", "*********************")

# COMMAND ----------

dbutils.fs.ls('abfss://bronze@nyctaxistorageritik.dfs.core.windows.net')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Reading

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Importing Libraries

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Reading CSV DATA

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trip Type Data

# COMMAND ----------

df_trip_type = spark.read.format('csv')\
                         .option('inferSchema', True)\
                         .option('header', True)\
                         .load('abfss://bronze@nyctaxistorageritik.dfs.core.windows.net/trip_type')

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trip Zone Data

# COMMAND ----------

df_trip_zone = spark.read.format('csv')\
                         .option('inferSchema', True)\
                         .option('header', True)\
                         .load('abfss://bronze@nyctaxistorageritik.dfs.core.windows.net/trip_zone')

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trip Data

# COMMAND ----------

df_trip = spark.read.format('parquet')\
                    .schema(mySchema)\
                    .option('header', True)\
                    .option('recursiveFileLookup', True)\
                    .load('abfss://bronze@nyctaxistorageritik.dfs.core.windows.net/trips2023data')

# COMMAND ----------

mySchema = '''
            VendorID BIGINT,
            lpep_pickup_datetime TIMESTAMP,
            lpep_dropoff_datetime TIMESTAMP,
            store_and_fwd_flag STRING,
            RatecodeID BIGINT,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            passenger_count BIGINT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            extra DOUBLE,
            mta_tax DOUBLE,
            tip_amount DOUBLE,
            tolls_amount DOUBLE,
            ehail_fee DOUBLE,
            improvement_surcharge DOUBLE,
            total_amount DOUBLE,
            payment_type BIGINT,
            trip_type BIGINT,
            congestion_surcharge DOUBLE
           '''

# COMMAND ----------

df_trip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transformation for taxi trip type

# COMMAND ----------

df_trip_type.display()

# COMMAND ----------

df_trip_type.withColumnRenamed('description', 'trip_description').display()

# COMMAND ----------

df_trip_type.write.format('parquet')\
            .mode('append')\
            .option('path', 'abfss://silver@nyctaxistorageritik.dfs.core.windows.net/trip_type')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trip Zone Transformation

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone = df_trip_zone.withColumn('Zone1', split(col('Zone'), '/')[0])\
                           .withColumn('Zone2', split(col('Zone'), '/')[1])

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

df_trip_zone.write.format('parquet')\
                  .mode('append')\
                  .option('path', 'abfss://silver@nyctaxistorageritik.dfs.core.windows.net/trip_zone')\
                  .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Trip data transformation

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.withColumn('trip_date', to_date('lpep_pickup_datetime'))\
        .withColumn('trip_year', year('lpep_pickup_datetime'))\
        .withColumn('trip_month', month('lpep_pickup_datetime'))

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip = df_trip.select('VendorID', 'PULocationID', 'DOLocationID', 'trip_distance', 'fare_amount', 'total_amount')

# COMMAND ----------

df_trip.display()

# COMMAND ----------

df_trip.write.format('parquet')\
             .mode('append')\
             .option('path', 'abfss://silver@nyctaxistorageritik.dfs.core.windows.net/trip2023data')\
             .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Analysis

# COMMAND ----------

display(df_trip)

# COMMAND ----------

