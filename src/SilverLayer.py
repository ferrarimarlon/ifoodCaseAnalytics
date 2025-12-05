# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer

# COMMAND ----------

# MAGIC %md
# MAGIC Uniformizar os contratos de táxis, garantindo extração de potencial analítico pleno. 
# MAGIC
# MAGIC Estratégia em overwrite para garantia de consistência devido à tendência de reprocessamentos desta camada.
# MAGIC Desabilita alteração do schema e adiciona validação de schema para evitar drift
# MAGIC
# MAGIC Particionamento físico mantido

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F

unified_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("pickup_datetime", TimestampType(), True),    # unified column
    StructField("dropoff_datetime", TimestampType(), True),   # unified column
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),  # yellow only
    StructField("trip_type", IntegerType(), True),   # green only
    StructField("ehail_fee", DoubleType(), True),    # green only
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("cab_type", StringType(), True),
])

yellow_path = "/Volumes/workspace/default/taxis_bronze/yellow/"
green_path  = "/Volumes/workspace/default/taxis_bronze/green/"

df_yellow = spark.read.format("delta").load(yellow_path)
df_green  = spark.read.format("delta").load(green_path)

# Rename columns for unified schema
df_yellow = df_yellow.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                     .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
                     .withColumn("trip_type", F.lit(None).cast(IntegerType())) \
                     .withColumn("ehail_fee", F.lit(None).cast(DoubleType()))

df_green  = df_green.withColumnRenamed("lpep_pickup_datetime", "pickup_datetime") \
                    .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime") \
                    .withColumn("airport_fee", F.lit(None).cast(DoubleType()))

# Select columns in unified order
df_yellow = df_yellow.select([f.name for f in unified_schema])
df_green  = df_green.select([f.name for f in unified_schema])


unified_path = "/Volumes/workspace/default/taxis_silver"

# Concatenate both
df_unified = df_yellow.unionByName(df_green)

df_unified = df_unified.dropDuplicates(["VendorID", "pickup_datetime", "year", "month", "cab_type"])

(df_unified.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "false")       # protege o schema
    .option("validateSchema", "true")         # falha se houver incompatibilidade
    .partitionBy("year", "month", "cab_type")
    .save(unified_path)
)



# COMMAND ----------

