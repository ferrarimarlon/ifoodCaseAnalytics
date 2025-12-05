# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Conflito de Schemas - Estudo do Contrato a ser implementado
# MAGIC Ao analisar o dicionário de dados dos datasets, vemos que os Taxis Green e Yellow possuem divergências de schemas entre si, indicando sistemas transacionais diferentes e que, portanto, precisam manter-se fiéis em relação a eles na camada Bronze.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Analisando os schemas

# COMMAND ----------

import pandas as pd

green_schema = set(pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2023-01.parquet").columns)
yellow_schema = set(pd.read_parquet("https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet").columns)

print("Em comum: ", green_schema.intersection(yellow_schema))
print("Apenas Green ", green_schema.difference(yellow_schema))
print("Apenas Yellow ", yellow_schema.difference(green_schema))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Construção do Contrato de Dados

# COMMAND ----------

# MAGIC %md
# MAGIC É necessária a implementação de 2 contratos em separado para garantia da continuidade e robustez de cada origem, dadas que são diferentes. Mergear neste momento seria um erro, perdendo a linhagem de dados de cada origem.

# COMMAND ----------

import os
import requests
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType
)

class YellowTaxiContract:
    def __init__(self):
        self.input_schema = StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("tpep_pickup_datetime", TimestampType(), True),
            StructField("tpep_dropoff_datetime", TimestampType(), True),
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
            StructField("airport_fee", DoubleType(), True),
        ])

    def full_schema(self):
        return (self.input_schema
            .add(StructField("year", IntegerType(), True))
            .add(StructField("month", IntegerType(), True))
            .add(StructField("cab_type", StringType(), True))
        )


class GreenTaxiContract:
    def __init__(self):
        self.input_schema = StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("lpep_pickup_datetime", TimestampType(), True),
            StructField("lpep_dropoff_datetime", TimestampType(), True),
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
            StructField("trip_type", IntegerType(), True),
            StructField("ehail_fee", DoubleType(), True),
        ])

    def full_schema(self):
        return (self.input_schema
            .add(StructField("year", IntegerType(), True))
            .add(StructField("month", IntegerType(), True))
            .add(StructField("cab_type", StringType(), True))
        )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Pipeline de ETL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escolha técnica: 
# MAGIC
# MAGIC Incremental-append em Delta Lake. 
# MAGIC
# MAGIC Justificativa: devido à necessidade de idempotência e ganho em se evitar redundância de inserts, trazendo melhoria de performance.
# MAGIC
# MAGIC ### Particionamento
# MAGIC Tipo: físico por metadados e tipo de táxi
# MAGIC
# MAGIC Justificativa: garantia de pushdown predicate em filtros usados nas análises (mês e tipo de táxi)
# MAGIC
# MAGIC Possui controle de duplicidade para origens redundantes, levando-se em conta as diferenças de layout de cada tipo de táxi.

# COMMAND ----------

class ETLNYC:
    BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    def __init__(self, spark, contract, delta_path, local_dir="/Volumes/workspace/default/taxis_landing"):
        self.spark = spark
        self.contract = contract
        self.delta_path = delta_path
        self.local_dir = local_dir
        os.makedirs(self.local_dir, exist_ok=True)

    def build_url(self, cab_type, year, month):
        return f"{self.BASE}/{cab_type}_tripdata_{year}-{month:02d}.parquet"

    def download_file(self, cab_type, year, month):
        url = self.build_url(cab_type, year, month)
        local_path = f"{self.local_dir}/{cab_type}_{year}_{month:02d}.parquet"
        if not os.path.exists(local_path):
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(local_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
        return local_path

    def read_via_spark(self, cab_type, year, month):
        path = self.download_file(cab_type, year, month)
        df = self.spark.read.parquet(path)

        # Garantir todas as colunas do contrato
        for field in self.contract.input_schema:
            if field.name in df.columns:
                df = df.withColumn(field.name, df[field.name].cast(field.dataType))
            else:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))

        df = df.select([field.name for field in self.contract.input_schema])
        df = df.withColumn("year", F.lit(int(year))) \
               .withColumn("month", F.lit(int(month))) \
               .withColumn("cab_type", F.lit(cab_type))

        # Deduplicar usando datetime correto
        datetime_col = "tpep_pickup_datetime" if cab_type == "yellow" else "lpep_pickup_datetime"
        df = df.dropDuplicates(["VendorID", datetime_col, "year", "month"])

        return df

    def merge_into_delta(self, df, cab_type):
        if DeltaTable.isDeltaTable(self.spark, self.delta_path):
            delta = DeltaTable.forPath(self.spark, self.delta_path)
            delta_alias = delta.alias("t")
            df_alias = df.alias("s")

            datetime_col = "tpep_pickup_datetime" if cab_type == "yellow" else "lpep_pickup_datetime"

            cond = (
                (F.col(f"t.VendorID") == F.col("s.VendorID")) &
                (F.col(f"t.{datetime_col}") == F.col(f"s.{datetime_col}")) &
                (F.col("t.year") == F.col("s.year")) &
                (F.col("t.month") == F.col("s.month"))
            )

            delta_alias.merge(df_alias, cond)\
                .whenNotMatchedInsertAll()\
                .execute()
        else:
            (df.write.format("delta")
                .mode("overwrite")
                .partitionBy("year", "month")  # sem cab_type aqui
                .save(self.delta_path)
            )