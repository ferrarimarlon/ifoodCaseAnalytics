# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Quality
# MAGIC Dimensões analisadas:
# MAGIC 1. Schema
# MAGIC 2. Nulidade
# MAGIC 3. Regras de domínio
# MAGIC 4. Consistência temporal
# MAGIC 5. Outliers
# MAGIC 6. Duplicidade

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

def run_data_quality(df):

    dq_results = {}

    # -------------------------
    # 1. Validação de schema
    # -------------------------
    expected_fields = {
        "VendorID": "int",
        "pickup_datetime": "timestamp",
        "dropoff_datetime": "timestamp",
        "passenger_count": "int",
        "trip_distance": "double",
        "RatecodeID": "int",
        "store_and_fwd_flag": "string",
        "PULocationID": "int",
        "DOLocationID": "int",
        "payment_type": "int",
        "fare_amount": "double",
        "extra": "double",
        "mta_tax": "double",
        "tip_amount": "double",
        "tolls_amount": "double",
        "improvement_surcharge": "double",
        "total_amount": "double",
        "congestion_surcharge": "double",
        "airport_fee": "double",
        "trip_type": "int",
        "ehail_fee": "double",
        "year": "int",
        "month": "int",
        "cab_type": "string"
    }

    schema_errors = []
    for col_name, col_type in expected_fields.items():
        if col_name not in df.columns:
            schema_errors.append(f"Missing column: {col_name}")
        else:
            spark_type = df.schema[col_name].dataType.simpleString()
            if spark_type != col_type:
                schema_errors.append(f"Type mismatch: {col_name} expected {col_type}, found {spark_type}")

    dq_results["schema_errors"] = schema_errors


    # -------------------------
    # 2. Regras de nulidade
    # -------------------------
    null_checks = {
        "pickup_datetime": "no pickup datetime",
        "dropoff_datetime": "no dropoff datetime",
        "VendorID": "no vendor",
        "PULocationID": "no pickup location",
        "DOLocationID": "no dropoff location"
    }

    for col_name, desc in null_checks.items():
        dq_results[f"nulls_{col_name}"] = df.filter(F.col(col_name).isNull()).count()


    # -------------------------
    # 3. Regras de domínio
    # -------------------------
    domain_rules = {
        "passenger_count >= 0": df.filter("passenger_count < 0").count(),
        "trip_distance >= 0": df.filter("trip_distance < 0").count(),
        "fare_amount >= 0": df.filter("fare_amount < 0").count(),
        "total_amount >= 0": df.filter("total_amount < 0").count(),
    }

    dq_results["domain_rules"] = domain_rules


    # -------------------------
    # 4. Consistência temporal
    # -------------------------
    dq_results["negative_trip_time"] = df.filter(
        F.col("dropoff_datetime") < F.col("pickup_datetime")
    ).count()

    dq_results["trip_over_24h"] = df.filter(
        F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime") > 86400
    ).count()


    # -------------------------
    # 5. Outliers estruturais (z-score)
    # -------------------------
    numeric_cols = ["trip_distance", "fare_amount", "total_amount"]

    for c in numeric_cols:
        stats = df.select(
            F.mean(c).alias("mean"),
            F.stddev(c).alias("std")
        ).collect()[0]
        
        mean_c = stats["mean"]
        std_c = stats["std"] or 0

        if std_c > 0:
            # 4*std para valores extremamente absurdos
            outliers = df.filter(F.abs(F.col(c) - mean_c) > 4 * std_c).count()
        else:
            outliers = 0

        dq_results[f"outliers_{c}"] = outliers


    # -------------------------
    # 6. Perfilamento geral
    # -------------------------
    total = df.count()
    dq_results["row_count"] = total

    dq_results["distinct_keys"] = df.select(
        "VendorID", "pickup_datetime", "year", "month", "cab_type"
    ).distinct().count()

    dq_results["duplicate_keys"] = total - dq_results["distinct_keys"]

    return dq_results


# COMMAND ----------

silver_path = "/Volumes/workspace/default/taxis_silver/"

silver_df = spark \
  .read \
  .format("delta") \
  .load(silver_path) \
  .filter("year = 2023 AND month <= 5") \

dq = run_data_quality(silver_df)

for k, v in dq.items():
    print(k, "=>", v)
