# Databricks notebook source
# MAGIC %md
# MAGIC ### Landing Layer

# COMMAND ----------

volume_name = "taxis_landing"

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {volume_name}
COMMENT 'Volume interno para Landing Layer'
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer

# COMMAND ----------

volume_name = "taxis_bronze"

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {volume_name}
COMMENT 'Volume interno para Bronze Layer'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer

# COMMAND ----------

volume_name = "taxis_silver"

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {volume_name}
COMMENT 'Volume interno para Silver Layer'
""")

# COMMAND ----------

volume_name = "taxis_etl_tests"

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {volume_name}
COMMENT 'Volume interno para Testes'
""")

# COMMAND ----------

