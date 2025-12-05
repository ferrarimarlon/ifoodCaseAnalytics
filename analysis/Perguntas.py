# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Análise Exploratória Escolhida: Viagens x Receita

# COMMAND ----------

# MAGIC %md
# MAGIC O objetivo é analisar as viagens mais rentáveis. Para isso, enriqueço os dados da Silver com as coordenadas das zonas de cada Táxi registrado. Desde 2017, esse dataset não possui coordendas de cada viagem (pickup/dropoff) devido à privacidade.
# MAGIC
# MAGIC #### Estratégia
# MAGIC Utilizar o centro geométrico (centroide) de cada zona relativa ao táxi dado que:
# MAGIC * PULocationID  (ID da zona onde o passageiro entrou)
# MAGIC * DOLocationID  (ID da zona onde o passageiro saiu)
# MAGIC
# MAGIC Assim, superamos a limitação de localização exata e ainda nos mantemos em alta fidelidade analítica para insights como:
# MAGIC * Onde acontecem mais pickups
# MAGIC * Onde valores são mais altos
# MAGIC * Onde táxis são mais frequentes
# MAGIC
# MAGIC #### Origem das Zonas:
# MAGIC [Documentação principal](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), seção "Taxi Zone Maps and Lookup Tables", link "Taxi Zone Shapefile (PARQUET)"

# COMMAND ----------

!pip install geopandas

# COMMAND ----------

import geopandas as gpd

shp_url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
gdf = gpd.read_file(shp_url)

# reprojetar para WGS84
gdf = gdf.to_crs(epsg=4326)


# COMMAND ----------

gdf["lat"] = gdf.geometry.centroid.y
gdf["lon"] = gdf.geometry.centroid.x

# COMMAND ----------

spark_zones = spark.createDataFrame(gdf[["LocationID", "lat", "lon"]])

# COMMAND ----------

from pyspark.sql import functions as F

silver = (
    spark.read.format("delta")
    .load("/Volumes/workspace/default/taxis_silver")
    .filter("year = 2023 AND month <= 5")
)

df_geo = (
    silver.alias("s")
    .join(spark_zones.alias("z"), F.col("s.PULocationID") == F.col("z.LocationID"), "inner")
    .select("s.total_amount", "s.cab_type", "z.lat", "z.lon")
)


# COMMAND ----------

df_small = df_geo
pdf = df_small.toPandas()

# COMMAND ----------

!pip install folium

# COMMAND ----------

import folium
from folium.plugins import HeatMap

# reduz a amostra para garantir render
pdf = df_geo.select("lat", "lon").limit(5000).toPandas()

# mapa básico, sem modo detalhado
m = folium.Map(
    location=[40.72, -74.0],
    zoom_start=11,
    tiles="OpenStreetMap"      # <<< remove modo detalhado
)

# heatmap básico
HeatMap(
    pdf[["lat", "lon"]].values,
    radius=8,
    blur=10
).add_to(m)

m


# COMMAND ----------

# gera DF para plots
silver_df = spark.read \
    .format("delta").load("/Volumes/workspace/default/taxis_silver") \
    .filter("year = 2023 AND month <= 5")

#habilita queries SQL
silver_df \
    .createOrReplaceTempView("silver_df")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Top zonas por receita total e ticket médio

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     PULocationID,
# MAGIC     COUNT(*) AS volume,
# MAGIC     SUM(total_amount) AS receita,
# MAGIC     SUM(total_amount) / COUNT(*) AS ticket_medio
# MAGIC FROM silver_df
# MAGIC GROUP BY PULocationID
# MAGIC ORDER BY receita DESC
# MAGIC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Horas de Maior Receita

# COMMAND ----------

import matplotlib.pyplot as plt
import numpy as np
from pyspark.sql import functions as F

# agrega receita por hora e mês
df_hr = (silver_df
        .withColumn("hora", F.hour("pickup_datetime"))
        .groupBy("hora", "month")
        .agg(F.sum("total_amount").alias("receita"))
        .orderBy("hora", "month")
)

pdf = df_hr.toPandas()

# pivot para matriz (horas x meses)
mat = pdf.pivot(index="hora", columns="month", values="receita").fillna(0)

# plot
plt.figure(figsize=(10, 6))
plt.imshow(mat, aspect='auto', cmap='hot')

plt.colorbar(label="Receita Total (USD)")

plt.xlabel("Mês")
plt.ylabel("Hora do Dia")
plt.title("Heatmap: Receita Total por Hora x Mês (Jan-Maio/2023)")

plt.xticks(ticks=np.arange(len(mat.columns)), labels=mat.columns)
plt.yticks(ticks=np.arange(24), labels=np.arange(24))

plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Picos de receita entre 14 e 18, indicando congestionamentos ou taxas diferenciadas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Perguntas do Case

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.  Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     year,
# MAGIC     month,
# MAGIC     AVG(total_amount) AS media_total_amount
# MAGIC FROM silver_df
# MAGIC WHERE cab_type = 'yellow'
# MAGIC GROUP BY year, month
# MAGIC ORDER BY year, month
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

# COMMAND ----------

# MAGIC %md
# MAGIC Obs.: Não existem horas nulas pelo tratamento de normalização entre lpep e tpep na camada Silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     HOUR(pickup_datetime) AS hora,
# MAGIC     AVG(passenger_count) AS media_passageiros
# MAGIC FROM silver_df
# MAGIC WHERE month = 5
# MAGIC GROUP BY HOUR(pickup_datetime)
# MAGIC ORDER BY hora
# MAGIC