# Databricks notebook source
# MAGIC %run ./BronzeLayer

# COMMAND ----------

class Runner:
    def __init__(self, delta_path="/Volumes/workspace/default/taxis_bronze"):
        self.spark = (SparkSession.builder
            .appName("NYC Trips ETL")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate()
        )

        self.delta_path = delta_path
        self.init_delta()

    def init_delta(self):
        # Cria Deltas vazios separados
        for cab_type, contract in [("yellow", YellowTaxiContract()), ("green", GreenTaxiContract())]:
            path = os.path.join(self.delta_path, cab_type)
            if not DeltaTable.isDeltaTable(self.spark, path):
                schema = contract.full_schema()
                empty_df = self.spark.createDataFrame([], schema)
                (empty_df.write.format("delta")
                    .partitionBy("year", "month")  # sem cab_type
                    .mode("overwrite")
                    .save(path)
                )

    def run(self, years=[2023]):
        for cab_type, contract in [("yellow", YellowTaxiContract()), ("green", GreenTaxiContract())]:
            etl = ETLNYC(self.spark, contract, os.path.join(self.delta_path, cab_type))
            for year in years:
                for month in range(1, 13):
                    df = etl.read_via_spark(cab_type, year, month)
                    etl.merge_into_delta(df, cab_type)

# COMMAND ----------

if __name__ == "__main__":
    Runner().run(years=[2023])