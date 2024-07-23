# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
from tqdm import tqdm
fe = FeatureEngineeringClient()

# COMMAND ----------

def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

def table_exists(catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                .filter(f"database='{database}' and tableName='{table}'")
                .count())
    return count == 1

tablename = "pontos"
query = import_query(f"{tablename}.sql")
# dates = ['2024-02-01', '2024-03-01', '2024-04-01', '2024-05-01', '2024-06-01', '2024-07-01']
dates = ['2024-07-22']

if not table_exists("feature_store", "upsell", tablename):
    df = spark.sql(query.format(dt_ref=dates.pop(0)) )
    fe.create_table(df=df,
                    name=f"feature_store.upsell.{tablename}",
                    primary_keys=["dtRef", "idCliente"],
                    partition_columns=['dtRef'],
                    schema=df.schema)

for d in tqdm(dates):
    df = spark.sql(query.format(dt_ref=d) )
    fe.write_table(df=df,
                name=f"feature_store.upsell.{tablename}",
                mode="merge")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM feature_store.upsell.pontos
# MAGIC order by idCliente, dtRef
