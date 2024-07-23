# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
import mlflow
fe = FeatureEngineeringClient()

def import_query(path):
    with open(path, "r") as open_file:
        return open_file.read()

# COMMAND ----------

lookups = [
    FeatureLookup(table_name="feature_store.upsell.pontos", lookup_key=['dtRef', "idCliente"]),
    FeatureLookup(table_name="feature_store.upsell.transacoes", lookup_key=['dtRef', "idCliente"]),
]

df_target = (spark.table("feature_store.upsell.pontos")
                  .filter("dtRef = '2024-07-22'")
                  .select("dtRef", 'idCliente'))

predict_set = fe.create_training_set(df=df_target, label=None, feature_lookups=lookups)
df_predict = predict_set.load_df().toPandas()
df_predict.head()

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
model_pyfunc = mlflow.pyfunc.load_model("models:/feature_store.upsell.apoena_databricks_activate/2")
model_sklearn = mlflow.sklearn.load_model(f'runs:/{model_pyfunc.metadata.run_id}/model')

# COMMAND ----------

df_predict['proba'] =  model_sklearn.predict_proba(df_predict[model_sklearn.feature_names_in_])[:,1]
df_predict

# COMMAND ----------


