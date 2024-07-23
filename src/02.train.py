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

query = import_query("activate.sql")
df_target = spark.sql(query)

traning_set = fe.create_training_set(df=df_target, label="flActivate", feature_lookups=lookups)
df_traning = traning_set.load_df()

# COMMAND ----------

from sklearn import model_selection
from sklearn import ensemble
from sklearn import metrics

df_train = df_traning.toPandas()

features = df_train.columns.tolist()[2:-1]
target = 'flActivate'

X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features], df_train[target],
                                                                    test_size=0.2,
                                                                    random_state=42)

# COMMAND ----------

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Users/teomewhy@gmail.com/apoena_databricks")
mlflow.sklearn.autolog()

with mlflow.start_run():

    clf = ensemble.RandomForestClassifier(n_estimators=150,
                                          max_depth=5,
                                          min_samples_leaf=10,
                                          random_state=42)
    clf.fit(X_train, y_train)

    pred_train = clf.predict(X_train)
    pred_test = clf.predict(X_test)

    prob_train = clf.predict_proba(X_train)[:,1]
    prob_test = clf.predict_proba(X_test)[:,1]

    acc_train = metrics.accuracy_score(y_train, pred_train)
    acc_test = metrics.accuracy_score(y_test, pred_test)

    auc_train = metrics.roc_auc_score(y_train, prob_train)
    auc_test = metrics.roc_auc_score(y_test, prob_test)

    metricas = {
        "acc_train": acc_train,
        "acc_test": acc_test,
        "auc_train": auc_train,
        "auc_test": auc_test,
    }

    mlflow.log_metrics(metricas)

    fe.log_model(
        model=clf,
        flavor=mlflow.sklearn,
        artifact_path="apoena_databricks_activate",
        training_set=traning_set,
        registered_model_name="feature_store.upsell.apoena_databricks_activate",
    )


# COMMAND ----------


