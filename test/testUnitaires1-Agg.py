import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import numpy as np

# Créer une session Spark
spark = SparkSession.builder.appName("Test").getOrCreate()

# DataFrame pandas
df = pd.DataFrame(
    {
        "patient_id": [1, 2, 3, 4, 5],
        "age": [34, 45, 23, 64, 52],
        "department": [
            "Cardiology",
            "Neurology",
            "Cardiology",
            "Orthopedics",
            "Cardiology",
        ],
        "visit_count": [10, 12, 5, 8, 9],
    }
)

# GroupBy et calculs statistiques
agg_df = (
    df.groupby("department")
    .agg({"visit_count": "sum", "age": ["mean", "max"]})
    .reset_index()
)
agg_df.columns = [
    "department",
    "visit_count_sum",
    "age_mean",
    "age_max",
]  # Renommer les colonnes pour faciliter la comparaison

# DataFrame pyspark
emp = [
    (1, 34, "Cardiology", 10),
    (2, 45, "Neurology", 12),
    (3, 23, "Cardiology", 5),
    (4, 64, "Orthopedics", 8),
    (5, 52, "Cardiology", 9),
]
empColumns = ["patient_id", "age", "department", "visit_count"]

empDF = spark.createDataFrame(data=emp, schema=empColumns)

# GroupBy et calculs statistiques
group = empDF.groupBy("department").agg(
    F.sum(F.col("visit_count")).alias("visit_count_sum"),
    F.avg(F.col("age")).alias("age_mean"),
    F.max(F.col("age")).alias("age_max"),
)


def test_dataframe_equality5():
    # Convertir le DataFrame PySpark en DataFrame pandas
    group_pandas = group.toPandas()

    # Convertir les deux DataFrames en chaînes de caractères
    agg_df_str = agg_df.astype(str)
    group_pandas_str = group_pandas.astype(str)

    # Réorganiser les colonnes de manière à ce que l'ordre ne soit pas un problème
    agg_df_str_sorted = agg_df_str.sort_values(by=["department"]).reset_index(drop=True)
    group_pandas_str_sorted = group_pandas_str.sort_values(
        by=["department"]
    ).reset_index(drop=True)

    # Comparer les DataFrames
    assert agg_df_str_sorted.equals(
        group_pandas_str_sorted
    ), "Les valeurs des DataFrames ne sont pas identiques."

    # Exécuter le test
    test_dataframe_equality5()
