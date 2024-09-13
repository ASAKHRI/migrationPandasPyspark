import pytest
import pandas as pd
from pyspark.testing import assertDataFrameEqual
from pyspark.sql import SparkSession
from pyspark.sql import functions as F 
import pyspark.testing
from pyspark.testing.utils import assertDataFrameEqual

import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


spark = SparkSession.builder.appName("Read CSV with PySpark").getOrCreate()
def testing(): 
    df = pd.DataFrame(
    {
        'patient_id': [1, 2, 3, 4, 5],
        'age': [34, np.nan, 50, np.nan, 15],
        'department': ['Cardiology', 'Neurology', 'Orthopedics', np.nan, 'Neurology'],
    }
    )
    # Remplacement des valeurs manquantes
    df['age'].fillna(df['age'].mean(), inplace=True)
    df['department'].fillna('Unknown', inplace=True)
    
    ################
    
    ddF = [(1,34,"Cardiology"), \
           (2,None,"Neurology"), \
           (3,50,"Orthopedics"),\
           (4,None,None),\
           (5,15,"Neurology")  
  ]
    empColumns = ["patient_id","age","diagnosis"]

    ddF = spark.createDataFrame(data=ddF, schema = empColumns)
    mean_val=ddF.select(F.mean(ddF.age)).head()[0]
    test = ddF.na.fill(mean_val,subset=['age']).fillna(value = "Unknown",subset=['diagnosis'])
    
    result_pdf = test.select("*").toPandas()
   
    expected_data = [
            (1, 34, "Cardiology"),
            (2, 33, "Neurology"),
            (3, 50, "Orthopedics"),
            (4, 33, "Unknown"),
            (5, 15, "Neurology")
        ]

    assertDataFrameEqual(df, result_pdf)  