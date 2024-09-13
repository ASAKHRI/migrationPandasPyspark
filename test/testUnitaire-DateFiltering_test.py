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
def testing2(): 
    df = pd.DataFrame(
    {
        'patient_id': [1, 2, 3, 4, 5],
        'age': [34, 45, 50, 20, 15],
        'department': [
            'Cardiology',
            'Neurology',
            'Orthopedics',
            'Cardiology',
            'Neurology',
        ],
    }
)
    filtered_df = df[df['age'] > 30]
    
    ################
    
    dF = [(1,34,"Cardiology"), \
           (2,45,"Neurology"), \
           (3,50,"Orthopedics"),\
           (4,20,"Cardiology"),\
           (5,15,"Neurology")  
  ]
    empColumns = ["patient_id","age","department"]

    dF = spark.createDataFrame(data=dF, schema = empColumns)
    df2 = dF.filter(dF.age>30)
    
     
    dF2_result = df2.toPandas()
    filtered_df_str = filtered_df.astype(str)
    df_spark_filtered_pandas_str = dF2_result.astype(str)
    
    assert filtered_df_str.equals(df_spark_filtered_pandas_str)
