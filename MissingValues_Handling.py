import numpy as np
import pandas as pd

# Données avec des valeurs manquantes
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

df
