import pandas as pd

# Données
df = pd.DataFrame({
    'patient_id': [1, 2, 3, 4, 5],
    'age': [34, 45, 23, 64, 52],
    'department': ['Cardiology', 'Neurology', 'Cardiology', 'Orthopedics', 'Cardiology'],
    'visit_count': [10, 12, 5, 8, 9]
})

# GroupBy et calculs statistiques
agg_df = df.groupby('department').agg({
    'visit_count': 'sum',
    'age': ['mean', 'max']
}).reset_index()

agg_df
