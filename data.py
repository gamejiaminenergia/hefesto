import pandas as pd
import os
import glob


data_dir = 'data'
files = glob.glob(os.path.join(data_dir, '*.json'))
df=[pd.read_json(file) for file in files]
df=pd.concat(df, ignore_index=True)
df=df[['CodigoSICAgente','NombreAgente','ActividadAgente']]
df=df.drop_duplicates(subset=['CodigoSICAgente'],keep='first')
df=df.sort_values(by=['CodigoSICAgente'])
df.to_csv('data.csv', index=False)