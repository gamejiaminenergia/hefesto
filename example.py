import pandas as pd
import numpy as np
import requests
import datetime
import dataset
from dotenv import load_dotenv
import os
import json

def build_database_url() -> str:
    """
    Construye la URL de conexión a la base de datos PostgreSQL
    utilizando variables de entorno.
    """
    load_dotenv()

    user = os.getenv('POSTGRES_USER', 'postgres')
    password = os.getenv('POSTGRES_PASSWORD', 'postgres')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', 5432)
    db = os.getenv('POSTGRES_DB', 'postgres')

    return f'postgresql://{user}:{password}@{host}:{port}/{db}'

def get_data_api(date):
    url = "https://www.simem.co/backend-files/api/PublicData"
    params = {
        'datasetId': '972263',
        'startDate': date.strftime('%Y-%m-%d'),
        'endDate': date.strftime('%Y-%m-%d')
    }
    
    try:
        print(f"Descargando datos para {date.strftime('%Y-%m-%d')}...")
        response = requests.get(url, params=params, timeout=60*10)
        response.raise_for_status()
        
        data = response.json()['result']['records']
        
        return data
    except Exception as e:
        print(f"Error al obtener datos para {date}: {str(e)}")
        return None


def download_and_save_data(start_date, end_date):
    URL = build_database_url()
    db = dataset.connect(URL)

    dates = pd.date_range(start=start_date, end=end_date)
    table = db['data']
    for date in dates:
        if table.find_one(date=date.strftime('%Y-%m-%d')):
            print(f"Datos para {date.strftime('%Y-%m-%d')} ya existen")
            continue
        else:
            table.insert({"date": date.strftime('%Y-%m-%d'), "data": json.dumps(get_data_api(date))})
            print(f"Datos para {date.strftime('%Y-%m-%d')} guardados")

def fetch_data_from_database():
    URL = build_database_url()
    db = dataset.connect(URL)
    table = db['data']
    df = pd.concat((pd.json_normalize(json.loads(data['data'])) for data in table.all()), ignore_index=True)
    df["CodigoSICAgente"]=df["CodigoSICAgente"].str[:3]
    df=df.drop_duplicates(subset=['CodigoSICAgente'], keep='last', ignore_index=True)
    rename_columns={'CodigoSICAgente':'company_code', 'NombreAgente':'company_name'}
    df=df.rename(columns=rename_columns)[rename_columns.values()]
    df.reset_index(inplace=True)
    df.to_json('companies.json', orient='records')

    # df=df.drop_duplicates(subset=['CodigoSICAgente'], keep='last', ignore_index=True)
    # rename_columns={'CodigoSICAgente':'agent', 'NombreAgente':'agent_name', 'ActividadAgente':'activity'}
    # df=df.rename(columns=rename_columns)[rename_columns.values()]
    # df["agent"]=df["agent"].str[:3]
    # df['activity']=df['activity'].str.upper()
    # df = df.groupby(['agent', 'agent_name'], as_index=False).agg({'activity': 'unique'})
    # df['activity'] = df['activity'].apply(list)
    # df.to_json('data.json', orient='records')

    

def main():
    start_date = datetime.date(2025, 1, 1)
    end_date = datetime.date(2025, 5, 30)
    download_and_save_data(start_date, end_date)

    df = fetch_data_from_database()
    
    print(df)

    

if __name__ == "__main__":
    main()













