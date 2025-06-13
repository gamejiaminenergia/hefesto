# %%
import os
import pandas as pd
import numpy as np
import datetime
import dataset
from dotenv import load_dotenv
from pathlib import Path
import json

# %%
def create_url() -> str:
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

# %%
def calculate_market_analysis():
    return "hello world"


# %%
def create_table_data():
    url = create_url()
    db = dataset.connect(url=url)
    table = db['data']
    # table.drop()

    for data in json.load(Path("data/companies.json").open('r', encoding='utf-8')):
        df = pd.json_normalize(
            data=json.load(Path("data/market_analysis_template.json").open('r', encoding='utf-8')), 
            record_path=["assigned_prompts"], 
            meta=["company_name", "company_code", "system_id", "system_name", "system_description", "system_template"]
        )

        df["company_name"] = df["company_name"].apply(lambda x: x.format(company_name=data["company_name"]))
        df["company_code"] = df["company_code"].apply(lambda x: x.format(company_code=data["company_code"]))
        
        df["prompt"] = df["prompt"].apply(lambda x: x.format(company_name=data["company_name"]))
        
        df = df.assign(created_at=datetime.datetime.now(), updated_at=datetime.datetime.now(), status=0, processed=False)
        
        for record in df.to_dict(orient="records"):
            # Check if record with same company_code and prompt_id already exists 
            if not table.find_one(company_code=record["company_code"], prompt_id=record["prompt_id"]):
                table.insert(record)
                

if __name__ == "__main__":
    create_table_data()
    




