import os
import pandas as pd
from airflow.models import Variable
import requests
from dotenv import load_dotenv
from utils import api_url_configurations

load_dotenv('/opt/airflow/.env')

def ingest_data_fixture(fixture_date):

    """
    se traen los datos de football api y se guardan en un parquet en una carpeta temporal 
    """
    url, headers = api_url_configurations()
    url = "https://v3.football.api-sports.io/fixtures"
    params = {
        'date': fixture_date  
    }
    api_key = Variable.get("x-apisports-key")
    
    headers = {
        'x-apisports-key': api_key
    }
    
    response = requests.get(url + "fixtures", headers=headers, params=params)

    if response.status_code != 200:
        raise Exception("error al querer buscar el fixture del dia") 

    data = response.json()

    df_day_fixture = pd.DataFrame(data['response'])  

    parquet_directory = './temp/extract/fixture'
    if not os.path.exists(parquet_directory):
        os.makedirs(parquet_directory)
    parquet_filename = f"match_{fixture_date}.parquet"
    parquet_path = os.path.join(parquet_directory, parquet_filename)
    df_day_fixture.to_parquet(parquet_path)
    
    print(f"Data for {fixture_date} saved to {parquet_path}")
    
    return data['response']

