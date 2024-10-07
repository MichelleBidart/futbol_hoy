import api_url_configurations as api_url_configurations 
import requests
import csv
import redshift_utils as redshift_utils
import pandas as pd
import os
import awswrangler as wr


def extract_countries():
    url, headers = api_url_configurations.get_api_url_headers()
    response = requests.get(f'{url}/countries', headers=headers) 
    countries = response.json()['response']
    
    os.makedirs('./temp/extract/countries', exist_ok=True)

    csv_path = './temp/extract/countries/countries.csv'
    
    with open(csv_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['name', 'code'])
        for country in countries:
            writer.writerow([country['name'], country['code']])
    
    print(f"Datos de países guardados en {csv_path}")
    return csv_path


def transform_countries(csv_path):

    csv_path_argentina = './temp/extract/countries/countries_arg.csv'

    countries_df = pd.read_csv(csv_path)

    argentina_df = countries_df[countries_df['code'] == 'AR']

    print(argentina_df)

    csv_path = argentina_df.to_csv('./temp/extract/countries/countries_arg.csv', index=False)

    return csv_path_argentina



def load_to_redshift(csv_path, table_name):
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()

    with conn.cursor() as cursor:
        cursor.execute(f'DELETE FROM "{schema}"."{table_name}"')
        conn.commit()
        print(f"Datos eliminados de la tabla {schema}.{table_name}.")

    df = pd.read_csv(csv_path)
    
    wr.redshift.to_sql(
        df=df,
        con=conn,
        table=table_name,
        schema=schema,
        mode='append', 
        use_column_names=True,
        lock=True,
        index=False
    )

    print(f"Datos cargados en la tabla {schema}.{table_name}")



def etl_countries():
 
    csv_path = extract_countries()

    print("csv_path return: ", csv_path)

    argentina_csv_path = transform_countries(csv_path)

    print("argentina_csv_path return: ", argentina_csv_path)
    
    load_to_redshift(argentina_csv_path, 'country')


if __name__ == '__main__':
    etl_countries()
