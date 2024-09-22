import pandas as pd
import sqlite3

import os
import sqlite3


db_directory = './temp/load/database/'
db_path = os.path.join(db_directory, 'local_database.db')


if not os.path.exists(db_directory):
    os.makedirs(db_directory)
    print(f"Directorio creado: {db_directory}")


conn = sqlite3.connect(db_path)
print(f"Base de datos conectada/creada en: {db_path}")


cur = conn.cursor()
cur.execute("""
    CREATE TABLE IF NOT EXISTS countries (
        country_code TEXT PRIMARY KEY,
        country_name TEXT,
        country_flag TEXT
    )
""")
conn.commit()
df_transformed = pd.read_csv('./temp/extract/countries/countries_arg.csv')
df_transformed.to_sql('countries', conn, if_exists='replace', index=False)
conn.close()
print("Datos de países cargados en la base de datos local en la tabla 'countries'.")


conn.close()
print("Conexión a la base de datos cerrada.")