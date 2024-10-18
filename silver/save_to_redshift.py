import pandas as pd
import utils.redshift_utils as redshift_utils
import awswrangler as wr
from utils import constants

def save_fixture_to_database(df_match, df_status, conn) -> None:
    """
    Guarda los datos de los partidos y los estados en la base de datos Redshift.

    Args:
        df_match (pd.DataFrame): DataFrame que contiene los datos de los partidos.
        df_status (pd.DataFrame): DataFrame que contiene los datos de los estados.
    """

    schema = redshift_utils.get_schema()
    print(f'schema {schema}')
    
    wr.redshift.to_sql(
        df=df_match,
        con=conn,
        table=constants.Config.TABLE_MATCH,
        schema="2024_michelle_bidart_schema",
        mode='append',
        use_column_names=True,
        lock=True,
        index=False
    )

    wr.redshift.to_sql(
        df=df_status,
        con=conn,
        table=constants.Config.TABLE_STATUS,
        schema="2024_michelle_bidart_schema",
        mode='append',
        use_column_names=True,
        lock=True,
        index=False
    )


    print(f'Datos de los partidos y los estados cargados en Redshift correctamente.')
