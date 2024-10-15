import pandas as pd
from airflow.models import Variable
import utils.redshift_utils as redshift_utils
import awswrangler as wr
from typing import List, Tuple, Optional
from utils import constants


def clean_fixture(fixtures: List[dict]) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Procesa los fixtures para la liga de Argentina, valida la integridad de los datos y los carga en Redshift.
    
    Args:
        fixtures (List[dict]): Lista de diccionarios que contienen los datos de los fixtures obtenidos de la API.
    
    Returns:
        Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]: DataFrames de los datos de los partidos (match) y estados (status).
        Devuelve None en caso de no haber datos para cargar.
    """
  
    fixture_argentina = [fixture for fixture in fixtures if fixture['league']['country'] == 'Argentina']

    print(f'El fixture de Argentina es: {fixture_argentina}')
    if not fixture_argentina:
        print('No fixture data was pulled.')
        return None, None
    
    conn = redshift_utils.get_redshift_connection()
    schema = redshift_utils.get_schema()
    
    match_data = []
    status_data = []

    for fixture_info in fixture_argentina:
        fixture = fixture_info['fixture']
        score = fixture_info['score']
        league_info = fixture_info['league']

        home_score = (score['halftime']['home'] or 0) + (score['fulltime']['home'] or 0) + (score['extratime']['home'] or 0)
        away_score = (score['halftime']['away'] or 0) + (score['fulltime']['away'] or 0) + (score['extratime']['away'] or 0)

        match_id = fixture['id']
        venue_id = fixture['venue']['id']
        team_home_id = fixture_info['teams']['home']['id']
        team_away_id = fixture_info['teams']['away']['id']
        league_id = league_info['id']

        existing_match = pd.read_sql(f'SELECT 1 FROM "{schema}".{constants.Config.TABLE_MATCH} WHERE id = {match_id}', con=conn)
        if not existing_match.empty:
            print("El partido ya existe")
            continue  

        existing_team_home = pd.read_sql(f'SELECT 1 FROM "{schema}".team WHERE id = {team_home_id}', con=conn)
        existing_team_away = pd.read_sql(f'SELECT 1 FROM "{schema}".team WHERE id = {team_away_id}', con=conn)
        existing_league = pd.read_sql(f'SELECT 1 FROM "{schema}".league WHERE league_id = {league_id}', con=conn)

        if existing_team_home.empty or existing_team_away.empty or existing_league.empty:
            raise Exception(f"Validación de FK fallida para el partido {match_id}. Omitiendo este fixture.")


        if pd.isnull(team_home_id) or pd.isnull(team_away_id) or pd.isnull(league_id):
            raise Exception(f"Valor nulo detectado en FK en el partido {match_id}. Omitiendo este fixture.")

        match_data.append({
            'id': match_id,
            'date': fixture['date'],
            'timezone': fixture['timezone'],
            'referee': fixture.get('referee', None),
            'venue_id': venue_id,
            'team_home_id': team_home_id,
            'team_away_id': team_away_id,
            'home_score': home_score,
            'away_score': away_score,
            'penalty_home': score['penalty']['home'],
            'penalty_away': score['penalty']['away'],
            'league_id': league_id,
            'season_year': league_info['season'],
            'period_first': fixture['periods']['first'],
            'period_second': fixture['periods']['second']
        })

        status = fixture['status']
        status_data.append({
            'id': match_id,
            'description': status['long']
        })

    df_match = pd.DataFrame(match_data)
    df_status = pd.DataFrame(status_data)
    print(f'los resultados del match son {df_match}')
    print(f'los resultados del status son {df_status}')

    df_status = pd.DataFrame(status_data)

    if df_match.empty and df_status.empty:
        print("No hay datos para cargar.")
        return None, None

    constants.Config.TABLE_MATCH 

    wr.redshift.to_sql(
        df=df_match,
        con=conn,
        table=constants.Config.TABLE_MATCH ,
        schema=schema,
        mode='append',
        use_column_names=True,
        lock=True,
        index=False
    )

    wr.redshift.to_sql(
        df=df_status,
        con=conn,
        table=constants.Config.TABLE_STATUS,
        schema=schema,
        mode='append',
        use_column_names=True,
        lock=True,
        index=False
    )

    print(f'Datos cargados en Redshift correctamente.{df_match}')

    return match_data, status_data
