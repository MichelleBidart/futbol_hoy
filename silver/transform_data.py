import pandas as pd
import utils.redshift_utils as redshift_utils
from utils import constants


def clean_fixture(fixtures, conn):
    """
    Procesa los fixtures para la liga de Argentina, valida la integridad de los datos 
    
    Args:
        fixtures (List[dict]): Lista de diccionarios que contienen los datos de los fixtures obtenidos de la API.
    
    Returns:
        Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]: DataFrames de los datos de los partidos (match) y estados (status).
        Devuelve None en caso de no haber datos para cargar.
    """
    #busco solo las ligas argentinas
    fixture_argentina = [fixture for fixture in fixtures if fixture['league']['country'] == 'Argentina']

    if not fixture_argentina:
        print('No fixture data was pulled.')
        return None, None
    
    print(f'El fixture de torneos Argentinos es: {fixture_argentina}')
    
    schema = redshift_utils.get_schema()
    
    match_data = []
    status_data = []

    for fixture_info in fixture_argentina:
        fixture = fixture_info['fixture']
        score = fixture_info['score']
        league_info = fixture_info['league']

        home_score = (score['fulltime']['home'] or 0) + (score['extratime']['home'] or 0)
        away_score = (score['fulltime']['away'] or 0) + (score['extratime']['away'] or 0)

        match_id = fixture['id']
        venue_id = fixture['venue']['id']
        team_home_id = fixture_info['teams']['home']['id']
        team_away_id = fixture_info['teams']['away']['id']
        league_id = league_info['id']


        existing_match = pd.read_sql(f'SELECT 1 FROM "{schema}".{constants.Config.TABLE_MATCH} WHERE id = {match_id}', con=conn)
        if not existing_match.empty:
            print(f"El partido con ID {match_id} ya existe. Saltando este fixture.")
            continue  
        
        existing_team_home = pd.read_sql(f'SELECT 1 FROM "{schema}".team WHERE id = {team_home_id}', con=conn)
        existing_team_away = pd.read_sql(f'SELECT 1 FROM "{schema}".team WHERE id = {team_away_id}', con=conn)
        existing_league = pd.read_sql(f'SELECT 1 FROM "{schema}".league WHERE league_id = {league_id}', con=conn)


        if existing_team_home.empty or existing_team_away.empty or existing_league.empty:
            raise Exception(f"Validación de FK fallida para el partido {match_id}. Omitiendo este fixture.")

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
    if not status_data or not match_data:
        print("No hay datos en match_data o status_data.")
        return None, None

    df_match = pd.DataFrame(match_data)
    df_status = pd.DataFrame(status_data)

    if df_match.empty and df_status.empty:
        print("No hay df datos para cargar.")
        return None, None


    return df_match, df_status