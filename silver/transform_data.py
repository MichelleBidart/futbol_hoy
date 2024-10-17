import pandas as pd
import utils.redshift_utils as redshift_utils
from utils import constants


def clean_fixture(fixtures):
    """
    Procesa los fixtures para la liga de Argentina, valida la integridad de los datos 
    
    Args:
        fixtures (List[dict]): Lista de diccionarios que contienen los datos de los fixtures obtenidos de la API.
    
    Returns:
        Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]: DataFrames de los datos de los partidos (match) y estados (status).
        Devuelve None en caso de no haber datos para cargar.
    """
  
    fixture_argentina = [fixture for fixture in fixtures if fixture['league']['country'] == 'Argentina']

    print(f'El fixture de torneos Argentinos es: {fixture_argentina}')

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
            print(f"El partido con ID {match_id} ya existe. Saltando este fixture.")
            continue  
        
        existing_team_home = pd.read_sql(f'SELECT 1 FROM "{schema}".team WHERE id = {team_home_id}', con=conn)
        existing_team_away = pd.read_sql(f'SELECT 1 FROM "{schema}".team WHERE id = {team_away_id}', con=conn)
        existing_league = pd.read_sql(f'SELECT 1 FROM "{schema}".league WHERE league_id = {league_id}', con=conn)

        print(f'pasa los existing ')

        if existing_team_home.empty or existing_team_away.empty or existing_league.empty:
            raise Exception(f"Validación de FK fallida para el partido {match_id}. Omitiendo este fixture.")

        print(f'pasa la validacion de fk ')

        date_transform = pd.to_datetime(fixture['date']).strftime('%Y-%m-%d %H:%M:%S')

        match_data.append({
            'id': match_id,
            'date': date_transform,
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
    print(f'status data {status_data}')
    return match_data, status_data