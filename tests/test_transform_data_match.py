import pytest
import pandas as pd
from unittest.mock import patch
from silver.transform_data import clean_fixture 

@pytest.fixture
def fixtures_ya_existente():
    return [{
        'fixture': {
            'id': 1158794,
            'referee': 'N. Arasa',
            'timezone': 'UTC',
            'date': '2024-08-10T00:00:00+00:00',
            'periods': {'first': 1723248000, 'second': 1723251600},
            'venue': {'id': 99, 'name': 'Estadio Presidente Juan Domingo Perón'},
            'status': {'long': 'Match Finished', 'short': 'FT', 'elapsed': 90}
        },
        'league': {
            'id': 128,
            'name': 'Liga Profesional Argentina',
            'country': 'Argentina',
            'season': 2024
        },
        'teams': {
            'home': {'id': 436, 'name': 'Racing Club'},
            'away': {'id': 434, 'name': 'Gimnasia L.P.'}
        },
        'score': {
            'halftime': {'home': 0, 'away': 1},
            'fulltime': {'home': 0, 'away': 1},
            'extratime': {'home': None, 'away': None},
            'penalty': {'home': None, 'away': None}
        }
    }]

@pytest.fixture
def fixtures_con_argentina():
    return [{
        'fixture': {
            'id': 1158794,
            'referee': 'N. Arasa',
            'timezone': 'UTC',
            'date': '2024-08-10T00:00:00+00:00',
            'periods': {'first': 1723248000, 'second': 1723251600},
            'venue': {'id': 99, 'name': 'Estadio Presidente Juan Domingo Perón'},
            'status': {'long': 'Match Finished', 'short': 'FT', 'elapsed': 90}
        },
        'league': {
            'id': 128,
            'name': 'Liga Profesional Argentina',
            'country': 'Argentina',
            'season': 2024
        },
        'teams': {
            'home': {'id': 436, 'name': 'Racing Club'},
            'away': {'id': 434, 'name': 'Gimnasia L.P.'}
        },
        'score': {
            'halftime': {'home': 0, 'away': 1},
            'fulltime': {'home': 0, 'away': 1},
            'extratime': {'home': None, 'away': None},
            'penalty': {'home': None, 'away': None}
        }
    }]

@pytest.fixture
def sample_fixtures():
    return [{
        'fixture': {
            'id': 1158794,
            'referee': 'N. Arasa',
            'timezone': 'UTC',
            'date': '2024-08-10T00:00:00+00:00',
            'periods': {'first': 1723248000, 'second': 1723251600},
            'venue': {'id': 99, 'name': 'Estadio Presidente Juan Domingo Perón'},
            'status': {'long': 'Match Finished', 'short': 'FT', 'elapsed': 90}
        },
        'league': {
            'id': 128,
            'name': 'Liga Profesional Argentina',
            'country': 'Argentina',
            'season': 2024
        },
        'teams': {
            'home': {'id': 436, 'name': 'Racing Club'},
            'away': {'id': 434, 'name': 'Gimnasia L.P.'}
        },
        'score': {
            'halftime': {'home': 0, 'away': 1},
            'fulltime': {'home': 0, 'away': 1},
            'extratime': {'home': None, 'away': None},
            'penalty': {'home': None, 'away': None}
        }
    }]
@pytest.fixture
def fixtures_sin_argentina():
    return [{
        'fixture': {
            'id': 12345,
            'referee': 'A. Referee',
            'timezone': 'UTC',
            'date': '2024-08-10T00:00:00+00:00',
            'periods': {'first': 1723248000, 'second': 1723251600},
            'venue': {'id': 99, 'name': 'Non-Argentina Venue'},
            'status': {'long': 'Match Scheduled', 'short': 'MS', 'elapsed': None},
        },
        'league': {
            'id': 999, 'name': 'Other League', 'country': 'Other Country', 'season': 2024
        },
        'teams': {
            'home': {'id': 9999, 'name': 'Other Home Team'},
            'away': {'id': 8888, 'name': 'Other Away Team'},
        },
        'score': {
            'halftime': {'home': 0, 'away': 0},
            'fulltime': {'home': 0, 'away': 0},
            'extratime': {'home': None, 'away': None},
            'penalty': {'home': None, 'away': None},
        }
    }]
@patch('silver.transform_data.pd.read_sql')
@patch('silver.transform_data.redshift_utils.get_schema', return_value='test_schema')
@patch('silver.transform_data.redshift_utils.get_redshift_connection')
def test_clean_fixture_valid_data(mock_get_redshift_connection, mock_get_schema, mock_read_sql, sample_fixtures):
    
    def mock_read_sql_side_effect(query, con):
        if 'team WHERE id = 436' in query:
            return pd.DataFrame({'id': [436]})  
        elif 'team WHERE id = 434' in query:
            return pd.DataFrame({'id': [434]})  
        elif 'league WHERE league_id = 128' in query:
            return pd.DataFrame({'league_id': [128]})  
        elif 'match WHERE id = 1158794' in query:
            return pd.DataFrame()  
        return pd.DataFrame()  
    
    mock_read_sql.side_effect = mock_read_sql_side_effect

    df_match, df_status = clean_fixture(sample_fixtures, mock_get_redshift_connection)


    assert df_match is not None
    assert df_status is not None
    assert not df_match.empty
    assert not df_status.empty


    assert df_match.iloc[0]['id'] == 1158794
    assert df_match.iloc[0]['referee'] == 'N. Arasa'
    assert df_match.iloc[0]['timezone'] == 'UTC'
    assert df_match.iloc[0]['date'] == '2024-08-10T00:00:00+00:00'
    assert df_match.iloc[0]['venue_id'] == 99
    assert df_match.iloc[0]['team_home_id'] == 436
    assert df_match.iloc[0]['team_away_id'] == 434
    assert df_match.iloc[0]['home_score'] == 0
    assert df_match.iloc[0]['away_score'] == 1
    assert df_match.iloc[0]['penalty_home'] is None
    assert df_match.iloc[0]['penalty_away'] is None
    assert df_match.iloc[0]['league_id'] == 128
    assert df_match.iloc[0]['season_year'] == 2024
    assert df_match.iloc[0]['period_first'] == 1723248000
    assert df_match.iloc[0]['period_second'] == 1723251600
    assert df_status.iloc[0]['id'] == 1158794
    assert df_status.iloc[0]['description'] == 'Match Finished'

@patch('silver.transform_data.redshift_utils.get_schema', return_value='test_schema')
@patch('silver.transform_data.redshift_utils.get_redshift_connection')
def test_clean_fixture_no_argentina_fixtures(mock_get_redshift_connection, mock_get_schema, fixtures_sin_argentina):
    """
    Verifica que la función clean_fixture devuelva None si no hay fixtures de Argentina.
    """

    df_match, df_status = clean_fixture(fixtures_sin_argentina, mock_get_redshift_connection)
    
    assert df_match is None, "df_match debería ser None cuando no hay fixtures de Argentina"
    assert df_status is None, "df_status debería ser None cuando no hay fixtures de Argentina"

@patch('silver.transform_data.pd.read_sql')
@patch('silver.transform_data.redshift_utils.get_schema', return_value='test_schema')
@patch('silver.transform_data.redshift_utils.get_redshift_connection')
def test_clean_fixture_partido_ya_existente(mock_get_redshift_connection, mock_get_schema, mock_read_sql, fixtures_ya_existente):
    """
    Test para fixtures de Argentina donde el partido ya existe en la base de datos.
    """
    def mock_read_sql_side_effect(query, con):
        if 'match WHERE id = 1158794' in query:
            return pd.DataFrame({'id': [1158794]})  #
        return pd.DataFrame()
    
    mock_read_sql.side_effect = mock_read_sql_side_effect

    df_match, df_status = clean_fixture(fixtures_ya_existente, mock_get_redshift_connection)
    assert  f"El partido con ID 1158794 ya existe. Saltando este fixture."   
    assert df_match is None
    assert df_status is None

@patch('silver.transform_data.pd.read_sql')
@patch('silver.transform_data.redshift_utils.get_schema', return_value='test_schema')
@patch('silver.transform_data.redshift_utils.get_redshift_connection')
def test_clean_fixture_datos_validos(mock_get_redshift_connection, mock_get_schema, mock_read_sql, fixtures_con_argentina):
    """
    Test para fixtures de Argentina con datos válidos y completos.
    """
    def mock_read_sql_side_effect(query, con):
        if 'team WHERE id = 436' in query:
            return pd.DataFrame({'id': [436]})  
        elif 'team WHERE id = 434' in query:
            return pd.DataFrame({'id': [434]}) 
        elif 'league WHERE league_id = 128' in query:
            return pd.DataFrame({'league_id': [128]})  
        elif 'match WHERE id = 1158794' in query:
            return pd.DataFrame()  
        return pd.DataFrame()

    mock_read_sql.side_effect = mock_read_sql_side_effect

    df_match, df_status = clean_fixture(fixtures_con_argentina, mock_get_redshift_connection)

    assert df_match is not None
    assert df_status is not None
    assert not df_match.empty
    assert not df_status.empty
    assert df_match.iloc[0]['id'] == 1158794
    assert df_status.iloc[0]['id'] == 1158794
    assert df_status.iloc[0]['description'] == 'Match Finished'




