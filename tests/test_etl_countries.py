from unittest.mock import patch, Mock
from etl.etl_countries import extract_countries
from etl.etl_countries import transform_countries    
import pytest
import pandas as pd
from utils import parquet_operations

def test_transform_countries_name_null():

    countries = [
        {'name': 'Argentina', 'code': 'AR1234567890'},
        {'name': 'Brasil', 'code': 'BR'},
        {'name': None, 'code': 'EC'}
    ]


    with pytest.raises(ValueError, match="La columna 'name' contiene valores nulos, lo cual no está permitido."):
        transform_countries(countries)

def test_transform_countries_code_too_long():

    countries = [
        {'name': 'Argentina', 'code': 'AR1234567890'},
        {'name': 'Brasil', 'code': 'BR'},
        {'name': 'Ecuador', 'code': 'masdediezcaracteres'}
    ]


    with pytest.raises(ValueError, match="La columna 'code' debe ser null o un string con un máximo de 10 caracteres."):
        transform_countries(countries)

def test_transform_countries_success():

    countries = [
        {'name': 'Argentina', 'code': 'AR'},
        {'name': 'Brasil', 'code': 'BR'},
        {'name': 'Ecuador', 'code': 'EC'}
    ]


    result_df = transform_countries(countries)

    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty
    assert 'name' in result_df.columns
    assert 'code' in result_df.columns

def test_transform_countries_remove_duplicates():

    countries = [
        {'name': 'Argentina', 'code': 'AR'},
        {'name': 'Brasil', 'code': 'BR'},
        {'name': 'Brasil', 'code': 'BR'},  
        {'name': 'Ecuador', 'code': 'EC'}
    ]

    result_df = transform_countries(countries)

    assert isinstance(result_df, pd.DataFrame)
    assert not result_df.empty

    assert result_df['code'].duplicated().sum() == 0

    assert len(result_df) == 3