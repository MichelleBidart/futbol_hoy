from unittest.mock import patch, Mock
from etl.etl_countries import extract_countries
from etl.etl_countries import transform_countries    
import pytest
import pandas as pd
import os

@patch('etl.etl_countries.requests.get')
@patch('etl.etl_countries.api_url_configurations.get_api_url_headers')
def test_extract_countries_failare(mock_get_headers, mock_get):
    mock_get_headers.return_value = ("http://mockurl.com/", {"Authorization": "test"})
    
    mock_get.status_code = 400
 
    
    with pytest.raises(Exception) as exc_info:
        extract_countries()
    
    assert str(exc_info.value) == "Error al obtener los datos de http://mockurl.com/countries"

@patch('etl.etl_countries.requests.get')
@patch('etl.etl_countries.api_url_configurations.get_api_url_headers')
def test_extract_countries_success(mock_get_headers, mock_get):

    mock_get_headers.return_value = ("http://mockurl.com/", {"Authorization": "test"})
    
    mock_get.return_value.status_code = 200

    mock_get.return_value.json.return_value = {
        'response': [
            {'code': 'AR', 'name': 'Argentina', 'flag': 'flag_url_Argentina'},
            {'code': 'BR', 'name': 'Brazil', 'flag': 'flag_url_Brazil'}
        ]
    }


    countries = extract_countries()


    expected_output = [
        {'code': 'AR', 'name': 'Argentina', 'flag': 'flag_url_Argentina'},
        {'code': 'BR', 'name': 'Brazil', 'flag': 'flag_url_Brazil'}
    ]
    
    assert countries == expected_output

