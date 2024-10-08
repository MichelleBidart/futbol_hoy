import sys
import os
from etl import etl_countries

from unittest.mock import patch, Mock



@patch('etl.etl_countries.requests.get')
@patch('etl.etl_countries.api_url_configurations.get_api_url_headers')
def test_extract_countries_success(mock_get_headers, mock_get):

    mock_get_headers.return_value = ("http://mockurl.com", {"Authorization": "Bearer mocktoken"})

    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'response': [{'name': 'Argentina', 'code': 'AR'}, {'name': 'Brazil', 'code': 'BR'}]
    }
    mock_get.return_value = mock_response


    countries = etl_countries.extract_countries()


    expected_output = [{'name': 'Argentina', 'code': 'AR'}, {'name': 'Brazil', 'code': 'BR'}]
    assert countries == expected_output
