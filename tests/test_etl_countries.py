from unittest.mock import patch, Mock
from etl.etl_countries import extract_countries
from etl.etl_countries import transform_countries    
import pytest
import pandas as pd
from utils import parquet_operations

@patch('utils.parquet_operations.read_parquet_file') 
def test_transform_countries(mock_read_parquet):

    mock_read_parquet.return_value = pd.DataFrame({
        'name': ['Argentina', 'Brasil', None],  
        'code': ['AR1234567890', 'BR', 'EC']  
    })

    with pytest.raises(ValueError, match="La columna 'name' contiene valores nulos, lo cual no está permitido."):
        transform_countries()


