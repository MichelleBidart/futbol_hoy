from unittest.mock import patch, Mock
from etl.etl_countries import extract_countries
from etl.etl_countries import transform_countries    
import pytest
import pandas as pd
import os
