class Config:
    """
    clase donde se guardan las constantes
    """
    
    BASE_TEMP_PATH = './temp/extract/'
    
    COUNTRY_FOLDER = 'countries'
    COUNTRIES_FILE = 'countries.parquet'
    COUNTRIES_TRANSFORM_FILE = 'countries_transform.parquet'
    CONTRIES_ARGENINA_FILE_READ = './temp/extract/countries/countries.parquet'
    
    TEAM_FOLDER = 'teams'
    TEAM_ARGENTINA_FILE = 'teams_argentina.parquet'

    VENUES_FOLDER = 'venues'
    VENUES_ARGENTINA_FILE = 'venues_argentina.parquet'

    TABLE_NAME_COUNTRY = "country"
    TABLE_NAME_TEAM = "team"
    TABLE_NAME_VENUE = "venue"


    MATCH_FOLDER = 'fixture'
    TABLE_MATCH = "match"
    TABLE_STATUS = "status"
    TABLE_LEAGUE = "league"