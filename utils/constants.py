class Config:
    """
    clase donde se guardan las constantes
    """
    
    BASE_TEMP_PATH = './temp/extract/'
    
    COUNTRY_FOLDER = 'countries'
    CONTRIES_FILE = 'countries.parquet'
    CONTRIES_ARGENINA_FILE_READ = './temp/extract/countries/countries.parquet'
    
    TEAM_FOLDER = 'teams'
    TEAM_ARGENTINA_FILE = 'teams_argentina.parquet'

    VENUES_FOLDER = 'venues'
    VENUES_ARGENTINA_FILE = 'venues_argentina.parquet'


    argentina_teams_parquet_path = './temp/extract/teams/transformed_teams_arg.parquet'
    venues_parquet_transformed_path = './temp/extract/venues/transformed_venues_arg.parquet'

    TABLE_NAME_COUNTRY = "country"
    TABLE_NAME_TEAM = "team"
    TABLE_NAME_VENUE = "venue"