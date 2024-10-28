from bronze.ingest_fixture import ingest_data_fixture
from bronze.create_parquet import create_parquet
from datetime import timedelta


def run_bronze(execution_date, **kwargs):
    """
    Ejecuta la ingesta de datos del día anterior y los guarda en formato Parquet.
    """

    fixture_date = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    fixture_data = ingest_data_fixture(fixture_date)

    file_path = create_parquet(fixture_data, fixture_date)

    return file_path
