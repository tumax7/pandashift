"""Minor tests for create table"""
from src.pandashift import create_table_from_df
from .utils import config, generate_test_df

def test_bad_sort_keys():
    """Bad sortkeys"""
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    df = generate_test_df()

    response=False
    try:
        create_table_from_df(df, table_name, sortkeys=1123)
    except TypeError:
        response = True

    assert response is True
