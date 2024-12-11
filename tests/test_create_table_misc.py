from .utils import config, generate_test_df
import os
from src.pandashift import read_query, execute_query, create_table_from_df

def test_bad_sort_keys():
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    df = generate_test_df()

    response=False
    try:
        create_table_from_df(df, table_name, sortkeys=1123)
    except TypeError:
        response = True
    
    assert response == True
