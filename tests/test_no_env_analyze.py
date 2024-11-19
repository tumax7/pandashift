import sys
import pandas as pd
from utils import config, generate_test_df, test_credentials
sys.path.append('../')

from src.pandashift import read_query, execute_query, load_df, create_table_from_df

def test_int_no_env():
    table_name = f"{config['specified_schema']}.{config['test_table_name']}_int"

    # Generating df
    df = generate_test_df()

    create_table_from_df(df, table_name,credentials = test_credentials)

    load_df(df,table_name = table_name,credentials = test_credentials, perform_analyze=True)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10",credentials = test_credentials)

    execute_query(f"DROP TABLE {table_name}",credentials = test_credentials)
    
    assert result.shape[0]==10

