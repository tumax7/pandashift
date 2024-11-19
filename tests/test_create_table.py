import sys
import pandas as pd
from utils import config, generate_test_df
sys.path.append('../')

from src.pandashift import read_query, execute_query, load_df, create_table_from_df

def test_timestamp():
    types_tested = ['TIMESTAMP','DATE']
    table_name = f"{config['specified_schema']}.test_create_table"
    #table_name = f"{config['specified_schema']}.{config['test_table_name']}_timestamp"
    
    # Generating df
    df = generate_test_df()
    sortkey_col = df.columns[0]
    
    create_table_from_df(df, table_name, sortkeys=[sortkey_col], distkey=sortkey_col)

    load_df(df, table_name = table_name)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10")

    execute_query(f"DROP TABLE {table_name}")
    
    assert result.shape[0]==10