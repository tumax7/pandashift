import sys
import pandas as pd
from utils import config, int_generator_function, test_credentials
sys.path.append('../')

from src.pandashift.functions import read_query, execute_query, load_df

def test_int_no_env():
    types_tested = ['SMALLINT','INTEGER','BIGINT']
    table_name = f"{config['specified_schema']}.{config['test_table_name']}_int"

    # Generating df
    df = pd.DataFrame(columns = [t.lower() for t in types_tested])
    for t in types_tested:
        df[t.lower()] = [int_generator_function(t) for i in range(1000)]
    
    execute_query(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
    smallint SMALLINT,
    integer INTEGER,
    bigint BIGINT
    )
    ''', credentials = test_credentials)

    load_df(df, table_name = table_name,credentials = test_credentials, perform_analyze=True)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10",credentials = test_credentials)

    execute_query(f"DROP TABLE {table_name}",credentials = test_credentials)
    
    assert result.shape[0]==10
