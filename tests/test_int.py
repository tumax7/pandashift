import sys
import pandas as pd
from utils import config, int_generator_function
sys.path.append('../')

from src.pandashift.functions import read_query, execute_query, load_df

def test_int():
    types_tested = ['SMALLINT','INTEGER','BIGINT']
    
    # Generating df
    df = pd.DataFrame(columns = [t.lower() for t in types_tested])
    for t in types_tested:
        df[t.lower()] = [int_generator_function(t) for i in range(1000)]
    
    execute_query(f'''
    CREATE TABLE IF NOT EXISTS {config['specified_schema']}.{config['test_table_name']}_int (
    smallint SMALLINT,
    integer INTEGER,
    bigint BIGINT
    )
    ''')

    load_df(df, f"{config['specified_schema']}.{config['test_table_name']}_int")

    result = read_query(f"SELECT * FROM {config['specified_schema']}.{config['test_table_name']}_int LIMIT 10")


    execute_query(f"DROP TABLE {config['specified_schema']}.{config['test_table_name']}_int")
    
    assert result.shape[0]==10
