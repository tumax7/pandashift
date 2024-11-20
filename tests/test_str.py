import sys
import pandas as pd
from utils import config, str_generator_function
sys.path.append('../')

from src.pandashift import read_query, execute_query, load_df

def test_str():
    types_tested = ['CHAR','VARCHAR']
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df = pd.DataFrame(columns = [t.lower() for t in types_tested])
    for t in types_tested:
        df[t.lower()] = [str_generator_function(t) for i in range(1000)]
    
    execute_query(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
    char CHAR(6),
    varchar VARCHAR(6),
    boolean BOOLEAN
    )
    ''')

    load_df(df, table_name = table_name)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10")

    execute_query(f"DROP TABLE {table_name}")
    
    assert result.shape[0]==10


