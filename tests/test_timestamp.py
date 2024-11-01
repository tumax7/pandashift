import sys
import pandas as pd
from utils import config, timestamp_generator_function
sys.path.append('../')

from src.pandashift.functions import read_query, execute_query, load_df

def test_timestamp():
    types_tested = ['TIMESTAMP','DATE']
    table_name = f"{config['specified_schema']}.{config['test_table_name']}_timestamp"
    
    # Generating df

    df = pd.DataFrame(columns = [t.lower() for t in types_tested])
    for t in types_tested:
        df[t.lower()] = [timestamp_generator_function() for i in range(1000)]
    
    execute_query(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMP,
    date DATE
    )
    ''')

    load_df(df, table_name = table_name)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10")

    execute_query(f"DROP TABLE {table_name}")
    
    assert result.shape[0]==10