from .utils import config, generate_test_df, timestamp_generator_function

from src.pandashift import read_query, execute_query, load_df

def test_timestamp():
    types_tested = ['TIMESTAMP','DATE']
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df = generate_test_df(types_tested)
    df['pure_timestamp'] = [timestamp_generator_function(add_error=False) for i in range(df.shape[0])]

    execute_query(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
    timestamp TIMESTAMP,
    date DATE,
    pure_timestamp TIMESTAMP
    )
    ''')

    load_df(df, table_name = table_name)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10")

    execute_query(f"DROP TABLE {table_name}")
    
    assert result.shape[0]==10