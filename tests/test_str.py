from .utils import config, generate_test_df

from src.pandashift import read_query, execute_query, load_df

def test_str():
    types_tested = ['CHAR','VARCHAR','BOOLEAN']
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df = generate_test_df(types_tested)
    
    execute_query(f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
    char CHAR(24),
    varchar VARCHAR(24),
    boolean BOOLEAN
    )
    ''')

    load_df(df, table_name = table_name)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10")

    execute_query(f"DROP TABLE {table_name}")
    
    assert result.shape[0]==10


