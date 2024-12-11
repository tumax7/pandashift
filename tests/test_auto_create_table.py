from .utils import config, generate_test_df

from src.pandashift import read_query, execute_query, load_df, create_table_from_df

def test_auto_create_table():
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df = generate_test_df()
    
    load_df(df,auto_create_table=True, table_name = table_name)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10")

    execute_query(f"DROP TABLE {table_name}")
    
    assert result.shape[0]==10
