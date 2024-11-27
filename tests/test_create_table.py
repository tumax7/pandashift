from .utils import config, generate_test_df

from src.pandashift import read_query, execute_query, load_df, create_table_from_df

def test_create_table():
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df = generate_test_df()
    sortkey_col = df.columns[0]
    
    create_table_from_df(df, table_name, sortkeys=[sortkey_col], distkey=sortkey_col,show_ddl=True, threshold = 0.5)


    load_df(df, table_name = table_name)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10")

    execute_query(f"DROP TABLE {table_name}")
    
    assert result.shape[0]==10