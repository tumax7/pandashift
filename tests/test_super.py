from .utils import config, generate_test_df

from src.pandashift import read_query, execute_query, load_df, create_table_from_df

def test_super():
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df = generate_test_df(['SUPER'], length = 100)
    sortkey_col = df.columns[0]
    
    create_table_from_df(df, table_name, show_ddl=True, threshold = 0.5)

    load_df(df, table_name = table_name)

    result = read_query(f'''SELECT COUNT(DISTINCT super_json.test) as json_test,
                                   COUNT(DISTINCT super_array[0]) as array_test 
                            FROM {table_name} 
                            LIMIT 10''')

    execute_query(f"DROP TABLE {table_name}")
    
    assert result['json_test'].sum()>0
    assert result['array_test'].sum()>0