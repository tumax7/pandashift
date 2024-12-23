"""Test main SUPER flow"""
from src.pandashift import read_query, execute_query, load_df, create_table_from_df
from .utils import config, generate_test_df, super_generator_function


def test_super():
    """Test main SUPER flow"""
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df_length = 1000
    df = generate_test_df(types_tested = ['SUPER'],
                          length = df_length)
    df['pure_dict'] =  [super_generator_function(subtype = 'json',
                                                add_error = False,
                                                return_str = False) for i in range(df_length)]
    df['pure_arr'] =  [super_generator_function(subtype = 'array',
                                                add_error = False,
                                                return_str = False) for i in range(df_length)]

    create_table_from_df(df, table_name, show_ddl=True, threshold = 0.5)

    load_df(df, table_name = table_name)

    result = read_query(f'''SELECT COUNT(DISTINCT super_json.test) as json_test,
                                   COUNT(DISTINCT super_array[0]) as array_test,
                                   COUNT(DISTINCT pure_dict.test) as pure_json_test,
                                   COUNT(DISTINCT pure_arr[0]) as pure_array_test  
                            FROM {table_name}
                            LIMIT 10''')

    execute_query(f"DROP TABLE {table_name}")

    assert result['json_test'].sum()>0
    assert result['array_test'].sum()>0
    assert result['pure_json_test'].sum()>0
    assert result['pure_array_test'].sum()>0
