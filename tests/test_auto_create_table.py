"""Auto create table test"""
from src.pandashift import read_query, execute_query, load_df

from .utils import config, generate_test_df

def test_auto_create_table():
    """Auto create table test"""
    table_name = f"{config['specified_schema']}.{config['test_table_name']}"
    execute_query(f'DROP TABLE IF EXISTS {table_name}')

    # Generating df
    df = generate_test_df()

    load_df(df,
            auto_create_table=True,
            auto_create_threshold=0.5,
            auto_create_show_ddl=False,
            table_name = table_name)

    result = read_query(f'''SELECT *,
                                   NVL(super_json.test,0)::INT as test_json_val,
                                   NVL(super_array[0],0)::FLOAT as test_arr_val  
                            FROM {table_name}
                            LIMIT 10''')

    execute_query(f"DROP TABLE {table_name}")
    assert result['test_json_val'].sum()>0
    assert result['test_arr_val'].sum()>0
    assert result.shape[0]==10
