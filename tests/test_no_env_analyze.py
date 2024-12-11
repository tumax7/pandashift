"""Test main flow with credentials passed"""
from src.pandashift import read_query, execute_query, load_df, create_table_from_df

from .utils import config, generate_test_df, test_credentials

def test_no_env():
    """Test main flow with credentials passed"""

    table_name = f"{config['specified_schema']}.{config['test_table_name']}"


    execute_query(f'DROP TABLE IF EXISTS {table_name}',credentials = test_credentials)

    # Generating df
    df = generate_test_df()

    # Test no execute
    create_table_from_df(df, table_name,credentials = test_credentials,
                         threshold = 0.5, no_execute=True)

    # Regular run
    create_table_from_df(df, table_name,credentials = test_credentials, threshold = 0.5)

    load_df(df,table_name = table_name,credentials = test_credentials, perform_analyze=True)

    result = read_query(f"SELECT * FROM {table_name} LIMIT 10",credentials = test_credentials)

    execute_query(f"DROP TABLE {table_name}",credentials = test_credentials)

    assert result.shape[0]==10
