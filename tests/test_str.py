"""Test main string flow"""
from .utils import run_type_test

def test_str():
    """Test main string flow"""
    types_tested = ['CHAR','VARCHAR','BOOLEAN']

    redshift_column_types = {'char':'CHAR(24)',
                             'varchar':'VARCHAR(24)',
                             'boolean':'BOOLEAN'}
    result = run_type_test(types_tested,redshift_column_types)

    assert result.shape[0]==10
