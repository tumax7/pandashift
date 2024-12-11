"""Test main flow of ints"""
from .utils import run_type_test


def test_int():
    """Test main flow of ints"""
    types_tested = ['SMALLINT','INTEGER','BIGINT']

    redshift_column_types = {'smallint':'SMALLINT',
                            'integer':'INTEGER',
                            'bigint':'BIGINT'}
    result = run_type_test(types_tested,redshift_column_types)

    assert result.shape[0]==10
