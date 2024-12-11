"""Test main flow of floats"""
from .utils import run_type_test


def test_float():
    """Test main flow of floats"""
    types_tested = ['DECIMAL','REAL','DOUBLE']

    redshift_column_types = {'decimal':'DECIMAL',
                            'real':'REAL',
                            'double':'DOUBLE PRECISION'}
    result = run_type_test(types_tested,redshift_column_types)

    assert result.shape[0]==10
