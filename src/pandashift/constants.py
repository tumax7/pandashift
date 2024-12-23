"""This module contains constants used throughout the package"""
import numpy as np

default_conn_mappings = {
                "host":"REDSHIFT_HOST",
                "port":"REDSHIFT_PORT",
                "dbname":"REDSHIFT_DATABASE",
                "user":"REDSHIFT_USER",
                "password":"REDSHIFT_PASSWORD"
                }

numpy_to_redshift_mappings = {
            np.dtype('int64'):'INT',
            np.dtype('<M8[ns]'):'TIMESTAMP',
            np.dtype('float64'):'FLOAT',
            np.dtype('bool'):'BOOLEAN',
            np.dtype('O'):'VARCHAR'
            }

DOUBLE_QUOTE_VAR = '#double_qoute#'
SINGLE_QUOTE_VAR = '#single_quote#'
NULL_VAR = '#none_qoute#'

escape_chars = {
                DOUBLE_QUOTE_VAR: '\"',
                SINGLE_QUOTE_VAR: "''",
                NULL_VAR: "NULL"
                }
