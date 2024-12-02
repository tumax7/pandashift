import numpy as np

default_connection_mappings = {
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