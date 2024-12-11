"""This module does reading and executing for the queries."""
import os
import psycopg
import pandas as pd
from .constants import default_conn_mappings


def read_query(query,
               credentials=None):
    """Connects to Redshift Database and saves result to pandas DataFrame.
    Parameters:
        query:  The query for execution (only SELECT will work)
        credentials:    Credentials to use for connection if any 
                        (if None will check for default in default_conn_mappings)
    Returns:
        result: pandas DataFrame with all the results
    """
    if credentials is None:
        credentials = {k:os.getenv(v) for k,v in default_conn_mappings.items()}
        missing_fields = [default_conn_mappings[k] for k,v in credentials.items() if v is None]
        if missing_fields:
            missing_field_str = ',\n'.join(missing_fields)
            raise NameError(f'''Please pass a connection
                                or set the following env variables :\n{missing_field_str}''')
    with psycopg.connect(**credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = pd.DataFrame(cursor.fetchall(), columns = [d[0] for d in cursor.description])
    return result


def execute_query(query, credentials=None):
    """Connects to Redshift Database and saves result to pandas DataFrame.
    Parameters:
        query:  The query for execution (only non SELECT will work)
        credentials:    Credentials to use for connection if any 
                        (if None will check for default in default_conn_mappings)
    """
    if credentials is None:
        credentials = {k:os.getenv(v) for k,v in default_conn_mappings.items()}
        missing_fields = [default_conn_mappings[k] for k,v in credentials.items() if v is None]
        if missing_fields:
            missing_field_str = ',\n'.join(missing_fields)
            raise NameError(f'''Please pass a connection dict
                                or set the following env variables :\n{missing_field_str}''')
    with psycopg.connect(**credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
    return 'Success'
