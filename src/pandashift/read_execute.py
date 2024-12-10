"""This module does reading and executing for the queries."""
import os
import psycopg
import pandas as pd
from .constants import default_conn_mappings


def read_query(query,
               credentials=None):
    """Reads the results of query and places them into pandas DataFrame.
    If there is no credentials variable passed will check for credentials
    in default locations (default_conn_mappings)"""
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
    """Executes the results of query and returns "Success" if the query was run
    If there is no credentials variable passed will check for credentials
    in default locations (default_conn_mappings)"""
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
