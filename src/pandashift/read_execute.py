import os
import psycopg
import numpy as np
import pandas as pd
from .constants import default_connection_mappings


def read_query(query,
               credentials=None):
    if credentials == None:        
        credentials = {k:os.getenv(default_connection_mappings[k]) for k in default_connection_mappings.keys()}
        missing_fields = [default_connection_mappings[k] for k in credentials.keys() if credentials[k]==None]
        if missing_fields:
            missing_field_str = ',\n'.join(missing_fields)
            raise Exception(f'''Please pass a connection dict or set the following env variables :\n{missing_field_str}''')
    connection_clause = psycopg.connect(**credentials)
    with connection_clause as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            result = pd.DataFrame(cursor.fetchall(), columns = [desc[0] for desc in cursor.description])
    return result


def execute_query(query, credentials=None):
    if credentials == None:        
        credentials = {k:os.getenv(default_connection_mappings[k]) for k in default_connection_mappings.keys()}
        missing_fields = [default_connection_mappings[k] for k in credentials.keys() if credentials[k]==None]
        if missing_fields:
            missing_field_str = ',\n'.join(missing_fields)
            raise Exception(f'''Please pass a connection dict or set the following env variables :\n{missing_field_str}''')
    with psycopg.connect(**credentials) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
        conn.commit()
    return 'Success'
