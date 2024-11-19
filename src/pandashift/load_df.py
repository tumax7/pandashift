import numpy as np
from decimal import Decimal
from datetime import datetime, date
import pandas as pd
import re

from .read_execute import execute_query


def parse_dtype(s):
    non_null_vals = s.dropna()
    if len(non_null_vals):
        return type(non_null_vals.iloc[0])
    else:
        return float

def load_df(init_df,
            table_name='analytics.sessions',
            credentials = None,
            verify_column_names = True,
            empty_str_as_null = True,
            maximum_insert_length=16000000,
            perform_analyze = False
            ):

    # Defining variables for batch splitting
    if verify_column_names:
        header_of_string = f'''INSERT INTO {table_name}({','.join(init_df.columns)}) VALUES \n'''
    else:
        header_of_string = f'''INSERT INTO {table_name} VALUES \n'''
    temp_total_length = len(header_of_string)
    temp_insert_string = ''
    batch_counter = 1

    # String replacements
    replacements = {
                    "#double_qoute#": '\"',
                    "#single_quote#": "''",
                    "#none_qoute#": "NULL",
                    "'#none_qoute#'": "NULL"
                    }
    if empty_str_as_null:
        replacements["''"] = "NULL"
    
    # To avoid altering current data
    df = init_df.copy()

    if df.shape[0]==0:
        return 'Nothing to write'
    
    # Compiling re to improve performance
    pattern = re.compile("|".join(replacements.keys()))
    
    # Using custom escape characters
    for col, datatype in df.dtypes.items(): 
        if datatype == 'object':
            real_dtype = parse_dtype(df[col])
            if real_dtype == Decimal:
                df[col] = df[col].astype(float).fillna('#none_qoute#')
            elif real_dtype == str:
                df[col] = df[col].fillna('#none_qoute#').astype(str).apply(lambda x:x.replace('"','#double_qoute#').replace("'","#single_quote#") if pd.notnull(x) else x)
            else:
                df[col] = df[col].fillna('#none_qoute#').astype(str)
        elif datatype in (date,datetime,np.datetime64,np.dtype('<M8[ns]')):
            df[col] = df[col].fillna('#none_qoute#').astype(str)
        else:
            df[col] = df[col].fillna('#none_qoute#')
            
    # Splitting dataframe into batches
    for i,row_arr in enumerate(df.values):
        unparsed_row = (str(tuple(row_arr))+',\n')
        if len(header_of_string)+len(unparsed_row)>=maximum_insert_length:
            # Handling large rows
            raise Exception(f'''Error row {i} larger that maximum insert length.\n\t   Edit the maximum_insert_length parameter, remove the row or opt for another method to load data https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html''')
        elif temp_total_length+len(unparsed_row)<maximum_insert_length:
            temp_insert_string = temp_insert_string+unparsed_row
            temp_total_length = temp_total_length+len(unparsed_row)
        else:
            print(f'Writing batch {batch_counter}')
            # Parsing for NULLS and quotes
            temp_insert_string = pattern.sub(lambda match: replacements[match.group(0)],temp_insert_string)
            # Inserting result
            execute_query(header_of_string +temp_insert_string[:-2] +';', credentials=credentials)
            batch_counter +=1
            temp_insert_string = unparsed_row
            temp_total_length = len(header_of_string)

    # Insering final batch
    print(f'Writing batch {batch_counter}')

    # Parsing for NULLS and quotes
    temp_insert_string = pattern.sub(lambda match: replacements[match.group(0)],temp_insert_string)
    execute_query(header_of_string+temp_insert_string[:-2] +';',credentials=credentials)

    # Updating table statistics
    if perform_analyze:
        print('Updating table statistics')
        execute_query(f'ANALYZE {table_name};',credentials=credentials)

    return f'''Success Wrote {df.shape[0]} rows'''
