"""This module contains functions related to loading pandas DataFrame to a table
Main idea is parsing DataFrame and preparing it for loading.
"""
from decimal import Decimal
from datetime import datetime, date
import json
import re
import pandas as pd
import numpy as np

from .constants import DOUBLE_QUOTE_VAR, SINGLE_QUOTE_VAR, NULL_VAR, escape_chars

from .read_execute import execute_query
from .create_table_from_df import create_table_from_df, test_super

def get_python_dtype(v):
    """This function returns python type of the variable"""
    response = float
    if (test_super(v))&(not isinstance(v, dict))&(not isinstance(v, list)):
        response =  type(json.loads(str(v)))
    else:
        response = type(v)
    return response

def best_dtype(series):
    """Identifies best datatype for a pandas Series"""
    series_length = series.dropna().shape[0]
    if series_length:
        dtypes =  series.dropna().apply(get_python_dtype).value_counts(normalize=True)
        top_dtype = dtypes.index[0]
    else:
        top_dtype = float
    return top_dtype

def escape_str(s,
               remove_double_quote = False,
               add_quotes = False):
    """Escaping string values for Redshift"""
    # Check for null before converting to str (otherwise can't be used for timestamps)
    if pd.notnull(s):
        s = str(s)
        if remove_double_quote:
            parsed_str = s.replace('"', '').replace("'",SINGLE_QUOTE_VAR)
        else:
            parsed_str = s.replace('"',DOUBLE_QUOTE_VAR).replace("'",SINGLE_QUOTE_VAR)

        if add_quotes:
            parsed_str = "'"+parsed_str+"'"
    else:
        parsed_str = NULL_VAR
    return parsed_str

def dict_parser(v):
    """Prepares dicts for redshift loading"""
    # Returns NULL if na val
    if pd.notnull(v):
        if isinstance(v, str):
            v = json.loads(v)
        # Removing double quotes from json because it causes redshift driver to error
        parsed_dict = {}
        for key,val in v.items():
            if (isinstance(key, str))&(isinstance(val, str)):
                parsed_dict[escape_str(key,True)] = escape_str(val,True)
            elif isinstance(key, str):
                parsed_dict[escape_str(key,True)] = val
            elif isinstance(val, str):
                parsed_dict[key] = escape_str(val,True)
            else:
                parsed_dict[key] = val
        result = "JSON_PARSE(\'"+json.dumps(parsed_dict)+'''\')'''
    else:
        result = NULL_VAR
    return result

def array_parser(v):
    """Prepares arrays for redshift loading"""
    if v not in (np.nan,None):
        prepped_arr = 'ARRAY('+str(v)[1:-1]+')'
    else:
        prepped_arr = NULL_VAR
    return prepped_arr

def escape_dataframe_values(df):
    """Escapes values of dataframe for later replacements"""
    # Using custom escape characters
    for col, datatype in df.dtypes.items():
        if datatype =='object':
            real_dtype = best_dtype(df[col])
            if real_dtype == Decimal:
                df[col] = df[col].astype(float).fillna(NULL_VAR)
            elif real_dtype == dict:
                df[col] = df[col].apply(dict_parser)
            elif real_dtype == list:
                df[col] = df[col].apply(array_parser)
            elif real_dtype == str:
                df[col] = df[col].apply(escape_str,add_quotes=True)
            else:
                df[col] = df[col].apply(escape_str,add_quotes=True)

        elif datatype in (date,datetime,np.datetime64,np.dtype('<M8[ns]')):
            df[col] = df[col].apply(escape_str,add_quotes=True)

        else:
            df[col] = df[col].fillna(NULL_VAR)
    return df

# Load batch df
def batch_load_dataframe(parsed_df,
                         table_name,
                         empty_str_as_null,
                         verify_column_names,
                         maximum_insert_length,
                         **kwargs):
    """Splits the dataframe into batches and inserts them into Redshift table"""
    # Defining variables for batch splitting
    if verify_column_names:
        header_of_string = f'''INSERT INTO {table_name}({','.join(parsed_df.columns)}) VALUES \n'''
    else:
        header_of_string = f'''INSERT INTO {table_name} VALUES \n'''
    temp_total_length = len(header_of_string)
    temp_insert_string = ''
    batch_counter = 1

    if empty_str_as_null:
        escape_chars["''"] = "NULL"

    # Compiling re to improve performance
    pattern = re.compile("|".join(escape_chars.keys()))

    # Splitting dataframe into batches
    for i,row_arr in enumerate(parsed_df.values):
        unparsed_row = '('+','.join([str(v) for v in row_arr]) +'),\n'
        if len(header_of_string)+len(unparsed_row)>=maximum_insert_length:
            # Handling large rows
            raise ValueError(f'''Error row {i} larger that maximum insert length.
            \tEdit the maximum_insert_length parameter,
            \tremove the row or opt for another method
            \tto load data https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html''')
        if temp_total_length+len(unparsed_row)<maximum_insert_length:
            temp_insert_string = temp_insert_string+unparsed_row
            temp_total_length = temp_total_length+len(unparsed_row)
        else:
            print(f'Writing batch {batch_counter}')
            # Parsing for NULLS and quotes
            temp_insert_string = pattern.sub(lambda match: escape_chars[match.group(0)],
                                                           temp_insert_string)
            # Inserting result
            execute_query(header_of_string +temp_insert_string[:-2] +';',
                          credentials=kwargs.get('credentials'))
            batch_counter +=1
            temp_insert_string = unparsed_row
            temp_total_length = len(header_of_string)

    # Insering final batch
    print(f'Writing batch {batch_counter}')

    # Parsing for NULLS and quotes
    temp_insert_string = pattern.sub(lambda match: escape_chars[match.group(0)],temp_insert_string)
    execute_query(header_of_string+temp_insert_string[:-2] +';',
                  credentials=kwargs.get('credentials'))

    return 'Success'

def load_df(init_df,
            table_name='analytics.sessions',
            verify_column_names = True,
            empty_str_as_null = True,
            maximum_insert_length=16000000,
            **kwargs
            ):
    """Loads pandas Dataframe to Redshift table.
    Parameters:
        df:	pandas Dataframe
        table_name:	The table that you want to load the df to
        credentials:	Credentials to use for connection if any
        auto_create_table:	Create the table if it doesn't exist, by default False
        verify_column_names:	The checks that the dataframe column order matches
                                the table column order, by default True
        empty_str_as_null:	This option will interpret empty string '' as NULL, by default True
        maximum_insert_length:	Maximum length of the insert statement, 
                                alter this if you get error exceeding the max statement length,
                                by default 16000000
        perform_analyze:	If this is true at the end of loading will run ANALYZE table,
                            by default False
    Returns:
        Number of rows loaded
    """
    if kwargs.get('auto_create_table') is True:
        create_table_from_df(init_df,
                             table_name,
                             credentials=kwargs.get('credentials'),
                             threshold=kwargs.get('auto_create_threshold'),
                             show_ddl=kwargs.get('auto_create_show_ddl')
                             )

    # To avoid altering current data
    df = init_df.copy()

    if df.shape[0]==0:
        return 'Nothing to write'

    # Escaping values
    df = escape_dataframe_values(df)

    # Batch loading df
    batch_load_dataframe(df,
                         table_name = table_name,
                         empty_str_as_null = empty_str_as_null,
                         verify_column_names = verify_column_names,
                         maximum_insert_length = maximum_insert_length,
                         credentials = kwargs.get('credentials'))

    # Updating table statistics
    if kwargs.get('perform_analyze') is True:
        print('Updating table statistics')
        execute_query(f'ANALYZE {table_name};',credentials=kwargs.get('credentials'))

    return f'''Success Wrote {df.shape[0]} rows'''
