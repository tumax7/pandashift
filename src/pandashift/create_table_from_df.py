"""This module contains functions related to creating table from pandas DataFrame
Main idea is converting pandas types to redshift types
"""
import json
from functools import partial
import pandas as pd
import numpy as np
from .constants import numpy_to_redshift_mappings
from .read_execute import execute_query

# Datatypes tests
def test_bool(s):
    """Performs test on string to check if it is boolean"""
    s = str(s)
    response = False
    parsed_str = s.strip().lower()
    if (parsed_str.find('true')>-1) & (len(parsed_str)==4):
        response = True
    elif (parsed_str.find('false')>-1) & (len(parsed_str)==5):
        response = True
    return response

def test_super(s):
    """Performs test on string to check if it is redshift super type"""
    response = False
    if (isinstance(s, dict))|(isinstance(s, list)):
        response = True
    else:
        try:
            json.loads(str(s))
            response = True
        except (json.JSONDecodeError, TypeError):
            pass
    return response

def test_date(s, mode):
    """Performs test on string to check if it is date or timestamp depending on mode"""
    response = False
    dt = pd.to_datetime(str(s), errors='coerce')
    hour_sum = dt.hour+dt.minute+dt.second
    if not pd.isnull(dt):
        if ((mode == 'TIMESTAMP') & (hour_sum>0))|((mode == 'DATE') & (hour_sum==0)):
            response = True
    return response

def test_num(s, mode):
    """Performs test on string to check if it is int or float depending on mode"""
    dtype = int
    if mode == 'FLOAT':
        dtype = float
    try:
        dtype(str(s))
        return True
    except ValueError:
        return False

def is_dtype(element: any,
             datatype: str) -> bool:
    """Combines all the type functions into one to check for datatype"""
    response = False
    if element is None:
        response = False
    elif datatype == 'BOOLEAN':
        response = test_bool(element)
    elif datatype == 'SUPER':
        response = test_super(element)
    elif datatype in ('TIMESTAMP','DATE'):
        response = test_date(element,mode = datatype)
    elif datatype in ('INTEGER','FLOAT'):
        response = test_num(element,mode = datatype)
    else:
        raise TypeError('The datatype is not suppourted')
    return response

def generate_ddl(dtype_dict: dict,
                 table_name: str,
                 sortkeys:list = None,
                 distkey:str = None) -> str:
    """Creates DDL for the table based on the column names and types"""
    init_str = f'''CREATE TABLE IF NOT EXISTS {table_name}(\n'''
    attr = [k+' '+dtype_dict[k] for k in dtype_dict]
    extra = ''
    if distkey:
        extra += f"DISTKEY({distkey})\n"
    if sortkeys:
        try:
            extra += f"SORTKEY({','.join(sortkeys)})"
        except TypeError as e:
            raise TypeError('Please input valid sortkeys (array of strings)') from e

    return init_str+',\n'.join(attr)+')\n'+extra+';'


def determine_dtypes(df,
                     threshold:float = 0.8) -> dict:
    """Determines datatypes for each column in dataframe.
    Parameters:
        df : Pandas dataframe
        threshold : float that determines the number past which the column is converted to datatype
    Returns:
        result_dict: dictionary that contains pandas DataFrame columns mapped to redshift dtypes
    """
    tested_dtypes = ['FLOAT',
                     'INTEGER',
                     'TIMESTAMP',
                     'DATE',
                     'BOOLEAN',
                     'SUPER']
    df_length = df.shape[0]
    sample_size = (df_length>100)*100 + (not df_length>100)*df_length

    test_df = df.sample(sample_size, replace=False)

    # Using pandas dtypes
    result_dict = {i:numpy_to_redshift_mappings[v] for i,v in test_df.dtypes.items()}
    object_fields = [k for k in result_dict if result_dict[k]=='VARCHAR']

    # If pandas type is object, run them through checks
    for c in object_fields:
        temp_arr = []
        for t in tested_dtypes:
            prepped_is_dtype = partial(is_dtype,datatype = t)
            temp_arr.append(test_df[c].dropna().apply(prepped_is_dtype).sum()/sample_size)
        top_value_index = np.argsort(temp_arr)[::-1][0]
        if temp_arr[top_value_index]>threshold:
            dtype = tested_dtypes[top_value_index]
        else:
            longest_string = df[c].apply(lambda x:len(str(x)) if pd.notnull(x) else 0).max()
            if longest_string != 0:
                dtype = f'VARCHAR({int(longest_string*1.5)})'
            else:
                dtype = 'FLOAT'
        result_dict[c] = dtype

    return result_dict

def create_table_from_df(df,
                         table_name: str,
                         threshold:float = 0.8,
                         show_ddl:bool = False,
                         no_execute:bool = False,
                         **kwargs) -> str:
    """Creates Redshift table from dataframe.
    Parameters:
        df:	The dataframe for loading
        table_name:	The table that you want to load the df to
        credentials:	Credentials to use for connection if any
        sortkeys:	Specify desired sortkeys, by default no sortkeys are used
        distkey:	Specify desired sortkeys, by default no distkeys are used
        threshold:	Specifies the threshold for datatype conversion 
                    (eg if more than 80% of values are float will create FLOAT column ). 
                    Must be a value between 0 and 1, by default 0.8
        show_ddl:	Prints the DDL of the created table, by default False
        no_execute:	Just creates DDL, but doesn't run it in database (useful for debugging),
                     by default False
    """
    sql_datatypes=determine_dtypes(df,threshold)
    ddl = generate_ddl(sql_datatypes,
                       table_name,
                       sortkeys = kwargs.get('sortkeys'),
                       distkey = kwargs.get('distkey'))
    if show_ddl:
        print(ddl)
    if not no_execute:
        execute_query(ddl,kwargs.get('credentials'))
    return 'Success'
