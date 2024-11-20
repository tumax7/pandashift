import pandas as pd
import numpy as np
from .constants import numpy_to_redshift_mappings
from .read_execute import execute_query


# Datatypes tests
def test_bool(s):
    s = str(s)
    response = False
    parsed_str = s.strip().lower()
    if (parsed_str.find('true')>-1) & (len(parsed_str)==4):
        response = True
    elif (parsed_str.find('false')>-1) & (len(parsed_str)==5):
        response = True
    return response

def test_date(s, mode):
    response = False
    dt = pd.to_datetime(s, errors='coerce')
    hour_sum = dt.hour+dt.minute+dt.second
    if not pd.isnull(dt):
        if ((mode == 'timestamp') & (hour_sum>0))|((mode == 'date') & (hour_sum==0)):
            response = True
    return response

def is_dtype(element: any,
             dtype) -> bool:
    if element is None: 
        return False        
    elif dtype == bool:
        return test_bool(element)
    elif dtype == 'timestamp':
        return test_date(element,mode = dtype)
    elif dtype == 'date':
        return test_date(element,mode = dtype)
    else:
        try:
            dtype(str(element))
            return True
        except ValueError:
            return False

def generate_ddl(dtype_dict: dict,
                 table_name: str,
                 sortkeys:list =[],
                 distkey:str = None) -> str:
    init_str = f'''CREATE TABLE IF NOT EXISTS {table_name}(\n'''
    attr = [k+' '+dtype_dict[k] for k in dtype_dict]
    extra = ''
    if distkey:
       extra += f"DISTKEY({distkey})\n"
    if sortkeys:
       extra += f"SORTKEY({','.join(sortkeys)})"

    return init_str+',\n'.join(attr)+')\n'+extra+';'


def determine_dtypes(df):
    df_length = df.shape[0]
    sample_size = (df_length>100)*100 + (not df_length>100)*df_length

    test_df = df.sample(sample_size, replace=False)

    # Using pandas dtypes
    result_dict = {i:numpy_to_redshift_mappings[v] for i,v in test_df.dtypes.items()}
    object_fields = [k for k in result_dict if result_dict[k]=='VARCHAR']
    tested_dtypes = {int:'INTEGER',
                     float:'FLOAT',
                     'timestamp':'TIMESTAMP',
                     'date':'DATE',
                     bool:'BOOLEAN'}

    # If pandas type is object, run them through checks
    for c in object_fields:
        temp_result = {}
        for t in tested_dtypes.keys():
            temp_result[tested_dtypes[t]] = test_df[c].dropna().apply(lambda x:is_dtype(x, t)).sum()/sample_size
        sorted_temp = dict(sorted(temp_result.items(), key=lambda item: item[1], reverse=True))
        arr_vals = list(sorted_temp.values())
        dtype = list(sorted_temp.keys())[0]
        if arr_vals[0]>0.5:
            if arr_vals[0]==arr_vals[1]:
                dtype = 'INTEGER'
        else:
            longest_string = test_df[c].apply(lambda x:len(x) if pd.notnull(x) else 0).max()
            if longest_string != 0:
                dtype = f'VARCHAR({int(longest_string*1.1)})'
            else:
                dtype = 'FLOAT'
        result_dict[c] = dtype

    return result_dict

def create_table_from_df(df,
                         table_name: str,
                         credentials = None,
                         sortkeys:list =[],
                         distkey:str = None) -> str:
    sql_datatypes=determine_dtypes(df)
    ddl = generate_ddl(sql_datatypes, table_name,sortkeys, distkey)
    execute_query(ddl,credentials)
    return 'Success'