import pandas as pd
import numpy as np
from .constants import numpy_to_redshift_mappings
from .read_execute import execute_query
import json

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

def test_super(s):
    response = False
    try:
        json.loads(s)
        response = True
    except:
        pass
    return response

def test_date(s, mode):
    response = False
    dt = pd.to_datetime(s, errors='coerce')
    hour_sum = dt.hour+dt.minute+dt.second
    if not pd.isnull(dt):
        if ((mode == 'TIMESTAMP') & (hour_sum>0))|((mode == 'DATE') & (hour_sum==0)):
            response = True
    return response

def test_num(s, mode):
    dtype = int
    if mode == 'FLOAT':
        dtype = float
    try:
        dtype(str(s))
        return True
    except ValueError:
        return False

def is_dtype(element: any,
             datatype) -> bool:
    if element is None: 
        return False        
    elif datatype == 'BOOLEAN':
        return test_bool(element)
    elif datatype == 'SUPER':
        return test_super(element)
    elif datatype in ('TIMESTAMP','DATE'):
        return test_date(element,mode = datatype)
    elif datatype in ('INTEGER','FLOAT'):
        return test_num(element,mode = datatype)
    else:
        raise Exception('The datatype is not suppourted')
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


def determine_dtypes(df, threshold = 0.8):
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
            temp_arr.append(test_df[c].dropna().apply(lambda x:is_dtype(x, t)).sum()/sample_size)
        top_value_index = np.argsort(temp_arr)[::-1][0]
        
        arr_vals = temp_arr[top_value_index]
        
        if arr_vals>threshold:
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
                         credentials = None,
                         sortkeys:list =[],
                         distkey:str = None,
                         threshold:float = 0.8,
                         show_ddl:bool = False,
                         no_execute:bool = False) -> str:
    sql_datatypes=determine_dtypes(df,threshold)
    ddl = generate_ddl(sql_datatypes, table_name,sortkeys, distkey)
    if show_ddl:
        print(ddl)
    if not no_execute:
        execute_query(ddl,credentials)
    return 'Success'