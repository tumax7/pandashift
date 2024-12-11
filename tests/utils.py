from random import randint, random
import os
import json
import numpy as np
from decimal import Decimal
from datetime import datetime, timedelta
import pandas as pd
import uuid

test_credentials ={
        "host":os.getenv("REDSHIFT_HOST"),
        "port":os.getenv("REDSHIFT_PORT"),
        "dbname":os.getenv("REDSHIFT_DATABASE"),
        "user":os.getenv("REDSHIFT_USER"),
        "password":os.getenv("REDSHIFT_PASSWORD")
        }


dir_path = os.path.dirname(os.path.realpath(__file__))
with open(dir_path+'/config.json', 'r') as f:
    config = json.load(f)

def int_generator_function(tested_dtype):

    # Base value
    if tested_dtype == 'SMALLINT':
        val = randint(-32768, 32767)
    elif tested_dtype == 'INTEGER':
        val = randint(-2147483648, 2147483647)
    elif tested_dtype == 'BIGINT':
        val = randint(-9223372036854775808, 9223372036854775807)

    # Adding random errors NULLS and floats and strings
    random_val = random()
    if (0<=random_val)&(random_val<0.1):
        val = np.nan
    elif (0.1<=random_val)&(random_val<0.2):
        val = str(val)
    elif (0.2<=random_val)&(random_val<0.3):
        val = None
    return val

def float_generator_function(tested_dtype):
    # Base
    if tested_dtype == 'DECIMAL':
        val = Decimal(str(random()*100)[:5])
    else:
        val = random()*10000

    # Adding random errors NULLS and floats and strings
    random_val = random()
    if (0<=random_val)&(random_val<0.1):
        val = np.nan
    elif (0.1<=random_val)&(random_val<0.2):
        val = str(val)
    elif (0.2<=random_val)&(random_val<0.3):
        val = None
    return val

def timestamp_generator_function(add_error = True):
    # Base
    val = datetime.now() - timedelta(seconds=randint(1,1000000))

    if add_error:
        # Adding random errors NULLS and floats and strings
        random_val = random()
        if (0<=random_val)&(random_val<0.1):
            val = np.nan
        elif (0.1<=random_val)&(random_val<0.2):
            val = val.date()
        elif (0.2<=random_val)&(random_val<0.3):
            val = str(val)
        elif (0.3<=random_val)&(random_val<0.4):
            val = None
        elif (0.4<=random_val)&(random_val<0.5):
            val = str(val.date())

    return val


def str_generator_function(tested_dtype):
    
    # Base
    if tested_dtype == 'BOOLEAN':
        val = bool(round(random()))
    else:
        val = uuid.uuid4().hex[:6]

    # Adding random errors NULLS and floats and strings
    random_val = random()
    if (0<=random_val)&(random_val<0.1):
        val = np.nan
    elif (0.1<=random_val)&(random_val<0.2):
        val = None
    elif (0.3<=random_val)&(random_val<0.31):
        val = val*4
    elif (0.31<=random_val)&(random_val<0.5):
        val = str(val)
    return val

def super_generator_function(subtype = 'json'):
    
    # Base
    if subtype == 'json':
        val = json.dumps({'test':round(random()*100)})
    elif subtype == 'array':
        val = str([round(random()*100,1) for i in range(3)])
    #elif subtype == 'tuple': Tuple not suppourted yet
    #    val = str((round(random()*100,1),round(random()*100,1),round(random()*100,1)))
    
    # Adding random errors NULLS and floats and strings
    random_val = random()
    if (0<=random_val)&(random_val<0.1):
        val = np.nan
    elif (0.1<=random_val)&(random_val<0.2):
        val = None
    return val


def generate_test_df(types_tested = ['DATE','INTEGER','DOUBLE','VARCHAR','SUPER'], length = 1000):
    df = pd.DataFrame(columns = [t.lower() for t in types_tested])
    for t in types_tested:
        if t in ('DECIMAL','REAL','DOUBLE'):
            df[t.lower()] = [float_generator_function(t) for i in range(length)]
        elif t in ('SMALLINT','INTEGER','BIGINT'):
            df[t.lower()] = [int_generator_function(t) for i in range(length)]
        elif t in ('CHAR','VARCHAR'):
            df[t.lower()] = [str_generator_function(t) for i in range(length)]
        elif t in ('TIMESTAMP','DATE'):
            df[t.lower()] = [timestamp_generator_function() for i in range(length)]
        elif t == 'SUPER':
            for subtype in ['json','array']: 
                df[t.lower()+'_'+subtype] = [super_generator_function(subtype) for i in range(length)]
    return df
