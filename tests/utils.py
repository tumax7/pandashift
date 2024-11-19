from random import randint, random
import os
import json
import numpy as np
from decimal import Decimal
from datetime import datetime, timedelta
import pandas as pd
import uuid

from dotenv import load_dotenv
load_dotenv()

test_credentials ={
        "host":os.getenv("REDSHIFT_HOST"),
        "port":os.getenv("REDSHIFT_PORT"),
        "dbname":os.getenv("REDSHIFT_DATABASE"),
        "user":os.getenv("REDSHIFT_USER"),
        "password":os.getenv("REDSHIFT_PASSWORD")
        }

with open(os.path.abspath('config.json'), 'r') as f:
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

def timestamp_generator_function():
    # Base
    val = datetime.now() - timedelta(seconds=randint(1,1000000))

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
        val = bool(round(random.random()))
    else:
        val = uuid.uuid4().hex.upper()[:6]

    # Adding random errors NULLS and floats and strings
    random_val = random()
    if (0<=random_val)&(random_val<0.1):
        val = np.nan
    elif (0.1<=random_val)&(random_val<0.2):
        val = None

    return val


def generate_test_df():
    df = pd.DataFrame()
    df['test_timestamp'] = [timestamp_generator_function() for i in range(1000)]
    df['test_int'] =  [int_generator_function('INTEGER') for i in range(1000)]
    df['test_float'] = [float_generator_function('DOUBLE') for i in range(1000)]
    df['test_varchar'] = [str_generator_function('VARCHAR') for i in range(1000)]
    # Making lower case string to avoid timestamp scan error
    df['test_varchar'] = df['test_varchar'].apply(lambda x:x.lower() if pd.notnull(x) else x)
    
    return df
