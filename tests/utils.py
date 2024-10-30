import random
import os
import json
import numpy as np
from decimal import Decimal

from dotenv import load_dotenv
load_dotenv()

with open(os.path.abspath('config.json'), 'r') as f:
    config = json.load(f)

def int_generator_function(tested_dtype):

    # Base value
    if tested_dtype == 'SMALLINT':
        val = random.randint(-32768, 32767)
    elif tested_dtype == 'INTEGER':
        val = random.randint(-2147483648, 2147483647)
    elif tested_dtype == 'BIGINT':
        val = random.randint(-9223372036854775808, 9223372036854775807)

    # Adding random errors NULLS and floats and strings
    random_val = random.random()
    if (0<=random_val)&(random_val<0.1):
        val = np.nan
    elif (0.1<=random_val)&(random_val<0.2):
        val = val*0.99
    elif (0.2<=random_val)&(random_val<0.3):
        val = str(val)
    elif (0.3<=random_val)&(random_val<0.4):
        val = None
    return val

def float_generator_function(tested_dtype):
    # Base
    if tested_dtype == 'DECIMAL':
        val = Decimal(str(random.random()*100)[:5])
    else:
        val = random.random()*10000

    # Adding random errors NULLS and floats and strings
    random_val = random.random()
    if (0<=random_val)&(random_val<0.1):
        val = np.nan
    elif (0.1<=random_val)&(random_val<0.2):
        val = val*0.99
    elif (0.2<=random_val)&(random_val<0.3):
        val = str(val)
    elif (0.3<=random_val)&(random_val<0.4):
        val = None
    return val
