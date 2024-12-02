# pandashift

## Overview
Pandashift integrates Pandas with Amazon Redshift for smooth ETL processes and data manipulation. Specifically designed for Redshift in Python, it simplifies workflows by providing easy bulk inserts and automatically handling type conversions, including the ability to convert Pandas types to Redshift types. Data can be pulled from Redshift into Pandas using the function `read_query` for quick analysis and data processing in Python. Whether creating DataFrames from Redshift or performing ETL tasks, taking advantage of Pandas and Pandashift provides flexibility and efficiency for scalable data workflows.

## Installing

``` shell
pip install pandashift
```

## Usage
There are 2 ways work with this package

1. Setting up environment variables 
2. Passing credentials on every call

Below are examples of both approaches

### With environment variables set up

``` python
from pandashift import read_query, execute_query, load_db, create_table_from_df

# Read Data
df = read_query('SELECT * FROM public.test')

# Execute statement (aka table create/drop/etc)
execute_query('DROP public.test')

# Create the table from df
create_table_from_df(df, 'public.test')

# Loading dataframe
load_df(df, table_name ='public.test')
```

### No environment variables
``` python
from pandashift import read_query, execute_query, load_db, create_table_from_df

creds = {
        "host":"YOUR HOST",
        "port":"YOUR PORT",
        "dbname":"YOUR DATABASE",
        "user":"YOUR USER",
        "password":"YOUR PASSWORD"
        }

# Read Data
df = read_query('SELECT * FROM public.test',credentials = creds)

# Execute statement (aka table create/drop/etc)
execute_query('DROP public.test',credentials = creds)

# Create the table from df
create_table_from_df(df, 'public.test', credentials = creds)

# Loading dataframe
load_df(df, table_name = 'public.test',credentials = creds)
```

## Functions

### load_df


| Parameter             | Usage           |
|-----------------------|--------------------------------------------------------------------------------|
| init_df               | The dataframe for loading|
| table_name            | The table that you want to load the df to|
| credentials           | Credentials to use for connection if any|
| auto_create_table     | Create the table if it doesn't exist, by default **False**|
| verify_column_names   | The checks that the dataframe column order matches the table column order, by default|
| empty_str_as_null     | This option will interpret empty string '' as NULL, by default  **True**|
| maximum_insert_length | Maximum length of the insert statement, alter this if you get error exceeding the max statement length, by default **16000000**|
| perform_analyze       | If this is true at the end of loading will run ANALYZE table, by default  **False**|


### create_table_from_df

| <br>Parameter             | Usage           |
|-----------------------|--------------------------------------------------------------------------------|
| df                    | The dataframe for loading|
| table_name            | The table that you want to load the df to|
| credentials           | Credentials to use for connection if any|
| sortkeys              | Specify desired sortkeys, by default no sortkeys are used|
| distkey               | Specify desired sortkeys, by default no distkeys are used|
| threshold             | Specifies the threshold for datatype conversion (eg if more than 80% of values are float will create FLOAT column ). Must be a value between 0 and 1, by default **0.8**|
| show_ddl              | Prints the DDL of the created table, by default **False** |
| no_execute            | Just creates DDL, but doesn't run it in database (useful for debugging), by default **False** |


Currently only the these datatypes are suppourted: 
* SMALLINT
* INTEGER
* BIGINT
* DECIMAL
* REAL
* DOUBLE
* TIMESTAMP
* DATE
* CHAR
* VARCHAR
* BOOLEAN


## Testing

In order to run local testing

1. Create an environment with required modules
``` bash
pip install -r requirements.txt
```

2. Run the following
```
pytest
```