# pandashift
Wrapper for working with Amazon Redshift using pandas without the use of S3

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
from pandashift import read_query, execute_query, load_db

# Read Data
df = read_query('SELECT * FROM public.test')

# Execute statement (aka table create/drop/etc)
execute_query('TRUNCATE public.test')

# Loading dataframe
load_df(df, table_name ='public.test')
```

### No environment variables
``` python
from pandashift import read_query, execute_query, load_db

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
execute_query('TRUNCATE public.test',credentials = creds)

# Loading dataframe
load_df(df, table_name = 'public.test',credentials = creds)
```

## Functions

### load_df


| Parameter             | Usage                                                                                                                           |
|-----------------------|---------------------------------------------------------------------------------------------------------------------------------|
| init_df               | The dataframe for loading                                                                                                       |
| table_name            | The table that you want to load the df to                                                                                       |
| credentials           | Credentials to use for connection if any                                                                                        |
| verify_column_names   | The checks that the dataframe column order matches the table column order, by default **True**                                  |
| empty_str_as_null     | This option will interpret empty string '' as NULL, by default  **True**                                                        |
| maximum_insert_length | Maximum length of the insert statement, alter this if you get error exceeding the max statement length, by default **16000000** |
| perform_analyze       | If this is true at the end of loading will run ANALYZE table, by default  **False**                                             |

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

2. Run the following from the `/tests` directory
```
pytest
```
