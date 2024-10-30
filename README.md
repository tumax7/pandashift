# pandashift
Wrapper for working with Amazon Redshift using pandas


## Installing

``` shell
pip install pandashift
```

## Usage
### Quick Start
With environment variables

``` python
from pandashift import read_query

result = read_query('SELECT 1')
```

Without environment variables
``` python
from pandashift import read_query

creds = {
        "host":"YOUR HOST",
        "port":"YOUR PORT",
        "dbname":"YOUR DATABASE",
        "user":"YOUR USER",
        "password":"YOUR PASSWORD"
        }

result = read_query('SELECT 1',creds)
```
