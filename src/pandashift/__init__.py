"""
pandashift

Module for quick and easy access to redshift using pandas

Basic Documentation
-------------------
https://github.com/tumax7/pandashift

Functions
_________

read_query: Runs SELECT statements

execute_query: Executes non SELECT statements

load_df: Loads DataFrame to database

create_table_from_df: Creates table for DataFrame in database

"""
import os
from dotenv import load_dotenv
from .read_execute import read_query, execute_query
from .load_df import load_df
from .create_table_from_df import create_table_from_df
os.environ["PGCLIENTENCODING"] = "utf-8"
load_dotenv()
