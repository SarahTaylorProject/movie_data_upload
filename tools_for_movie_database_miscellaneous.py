# Generic functions useful for movie XML/database project
# Work in progress

# Many sourced from Sarah's other libraries
# But they are compiled here on the assumption that it is preferable for the movie script to be self-contained

import pandas as pd
import xml.etree.ElementTree as etree
import os
import traceback
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import CreateSchema
import csv
import shutil
import codecs

import local_creds

## functions to assist use with connecting to database (mysql or postgres)

def return_database_type_from_local_creds():
  result = None
  try:
    database_type = local_creds.database_type
    return(database_type)
  except:
    traceback.print_exc()
    return(result) 

def return_database_name_from_local_creds():
  result = ''
  try:
    database_name = local_creds.database_name
    return(database_name)
  except:
    traceback.print_exc()
    return(result) 

def return_host_from_local_creds():
  result = 'localhost'
  try:
    host = local_creds.host
    return(host)
  except:
    traceback.print_exc()
    return(result) 


def return_port_from_local_creds():
  result = None
  try:
    database_port = local_creds.port
    return(database_port)
  except:
    traceback.print_exc()
    return(result) 


def return_postgres_or_mysql_engine_from_local_creds(auto_commit=True):
  result = None
  """
  Note: if database_type is None, will default to trying Postgres
  """
  try:
    database_type = return_database_type_from_local_creds()
    database_name = return_database_name_from_local_creds()
    host = return_host_from_local_creds()
    port = return_port_from_local_creds()

    if (database_type == None):
      database_type = "postgres"

    if (database_type == "postgres"):
      if (port == None):
        port = "5432"
      if (database_name == ""):
        database_name = "postgres"
      current_engine = return_postgres_engine(username=local_creds.username, 
        password=local_creds.password, 
        host=host,
        port=port,        
        database_name=database_name,
        auto_commit=auto_commit)
    elif (database_type == "mysql"):
      if (port == None):
        port = "3306"
      current_engine = return_mysql_engine(username=local_creds.username, 
        password=local_creds.password, 
        host=host,
        port=port,        
        database_name=database_name,
        auto_commit=auto_commit)

    return(current_engine)
  except:
    traceback.print_exc()
    return(result)


def return_postgres_or_mysql_engine(username=None, 
  password=None, 
  host="localhost",
  port=None,
  database_name="", 
  database_type=None,
  auto_commit=True):
  result = None
  """
  Note: if database_type is None, will default to trying Postgres
  """
  try:
    if (database_type == None):
      database_type = "postgres"

    if (database_type == "postgres"):
      if (port == None):
        port = "5432"
      if (database_name == ""):
        database_name = "postgres"
      current_engine = return_postgres_engine(username=username, 
        password=password, 
        host=host,
        port=port,        
        database_name=database_name,
        auto_commit=auto_commit)
    elif (database_type == "mysql"):
      if (port == None):
        port = "3306"
      current_engine = return_mysql_engine(username=username, 
        password=password, 
        host=host,
        port=port,        
        database_name=database_name,
        auto_commit=auto_commit)

    return(current_engine)
  except:
    traceback.print_exc()
    return(result)


def return_mysql_engine(username=None, password=None, host="localhost", port="3306", database_name="", auto_commit=True):
  """
  Note: relies on having installed mysql-connector-python
  """
  result = None
  try:
    url = 'mysql+mysqlconnector://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, database_name)
    current_engine = create_engine(url)
    print(current_engine)
    if (auto_commit == True):
      print("Setting connection to autocommit")
      current_engine = current_engine.execution_options(autocommit=True)
    return(current_engine)
  except:
    traceback.print_exc()
    return(result)


def return_postgres_engine(username=None, password=None, host="localhost", port="5432", database_name="", auto_commit=True):
  result = None
  try:
    url = 'postgresql://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, database_name)
    current_engine = create_engine(url)
    print(current_engine)
    if (auto_commit == True):
      print("Setting connection to autocommit")
      current_engine = current_engine.execution_options(autocommit=True)
    return(current_engine)
  except:
    traceback.print_exc()
    return(result)


def push_dataframe_to_table_engine_only(engine, dataframe, schema_name, table_name, if_exists='replace', verbose=True):
  """
  This function takes in a Pandas dataframe, connects to the required database and creates a new table from the dataframe.
  If the table already exists, it replaces it (default), or appends to it (if_exists = 'append').
  Uses the engine only (not a cursor)
  Returns False if error encountered
  If successful, returns the new table name.
  """
  result = False
  try:
    dataframe.to_sql(name=table_name, con=engine, schema=schema_name, if_exists=if_exists, index=False)
    if (verbose==True):
      if (if_exists == 'append'):
        print(table_name + ' has been successfully appended.')
      else:
        print(table_name + ' has been successfully (re)created.')
    result = True
    return(result)
  except:
    traceback.print_exc()
    return(result)


def check_if_schema_exists_with_engine(engine, schema_name):
  """
  Looks in the database accessed via the ENGINE to see whether the schema is present.
  This is NOT dialect-dependent, it uses sqlalchemy 
  Returns True if the schema exists.
  Returns False if a) the schema is found, or b) the sql returns an error.
  """
  result = False
  try:
    print("Checking if schema exists: {}".format(schema_name))
    inspector = inspect(engine)
    schema_list = inspector.get_schema_names()
    print("Schema list:")
    print(schema_list)
    if (schema_name in schema_list):
      result = True
    print(result)
    return (result)
  except:
    traceback.print_exc()
    return (result)


def create_schema_with_engine(engine, schema_name):
  """
  Creates a schema via the ENGINE rather than cursor.
  This is NOT dialect-dependent, it uses sqlalchemy and an engine
  First checks if the schema_name is None: if so, it skips everything and returns True
  Then checks if schema exists already: if it does, it won't try to recreate
  If it does not already exist, it attempts to create it
  Returns True if the schema exists at the end
  Returns False if errors encountered
  """
  result = False
  try:
    if (schema_name == None):
      print("Skipping schema creation, as schema_name set to {}".format(schema_name))
      result = True
      return(result)

    schema_exists = check_if_schema_exists_with_engine(engine, schema_name)
    if (schema_exists == True):
      print("Schema name {} already exists, do not need to create".format(schema_name))
      result = True
    else:
      print("Attempting to create schema: {}".format(schema_name))
      engine.execute(CreateSchema(schema_name))
      schema_exists = check_if_schema_exists_with_engine(engine, schema_name)
      result = schema_exists
    return (result)
  except:
    traceback.print_exc()
    return (result)


## Some functions written quickly for parsing XML

def return_text_from_element_xml_find(input_element, attribute_name, none_result=''):
  result = None
  try:
    find_result = return_element_from_xml_find(input_element=input_element, attribute_name=attribute_name)
    if (find_result == None):
      text_result = none_result
    else:
      text_result = find_result.text
      if (text_result == None):
        text_result = none_result
    return(text_result)
  except:
    traceback.print_exc()
    return(result)


def return_element_from_xml_find(input_element, attribute_name):
  result = None
  try:
    result = input_element.find(attribute_name)
    return(result)
  except:
    traceback.print_exc()
    return(result)


def return_element_list_from_xml_find_all(input_element, attribute_name):
  result = []
  try:
    result = input_element.findall(attribute_name)
    return(result)
  except:
    traceback.print_exc()
    return(result)


def return_tree_from_input_file_with_enforced_encoding_on_error(full_file_name, 
  suggested_encoding="utf-8", 
  try_copying=True,
  output_directory=""):
  result = None
  try:
    tree = return_tree_from_input_file(full_file_name=full_file_name)
    if (tree == None and try_copying == True):
      print("Error reading file, will attempt forcing to encoding: {}".format(suggested_encoding))
      backup_file_name = copy_file_with_suffix(input_file_name=full_file_name, output_file_suffix="_backup_copy", new_directory=output_directory)
      if (backup_file_name != False):
        print("Made backup file to: {}".format(backup_file_name))
        print("Now trying to copy back with enforced encoding: {}".format(suggested_encoding))
        copy_result = copy_file_with_enforced_encoding(input_file_name=backup_file_name, 
          output_file_name=full_file_name, 
          output_coding=suggested_encoding)
        tree = return_tree_from_input_file(full_file_name=full_file_name)
      else:
        print("Couldn't make backup file, so can't try enforced encoding, sorry...")
    return(tree)
  except:
    traceback.print_exc()
    return(result)  


def return_tree_from_input_file(full_file_name):
  result = None
  try:
    tree = etree.parse(full_file_name)   
    return(tree)
  except:
    traceback.print_exc()
    return(result)  


## Some quickly written functions for lists

def return_first_number_of_matching_item_in_list(input_list, search_item):
  result = False
  try:
    i = 0
    for item in input_list:
      if item == search_item:
        return(i)
      i = i+1
    return(result)
  except:
    traceback.print_exc()
    return(None)


## Some quickly written functions for exporting dataframes

def export_df_to_csv(current_df, csv_file_name):
  result = False
  try:
    current_df.to_csv(csv_file_name)
    result = True
    return(result)
  except:
    traceback.print_exc()
    return(result)


def upload_df_to_database(output_table_name, current_engine, schema_name, if_exists='append', index=False):
  result = True
  try:
    current_df.to_sql(name=output_table_name, con=current_engine, schema=schema_name, if_exists=if_exists, index=index)
    result = True
    return(result)
  except:
    traceback.print_exc()
    return(result)


## from tools_for_file_copy

def copy_file_with_enforced_encoding(input_file_name, output_file_name, output_coding="utf-8"):
  """
  This function can be handy for database input. It makes a copy of a file, in enforced encoding, defaulting to UTF8,
  Returns False if problems encountered.
  If successful returns True and creates a copy of the input file, in the specified location (output_file_name)
  """
  result = False
  print(input_file_name)
  try:
    with codecs.open(input_file_name) as input_file:
      with codecs.open(output_file_name, "w", encoding=output_coding) as output_file:
        shutil.copyfileobj(input_file, output_file)
        result = True    
    
    return(result)
  except:
    traceback.print_exc()
    print("Could not copy file %s\n" %input_file_name)
    return(result)


def copy_file_with_suffix(input_file_name, output_file_suffix="_copy", new_directory=None):
  """
  This function makes a copy of the file named input_file_name, but with a given suffix
  Defaults to the same directory but this can be overwritten
  Defaults to the suffix "_copy" but this can be overwritten
  By using SHUTIL it WILL overwrite the file if it exists
  If successful, it returns the newly copied file name
  If any errors are encountered, it exits and returns False
  """
  result = False
  try:
    print(input_file_name)
    print(new_directory)
    new_file_name = os.path.splitext(input_file_name)[0] + output_file_suffix + os.path.splitext(input_file_name)[1]
    if (new_directory != None):
      new_file_name = new_directory + os.path.normpath("/") + os.path.basename(new_file_name)
    shutil.copyfile(input_file_name, new_file_name)
    return(new_file_name)
  except:
    traceback.print_exc()
    return(result)


