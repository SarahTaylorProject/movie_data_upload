# Generic functions useful for movie XML/database project
# Work in progress

# Many sourced from Sarah's other libraries
# But they are compiled here on the assumption that it is preferable for the movie script to be self-contained

import pandas as pd
import xml.etree.ElementTree as etree
import os
import traceback
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import csv
import shutil
import codecs


## Some functions copied from tools_for_db

def create_schema(cur, schema_name=None):
  """
  This function creates a new schema space if it does not exist already on the database cursor connection (cur)
  If no schema name is passed in the function will return True but not do anything.
  If a schema_name is passed in it will check whether the schema exists or not, then either a) create a new schema if it
  did not already exist, or b) leave the existing schema by the same name.
  It will only return True if either: a) the schema already exists and does not need to be created, or b) the schema was created successfully.
  If an error is encountered it returns False, most likely without having also created the schema (although this is not impossible).
  """
  result = False
  try:
    if (schema_name != None):
      exists = check_if_schema_exists(cur, schema_name)
      if not exists:
        sql_string = "CREATE SCHEMA " + schema_name + ";"
        print(sql_string)
        cur.execute(sql_string)

        commit_result = commit_session(session=cur)
        if (commit_result == False):
          print("Note: issuing with committing session, but will return True for function 'create_view'")

      else:
        print("The schema {} already exists, I will not attempt to recreate it.".format(schema_name))
    result = True
    return(result)
  except:
    traceback.print_exc()
    return(result)


def return_local_postgres_engine(username=None, password=None, host="localhost", port="5432", database_name="movie_test", auto_commit=True):
  result = None
  try:
    url = 'postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, database_name)
    current_engine = create_engine(url)
    print(current_engine)
    if (auto_commit == True):
      print("Setting connection to autocommit")
      current_engine = current_engine.execution_options(autocommit=True)
    return(current_engine)
  except:
    traceback.print_exc()
    return(result)


def return_local_postgres_session(username=None, password=None, host="localhost", port="5432", database_name="movie_test"):
  result = None
  try:
    current_engine = return_local_postgres_engine(username=username, password=password, host=host, port=port, database_name=database_name)
    print("engine: ")
    print(current_engine)
    if (current_engine != None):      
      result = return_session(current_engine)
      print("session: ")
      print(result)
    return(result)
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


def return_session(current_engine):
  """
  This function takes an engine that has already been created, and returns a session built from it.
  If successful, returns a session object.
  If errors encountered, returns False.
  """
  result = False
  try:
    Session = sessionmaker(bind=current_engine)
    current_session = Session()
    return(current_session)
  except:
    traceback.print_exc()
    return (result)


def commit_session(session):
  """
  This function attempts to commit a session. There are times when it will be passed a cursor however, and in these
  cases it will return False, but quietly.
  It will return True if the session is successfully committed, and False if the session is NOT committed,
  OR if it was handed a cursor.
  """
  result = False
  try:
    session.commit()
    result = True
    return(result)
  except:
    return(result)


def determine_connection_object_type(cur):
  """
  This function takes in some kind of connection object (ie a cursor, a session, etc), and tries to determine what
  it is. It does this in a very non-elegant way, by determining its type and searching that type for keywords like
  Connection and Session. If it cannot identify the type, it returns False.
  Note that 'ursor' is not a typo - some have a capital C and others a lowercase. This may be useful in determining
  if a cursor is set to autocommit or not - but if not, we should set the string to lowercase and work with 'cursor' etc.
  Note: we should add to this function with other types of objects (engine?) as needed.
  """
  result = False
  try:
    class_str = str(type(cur))

    if 'ursor' in class_str:
      result = 'cursor'
    elif 'ession' in class_str:
      result = 'session'
    elif 'ngine' in class_str:
      result = 'engine'
    elif 'onnection' in class_str:
      result = 'connection'
    else:
      result = False
    return(result)
  except:
    traceback.print_exc()
    return(result)


def return_dialect(session_or_engine):
  """
  This function returns the SQL dialect of the given database session or engine
  Passes the input to return_session_dialect function first, if this throws an error, attempts to pass to return_engine_dialect
  instead. Note that this means a traceback error message may print, but does not mean that the function is breaking
  as it will happily continue.
  Returns the dialect if successfully identified, returns None otherwise.
  """
  result = None
  try:
    result = return_session_dialect(session=session_or_engine)
    if (result == None):
      result = return_engine_dialect(engine=session_or_engine)
    return(result)
  except:
    return(result)


def return_session_dialect(session):
  """
  This function returns the SQL dialect of the given database session
  Returns the dialect if successfully identified, returns None otherwise.
  """
  result = None
  try:
    result = session.bind.dialect.name
    return(result)
  except:
    return(result)


def check_if_schema_exists(cur, schema_name, dialect='postgresql'):
  """
  Looks in the database accessed via the cursor to see whether the schema is present.
  This is dialect-dependent, so includes a check of dialect (postgresql, oracle, mssql).
  Note: the USERNAME for oracle is not a typo, it seems to define schemas as users.
  Returns True if the schema exists.
  Returns False if a) the schema is found, or b) the sql returns an error.
  """
  result = False
  try:
    type_of_cur = determine_connection_object_type(cur)
    #print(type_of_cur)
    if type_of_cur not in ['session', 'cursor']:
      print("Warning: This connection type does not appear to be either a cursor or a session. I will attempt to proceed as for a cursor, but this may return False.")
      type_of_cur = 'cursor'

    if type_of_cur == 'session':
      dialect = return_dialect(cur)

    #print('dialect : {}'.format(dialect))

    if (dialect == 'postgresql'):
      sql_qry = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{}';".format(schema_name)
    elif (dialect == 'mssql'):
      sql_qry = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{}';".format(schema_name)
    elif (dialect == 'oracle'):
      sql_qry = "SELECT USERNAME FROM SYS.ALL_USERS WHERE USERNAME = '{}'".format(schema_name)
    else:
      print("Sorry, dialect not currently understood. Returning False.")
      return(result)

    if type_of_cur == 'session':
      sql_result = cur.execute(sql_qry).fetchone()
    elif type_of_cur == 'cursor':
      cur.execute(sql_qry)
      sql_result = cur.fetchone()
    else:
      sql_result = None

    if (sql_result == None):
      result = False
    else:
      result = True

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
