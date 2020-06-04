# Functions specifically for movie XML/database project

import pandas as pd
import xml.etree.ElementTree as etree
import os
import sys
import traceback

import ast
import time
import datetime

# from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
import csv

import local_creds
from tools_for_movie_database_miscellaneous import *

def movie_upload_main(search_directory="",
  output_schema_name="",
  log_file_parent_directory_name=None,
  current_search_file_type_dictionary = {'theater': 'T.XML', 'movie': 'I.XML', 'screening': 'S.XML'},
  export_to_check_files = False,
  upload_to_database = True,
  list_of_country_codes_to_exclude = [],
  list_of_country_codes_to_only_include = []):
  result = False
  try:
    if (log_file_parent_directory_name == None):
      # if this input is left as None, will default to creating a subdirectory for the output log files, where the script is running
      output_directory = os.path.dirname(sys.argv[0]) + os.path.normpath("/") + "schema_" + str(output_schema_name) + os.path.normpath("/")
    else:
      output_directory = log_file_parent_directory_name + os.path.normpath("/") + "schema_" + str(output_schema_name) + os.path.normpath("/")
    
    if not os.path.exists(output_directory):
      print("Attempting to create directory for log files: {}".format(output_directory))
      os.makedirs(output_directory)       

    # conduct initial search of files, put results in a summary CSV
    current_search_type_list = list(current_search_file_type_dictionary.keys())
    current_search_type_string = "_".join(current_search_type_list)
    
    search_file_name = output_directory + os.path.normpath("/") + "search_results_" + current_search_type_string + "_" + time.strftime("%Y%m%d").replace("/", "") + ".csv"
    search_file_count_dictionary = return_search_file_count_dictionary_and_write_file_with_counts(output_file_name=search_file_name, 
      search_directory=search_directory,
      search_file_type_dictionary=current_search_file_type_dictionary)

    print("\nSTARTING SEARCH AND TEST")
    print("Current search directory: {}".format(search_directory))
    print("Current search file type dictionary: {}".format(current_search_file_type_dictionary))

    for file_type, file_count in search_file_count_dictionary.items():
      print("{}: {}".format(file_type, file_count))

    # open the search results CSV but skip first row; this will be the list of files to read through and try to process
    search_file = open(search_file_name, "r")
    search_file_reader = csv.reader(search_file)
    search_file_header = list(next(search_file_reader))
    search_column_dictionary = {search_file_header[i] : i for i in range(0, len(search_file_header))}

    # set up log file: important for keeping track of things, annoyingly convoluted code though
    log_file_name = search_file_name.split(".")[0] + "_upload_log.csv"
    log_file = open(log_file_name, mode='w', newline='')
    log_file_writer = csv.writer(log_file, delimiter=',', quotechar='"')
    log_file_header = ['search_file_type', 'sub_directory', 'file_name', 'country_code_guess', 'country_code_included_in_this_upload', 'start_time', 'end_time', 'check_file_name', 'check_file_status', 'upload_table_name', 'upload_status', 'size', 'time_taken_seconds']
    log_file_writer.writerow(log_file_header)

    if (upload_to_database == True):
      database_type = return_database_type_from_local_creds()
      print("Local database type from local_creds: {}".format(database_type))
      current_engine = return_postgres_or_mysql_engine(username=local_creds.username, 
          password=local_creds.password, 
          host=local_creds.host,
          database_name=local_creds.database_name, 
          database_type=database_type)
      print(current_engine)
      if (current_engine == None):
        print("Error with engine, will not be able to run database uploads...")
        upload_to_database = False
        return(result)
      else:
        schema_result = create_schema_with_engine(engine=current_engine, schema_name=output_schema_name)
        if (schema_result != True):
          print("Error with schema, will not be able to run database uploads...")
          return(result)

    # loop through the results of the file search, trying to parse and upload each file, and logging results
    file_count = 0
    for row in search_file_reader:
      file_name_list = ast.literal_eval(row[search_column_dictionary['matching_file_list']])
      for file_name in file_name_list:
        file_count += 1
        print("\nReading files from: {}".format(search_file_name))
        print("File #: {}".format(file_count))
        print("File name: {}".format(file_name))
        start_time = datetime.datetime.now()
        print("Start time: {}".format(start_time))
        
        search_file_type = row[search_column_dictionary['search_file_type']]
        print("File type: {}".format(search_file_type))
        sub_directory = row[search_column_dictionary['sub_directory']]
        
        country_code_guess = sub_directory.replace("/", "")[-3:].upper()
        print("Country name guess: {}".format(country_code_guess))
        full_file_name = sub_directory + os.path.normpath("/") + file_name
        
        country_code_included = True
        if (len(list_of_country_codes_to_exclude) > 0):
          if (country_code_guess in list_of_country_codes_to_exclude):
            country_code_included = False
   
        if (len(list_of_country_codes_to_only_include) > 0):
          if (country_code_guess not in list_of_country_codes_to_only_include):
            country_code_included = False

        if (search_file_type == 'movie' and country_code_included == True):
          current_df = parse_movie_file(full_file_name=full_file_name, output_directory=output_directory)
        elif (search_file_type == 'screening' and country_code_included == True):
          current_df = parse_screening_file_shorter(full_file_name=full_file_name, output_directory=output_directory)
        elif (search_file_type == 'theater' and country_code_included == True):
          current_df = parse_theater_file(full_file_name=full_file_name, output_directory=output_directory)
        elif (country_code_included == False):
          print("COUNTRY CODE NOT INCLUDED, SKIPPING...")
          current_df = None
        else:
          print("File type unknown, skipping...")
          current_df = None

        upload_table_name = search_file_type + '_xml_extract'
        if (export_to_check_files):
          check_file_name = output_directory + os.path.normpath("/") + country_code_guess + '_' + search_file_type + '_' + file_name.split(".")[0] + "_CHECK_OUTPUT.csv"
        else:
          check_file_name = '-' 

        if (isinstance(current_df, pd.DataFrame)):
          size = len(current_df)
          print("Dataframe length: {}".format(size))
          current_df["country_code_guess"] = country_code_guess
          current_df["sub_directory"] = sub_directory
          current_df["file_name"] = file_name
          
          if (export_to_check_files == True):
            check_file_status = export_df_to_csv(current_df=current_df, csv_file_name=check_file_name)
          else:
            check_file_status = None
          if (upload_to_database == True):
            upload_table_status = push_dataframe_to_table_engine_only(engine=current_engine, 
              dataframe=current_df, 
              schema_name=output_schema_name, 
              table_name=upload_table_name, 
              if_exists='append', verbose=True)
          else:
            upload_table_status = None
        else:
          size = ''
          check_file_status = False
          upload_table_status = False

        end_time = datetime.datetime.now()
        print("End time: {}".format(end_time))
        time_taken_seconds = (end_time - start_time).total_seconds()
        print("Time taken (seconds): {}".format(time_taken_seconds))
        end_log_info = [search_file_type, sub_directory, file_name, country_code_guess, country_code_included, str(start_time), str(end_time), 
          check_file_name, check_file_status, upload_table_name, upload_table_status, size, time_taken_seconds]
        log_file_writer.writerow(end_log_info)

      result = log_file_name

    return(result)
  except:
    traceback.print_exc()
    return(result)


def return_database_type_from_local_creds():
  result = None
  try:
    database_type = local_creds.database_type
    return(database_type)
  except:
    traceback.print_exc()
    return(result) 



def return_search_file_count_dictionary(search_directory="", 
  search_file_type_dictionary={'movie': 'I.XML', 'screening': 'S.XML', 'theater': 'T.XML'}):
  result = None
  try:
    search_file_count_dictionary = {}
    for search_file_type, search_file_suffix in search_file_type_dictionary.items():
      search_file_count_dictionary[search_file_type] = 0
      search_file_suffix = search_file_type_dictionary[search_file_type]

      for sub_directory, directory_list, file_list in os.walk(search_directory):
        for file_name in file_list:
          if file_name.endswith(search_file_suffix):
            print("Matching file found: '{}'".format(file_name))
            search_file_count_dictionary[search_file_type] += 1
    return(search_file_count_dictionary)
  except:
    traceback.print_exc()
    return(result)


def return_search_file_count_dictionary_and_write_file_with_names(output_file_name="search_results_with_names.csv", 
  search_directory="", search_file_type_dictionary={'movie': 'I.XML', 'screening': 'S.XML', 'theater': 'T.XML'}):
  result = None
  try:
    search_file_count_dictionary = {}
    output_file = open(output_file_name, mode='w', newline='')
    output_file_writer = csv.writer(output_file, delimiter=',', quotechar='"')

    for search_file_type, search_file_suffix in search_file_type_dictionary.items():
      search_file_count_dictionary[search_file_type] = 0
      search_file_suffix = search_file_type_dictionary[search_file_type]

      for sub_directory, directory_list, file_list in os.walk(search_directory):
        for file_name in file_list:
          if file_name.endswith(search_file_suffix):
            print("Matching file found: '{}'".format(file_name))
            search_file_count_dictionary[search_file_type] += 1
            output_file_writer.writerow([sub_directory, file_name, search_file_type])
    return(search_file_count_dictionary)
  except:
    traceback.print_exc()
    return(result)


def return_search_file_count_dictionary_and_write_file_with_counts(output_file_name="search_results.csv", search_directory="", 
  search_file_type_dictionary={'movie': 'I.XML', 'screening': 'S.XML', 'theater': 'T.XML'}):
  result = None
  try:
    search_file_count_dictionary = {}
    output_file = open(output_file_name, mode='w', newline='')
    output_file_writer = csv.writer(output_file, delimiter=',', quotechar='"')
    output_file_writer.writerow(["search_file_type", "sub_directory", "matching_file_count", "matching_file_list"])
    for search_file_type, search_file_suffix in search_file_type_dictionary.items():
      search_file_count_dictionary[search_file_type] = 0
      search_file_suffix = search_file_type_dictionary[search_file_type]
      for sub_directory, directory_list, file_list in os.walk(search_directory):
        directory_count = len(directory_list)
        if (directory_count == 0):
          matching_file_count = 0
          matching_file_name_list = []
          for file_name in file_list:
            if file_name.endswith(search_file_suffix):
              search_file_count_dictionary[search_file_type] += 1
              matching_file_count += 1
              matching_file_name_list.append(file_name)
          output_file_writer.writerow([search_file_type, sub_directory, matching_file_count, matching_file_name_list])
    print("Have written search results to: {}".format(output_file_name))
    return(search_file_count_dictionary)
  except:
    traceback.print_exc()
    return(result)


def parse_movie_file(full_file_name="C:/scratch/121130I.xml", 
  column_names=["movie_id", "movie_title", "parent_id", "running_time", "rating", 
  "release_date", "release_notes", "distributor",
  "genre_list_string", "director_list_string", "writer_list_string", 
  "producer_list_string", "cast_list_string"], 
  output_divider="; ", suggested_encoding="utf-8", output_directory=""):
  result = None
  '''
  Rudimentary function to test reading in MOVIE file to a dataframe
  Should be able to be made more generic, and add more columns
  Current reads cast in as a single string otherwise there will be a separate row for each cast member
  Perhaps that should be a different function, for reading to cast table?
  If successful, returns dataframe with columns as per column_list
  If any error encountered, returns None
  '''
  try:
    tree = return_tree_from_input_file_with_enforced_encoding_on_error(full_file_name=full_file_name, 
      suggested_encoding=suggested_encoding,
      output_directory=output_directory)   
    if (tree == None):
      print("SORRY, COULD NOT READ THIS FILE, WILL SKIP:\n{}\n".format(full_file_name))
      return(result)
    root = tree.getroot()
    
    dataframe = pd.DataFrame(columns=column_names)
    movie_count = 0
    for movie in root:
      movie_count += 1
    
      movie_id = return_text_from_element_xml_find(movie, "movie_id")

      movie_title = return_text_from_element_xml_find(movie, "title")
      
      parent_id = return_text_from_element_xml_find(movie, "parent_id")

      running_time = return_text_from_element_xml_find(movie, "running_time")
      rating = return_text_from_element_xml_find(movie, "rating")
      release_date = return_text_from_element_xml_find(movie, "release_date")
      release_notes = return_text_from_element_xml_find(movie, "release_notes")
      distributor = return_text_from_element_xml_find(movie, "distributor")

      cast_element_list = return_element_list_from_xml_find_all(movie, "cast")
      cast_list = [cast_member.text for cast_member in cast_element_list]
      cast_list_string = output_divider.join(cast_list)

      genre_element_list = return_element_list_from_xml_find_all(movie, "genre")
      genre_list = [genre.text for genre in genre_element_list]
      genre_list_string = output_divider.join(genre_list)

      director_element_list = return_element_list_from_xml_find_all(movie, "director")
      director_list = [director.text for director in director_element_list]
      director_list_string = output_divider.join(director_list)

      writer_element_list = return_element_list_from_xml_find_all(movie, "writer")
      writer_list = [writer.text for writer in writer_element_list]
      writer_list_string = output_divider.join(writer_list)

      producer_element_list = return_element_list_from_xml_find_all(movie, "producer")
      producer_list = [producer.text for producer in producer_element_list]
      producer_list_string = output_divider.join(producer_list)

      dataframe = dataframe.append(pd.Series([movie_id, 
        movie_title, 
        parent_id, 
        running_time,
        rating,
        release_date,
        release_notes,
        distributor,
        genre_list_string,
        director_list_string,
        writer_list_string,
        producer_list_string, 
        cast_list_string], index=column_names), ignore_index=True)

    return(dataframe)
  except:
    traceback.print_exc()
    return(result)


def parse_theater_file(full_file_name="C:/scratch/121130I.xml", 
  column_names=["theater_id", "theater_name", "theater_country", 
  "theater_city", "theater_market", "theater_closed_reason", 
  "theater_zip", "theater_screens", "theater_type_string",
  "theater_lat", "theater_lon"], 
  output_divider="; ", suggested_encoding="utf-8", output_directory=""):
  result = None
  '''
  Sarah 4/3/20
  Rudimentary function to test reading in a THEATER file to a dataframe
  '''
  try:
    tree = return_tree_from_input_file_with_enforced_encoding_on_error(full_file_name=full_file_name, 
      suggested_encoding=suggested_encoding,
      output_directory=output_directory)   
    if (tree == None):
      print("SORRY, COULD NOT READ THIS FILE, WILL SKIP:\n{}\n".format(full_file_name))
      return(result)
    root = tree.getroot()
    
    dataframe = pd.DataFrame(columns=column_names)
    theater_count = 0
    for theater in root:
      theater_count += 1
    
      theater_id = return_text_from_element_xml_find(theater, "theater_id")
      theater_name = return_text_from_element_xml_find(theater, "theater_name")  
      theater_country = return_text_from_element_xml_find(theater, "theater_country")

      theater_city = return_text_from_element_xml_find(theater, "theater_city")
      theater_market = return_text_from_element_xml_find(theater, "theater_market")      
      theater_closed_reason = return_text_from_element_xml_find(theater, "theater_closed_reason")

      theater_zip = return_text_from_element_xml_find(theater, "theater_zip")
      theater_screens = return_text_from_element_xml_find(theater, "theater_screens")

      theater_type_element_list = return_element_list_from_xml_find_all(theater, "theater_types")
      theater_type_list = []
      for theater_type in theater_type_element_list:
        current_theater_type_element_list = return_element_list_from_xml_find_all(theater_type, "type")
        current_theater_type_list = [current_theater_type.text for current_theater_type in current_theater_type_element_list]        
        theater_type_list.extend(current_theater_type_list)

      theater_type_string = output_divider.join(theater_type_list)
      theater_lat =  return_text_from_element_xml_find(theater, "theater_lat")
      theater_lon =  return_text_from_element_xml_find(theater, "theater_lon")  

      dataframe = dataframe.append(pd.Series([theater_id, 
        theater_name,
        theater_country,
        theater_city,
        theater_market,
        theater_closed_reason,
        theater_zip,
        theater_screens,
        theater_type_string,
        theater_lat,
        theater_lon], index=column_names), ignore_index=True)

    return(dataframe)
  except:
    traceback.print_exc()
    return(result)


def parse_screening_file_shorter(full_file_name="C:/scratch/121130S.xml", 
  column_names=["movie_id", "theater_id", "main_show_date", "show_time_count"], 
  output_divider="; ", suggested_encoding="utf-8", output_directory=""):
  result = None
  '''
  Rudimentary function to test reading in a SCREENING file to a dataframe
  SHORTER version made April 27th 2020: not as much information retained

  '''
  try:
    tree = return_tree_from_input_file_with_enforced_encoding_on_error(full_file_name=full_file_name, 
      suggested_encoding=suggested_encoding,
      output_directory=output_directory)   
    if (tree == None):
      print("SORRY, COULD NOT READ THIS FILE, WILL SKIP:\n{}\n".format(full_file_name))
      return(result)

    root = tree.getroot()
    dataframe = pd.DataFrame(columns=column_names)
    screening_count = 0
    for screening in root:
      screening_count += 1
      
      movie_id = return_text_from_element_xml_find(screening, "movie_id")
      
      theater_id = return_text_from_element_xml_find(screening, "theater_id")
      
      show_date_element = return_element_from_xml_find(screening, "show_date")
      main_show_date = show_date_element.attrib["date"]

      show_time_count = 0
      
      show_date_element_list = return_element_list_from_xml_find_all(screening, "show_date")
      for show_date_element in show_date_element_list:

        current_showtime_string = return_text_from_element_xml_find(show_date_element, "showtimes")

        if (len(current_showtime_string) > 0):
          show_time_count += len(current_showtime_string.split(","))

      dataframe = dataframe.append(pd.Series([movie_id, 
        theater_id,
        main_show_date,
        show_time_count], index=column_names), ignore_index=True)

    return(dataframe)
  except:
    traceback.print_exc()
    return(result)
