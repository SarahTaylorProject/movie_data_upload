'''
Sarah testing reads of XML files for Kinomatics movies and screeenings
Lots of work to do, but can demonstrate that it is possible to a) read the XML files, and b) push them to a PostgreSQL database
Needs a local_creds file with valid username, password, database name for a local PostgreSQL database
'''

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import os
import sys
import traceback

#import local_creds
import ast
import time
import datetime

from tools_for_movie_database import *
from tools_for_movie_database_miscellaneous import *


## TEST MAIN
#search_directory = "C:/Scratch/movie_test_inputs/very_small_test_set/"

# June choice 1: change this to your search directory of choice
search_directory = "C:/Scratch/movie_test_inputs/1st donwload from Christopher/mapping-contemporary-cinema-flow-raw-data/Mapping-Contemporary-Cinema-Flow/WWM-raw-data/2012/12/"
search_directory = "C:/Scratch/movie_test_inputs/small_test_set/"

# June choice 2: change this to a useful schema name of your choice
#output_schema_name = "testing_data_from_dec2012"
output_schema_name = "testing"

# June choice 3: change this by un-commenting the choice of file type you want
# ...the last line assigned current_search_file_type_dictionary, uncommented in white, will be the one that is kept
current_search_file_type_dictionary = {'theater': 'T.XML', 'movie': 'I.XML', 'screening': 'S.XML'}

# June choice 4: only change these if you want to run some tests
export_to_check_files = False
upload_to_database = True

# June choice 5: country codes to specifically exclude in this upload
list_of_country_codes_to_exclude = ['USA', 'CHN', 'ARG', 'CZE']

# June choice 6: country codes to ONLY include
list_of_country_codes_to_only_include = []


# June: everything below here is automatic
upload_result = movie_upload_main(search_directory=search_directory,
  output_schema_name=output_schema_name,
  current_search_file_type_dictionary = current_search_file_type_dictionary,
  export_to_check_files = export_to_check_files,
  upload_to_database = upload_to_database,
  list_of_country_codes_to_exclude = list_of_country_codes_to_exclude)

print(upload_result)
