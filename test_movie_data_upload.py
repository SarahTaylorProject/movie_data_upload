'''
Sarah testing reads of XML files for Kinomatics: can process movies, theaters, screenings
Make CHOICES here, and run this file
'''

from tools_for_movie_database import *

## MAIN

# Choice 1: change this to your search directory of choice
#search_directory = "C:/Scratch/movie_test_inputs/1st donwload from Christopher/mapping-contemporary-cinema-flow-raw-data/Mapping-Contemporary-Cinema-Flow/WWM-raw-data/2012/12/"
search_directory = "C:/Scratch/movie_test_inputs/small_test_set/"

# Choice 2: change this to a useful schema name of your choice
output_schema_name = "testing"

# Choice 3: which types of files to upload (leave this to run all three types)
current_search_file_type_dictionary = {'theater': 'T.XML', 'movie': 'I.XML', 'screening': 'S.XML'}

# Choice 4: choice of whether to upload to DB and/or whether to make test CSV copies
# only change these if you want to run some tests
export_to_check_files = False
upload_to_database = True

# Choice 5: country codes to specifically EXCLUDE in this upload
# This can be VERY helpful for excluding the large countries and running them later
list_of_country_codes_to_exclude = ['USA', 'CHN']

# Choice 6: country codes to ONLY include in this upload
list_of_country_codes_to_only_include = []


upload_result = movie_upload_main(search_directory=search_directory,
  output_schema_name=output_schema_name,
  current_search_file_type_dictionary = current_search_file_type_dictionary,
  export_to_check_files = export_to_check_files,
  upload_to_database = upload_to_database,
  list_of_country_codes_to_exclude = list_of_country_codes_to_exclude)

print(upload_result)