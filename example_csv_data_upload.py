import sys
import pandas

from tools_for_movie_database import *

database_name = "movie_test"
to_schema_name = "test"
source_directory_name = "C:/Scratch/"
csv_file_name = "test.csv"
to_table_name = "test_table"
if_exists = "replace"

from tools_for_movie_database import *

full_file_name = source_directory_name + os.path.normpath("/") + csv_file_name
print("Attempting to upload CSV:")
print()

current_engine = return_postgres_or_mysql_engine_from_local_creds()
if (current_engine == None):
  print("Error with engine, will not be able to run database uploads...")
  sys.exit()
else:
  schema_result = create_schema_with_engine(engine=current_engine, schema_name=to_schema_name)
  if (schema_result == False):
    print("Error with schema, will not be able to run database uploads...")
    sys.exit()

current_df = pandas.read_csv(full_file_name)
upload_table_status = push_dataframe_to_table_engine_only(engine=current_engine, 
  dataframe=current_df, 
  schema_name=to_schema_name, 
  table_name=to_table_name, 
  if_exists=if_exists, verbose=True)

print("Upload status:")
print(upload_table_status)