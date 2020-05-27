# Movie XML data upload

## Python code for a very specific task!

Made for uploading the numerous XML files with movie information. Extracts this data from many XML files within particular folders, to a PostgreSQL database formatted to the interests of our research project.

# Requirements

- Local PostgreSQL server with a blank database and with write access
- A "local_creds.py" file with the following items:

```    
username = [postgres username]
password = [postgres password]
database_name = [your database name]
```

- Python packages needed in addition to usual ones:
sqlalchemy, psycopg2, pandas, xml

# Important!

Make the choices about what types of files you want to upload, in the run_movie_data_upload.py file.
There are some important choices to make, and others which you can leave to default.

## Choice 1: change this to your search directory of choice, where the XML FILES ARE
Very likely, this will be a sub directory, such as for a year of film data (2012, 2013), or a month
If you are feeling very confident, point it towards the parent directory of the XML files
But that is likely to take a long time, and it can help to run the task in chunks

For example, my test dataset:
```
search_directory = "C:/Scratch/movie_test_inputs/very_small_test_set/"
```

Or, for example, just the 2012 data:
```
search_directory = "C:/Scratch/movie_test_inputs/2012/"
```

## Choice 1b: change parent directory for where to output LOG FILES
The script will still make a subdirectory underneath this, with the same name as output_schema_name
But you can override which parent directory it will put this in
Leave as None to default to creating sub directories where the Python script is running
It is not recommended to put this within the source directories, as they can be labyrinthian directories!

For example:
```
log_file_parent_directory_name = "C:/Scratch/movie_log_files/"
```

## Choice 2: change this to a useful schema name of your choice
This is useful for keeping track of incremental updates
It is the schema name that will be created and populated on Postgres
It also becomes the subdirectory name for the log files
For example:
```
output_schema_name = "testing_small"
```
Or, more usefullly:
```
output_schema_name = "2012"
```

## Choice 3: country codes to specifically EXCLUDE in this upload (if any)
This can be VERY helpful for excluding the large countries and running them later
Defaults to excluding USA and China
To NOT exclude any country codes, make this an empty list []

For example, to exclude USA and China:
```
list_of_country_codes_to_exclude = ['USA', 'CHN']
```

For example, to not exclude any country code:
```
list_of_country_codes_to_exclude = []
```

## Choice 4: country codes to ONLY include in this upload (if any)
Note: this means, ONLY including these codes, to the exclusion of ALL OTHERS
It is most likely that this variable would only be non-blank when catching up on countries previously excluded
This is not a variable that would be changed often, but it is important for particular scenarios

For example, to ONLY upload CHINA files:
```
list_of_country_codes_to_only_include = ['CHN']
```


Or to NOT have any country code to the exclusion of others:
```
list_of_country_codes_to_only_include = []
```

## Choice 5: which types of files to upload (leave this to run all three types)
Change the dictionary if you want only some file types to be searched for and processed
This can be helpful for delaying the hard work of screening files, for example
As per above, This is not a variable that would be changed often, but it is important for particular scenarios

For example, to search for all three file types (theater, movie, screening):
```
current_search_file_type_dictionary = {'theater': 'T.XML', 'movie': 'I.XML', 'screening': 'S.XML'}
```

Or to search for ONLY theater files:
```
current_search_file_type_dictionary = {'theater': 'T.XML'}
```

Or to search for ONLY screening files:
```
current_search_file_type_dictionary = {screening': 'S.XML'}
```

Or to search for theater and movie files, but not screenings:
```
current_search_file_type_dictionary = {'theater': 'T.XML', 'movie': 'I.XML'}
```

## Choice 6: choice of whether to upload to DB and/or whether to make test CSV copies
only change these if you want to run some tests!
the check files can be handy, they are CSV versions of what will be uploaded to the database

```
export_to_check_files = False
upload_to_database = True
```
