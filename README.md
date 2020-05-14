# Movie XML data upload

## Python code for a very specific task!

Made for uploading the numerous XML files used by the Kinomatics research team
Extracts data from many XML files within particular folders, to a PostgreSQL database formatted to the interests of the Kinomatics team

# Requirements

- Local PostgreSQL server with a blank database and write access
- A "local_creds.py" file with the following items:
    
username = [postgres username]
password = [postgres password]
database_name = [your database name]

- Python packages needed in addition to usual ones:
    sqlalchemy
    psycopg2
    pandas
    xml

# Important

Make the choices about what types of files you want to upload, in the test_movie_