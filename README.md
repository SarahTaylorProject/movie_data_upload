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

Make the choices about what types of files you want to upload, in the run_movie_data_upload.py file
Choose the source directory in which the XML files can be found. 
Choose which types of file to upload: movies and/or theaters and/or screenings.
Choose which country codes to exclude (if any), or which country codes to ONLY include in this upload.