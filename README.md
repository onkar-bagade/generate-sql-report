# generate-sql-report
Simple utility that Outputs SQL Query result into .CSV report

# Installation
You will need to download code locally. Easiest way to do that is using git clone.
```
$ git clone https://github.com/DRAAGONBEAK/generate-sql-report.git
```

If you have any trouble doing this, you can download the zip folder of this repo and then extract the files to a locally.

Once you have repo cloned or extracted you can install the dependent modules using requirements.txt
```
$ pip install -r requirements.txt
```

# Configurations
After installing all dependencies open the config.ini file to configure database.
```
driver : ODBC_DRIVER to be used  ex. ODBC Driver 17 for SQL Server
database : Name of the database you are connecting to.
host : hostname for the database server
user : database user 
pwd : password
```

# Sample usage
```
python generate_report.py --ini_file config.ini --sql_query "SELECT * from tempdb.dbo.temp_table" --output_file "output_folder\output_filename.csv" --include_header
```
