# MonarchAPI

- PLEASE USE THE DEMO BRANCH

- Download python (3.10 or higher)
- Download ODBC Driver 18 for SQL Server
- pip install fastapi
- pip install uvicorn
- pip install pyodbc
- pip install pydantic
- pip install azure-identity
- pip install pandas
- pip install mysql-connector
- pip install python-multipart

uvicorn main:app --reload

# To Use:

- Make yourself admin of server using entra ID
- Allow your ip to access the database through the internet
- make sure server is running (might have to request twice or thrice before the server starts itself up)

- make sure your securtiy group allows public connections for aws

# Scope

- The scope of our project is smaller Sql Server databases on either Azure or AWS. We aim to move data from existing dbs to fresh/empty dbs. Some integration is possible, but only for the dbo schema.

# How to see api endpoints and use them

http://127.0.0.1:8000/docs

# End Points

- POST /Migrate/Azure_To_Aws: Takes inputs for connection strings for an azure database (to pull data out of) and a target AWS database. Aws database must either be empty or have non conflicted data (Can't have a schema already created that will be migrated unless it is dbo. User dbo tables must not have the same name as tables being migrated). Output is a replica of the Azure database in Aws some limitations apply and are noted below.

- POST /Migrate/AWS_To_Azure: Same as above except going from AWS db to Azure db

- GET /Export/Structure: Connects to db and returns a csv file with the structure of your database in terms of schemas and tables within schema

- GET /Export/Data: Connects to db and creates a folder for the database on your local file system. Creates a folder for each schema in the database, and in each folder two more folders are created. One folder is called "schema" and has a csv file for each table in that schema, where each table's name is the name of the csv. In each csv is all the columns of in the table and their attributes (length, precision, scale, null, primary key). The other folder is called "data" and has the same structure, except the csv files are comprised of the actual rows in the table.

- POST /Export/Data: Takes in a list of the above schema files and above data files. A new schema is created using the one specified (if dbo, nothing is created). The files are looped through and the api takes the schema files and creates tables using the column names and information in the csv files. After the schema files are looped through, the data files are looped through and the data is inserted into the tables. Order does not matter.

- POST /Import/General: Takes in a list of the above data files and creates tables using the names of the csv files. A new schema is created using the provided name (if dbo, nothing will be created). The columns will all be nvarchar(max)— hence the general upload. The data from the files will be inserted into the new tables.

# References used

- Connect to server and query: https://learn.microsoft.com/en-us/azure/azure-sql/database/azure-sql-python-quickstart?view=azuresql&tabs=windows%2Csql-inter
- Convert to csv: https://stackoverflow.com/questions/6081008/dump-a-numpy-array-into-a-csv-file
- list tables: https://dataedo.com/kb/query/azure-sql/list-of-tables-in-the-database
- list table info: https://www.sisense.com/blog/sql-cheat-sheet-retrieving-column-description-sql-server/

- Connect to server aws: https://stackoverflow.com/questions/58987218/cant-connect-to-sql-server-on-aws-rds-via-python
- Right split strings: https://stackoverflow.com/questions/3548673/how-can-i-replace-or-strip-an-extension-from-a-filename-in-python
- Column info per table https://stackoverflow.com/questions/2418527/sql-server-query-to-get-the-list-of-columns-in-a-table-along-with-data-types-no

# Note:

- No changes will be made if there is an error, nothing will be uploaded or saved
- Must have database/schema already created on upload (we cannot create a database in fast api)
- Upload doesn't have a foreign key or multiple primary keys in a constraint
- Must have Python 3.10 or higher
- Upload doesn't have uniqueness (sorry)
- Upload target must be empty
- make sure your credentials have access to both dbs and can read/write/create
- aws: will only grab from dbo schema
- import will only import to dbo
- Binary, for schema data import, saves as varchar(max) due to limitations in the decoder
- SLOW (4 minutes for 11MB DB)

# Future:

- implement more data scraping using sql - can grab multiple schemas, more table info, multiple databases
- host in cloud
- make more secure
- integrate (combine) tables and deal with duplicates (alter table/insert into existing table)
- implement constraints
- expand to more servers (Mysql, postgres, etc)

# My RDS server:

- port = 1433
- server = monarch.cjgga6i4mae6.us-east-2.rds.amazonaws.com
