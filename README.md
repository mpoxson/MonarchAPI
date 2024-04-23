# MonarchAPI

- PLEASE USE THE MAIN BRANCH
- When testing both directions, please make sure delete the data before switching directions if using the same databases (will say that schema is already created)
- In Azure, you can test if data was imported successfully using the SQL Editor
- In AWS, you can test if data was imported successfully using SQL Server Management Studio and connecting using the same information supplied in the form
- Murphy successfully tested each route manually, please contact him if there is an error or you need help setting up/connecting to a test database in either platform 

# Dependencies

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

# Access routes

- uvicorn main:app --reload
- python -m uvicorn main:app --reload (If the above doesn't work)

# Database Prerequisites:

- Database being migrated into (target database) must already be created
- Instances must be SQL Server
- Target Database must neither have an existing schema with the same name as one being moved into it (except 'dbo') nor have the same table names in the dbo schema

## Azure Specific

- Make yourself admin of server using entra ID (See: references)
- Allow your ip to access the database through the internet
- Make sure server is running (might have to request twice or thrice before the server starts itself up)

## AWS Specific

- Set up database to connect using SQL authentication
- Make sure database is accessible via the internet
- Make sure your security group allows public connections for aws on the port the database is using
- Once the RDS Instance is up and running, connect to the instance using SSMS to create a new database (Azure should do this automatically, RDS in AWS is only the server)
- The name of the database you create is what you will use for AWS Database name

# Scope

- The scope of our project is smaller Sql Server databases on either Azure or AWS. We aim to move data from existing dbs to fresh/empty dbs. Some integration is possible, but only for the dbo schema.

# How to see api endpoints and use them

- Assuming your machine is running 127.0.0.1:8000 for uvicorn
- List of all endpoints: http://127.0.0.1:8000/docs
- List of accessible UI forms: http://127.0.0.1:8000

# Endpoints

- Each endpoint has an associated form, just add '/Form' to the end of the route in the browser
- Use /Form routes to test below endpoints

- POST /Migrate/Azure_To_Aws: Takes inputs for connection strings for an Azure database (to pull data out of) and a target AWS database. Aws database must either be empty or have non-conflicting data (Can't have a schema already created that will be migrated unless it is dbo. User dbo tables must not have the same name as tables being migrated). Output is a replica of the Azure database in AWS. Some limitations apply and are noted below.
- POST /Migrate/AWS_To_Azure: Same as above except going from AWS db to Azure db
- GET /Azure/Export/Structure and GET /AWS/Export/Structure: Connects to db and returns a csv file with the structure of your database in terms of schemas and tables within schema
- GET /Azure/Export/Data and GET /AWS/Export/Data: Connects to db and creates a folder for the database on your local file system. Creates a folder for each schema in the database, and in each folder two more folders are created. One folder is called "schema" and has a csv file for each table in that schema, where each table's name is the name of the csv. In each csv is all the columns in the table and their attributes (length, precision, scale, null, primary key). The other folder is called "data" and has the same structure, except the csv files are comprised of the actual data rows from the table.
- POST /Azure/Import/Schema and POST /AWS/Import/Schema: Takes in a list of the above schema files and above data files. A new schema is created using the one specified (if dbo, nothing is created). The files are looped through and the api takes the schema files and creates tables using the column names and information in the csv files. After the schema files are looped through, the data files are looped through and the data is inserted into the tables. Order for the data files does not have to match the schema files.
- POST /Azure/Import/General and POST /AWS/Import/General: Takes in a list of the above data files and creates tables using the names of the csv files. A new schema is created using the provided name (if dbo, nothing will be created). The columns will all be nvarchar(max)— hence the general upload. The data from the files will be inserted into the new tables.

# References used

- Connect to server and query: https://learn.microsoft.com/en-us/azure/azure-sql/database/azure-sql-python-quickstart?view=azuresql&tabs=windows%2Csql-inter
- Connect to server aws: https://stackoverflow.com/questions/58987218/cant-connect-to-sql-server-on-aws-rds-via-python

- Convert to csv: https://stackoverflow.com/questions/6081008/dump-a-numpy-array-into-a-csv-file
- list tables: https://dataedo.com/kb/query/azure-sql/list-of-tables-in-the-database
- list table info: https://www.sisense.com/blog/sql-cheat-sheet-retrieving-column-description-sql-server/
- Right split strings: https://stackoverflow.com/questions/3548673/how-can-i-replace-or-strip-an-extension-from-a-filename-in-python
- Column info per table https://stackoverflow.com/questions/2418527/sql-server-query-to-get-the-list-of-columns-in-a-table-along-with-data-types-no

# Note:

- No changes will be made if there is an error, nothing will be uploaded or saved
- Must have database/schema already created on upload (we cannot create a database in fast api)
- Upload doesn't have foreign keys or primary keys (could not pull constraints)
- Must have Python 3.10 or higher
- Upload doesn't have uniqueness (sorry)
- Upload target must be empty
- Make sure your credentials have access to both dbs and can read/write/create
- Binary, for schema data import, saves as varchar(max) due to limitations in the decoder
- SLOW (4 minutes for 11MB DB)

# Future:

- Implement more data scraping using sql - can grab multiple schemas, more table info, multiple databases
- Host in cloud
- Make more secure
- Integrate (combine) tables and deal with duplicates (alter table/insert into existing table)
- Implement constraints
- Expand to more servers (Mysql, postgres, etc)
