# MonarchAPI

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

# How to see api endpoints and use them

http://127.0.0.1:8000/docs

# References used

- Connect to server and query: https://learn.microsoft.com/en-us/azure/azure-sql/database/azure-sql-python-quickstart?view=azuresql&tabs=windows%2Csql-inter
- Convert to csv: https://stackoverflow.com/questions/6081008/dump-a-numpy-array-into-a-csv-file
- list tables: https://dataedo.com/kb/query/azure-sql/list-of-tables-in-the-database
- list table info: https://www.sisense.com/blog/sql-cheat-sheet-retrieving-column-description-sql-server/

- Connect to server aws: https://stackoverflow.com/questions/58987218/cant-connect-to-sql-server-on-aws-rds-via-python
- Right split strings: https://stackoverflow.com/questions/3548673/how-can-i-replace-or-strip-an-extension-from-a-filename-in-python
- Column info per table https://stackoverflow.com/questions/2418527/sql-server-query-to-get-the-list-of-columns-in-a-table-along-with-data-types-no

# Note:

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
