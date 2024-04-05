# MonarchAPI

- Download python
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
- make sure server is running (might have to request once or twice before the server starts itself up)

- make sure your securtiy group allows public connections for aws

# How to see api endpoints and use them

http://127.0.0.1:8000/docs

# Potential References:

- Connect to aws rds: https://stackoverflow.com/questions/62627058/how-to-connect-to-aws-rds-mysql-database-with-python
- Import db: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MySQL.Procedural.Importing.NonRDSRepl.html
- export db: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/MySQL.Procedural.Exporting.NonRDSRepl.html
- importing db (azure): https://learn.microsoft.com/en-us/azure/dms/tutorial-mysql-azure-mysql-offline-portal
- exporting db (azure): https://learn.microsoft.com/en-us/azure/mysql/flexible-server/concepts-migrate-import-export

https://www.youtube.com/watch?v=JVtGKA6OVvM
https://www.youtube.com/watch?v=6joGkZMVX4o
https://www.youtube.com/watch?v=NOT_pScEVFc

https://stackoverflow.com/questions/58696716/how-to-export-databases-with-commands-azure-cli-on-python-script
https://learn.microsoft.com/en-us/python/api/azure-mgmt-sql/azure.mgmt.sql.operations.databasesoperations?view=azure-python

https://github.com/MicrosoftDocs/sql-docs/issues/9622
https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.Connecting.Python.html

# References used

- Connect to server and query: https://learn.microsoft.com/en-us/azure/azure-sql/database/azure-sql-python-quickstart?view=azuresql&tabs=windows%2Csql-inter
- Convert to csv: https://stackoverflow.com/questions/6081008/dump-a-numpy-array-into-a-csv-file
- list tables: https://dataedo.com/kb/query/azure-sql/list-of-tables-in-the-database
- list table info: https://www.sisense.com/blog/sql-cheat-sheet-retrieving-column-description-sql-server/

- Connect to server aws: https://stackoverflow.com/questions/58987218/cant-connect-to-sql-server-on-aws-rds-via-python

# Note:

- Must have database/schema already created on upload (we cannot create a database in fast api)
