from fastapi import FastAPI, File, UploadFile, HTTPException
import os
import mysql.connector
import pandas as pd
from io import StringIO
import pyodbc, struct
from azure import identity
import psycopg2
import sys
import boto3
import csv


app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

config = {
    'user': 'admin',
    'password': 'Cupcake0923&',
    'host': 'monarchapi1.c3amcyc0kkom.us-east-2.rds.amazonaws.com',
    'database': 'monarchapi1',
    'port': 3306,
}

driver_17 = '{ODBC Driver 17 for SQL Server}'
server_17 = 'server-sampledb.database.windows.net'
database_17 = 'sample-db'
username_17 = 'admin123'
password_17 = 'admin!123'
connection_string_17 = f"DRIVER={driver_17};SERVER={server_17};DATABASE={database_17};UID={username_17};PWD={password_17}"
    
@app.post("/azure/import")
async def import_iris(server_name: str, database_name: str, file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        try: 
            file_name = file.filename
            table_name = os.path.splitext(file_name)[0]

            contents = await file.read()
            df = pd.read_csv(StringIO(contents.decode('utf-8')))
            df = pd.read_csv(file_name)

            #conn = pyodbc.connect(connection_string_17)
            AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
            with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
                cursor = conn.cursor()

                # Create table
                create_table_query = f"CREATE TABLE {table_name} ("
                for column_name in df.columns:
                    column_name_cleaned = column_name.replace('.', '_')  # Replace dots with underscores
                    create_table_query += f"{column_name_cleaned} VARCHAR(255), "
                create_table_query = create_table_query[:-2] + ");"
                cursor.execute(create_table_query)

                # Insert data
                for index, row in df.iterrows():
                    placeholders = ",".join(["?"] * len(row))
                    columns = ",".join([col.replace('.', '_') for col in row.index])  # Replace dots with underscores
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, tuple(row))
                conn.commit()

                conn.close()

            return {"message": "Data imported successfully."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        return {"error": "Please upload a CSV file."}
   
    
#Need parameters for database name, server
AZURE_SQL_CONNECTIONSTRING=''

@app.get("/azure/tables/{server_name}/{database_name}")
def get_tables(server_name: str, database_name: str):
    output = ""
    column = []
    AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
    with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")

        
        results = (cursor.fetchall())
        for tupler in cursor.description:
            #get names of columns (first value in tuple)
            column.append(tupler[0])

        

        #find file path and dynamically change string
        output = f"File has been saved to: {create_csv(database_name, "table_info", results, column)}" 


    return output


@app.get("/azure/data/{server_name}/{database_name}")
def get_tables(server_name: str, database_name: str):
    output = ""
    
    AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
    with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")

        results = (cursor.fetchall())
        for table in results:
            if table != "table_name":
                table_schema = table[0]
                table_name = table[1]
                #select everything from each table, make into csv            
                cursor.execute(f"select * from {table_schema}.{table_name};")
                table_result = (cursor.fetchall())

                column = []
                for tupler in cursor.description:
                    #get names of columns (first value in tuple)
                    column.append(tupler[0])

                create_csv(f"{database_name}/{table_schema}", table_name, table_result, column)
            
        output = f"Files have been saved to: ./{database_name}"


    return output


@app.get("/azure/tables/columns")
def get_tables(server_name: str, database_name: str):
    output = ""
    column = []
    AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
    with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
        cursor = conn.cursor()
        cursor.execute("""SELECT TAB.name AS TableName, TAB.object_id AS ObjectID, COL.name AS ColumnName, TYP.name AS DataTypeName, TYP.max_length AS MaxLength 
                       From sys.columns COL INNER JOIN sys.tables TAB On COL.object_id = TAB.object_id 
                       INNER JOIN sys.types TYP ON TYP.user_type_id = COL.user_type_id 
                       WHERE TAB.name NOT LIKE 'BuildVersion' AND TAB.name NOT LIKE 'ErrorLog';""")

        
        results = (cursor.fetchall())
        for tupler in cursor.description:
            #get names of columns (first value in tuple)
            column.append(tupler[0])

        #find file path and dynamically change string
        output = f"File has been saved to: {create_csv(database_name, "column_info", results, column)}" 


    return output

def get_conn(AZURE_SQL_CONNECTIONSTRING : str):
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn

#AWS
################################################################################################

@app.get("/aws/tables")
def get_tables(port: str, server: str, username: str, password: str, database_name: str):
    #PORT=1433;SERVER=monarch.cjgga6i4mae6.us-east-2.rds.amazonaws.com;UID=;PWD=;db=monarchdb
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
        cur = conn.cursor()
        cur.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
            FROM %s.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'""" % (database_name))
        query_results = cur.fetchall()
        column = []
        for tupler in cur.description:
            #get names of columns (first value in tuple)
            column.append(tupler[0])

        #find file path and dynamically change string
        output = f"File has been saved to: {create_csv(database_name, "table_info", query_results, column)}" 
        return output
    
    except Exception as e:
        print("Database connection failed due to {}".format(e))   

@app.get("/aws/data")
def get_tables(port: str, server: str, username: str, password: str, database_name: str):
    output = ""
    
    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
    cur = conn.cursor()
    cur.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
            FROM %s.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'""" % (database_name))
    results = (cur.fetchall())
    for table in results:
        if table != "table_name":
            print(type(table))
            table_schema = table[0]
            table_name = table[1]
            #select everything from each table, make into csv       
            cur.execute(f"select * from {database_name}.{table_schema}.{table_name};")
            table_result = (cur.fetchall())

            column = []
            for tupler in cur.description:
                #get names of columns (first value in tuple)
                column.append(tupler[0])

            create_csv(f"{database_name}/{table_schema}", table_name, table_result, column)
            
    output = f"Files have been saved to: ./{database_name}"
    return output


@app.get("/aws/tables/columns")
def get_tables(port: str, server: str, username: str, password: str, database_name: str):

    conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
    cur = conn.cursor()
    cur.execute("""SELECT TAB.name AS TableName, TAB.object_id AS ObjectID, COL.name AS ColumnName, TYP.name AS DataTypeName, TYP.max_length AS MaxLength 
                From %s.sys.columns COL INNER JOIN %s.sys.tables TAB On COL.object_id = TAB.object_id 
                INNER JOIN %s.sys.types TYP ON TYP.user_type_id = COL.user_type_id;""" % (database_name, database_name, database_name))

    column = []
    results = (cur.fetchall())
    for tupler in cur.description:
        #get names of columns (first value in tuple)
        column.append(tupler[0])
            
    #find file path and dynamically change string
    output = f"File has been saved to: {create_csv(database_name, "column_info", results, column)}" 
    return output

@app.post("/aws/import")
async def import_iris(port: str, server: str, username: str, password: str, database_name: str, file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        try: 
            file_name = file.filename
            table_name = os.path.splitext(file_name)[0]

            contents = await file.read()
            df = pd.read_csv(StringIO(contents.decode('utf-8')))
            df = pd.read_csv(file_name)

            conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
            cursor = conn.cursor()

            # Create table
            create_table_query = f"CREATE TABLE {table_name} ("
            for column_name in df.columns:
                column_name_cleaned = column_name.replace('.', '_')  # Replace dots with underscores
                create_table_query += f"{column_name_cleaned} VARCHAR(255), "
            create_table_query = create_table_query[:-2] + ");"
            cursor.execute(create_table_query)

            # Insert data
            for index, row in df.iterrows():
                placeholders = ",".join(["?"] * len(row))
                columns = ",".join([col.replace('.', '_') for col in row.index])  # Replace dots with underscores
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, tuple(row))
            conn.commit()

            conn.close()

            return {"message": "Data imported successfully."}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        return {"error": "Please upload a CSV file."}

def create_csv(filepath: str, filename: str, data, column):
    # Define the directory path where you want to save the file
    directory_path = f"./{filepath}"

    # Check if the directory exists, and create it if it doesn't
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        
    # Define the full file path
    file_path = f"{directory_path}/{filename}.csv"

    with open(file_path, 'w', encoding="utf-8", newline='') as f_handle:
        writer = csv.writer(f_handle)
        header = column
        writer.writerow(header)
        for row in data:
            writer.writerow(row)

    return file_path

#SELECT schema_name FROM information_schema.schemata;
# ALL SCHEMA TABLES

#SELECT * FROM master.sys.databases
#All databases on server
        
#ToDo: 
# import using column info first
# upload folder (multiple tables)
# List of query strings
# Change name of functions
# output errors (try catch)
# api route that connects directly between the two without a local copy

        
# Create front end that is pretty
# Create targets for integrating (single table download, already have single table upload)