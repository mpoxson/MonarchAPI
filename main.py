import pyodbc, struct
from azure import identity

from fastapi import FastAPI
import pandas as pd

import psycopg2
import sys
import boto3
import os

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

#Need parameters for database name, server
AZURE_SQL_CONNECTIONSTRING=''

@app.get("/azure/tables/{server_name}/{database_name}")
def get_tables(server_name: str, database_name: str):
    output = ""
    column = []
    AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
    with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
        cursor = conn.cursor()
        cursor.execute("select schema_name(t.schema_id) as schema_name, t.name as table_name, t.create_date, t.modify_date from sys.tables t order by schema_name, table_name;")

        
        results = (cursor.fetchall())
        for tupler in cursor.description:
            #get names of columns (first value in tuple)
            column.append(tupler[0])
            
        results.insert(0, column)
        create_csv("table_info", results)

        #find file path and dynamically change string
        output = "File has been saved to Downloads "


    return output

@app.get("/azure/schemas/{server_name}/{database_name}")
def get_tables(server_name: str, database_name: str):
    output = ""
    
    AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
    with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
        cursor = conn.cursor()
        cursor.execute("select schema_name(t.schema_id) as schema_name, t.name as table_name from sys.tables t  order by schema_name, table_name;")

        results = (cursor.fetchall())
        for table in results:
            if table != "table_name":
                print(type(table))
                table_schema = table[0]
                table_name = table[1]
                #select everything from each table, make into csv
                print(f"select * from {table_schema}.{table_name};")             
                cursor.execute(f"select * from {table_schema}.{table_name};")
                table_result = (cursor.fetchall())

                column = []
                for tupler in cursor.description:
                    #get names of columns (first value in tuple)
                    column.append(tupler[0])

                table_result.insert(0, column)

                create_csv(f"{table_schema}_{table_name}", table_result)
            
        output = "File has been saved to Downloads "


    return output


@app.get("/azure/tables/columns")
def get_tables(server_name: str, database_name: str):
    output = ""
    column = []
    AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
    with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT TAB.name AS TableName, TAB.object_id AS ObjectID, COL.name AS ColumnName, TYP.name AS DataTypeName, TYP.max_length AS MaxLength From sys.columns COL INNER JOIN sys.tables TAB On COL.object_id = TAB.object_id INNER JOIN sys.types TYP ON TYP.user_type_id = COL.user_type_id;")

        
        results = (cursor.fetchall())
        for tupler in cursor.description:
            #get names of columns (first value in tuple)
            column.append(tupler[0])
            
        results.insert(0, column)
        create_csv("column_info", results)

        #find file path and dynamically change string
        output = "File has been saved to Downloads "


    return output

def create_csv(filename: str, data):
        df = pd.DataFrame(data)
        df.to_csv(f'{filename}.csv', index=False)


def get_conn(AZURE_SQL_CONNECTIONSTRING : str):
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn

@app.get("/aws")
def get_tables():
    ENDPOINT="monarch.cjgga6i4mae6.us-east-2.rds.amazonaws.com"
    PORT="1433"
    USER="admin"
    REGION="us-east-2"
    DBNAME="monarch"

    #gets the credentials from .aws/credentials
    #session = boto3.Session(profile_name='RDSCreds')
    #client = session.client('rds')

    #token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)

    try:
        #conn = psycopg2.connect(host=ENDPOINT, port=PORT, database=DBNAME, user=USER, password=token, sslrootcert="SSLCERTIFICATE")
        #conn = psycopg2.connect(host=ENDPOINT, port=PORT, user=USER, password=token, sslrootcert="SSLCERTIFICATE")
        conn = pyodbc.connect('DRIVER={SQL Server};PORT=1433;SERVER=monarch.cjgga6i4mae6.us-east-2.rds.amazonaws.com;UID=admin;PWD=Admin1!Aws;')
        cur = conn.cursor()
        cur.execute("""SELECT now()""")
        query_results = cur.fetchall()
        print(query_results)
    except Exception as e:
        print("Database connection failed due to {}".format(e))   
