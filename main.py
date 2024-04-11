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
from typing import List


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

##############################################################################################################################################

@app.post("/migrate/Azure_to_AWS")
async def azure_to_aws(azure_server_name: str, azure_database_name: str, port: str, server: str, username: str, password: str, database_name: str):
    try:
        AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (azure_server_name, azure_database_name)
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
        #     #get azure data
            con2 = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
            cursor2 = con2.cursor()

            cursor = conn.cursor()
            cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")

            results = (cursor.fetchall())
            for table in results:
                table_schema = table[0]
                table_name = table[1]
                column = []
                #select everything from each table, make into csv            
                cursor.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
                            c.max_length 'Max Length', c.precision , c.scale
                            FROM sys.columns c
                            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                            LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            WHERE c.object_id = OBJECT_ID('{table_schema}.{table_name}')""")
                table_result = (cursor.fetchall())   
                # Create table
                create_table_query = f"CREATE TABLE {database_name}.dbo.{table_name} ("
                iterrows = iter(table_result)
                for row in iterrows:
                        #column name (0)
                        column_name_cleaned = tuple(row)[0].replace('.', '_')  # Replace dots with underscores
                        # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale)
                        data_type = data_typer(tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6])
                        # null (2) 
                        nullable = nulls(tuple(row)[2])
                        # primary key (3)
                        #pk = primary(tuple(row)[3])
                        #put together
                        create_table_query += f"{column_name_cleaned} {data_type}{nullable}, "
                create_table_query = create_table_query[:-2] + ");"
                cursor2.execute(create_table_query)
                #print(create_table_query)

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

                    for i in range(len(column)):
                        column[i] = column[i].replace(".", "_")
                    
                    # Insert data
                    for row in table_result:
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join(column)  # Replace dots with underscores
                        sql = f"INSERT INTO {database_name}.dbo.{table_name} ({columns}) VALUES ({placeholders})"
                        cursor2.execute(sql, tuple(row))
                        #print(tuple(row))
        con2.commit()
        return "Migration Complete"
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/migrate/Aws_to_Azure")
async def aws_to_azure(azure_server_name: str, azure_database_name: str, port: str, server: str, username: str, password: str, database_name: str):
    try:
        AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (azure_server_name, azure_database_name)
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
            #get azure data
            con2 = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
            cursor = conn.cursor()

            cursor2 = con2.cursor()
            cursor2.execute(f"""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM {database_name}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")

            results = (cursor2.fetchall())
            for table in results:
                table_schema = table[0]
                table_name = table[1]
                column = []
                #select everything from each table, make into csv            
                cursor2.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
                            c.max_length 'Max Length', c.precision , c.scale
                            FROM {database_name}.sys.columns c
                            INNER JOIN {database_name}.sys.types t ON c.user_type_id = t.user_type_id
                            LEFT OUTER JOIN {database_name}.sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            LEFT OUTER JOIN {database_name}.sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            WHERE c.object_id = OBJECT_ID('{database_name}.{table_schema}.{table_name}')""")
                table_result = (cursor2.fetchall())   
                # Create table
                create_table_query = f"CREATE TABLE {azure_database_name}.dbo.{table_name} ("
                iterrows = iter(table_result)
                for row in iterrows:
                        #column name (0)
                        column_name_cleaned = tuple(row)[0].replace('.', '_')  # Replace dots with underscores
                        # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale)
                        data_type = data_typer(tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6])
                        # null (2) 
                        nullable = nulls(tuple(row)[2])
                        # primary key (3)
                        #pk = primary(tuple(row)[3])
                        #put together
                        create_table_query += f"{column_name_cleaned} {data_type}{nullable}, "
                create_table_query = create_table_query[:-2] + ");"
                cursor.execute(create_table_query)
                #print(create_table_query)

            for table in results:
                if table != "table_name":
                    table_schema = table[0]
                    table_name = table[1]
                    #select everything from each table, make into csv            
                    cursor2.execute(f"select * from {database_name}.{table_schema}.{table_name};")
                    table_result = (cursor2.fetchall())

                    column = []
                    for tupler in cursor2.description:
                        #get names of columns (first value in tuple)
                        column.append(tupler[0])

                    for i in range(len(column)):
                        column[i] = column[i].replace(".", "_")
                    
                    # Insert data
                    for row in table_result:
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join(column)  # Replace dots with underscores
                        sql = f"INSERT INTO {azure_database_name}.dbo.{table_name} ({columns}) VALUES ({placeholders})"
                        cursor.execute(sql, tuple(row))
                    #  print(tuple(row))
        conn.commit()
        return "Migration Complete"
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

###################################################################################################################################################################################

@app.get("/azure/tables/{server_name}/{database_name}")
def azure_get_tables(server_name: str, database_name: str):
    try:
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

            location = create_csv(database_name, "table_info", results, column)

            #find file path and dynamically change string
            output = f"File has been saved to: {location}" 


        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
   
@app.post("/azure/import/multiple")
async def azure_mult_table(server_name: str, database_name: str, files: List[UploadFile] = File(...)):
    try:
        AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
            cursor = conn.cursor()
            for file in files:
                if file.filename.endswith('.csv'):
                        file_name = file.filename
                        #table_name = os.path.splitext(file_name)[0]
                        table_name = file_name.rsplit( ".", 1 )[ 0 ]

                        contents = await file.read()
                        
                        df = pd.read_csv(StringIO(contents.decode('utf-8')))
                        #df = pd.read_csv(file_name)

                        #conn = pyodbc.connect(connection_string_17)

                        # Create table
                        create_table_query = f"CREATE TABLE dbo.{table_name} ("
                        for column_name in df.columns:
                            column_name_cleaned = column_name.replace('.', '_')  # Replace dots with underscores
                            create_table_query += f"{column_name_cleaned} VARCHAR(255), "
                        create_table_query = create_table_query[:-2] + ");"
                        cursor.execute(create_table_query)

                        # Insert data
                        for index, row in df.iterrows():
                            placeholders = ",".join(["?"] * len(row))
                            columns = ",".join([col.replace('.', '_') for col in row.index])  # Replace dots with underscores
                            sql = f"INSERT INTO dbo.{table_name} ({columns}) VALUES ({placeholders})"
                            cursor.execute(sql, tuple(row))
                else:
                    return {"error": "Please upload a CSV file."}
            conn.commit()
            return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/azure/import/schema")
async def azure_import_schema(server_name: str, database_name: str, files: List[UploadFile] = File(...)):
    try:
        AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
            cursor = conn.cursor()
            for file in files:
                if file.filename.endswith('.csv'):
                        file_name = file.filename
                        #table_name = os.path.splitext(file_name)[0]
                        table_name = file_name.rsplit( ".", 1 )[ 0 ]

                        contents = await file.read()
                        
                        df = pd.read_csv(StringIO(contents.decode('utf-8')))
                        #df = pd.read_csv(file_name)

                        #conn = pyodbc.connect(connection_string_17)

                        # Create table
                        create_table_query = f"CREATE TABLE dbo.{table_name} ("
                        for index, row in df.iterrows():
                            #column name (0)
                            column_name_cleaned = tuple(row)[0].replace('.', '_')  # Replace dots with underscores
                            # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale)
                            data_type = data_typer(tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6])
                            # null (2) 
                            nullable = nulls(tuple(row)[2])
                            # primary key (3)
                            #pk = primary(tuple(row)[3])
                            #put together
                            create_table_query += f"{column_name_cleaned} {data_type}{nullable}, "
                        create_table_query = create_table_query[:-2] + ");"
                        cursor.execute(create_table_query)
                else:
                    return {"error": "Please upload a CSV file."}
            conn.commit()
            return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

@app.post("/azure/import/schema/data")
async def azure_schema_data(server_name: str, database_name: str, files: List[UploadFile] = File(...)):
    try:
        AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
            cursor = conn.cursor()
            for file in files:
                if file.filename.endswith('.csv'):
                        file_name = file.filename
                        #table_name = os.path.splitext(file_name)[0]
                        table_name = file_name.rsplit( ".", 1 )[ 0 ]

                        data = []
                        head = 1
                        heads = []
                        contents = await file.read()  # Read the file contents
                        decoded_content = contents.decode('utf-8').splitlines()
                        csv_reader = csv.reader(decoded_content)
                        for row in csv_reader:
                            if (head == 1):
                                heads=row
                                head +=-1
                            else:
                                data.append(row)

                        for i in range(len(heads)):
                            heads[i] = heads[i].replace(".", "_")
                        
                        # Insert data
                        for row in data:
                            placeholders = ",".join(["?"] * len(row))
                            columns = ",".join(heads)  # Replace dots with underscores
                            sql = f"INSERT INTO dbo.{table_name} ({columns}) VALUES ({placeholders})"
                            cursor.execute(sql, tuple(row))
                else:
                    return {"error": "Please upload a CSV file."}
            conn.commit()
            return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
   
    
#Need parameters for database name, server
AZURE_SQL_CONNECTIONSTRING=''




@app.get("/azure/data/{server_name}/{database_name}")
def azure_get_data(server_name: str, database_name: str):
    
    try:
        output = ""
        
        AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
            cursor = conn.cursor()
            cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")

            init_results = (cursor.fetchall())

            for table in init_results:
                table_schema = table[0]
                table_name = table[1]

                cursor.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
                                c.max_length 'Max Length', c.precision , c.scale
                                FROM sys.columns c
                                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                                LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                                LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                                WHERE c.object_id = OBJECT_ID('{table_schema}.{table_name}')""")

                column = []
                results = (cursor.fetchall())
                for tupler in cursor.description:
                    #get names of columns (first value in tuple)
                    column.append(tupler[0])

                create_csv(f"{database_name}/{table_schema}/schema", table_name, results, column)

            for table in init_results:
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

                    create_csv(f"{database_name}/{table_schema}/data", table_name, table_result, column)
                
            output = f"Files have been saved to: ./{database_name}"


        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



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
def aws_get_tables(port: str, server: str, username: str, password: str, database_name: str):
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

        location = create_csv(database_name, "table_info", query_results, column)

        #find file path and dynamically change string
        output = f"File has been saved to: {location}" 
        return output
    
    except Exception as e:
        print("Database connection failed due to {}".format(e))  

@app.get("/aws/data")
def aws_get_data(port: str, server: str, username: str, password: str, database_name: str):
    try:
        output = ""
        
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
        cur = conn.cursor()
        cur.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM %s.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'""" % (database_name))
        init_results = (cur.fetchall())

        for table in init_results:
            table_schema = table[0]
            table_name = table[1]

            cur.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
                            c.max_length 'Max Length', c.precision , c.scale
                            FROM {database_name}.sys.columns c
                            INNER JOIN {database_name}.sys.types t ON c.user_type_id = t.user_type_id
                            LEFT OUTER JOIN {database_name}.sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            LEFT OUTER JOIN {database_name}.sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            WHERE c.object_id = OBJECT_ID('{database_name}.{table_schema}.{table_name}')""")

            column = []
            results = (cur.fetchall())
            for tupler in cur.description:
                #get names of columns (first value in tuple)
                column.append(tupler[0])

            create_csv(f"{database_name}/{table_schema}/schema", table_name, results, column)

        for table in init_results:
            if table != "table_name":
            #  print(type(table))
                table_schema = table[0]
                table_name = table[1]
                #select everything from each table, make into csv       
                cur.execute(f"select * from {database_name}.{table_schema}.{table_name};")
                table_result = (cur.fetchall())

                column = []
                for tupler in cur.description:
                    #get names of columns (first value in tuple)
                    column.append(tupler[0])

                create_csv(f"{database_name}/{table_schema}/data", table_name, table_result, column)
                
        output = f"Files have been saved to: ./{database_name}"
        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



    
@app.post("/aws/import/double")
async def aws_mult_table(port: str, server: str, username: str, password: str, database_name: str, files: List[UploadFile] = File(...)):
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
        cursor = conn.cursor()
        
        for file in files:
            if file.filename.endswith('.csv'):
                    file_name = file.filename
                    table_name = file_name.rsplit( ".", 1 )[ 0 ]

                    contents = await file.read()
                    
                    df = pd.read_csv(StringIO(contents.decode('utf-8')))



                    # Create table
                    create_table_query = f"CREATE TABLE {database_name}.dbo.{table_name} ("
                    for column_name in df.columns:
                        column_name_cleaned = column_name.replace('.', '_')  # Replace dots with underscores
                        create_table_query += f"{column_name_cleaned} VARCHAR(255), "
                    create_table_query = create_table_query[:-2] + ");"
                    cursor.execute(create_table_query)

                    # Insert data
                    for index, row in df.iterrows():
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join([col.replace('.', '_') for col in row.index])  # Replace dots with underscores
                        sql = f"INSERT INTO {database_name}.dbo.{table_name} ({columns}) VALUES ({placeholders})"
                        row_clean = []
                        for el in tuple(row):
                            row_clean.append(f"'{el}'")
                        
                        cursor.execute(sql, row_clean)
            else:
                return {"error": "Please upload a CSV file."}
        conn.commit()
        return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/aws/import/schema")
async def aws_import_schema(port: str, server: str, username: str, password: str, database_name: str, files: List[UploadFile] = File(...)):
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
        cursor = conn.cursor()
        for file in files:
            if file.filename.endswith('.csv'):
                    file_name = file.filename
                    #table_name = os.path.splitext(file_name)[0]
                    table_name = file_name.rsplit( ".", 1 )[ 0 ]

                    contents = await file.read()
                    
                    df = pd.read_csv(StringIO(contents.decode('utf-8')))
                    #df = pd.read_csv(file_name)

                    #conn = pyodbc.connect(connection_string_17)

                    # Create table
                    create_table_query = f"CREATE TABLE {database_name}.dbo.{table_name} ("
                    for index, row in df.iterrows():
                        #column name (0)
                        column_name_cleaned = tuple(row)[0].replace('.', '_')  # Replace dots with underscores
                        # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale)
                        data_type = data_typer(tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6])
                        # null (2) 
                        nullable = nulls(tuple(row)[2])
                        # primary key (3)
                        #pk = primary(tuple(row)[3])
                        #put together
                        create_table_query += f"{column_name_cleaned} {data_type}{nullable}, "
                    create_table_query = create_table_query[:-2] + ");"
                    cursor.execute(create_table_query)
            else:
                return {"error": "Please upload a CSV file."}
        conn.commit()
        return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/aws/import/schema/data")
async def aws_mult_table(port: str, server: str, username: str, password: str, database_name: str, files: List[UploadFile] = File(...)):
    try:
        conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
        cursor = conn.cursor()
        
        for file in files:
            if file.filename.endswith('.csv'):
                file_name = file.filename
                table_name = file_name.rsplit( ".", 1 )[ 0 ]

            
                data = []
                head = 1
                heads = []
                contents = await file.read()  # Read the file contents
                decoded_content = contents.decode('utf-8').splitlines()
                csv_reader = csv.reader(decoded_content)
                for row in csv_reader:
                    if (head == 1):
                        heads=row
                        head +=-1
                    else:
                        data.append(row)

                for i in range(len(heads)):
                    heads[i] = heads[i].replace(".", "_")
                
                # Insert data
                for row in data:
                    placeholders = ",".join(["?"] * len(row))
                    columns = ",".join(heads)  # Replace dots with underscores
                    sql = f"INSERT INTO {database_name}.dbo.{table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, tuple(row))
            else:
                return {"error": "Please upload a CSV file."}
        conn.commit()
        return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

###############################################################################################################################

def create_csv(filepath: str, filename: str, data, column):
    # Define the directory path where you want to save the file
    directory_path = f"./{filepath}"

    # Check if the directory exists, and create it if it doesn't
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        
    # Define the full file path
    file_path = f"{directory_path}/{filename}.csv"

    clean_data = [convert_blank_to_none(row) for row in data]

    with open(file_path, 'w', encoding="utf-8", newline='') as f_handle:
        writer = csv.writer(f_handle)
        header = column
        writer.writerow(header)
        for row in clean_data:
            writer.writerow(row)

    return file_path

def convert_blank_to_none(row):
    return [value if value != "" else None for value in row]

def data_typer(type: str, length, precision, scale):
    data_type = ""
    length = int(length)
    match type.lower():
        #Strings
        case "char":
            data_type= f"CHAR({divide_two(length)})"
        case "varchar":
            data_type= f"VARCHAR({divide_two(length)})"
        case "text":
            data_type= "TEXT"
        case "nchar":
            data_type= f"NCHAR({divide_two(length)})"
        case "nvarchar":
            data_type= f"NVARCHAR({divide_two(length)})"
        case "ntext":
            data_type= "NTEXT"
        case "binary":
            data_type= f"BINARY({length})"
        case "varbinary":
            data_type= f"VARBINARY({divide_two(length)})"
        case "image":
            data_type= "IMAGE"
        #numeric
        case "bit":
            data_type= "BIT"
        case "tinyint":
            data_type= "TINYINT"
        case "smallint":
            data_type= "SMALLINT"
        case "int":
            data_type= "INT"
        case "bigint":
            data_type= "BIGINT"
        case "decimal":
            data_type= f"DECIMAL({precision},{scale})"
        case "numeric":
            data_type= f"NUMERIC({precision},{scale})"
        case "smallmoney":
            data_type= "SMALLMONEY"
        case "money":
            data_type= "MONEY"
        case "float":
            data_type= f"FLOAT({length})"
        case "real":
            data_type= "REAL"
        #date and time
        case "datetime":
            data_type= "DATETIME"
        case "datetime2":
            data_type= "DATETIME2"
        case "smalldatetime":
            data_type= "SMALLDATETIME"
        case "date":
            data_type= "DATE"
        case "time":
            data_type= "TIME"
        case "datetimeoffset":
            data_type= "DATETIMEOFFSET"
        case "timestamp":
            data_type= "TIMESTAMP"
        #OTHER
        case "sql_variant":
            data_type= "sql_variant"
        case "uniqueidentifier":
            data_type= "uniqueidentifier"
        case "xml":
            data_type= "xml"
        case "cursor":
            data_type= "cursor"
        case "table":
            data_type= "table"
        case _:
            data_type= "NVARCHAR(MAX)"

    return data_type

def divide_two(len: int):
    length = int(len)
    if (length>255 or length <= 0):
        length = 'max'
    return f"{length}"

def maxxing(len: int):
    if (len/2 > 4000):
        return '(max)'
    return ""

def nulls(bools):
    if (bools == False):
        return " NOT NULL"
    else:
        return ""
    
def primary(pk):
    if (pk == True):
        return " PRIMARY KEY"
    else:
        return ""

#SELECT schema_name FROM information_schema.schemata;
# ALL SCHEMA TABLES

#SELECT * FROM master.sys.databases
#All databases on server
        
#ToDo: 
# output errors (try catch)
# ask for aws schemas

        
# Create front end that is pretty
# Create targets for integrating (single table download, already have single table upload)

##########################################################################################################

# @app.post("/azure/import/single")
# async def azure_single_table(server_name: str, database_name: str, file: UploadFile = File(...)):
#     if file.filename.endswith('.csv'):
#         try: 
#             file_name = file.filename
#             #table_name = os.path.splitext(file_name)[0]
#             table_name = file_name.rsplit( ".", 1 )[ 0 ]

#             contents = await file.read()
            
#             df = pd.read_csv(StringIO(contents.decode('utf-8')))
#             #df = pd.read_csv(file_name)

#             #conn = pyodbc.connect(connection_string_17)
#             AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
#             with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
#                 cursor = conn.cursor()

#                 # Create table
#                 create_table_query = f"CREATE TABLE dbo.{table_name} ("
#                 for column_name in df.columns:
#                     column_name_cleaned = column_name.replace('.', '_')  # Replace dots with underscores
#                     create_table_query += f"{column_name_cleaned} VARCHAR(255), "
#                 create_table_query = create_table_query[:-2] + ");"
#                 cursor.execute(create_table_query)

#                 # Insert data
#                 for index, row in df.iterrows():
#                     placeholders = ",".join(["?"] * len(row))
#                     columns = ",".join([col.replace('.', '_') for col in row.index])  # Replace dots with underscores
#                     sql = f"INSERT INTO dbo.{table_name} ({columns}) VALUES ({placeholders})"
#                     cursor.execute(sql, tuple(row))
#                 conn.commit()

#             return {"message": "Data imported successfully."}
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))
#     else:
#         return {"error": "Please upload a CSV file."}

# @app.get("/azure/tables/{server_name}/{database_name}")
# def azure_get_tables(server_name: str, database_name: str):
#     output = ""
#     column = []
#     AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
#     with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
#         cursor = conn.cursor()
#         cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
#             FROM INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")

        
#         results = (cursor.fetchall())
#         for tupler in cursor.description:
#             #get names of columns (first value in tuple)
#             column.append(tupler[0])

#         location = create_csv(database_name, "table_info", results, column)

#         #find file path and dynamically change string
#         output = f"File has been saved to: {location}" 


#     return output

# @app.get("/azure/tables/columns")
# def azure_get_columns(server_name: str, database_name: str):
#     output = ""
#     column = []
#     AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
#     with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
#         cursor = conn.cursor()
#         cursor.execute("""SELECT TAB.name AS TableName, COL.name AS ColumnName, TYP.name AS DataTypeName, TYP.max_length AS MaxLength 
#                        From sys.columns COL INNER JOIN sys.tables TAB On COL.object_id = TAB.object_id 
#                        INNER JOIN sys.types TYP ON TYP.user_type_id = COL.user_type_id 
#                        WHERE TAB.name NOT LIKE 'BuildVersion' AND TAB.name NOT LIKE 'ErrorLog';""")

        
#         results = (cursor.fetchall())
#         for tupler in cursor.description:
#             #get names of columns (first value in tuple)
#             column.append(tupler[0])

#         location = create_csv(database_name, "column_info", results, column)
#         #find file path and dynamically change string
#         output = f"File has been saved to: {location}" 


#     return output

# @app.get("/aws/tables/columns")
# def aws_get_columns(port: str, server: str, username: str, password: str, database_name: str):

#     conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
#     cur = conn.cursor()
#     cur.execute("""SELECT TAB.name AS TableName, TAB.object_id AS ObjectID, COL.name AS ColumnName, TYP.name AS DataTypeName, TYP.max_length AS MaxLength 
#                 From %s.sys.columns COL INNER JOIN %s.sys.tables TAB On COL.object_id = TAB.object_id 
#                 INNER JOIN %s.sys.types TYP ON TYP.user_type_id = COL.user_type_id;""" % (database_name, database_name, database_name))

#     column = []
#     results = (cur.fetchall())
#     for tupler in cur.description:
#         #get names of columns (first value in tuple)
#         column.append(tupler[0])

#     location = create_csv(database_name, "column_info", results, column)
            
#     #find file path and dynamically change string
#     output = f"File has been saved to: {location}" 
#     return output

# @app.post("/aws/import/single")
# async def aws_single_table(port: str, server: str, username: str, password: str, database_name: str, file: UploadFile = File(...)):
#     if file.filename.endswith('.csv'):
#         try: 
#             file_name = file.filename
#             table_name = file_name.rsplit( ".", 1 )[ 0 ]

#             contents = await file.read()
            
#             df = pd.read_csv(StringIO(contents.decode('utf-8')))

#             conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
#             cursor = conn.cursor()

#             # Create table
#             create_table_query = f"CREATE TABLE {database_name}.dbo.{table_name} ("
#             for column_name in df.columns:
#                 column_name_cleaned = column_name.replace('.', '_')  # Replace dots with underscores
#                 create_table_query += f"{column_name_cleaned} VARCHAR(255), "
#             create_table_query = create_table_query[:-2] + ");"
#             cursor.execute(create_table_query)

#             # Insert data
#             for index, row in df.iterrows():
#                 placeholders = ",".join(["?"] * len(row))
#                 columns = ",".join([col.replace('.', '_') for col in row.index])  # Replace dots with underscores
#                 sql = f"INSERT INTO {database_name}.dbo.{table_name} ({columns}) VALUES ({placeholders})"
#                 row_clean = []
#                 for el in tuple(row):
#                     row_clean.append(f"'{el}'")
                
#                 cursor.execute(sql, row_clean)
#             conn.commit()

#             return {"message": "Data imported successfully."}
#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))
#     else:
#         return {"error": "Please upload a CSV file."}

# @app.get("/aws/tables")
# def aws_get_tables(port: str, server: str, username: str, password: str, database_name: str):
#     #PORT=1433;SERVER=monarch.cjgga6i4mae6.us-east-2.rds.amazonaws.com;UID=;PWD=;db=monarchdb
#     try:
#         conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
#         cur = conn.cursor()
#         cur.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
#             FROM %s.INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_TYPE = 'BASE TABLE'""" % (database_name))
#         query_results = cur.fetchall()
#         column = []
#         for tupler in cur.description:
#             #get names of columns (first value in tuple)
#             column.append(tupler[0])

#         location = create_csv(database_name, "table_info", query_results, column)

#         #find file path and dynamically change string
#         output = f"File has been saved to: {location}" 
#         return output
    
#     except Exception as e:
#         print("Database connection failed due to {}".format(e))  
# @app.get("/aws/schema/columns")
# def aws_get_columns(port: str, server: str, username: str, password: str, database_name: str):

#     conn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30' % (port, server, username, password))
#     cursor = conn.cursor()

#     cursor.execute(f"""SELECT TABLE_SCHEMA, TABLE_NAME
#         FROM {database_name}.INFORMATION_SCHEMA.TABLES
#         WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")
    
#     results = (cursor.fetchall())
#     for table in results:
#         table_schema = table[0]
#         table_name = table[1]

#         cursor.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
#                         c.max_length 'Max Length', c.precision , c.scale
#                         FROM {database_name}.sys.columns c
#                         INNER JOIN {database_name}.sys.types t ON c.user_type_id = t.user_type_id
#                         LEFT OUTER JOIN {database_name}.sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
#                         LEFT OUTER JOIN {database_name}.sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
#                         WHERE c.object_id = OBJECT_ID('{database_name}.{table_schema}.{table_name}')""")

#         column = []
#         results = (cursor.fetchall())
#         for tupler in cursor.description:
#             #get names of columns (first value in tuple)
#             column.append(tupler[0])

#         location = create_csv(f"{database_name}/{table_schema}", table_name, results, column)
#         #find file path and dynamically change string
#     output = f"File has been saved to: ./{database_name}" 


#     return output

# @app.get("/azure/schema/columns")
# def azure_get_columns(server_name: str, database_name: str):
#     output = ""
#     AZURE_SQL_CONNECTIONSTRING = "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30" % (server_name, database_name)
#     with get_conn(AZURE_SQL_CONNECTIONSTRING) as conn:
#         cursor = conn.cursor()

#         cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
#             FROM INFORMATION_SCHEMA.TABLES
#             WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")
        
#         results = (cursor.fetchall())
#         for table in results:
#             table_schema = table[0]
#             table_name = table[1]

#             cursor.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
#                             c.max_length 'Max Length', c.precision , c.scale
#                             FROM sys.columns c
#                             INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
#                             LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
#                             LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
#                             WHERE c.object_id = OBJECT_ID('{table_schema}.{table_name}')""")

#             column = []
#             results = (cursor.fetchall())
#             for tupler in cursor.description:
#                 #get names of columns (first value in tuple)
#                 column.append(tupler[0])

#             location = create_csv(f"{database_name}/{table_schema}", table_name, results, column)
#             #find file path and dynamically change string
#         output = f"File has been saved to: ./{database_name}" 


#     return output