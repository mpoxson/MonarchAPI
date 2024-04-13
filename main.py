import csv
import os
import struct
from io import StringIO
from typing import List

import pandas as pd
import pyodbc
from azure import identity
from fastapi import FastAPI, File, HTTPException, UploadFile

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


##############################################################################################################################################


@app.post("/Migrate/Azure_To_AWS")
async def azure_to_aws(
    azure_server_name: str,
    azure_database_name: str,
    aws_port: str,
    aws_server: str,
    aws_username: str,
    aws_password: str,
    aws_database_name: str,
):
    try:
        # connect to aws
        aws_con = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30"
            % (aws_port, aws_server, aws_username, aws_password)
        )
        aws_cursor = aws_con.cursor()
        # connect to azure
        AZURE_SQL_CONNECTIONSTRING = (
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            % (azure_server_name, azure_database_name)
        )
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as azure_con:
            azure_cursor = azure_con.cursor()
            # Get all tables created by the user
            azure_cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")
            results = azure_cursor.fetchall()

            # list for each schema in the new database
            schema_list = ["dbo"]

            # for each table in the database
            for table in results:
                # get table name and table's schema
                table_schema = table[0]
                table_name = table[1]

                # list of columns in the table
                column = []

                # Query for column information for given table
                azure_cursor.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
                            c.max_length 'Max Length', c.precision , c.scale
                            FROM sys.columns c
                            INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                            LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            WHERE c.object_id = OBJECT_ID('{table_schema}.{table_name}')""")
                table_result = azure_cursor.fetchall()

                # Create Schema in aws. If schema already created it is skipped, otherwise the schema is created and the the schema name is added to the list
                if table_schema not in schema_list:
                    aws_cursor.execute(f"USE {aws_database_name};")
                    create_schema = f"CREATE SCHEMA {table_schema};"
                    aws_cursor.execute(create_schema)
                    schema_list.append(table_schema)

                # Create table in new database in correct schema
                create_table_query = (
                    f"CREATE TABLE {aws_database_name}.{table_schema}.{table_name} ("
                )

                # iterate through all the rows. Each row has the column information for a column in the table
                iterrows = iter(table_result)
                for row in iterrows:
                    # column name (0), Replace dots with underscores
                    column_name_cleaned = tuple(row)[0].replace(".", "_")

                    # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale). Sends information and returns DATATYPE(N) or DATATYPE(P,S)
                    data_type = data_typer(
                        tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6]
                    )

                    # null (2). Sends boolean for null, returns NOT NULL or NULL
                    nullable = nulls(tuple(row)[2])

                    # primary key (3)
                    # pk = primary(tuple(row)[3])

                    # Add column of table to create table query
                    create_table_query += (
                        f"{column_name_cleaned} {data_type}{nullable}, "
                    )
                # Create table execution
                create_table_query = create_table_query[:-2] + ");"
                aws_cursor.execute(create_table_query)

            # Grabs data from each table in the database
            for table in results:
                print("Entering table")
                if table != "table_name":
                    table_schema = table[0]
                    table_name = table[1]

                    # Select everything from the table
                    azure_cursor.execute(f"select * from {table_schema}.{table_name};")
                    table_result = azure_cursor.fetchall()

                    column = []
                    for tupler in azure_cursor.description:
                        # get names of columns (first value in tuple)
                        column.append(tupler[0])

                    # clean columns
                    for i in range(len(column)):
                        column[i] = column[i].replace(".", "_")

                    # Insert data
                    for row in table_result:
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join(column)
                        sql = f"INSERT INTO {aws_database_name}.{table_schema}.{table_name} ({columns}) VALUES ({placeholders})"
                        aws_cursor.execute(sql, tuple(row))
        # Make sure changes save. If error, changes won't be reflected in the database
        aws_con.commit()
        aws_cursor.close()
        aws_con.close()
        return "Migration Complete"
    except Exception as e:
        aws_cursor.close()
        aws_con.close()
        raise HTTPException(status_code=500, detail=str(e))


# For comments, refer to above. Same process different direction.
@app.post("/Migrate/AWS_To_Azure")
async def aws_to_azure(
    azure_server_name: str,
    azure_database_name: str,
    aws_port: str,
    aws_server: str,
    aws_username: str,
    aws_password: str,
    aws_database_name: str,
):
    try:
        aws_con = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30"
            % (aws_port, aws_server, aws_username, aws_password)
        )
        aws_cursor = aws_con.cursor()
        AZURE_SQL_CONNECTIONSTRING = (
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            % (azure_server_name, azure_database_name)
        )
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as azure_con:
            azure_cursor = azure_con.cursor()

            aws_cursor.execute(f"""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM {aws_database_name}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")

            results = aws_cursor.fetchall()
            for table in results:
                table_schema = table[0]
                table_name = table[1]
                column = []
                schema_list = ["dbo"]
                # AWS connection is top level "master database", must specify database when doing anything
                aws_cursor.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
                            c.max_length 'Max Length', c.precision , c.scale
                            FROM {aws_database_name}.sys.columns c
                            INNER JOIN {aws_database_name}.sys.types t ON c.user_type_id = t.user_type_id
                            LEFT OUTER JOIN {aws_database_name}.sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                            LEFT OUTER JOIN {aws_database_name}.sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                            WHERE c.object_id = OBJECT_ID('{aws_database_name}.{table_schema}.{table_name}')""")
                table_result = aws_cursor.fetchall()
                if table_schema not in schema_list:
                    azure_cursor.execute(f"USE {azure_database_name};")
                    create_schema = f"CREATE SCHEMA {table_schema};"
                    azure_cursor.execute(create_schema)
                    schema_list.append(table_schema)
                create_table_query = (
                    f"CREATE TABLE {azure_database_name}.{table_schema}.{table_name} ("
                )
                iterrows = iter(table_result)
                for row in iterrows:
                    # column name (0)
                    column_name_cleaned = tuple(row)[0].replace(".", "_")
                    # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale)
                    data_type = data_typer(
                        tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6]
                    )
                    # null (2)
                    nullable = nulls(tuple(row)[2])
                    # primary key (3)
                    # pk = primary(tuple(row)[3])
                    # put together
                    create_table_query += (
                        f"{column_name_cleaned} {data_type}{nullable}, "
                    )
                create_table_query = create_table_query[:-2] + ");"
                azure_cursor.execute(create_table_query)
            for table in results:
                print("Entering table")
                if table != "table_name":
                    table_schema = table[0]
                    table_name = table[1]

                    aws_cursor.execute(
                        f"select * from {aws_database_name}.{table_schema}.{table_name};"
                    )
                    table_result = aws_cursor.fetchall()

                    column = []
                    for tupler in aws_cursor.description:
                        column.append(tupler[0])

                    for i in range(len(column)):
                        column[i] = column[i].replace(".", "_")

                    for row in table_result:
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join(column)
                        sql = f"INSERT INTO {azure_database_name}.{table_schema}.{table_name} ({columns}) VALUES ({placeholders})"
                        azure_cursor.execute(sql, tuple(row))
        azure_con.commit()
        aws_cursor.close()
        aws_con.close()
        return "Migration Complete"
    except Exception as e:
        aws_cursor.close()
        aws_con.close()
        raise HTTPException(status_code=500, detail=str(e))


###################################################################################################################################################################################


@app.get("/Azure/Export/Structure")
def azure_get_tables(server_name: str, database_name: str):
    try:
        output = ""
        column = []
        # Connect to Azure
        AZURE_SQL_CONNECTIONSTRING = (
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            % (server_name, database_name)
        )
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as azure_con:
            # Get all schemas and their tables
            cursor = azure_con.cursor()
            cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")
            results = cursor.fetchall()

            for tupler in cursor.description:
                # get names of columns (first value in tuple)
                column.append(tupler[0])

            # Create a csv and save it in a folder with the same name as the database
            location = create_csv(database_name, "table_info", results, column)

            # find file path and dynamically change string
            output = f"File has been saved to: {location}"

        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/Azure/Export/Data")
def azure_get_data(server_name: str, database_name: str):
    try:
        output = ""
        # connect to azure
        AZURE_SQL_CONNECTIONSTRING = (
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            % (server_name, database_name)
        )
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as azure_con:
            # Get all user tables and their schema
            cursor = azure_con.cursor()
            cursor.execute("""SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME NOT LIKE 'BuildVersion' AND TABLE_NAME NOT LIKE 'ErrorLog'""")
            init_results = cursor.fetchall()

            # For each table in the database
            for table in init_results:
                # grab table name and schema name
                table_schema = table[0]
                table_name = table[1]

                # Get all column information from table
                cursor.execute(f"""SELECT DISTINCT c.name 'Column Name', t.Name 'Data type', c.is_nullable, ISNULL(i.is_primary_key, 0) 'Primary Key', 
                                c.max_length 'Max Length', c.precision , c.scale
                                FROM sys.columns c
                                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                                LEFT OUTER JOIN sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                                LEFT OUTER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                                WHERE c.object_id = OBJECT_ID('{table_schema}.{table_name}')""")
                column = []
                results = cursor.fetchall()

                for tupler in cursor.description:
                    # get names of columns (first value in tuple)
                    column.append(tupler[0])

                # create a csv with all the column information for that table in the schema. Table name is saved as file name, a new folder is created for the schema files for that database
                create_csv(
                    f"{database_name}/{table_schema}/schema",
                    table_name,
                    results,
                    column,
                )

            # loops through tables again
            for table in init_results:
                if table != "table_name":
                    table_schema = table[0]
                    table_name = table[1]

                    # select everything from each table, make into csv
                    cursor.execute(f"select * from {table_schema}.{table_name};")
                    table_result = cursor.fetchall()

                    column = []
                    for tupler in cursor.description:
                        # get names of columns (first value in tuple)
                        column.append(tupler[0])

                    # Create csv for all the data in that table, saved in data folder
                    create_csv(
                        f"{database_name}/{table_schema}/data",
                        table_name,
                        table_result,
                        column,
                    )

            output = f"Files have been saved to: ./{database_name}"
        return output
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/Azure/Import/Schema")
async def azure_import_schema(
    server_name: str,
    database_name: str,
    new_schema_name: str,
    schema_files: List[UploadFile] = File(...),
    data_files: List[UploadFile] = File(...),
):
    try:
        # connect to azure
        AZURE_SQL_CONNECTIONSTRING = (
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            % (server_name, database_name)
        )
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as azure_con:
            cursor = azure_con.cursor()
            # creates new schema, if schema is dbo (which is automatically created), it skips schema creation
            if new_schema_name.strip().lower() != "dbo":
                cursor.execute(f"USE {database_name};")
                create_schema = f"CREATE SCHEMA {new_schema_name};"
                cursor.execute(create_schema)

            # loops through schema files
            for file in schema_files:
                # checks to make sure file is a csv
                if file.filename.endswith(".csv"):
                    # grabs file name, file name will become table name
                    file_name = file.filename
                    table_name = file_name.rsplit(".", 1)[0]

                    # read file and turn it into a datafram
                    contents = await file.read()
                    df = pd.read_csv(StringIO(contents.decode("utf-8")))

                    # Create table
                    create_table_query = (
                        f"CREATE TABLE {new_schema_name}.{table_name} ("
                    )

                    # loop through rows of csv, each row is a column in the new table
                    for index, row in df.iterrows():
                        # column name (0), Replace dots with underscores
                        column_name_cleaned = tuple(row)[0].replace(".", "_")
                        # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale), returns DATATYPE(N) or DATATYPE(P,S)
                        data_type = data_typer(
                            tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6]
                        )
                        # null (2), returns NULL or NOT NULL
                        nullable = nulls(tuple(row)[2])
                        # primary key (3)
                        # pk = primary(tuple(row)[3])
                        # put together parts of create row
                        create_table_query += (
                            f"{column_name_cleaned} {data_type}{nullable}, "
                        )
                    # Create table
                    create_table_query = create_table_query[:-2] + ");"
                    cursor.execute(create_table_query)
                else:
                    return {"error": "Please upload a CSV file."}

            # Loops through data files
            for file in data_files:
                if file.filename.endswith(".csv"):
                    file_name = file.filename
                    table_name = file_name.rsplit(".", 1)[0]

                    data = []
                    head = 1
                    heads = []

                    contents = await file.read()

                    # reads content as an iterable
                    decoded_content = contents.decode("utf-8").splitlines()
                    csv_reader = csv.reader(decoded_content)

                    # first row in the csv contains the headers, which are saved
                    for row in csv_reader:
                        if head == 1:
                            heads = row
                            head += -1
                        else:
                            # The rest of the rows are saved in an array, blanks are converted to None
                            processed_row = [
                                None if cell.strip() == "" else cell for cell in row
                            ]
                            data.append(processed_row)

                    # Clean column names as necessary
                    for i in range(len(heads)):
                        heads[i] = heads[i].replace(".", "_")

                    # Insert data
                    for row in data:
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join(heads)
                        sql = f"INSERT INTO {new_schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
                        cursor.execute(sql, tuple(row))
                else:
                    return {"error": "Please upload a CSV file."}
            # Save changes
            azure_con.commit()
        return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# See above for comments
@app.post("/Azure/Import/General")
async def azure_mult_table(
    server_name: str,
    database_name: str,
    new_schema_name: str,
    files: List[UploadFile] = File(...),
):
    try:
        AZURE_SQL_CONNECTIONSTRING = (
            "Driver={ODBC Driver 18 for SQL Server};Server=tcp:%s.database.windows.net,1433;Database=%s;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30"
            % (server_name, database_name)
        )
        with get_conn(AZURE_SQL_CONNECTIONSTRING) as azure_con:
            cursor = azure_con.cursor()
            if new_schema_name.strip().lower() != "dbo":
                cursor.execute(f"USE {database_name};")
                create_schema = f"CREATE SCHEMA {new_schema_name};"
                cursor.execute(create_schema)
            for file in files:
                if file.filename.endswith(".csv"):
                    file_name = file.filename
                    table_name = file_name.rsplit(".", 1)[0]

                    contents = await file.read()

                    df = pd.read_csv(StringIO(contents.decode("utf-8")))

                    create_table_query = (
                        f"CREATE TABLE {new_schema_name}.{table_name} ("
                    )
                    # Each column created is just NVARCHAR(MAX) since this is a general import
                    for column_name in df.columns:
                        column_name_cleaned = column_name.replace(
                            ".", "_"
                        )  # Replace dots with underscores
                        create_table_query += f"{column_name_cleaned} NVARCHAR(MAX), "
                    create_table_query = create_table_query[:-2] + ");"
                    cursor.execute(create_table_query)
                    # Insert data
                    for index, row in df.iterrows():
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join(
                            [col.replace(".", "_") for col in row.index]
                        )  # Replace dots with underscores
                        sql = f"INSERT INTO {new_schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
                        row_clean = []
                        # floats need to be changed to strings
                        for el in tuple(row):
                            if type(el) is float:
                                row_clean.append(f"'{el}'")
                            else:
                                row_clean.append(el)
                        cursor.execute(sql, row_clean)
                else:
                    return {"error": "Please upload a CSV file."}

            azure_con.commit()
        return {"message": "Data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Creates connection to azure
def get_conn(AZURE_SQL_CONNECTIONSTRING: str):
    # Code comes directly from microsoft, check references
    credential = identity.DefaultAzureCredential(
        exclude_interactive_browser_credential=False
    )
    token_bytes = credential.get_token(
        "https://database.windows.net/.default"
    ).token.encode("UTF-16-LE")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = (
        1256  # This connection option is defined by microsoft in msodbcsql.h
    )
    conn = pyodbc.connect(
        AZURE_SQL_CONNECTIONSTRING,
        attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    )
    return conn


################################################################################################


# For comments, refer to azure
@app.get("/AWS/Export/Structure")
def aws_get_tables(
    port: str, server: str, username: str, password: str, database_name: str
):
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30"
            % (port, server, username, password)
        )
        cur = conn.cursor()
        cur.execute(
            """SELECT TABLE_SCHEMA, TABLE_NAME
            FROM %s.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'"""
            % (database_name)
        )
        query_results = cur.fetchall()
        column = []
        for tupler in cur.description:
            # get names of columns (first value in tuple)
            column.append(tupler[0])

        location = create_csv(database_name, "table_info", query_results, column)

        # find file path and dynamically change string
        output = f"File has been saved to: {location}"
        cur.close()
        conn.close()
        return output

    except Exception as e:
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# For comments, refer to azure
@app.get("/AWS/Export/Data")
def aws_get_data(
    port: str, server: str, username: str, password: str, database_name: str
):
    try:
        output = ""

        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30"
            % (port, server, username, password)
        )
        cur = conn.cursor()
        cur.execute(
            """SELECT TABLE_SCHEMA, TABLE_NAME
                FROM %s.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'"""
            % (database_name)
        )
        init_results = cur.fetchall()

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
            results = cur.fetchall()
            for tupler in cur.description:
                # get names of columns (first value in tuple)
                column.append(tupler[0])

            create_csv(
                f"{database_name}/{table_schema}/schema", table_name, results, column
            )

        for table in init_results:
            if table != "table_name":
                table_schema = table[0]
                table_name = table[1]
                # select everything from each table, make into csv
                cur.execute(
                    f"select * from {database_name}.{table_schema}.{table_name};"
                )
                table_result = cur.fetchall()

                column = []
                for tupler in cur.description:
                    # get names of columns (first value in tuple)
                    column.append(tupler[0])

                create_csv(
                    f"{database_name}/{table_schema}/data",
                    table_name,
                    table_result,
                    column,
                )

        output = f"Files have been saved to: ./{database_name}"
        cur.close()
        conn.close()
        return output
    except Exception as e:
        cur.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# For comments, refer to azure
@app.post("/AWS/Import/Schema")
async def aws_import_schema(
    port: str,
    server: str,
    username: str,
    password: str,
    database_name: str,
    new_schema_name: str,
    schema_files: List[UploadFile] = File(...),
    data_files: List[UploadFile] = File(...),
):
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30"
            % (port, server, username, password)
        )
        cursor = conn.cursor()
        if new_schema_name.strip().lower() != "dbo":
            cursor.execute(f"USE {database_name};")
            create_schema = f"CREATE SCHEMA {new_schema_name};"
            cursor.execute(create_schema)
        for file in schema_files:
            if file.filename.endswith(".csv"):
                file_name = file.filename
                table_name = file_name.rsplit(".", 1)[0]

                contents = await file.read()
                df = pd.read_csv(StringIO(contents.decode("utf-8")))

                # Create table
                create_table_query = (
                    f"CREATE TABLE {database_name}.{new_schema_name}.{table_name} ("
                )
                for index, row in df.iterrows():
                    # column name (0)
                    column_name_cleaned = tuple(row)[0].replace(
                        ".", "_"
                    )  # Replace dots with underscores
                    # 1 (data type) 4 (max lenth) 5 (precision) 6 (scale)
                    data_type = data_typer(
                        tuple(row)[1], tuple(row)[4], tuple(row)[5], tuple(row)[6]
                    )
                    # null (2)
                    nullable = nulls(tuple(row)[2])
                    # primary key (3)
                    # pk = primary(tuple(row)[3])
                    # put together
                    create_table_query += (
                        f"{column_name_cleaned} {data_type}{nullable}, "
                    )
                create_table_query = create_table_query[:-2] + ");"
                cursor.execute(create_table_query)
            else:
                cursor.close()
                conn.close()
                return {"error": "Please upload a CSV file."}
        for file in data_files:
            if file.filename.endswith(".csv"):
                file_name = file.filename
                table_name = file_name.rsplit(".", 1)[0]

                data = []
                head = 1
                heads = []
                contents = await file.read()  # Read the file contents
                decoded_content = contents.decode("utf-8").splitlines()
                csv_reader = csv.reader(decoded_content)
                for row in csv_reader:
                    if head == 1:
                        heads = row
                        head += -1
                    else:
                        processed_row = [
                            None if cell.strip() == "" else cell for cell in row
                        ]
                        data.append(processed_row)

                for i in range(len(heads)):
                    heads[i] = heads[i].replace(".", "_")

                # Insert data
                for row in data:
                    placeholders = ",".join(["?"] * len(row))
                    columns = ",".join(heads)  # Replace dots with underscores
                    sql = f"INSERT INTO {database_name}.{new_schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, tuple(row))
            else:
                cursor.close()
                conn.close()
                return {"error": "Please upload a CSV file."}
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Data imported successfully."}
    except Exception as e:
        cursor.close()
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))


# For comments, refer to azure
@app.post("/AWS/Import/General")
async def aws_mult_table(
    port: str,
    server: str,
    username: str,
    password: str,
    database_name: str,
    new_schema_name: str,
    files: List[UploadFile] = File(...),
):
    try:
        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 18 for SQL Server};PORT=%s;SERVER=%s;UID=%s;PWD=%s;Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30"
            % (port, server, username, password)
        )
        cursor = conn.cursor()
        if new_schema_name.strip().lower() != "dbo":
            cursor.execute(f"USE {database_name};")
            create_schema = f"CREATE SCHEMA {new_schema_name};"
            cursor.execute(create_schema)
        for file in files:
            if file.filename.endswith(".csv"):
                file_name = file.filename
                table_name = file_name.rsplit(".", 1)[0]

                contents = await file.read()

                df = pd.read_csv(StringIO(contents.decode("utf-8")))

                # Create table
                create_table_query = (
                    f"CREATE TABLE {database_name}.{new_schema_name}.{table_name} ("
                )
                for column_name in df.columns:
                    column_name_cleaned = column_name.replace(
                        ".", "_"
                    )  # Replace dots with underscores
                    create_table_query += f"{column_name_cleaned} NVARCHAR(max), "
                create_table_query = create_table_query[:-2] + ");"
                cursor.execute(create_table_query)

                # Insert data
                for index, row in df.iterrows():
                    placeholders = ",".join(["?"] * len(row))
                    columns = ",".join(
                        [col.replace(".", "_") for col in row.index]
                    )  # Replace dots with underscores
                    sql = f"INSERT INTO {database_name}.{new_schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
                    row_clean = []
                    for el in tuple(row):
                        if type(el) is float:
                            row_clean.append(f"'{el}'")
                        else:
                            row_clean.append(el)

                    cursor.execute(sql, row_clean)
            else:
                cursor.close()
                conn.close()
                return {"error": "Please upload a CSV file."}
        conn.commit()
        cursor.close()
        conn.close()
        return {"message": "Data imported successfully."}
    except Exception as e:
        cursor.close()
        conn.close()
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

    with open(file_path, "w", encoding="utf-8", newline="") as f_handle:
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
        # Strings
        case "char":
            data_type = f"CHAR({divide_two(length)})"
        case "varchar":
            data_type = f"VARCHAR({divide_two(length)})"
        case "text":
            data_type = "TEXT"
        case "nchar":
            data_type = f"NCHAR({divide_two(length)})"
        case "nvarchar":
            data_type = f"NVARCHAR({divide_two(length)})"
        case "ntext":
            data_type = "NTEXT"
        case "binary":
            data_type = f"NVARCHAR(max)"
        case "varbinary":
            data_type = f"NVARCHAR(max)"
        case "image":
            data_type = "IMAGE"
        # numeric
        case "bit":
            data_type = "BIT"
        case "tinyint":
            data_type = "TINYINT"
        case "smallint":
            data_type = "SMALLINT"
        case "int":
            data_type = "INT"
        case "bigint":
            data_type = "BIGINT"
        case "decimal":
            data_type = f"DECIMAL({precision},{scale})"
        case "numeric":
            data_type = f"NUMERIC({precision},{scale})"
        case "smallmoney":
            data_type = "SMALLMONEY"
        case "money":
            data_type = "MONEY"
        case "float":
            data_type = f"FLOAT({length})"
        case "real":
            data_type = "REAL"
        # date and time
        case "datetime":
            data_type = "DATETIME2"
        case "datetime2":
            data_type = "DATETIME2"
        case "smalldatetime":
            data_type = "SMALLDATETIME"
        case "date":
            data_type = "DATE"
        case "time":
            data_type = "TIME"
        case "datetimeoffset":
            data_type = "DATETIMEOFFSET"
        case "timestamp":
            data_type = "TIMESTAMP"
        # OTHER
        case "sql_variant":
            data_type = "sql_variant"
        case "uniqueidentifier":
            data_type = "uniqueidentifier"
        case "xml":
            data_type = "xml"
        case "cursor":
            data_type = "cursor"
        case "table":
            data_type = "table"
        case _:
            data_type = "NVARCHAR(MAX)"

    return data_type


def divide_two(len: int):
    length = int(len)
    if length > 255 or length <= 0:
        length = "max"
    return f"{length}"


def maxxing(len: int):
    if len / 2 > 4000:
        return "(max)"
    return ""


def nulls(bools):
    if bools == False:
        return " NOT NULL"
    else:
        return ""


def primary(pk):
    if pk == True:
        return " PRIMARY KEY"
    else:
        return ""


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
