from fastapi import FastAPI, UploadFile, File, HTTPException
import pyodbc
import csv
from io import StringIO
import os
import pandas as pd

app = FastAPI()

driver = '{ODBC Driver 17 for SQL Server}'
server = 'server-sampledb.database.windows.net'
database = 'sample-db'
username = 'admin123'
password = 'admin!123'
connection_string = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/azure-connection")
def azure_connection():
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        cursor.execute("SELECT @@version;")
        row = cursor.fetchone()
        return {"Message": "Connection successful", "Version": row[0]}

    except pyodbc.Error as error:
        return {"message": f"Error connecting to Azure SQL Database: {error}"}

    finally:
        if conn:
            conn.close()

@app.get("/import-iris")
def import_iris():
    try:
        file_name = 'iris.csv'
        table_name = os.path.splitext(file_name)[0]

        df = pd.read_csv(file_name)

        conn = pyodbc.connect(connection_string)
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