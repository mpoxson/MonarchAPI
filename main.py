from fastapi import FastAPI, Request, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse
from typing import List
import pyodbc
import pandas as pd
import os
import mysql.connector
import aiomysql # type: ignore

app = FastAPI()

html_form = """
<!DOCTYPE html>
<html>
<head>
    <title>Upload Files and Connect to Database</title>
</head>
<body>
    <h1>Upload Files and Connect to Database</h1>
    <form action="/upload-files/" method="post" enctype="multipart/form-data">
        <label>Server Name:</label><br>
        <input type="text" name="server_name"><br>
        <label>Database Name:</label><br>
        <input type="text" name="database_name"><br>
        <label>Username:</label><br>
        <input type="text" name="username"><br>
        <label>Password:</label><br>
        <input type="password" name="password"><br>
        <input type="file" name="files" multiple><br>
        <input type="submit" value="Upload and Connect">
    </form>
</body>
</html>
"""


DATABASE_CONFIG = {
    'user': 'admin',
    'password': 'Cupcake0923&',
    'host': 'monarchapi1.c3amcyc0kkom.us-east-2.rds.amazonaws.com',
    'database': 'monarchapi1',
    'port': 3306,
}

@app.get("/", response_class=HTMLResponse)
async def home():
    return html_form

@app.post("/upload-files/")
async def upload_files(request: Request, files: List[UploadFile] = File(...)):
    user_inputs = await request.form()
    driver = '{ODBC Driver 17 for SQL Server}'
    server_name = user_inputs["server_name"]
    database_name = user_inputs["database_name"]
    username = user_inputs["username"]
    password = user_inputs["password"]

    try:
        conn_str = (
            f"DRIVER={driver};SERVER={server_name};DATABASE={database_name};UID={username};PWD={password}"
        )

        with pyodbc.connect(conn_str) as conn:
            with conn.cursor() as cursor:
                for file in files:
                    with open(file.filename, "wb") as f:
                        f.write(file.file.read())

                    table_name = os.path.splitext(file.filename)[0]

                    df = pd.read_csv(file.filename)

                    create_table = f"CREATE TABLE [{table_name}] ("
                    for column_name in df.columns:
                        column_name_cleaned = column_name.replace(' ', '_').replace('-', '_').replace('.', '_')
                        create_table += f"[{column_name_cleaned}] NVARCHAR(255), "
                    create_table = create_table[:-2] + ");"
                    cursor.execute(create_table)

                    for index, row in df.iterrows():
                        placeholders = ",".join(["?"] * len(row))
                        columns = ",".join([f"[{col.replace(' ', '_').replace('-', '_').replace('.', '_')}]"
                                            for col in row.index])
                        insert_sql = f"INSERT INTO [{table_name}] ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_sql, tuple(row))
                    conn.commit()

        return {"message": f"{len(files)} tables created and data imported successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


