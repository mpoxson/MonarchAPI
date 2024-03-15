import os
import pyodbc, struct
from azure import identity

from fastapi import FastAPI
import pandas as pd
from datetime import datetime
from fastapi.responses import FileResponse

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}

#Need parameters for database name, server
AZURE_SQL_CONNECTIONSTRING='Driver={ODBC Driver 18 for SQL Server};Server=tcp:monarchserver.database.windows.net,1433;Database=monarch;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30'

@app.get("/tables")
def get_tables():
    output = ""
    column = []
    with get_conn() as conn:
        cursor = conn.cursor()
        cursor.execute("select schema_name(t.schema_id) as schema_name, t.name as table_name, t.create_date, t.modify_date from sys.tables t order by schema_name, table_name;")

        
        gang = (cursor.fetchall())
        for tupler in cursor.description:
            #get names of columns (first value in tuple)
            column.append(tupler[0])
            
        gang.insert(0, column)
        df = pd.DataFrame(gang)
        df.to_csv('filename.csv', index=False)
        print(len(gang))

        #find file path and dynamically change string
        output = "File has been saved to Downloads "


    return output

def get_conn():
    credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
    token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
    token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
    SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
    conn = pyodbc.connect(AZURE_SQL_CONNECTIONSTRING, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
    return conn