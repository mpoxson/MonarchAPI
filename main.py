from fastapi import FastAPI, File, UploadFile
import os
import mysql.connector
import pandas as pd
from io import StringIO

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

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...)):
    if file.filename.endswith('.csv'):
        contents = await file.read()

        # Define the directory path where you want to save the file
        directory_path = "./uploaded_files"

        # Check if the directory exists, and create it if it doesn't
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        
        # Define the full file path
        file_path = f"{directory_path}/{file.filename}"
        
        # Write the contents to a new file
        with open(file_path, "wb") as f:
            f.write(contents)
        
        # Optionally, you can now read the file into a pandas DataFrame
        # This is optional and depends on your further processing needs
        df = pd.read_csv(StringIO(contents.decode('utf-8')))
        
        return {"message": "CSV processed and saved successfully"}
    else:
        return {"error": "Please upload a CSV file."}
    
#Need parameters for database name, server
AZURE_SQL_CONNECTIONSTRING=''

@app.get("/tables/{server_name}/{database_name}")
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

@app.get("/aws-connection/")
def aws_connection():

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

        
    cursor.execute("SELECT 1")
    return {"Message": "Connection successful"}

    #except mysql.Error as error:
        #return {"message": f"Error connecting to MySQL database on AWS RDS: {error}"}

@app.get("/schemas/{server_name}/{database_name}")
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