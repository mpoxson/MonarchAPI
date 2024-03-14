from fastapi import FastAPI
from azure.identity import ClientSecretCredential
from azure.mgmt.sql import SqlManagementClient

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello World"}



credentials = ClientSecretCredential(
    client_id='0df6f4af-a67f-4483-9a00-f84e3d584edf',
    client_secret='Z8b8Q~r7y5R-gbDFj.Mbhds6zCxk9Yk8p-8jldbs',
    tenant_id='965de81c-fc98-4c53-9901-41bd93e26e06'
)

sql_client = SqlManagementClient(credentials, 'a7265b57-0f06-4af4-bf0b-ef7c691f13ba')

poller = sql_client.databases.begin_export(
    database_name='monarch',
    server_name='monarchserver',
    resource_group_name='MonarchGroup',
    parameters={
        # 'storage_key_type': 'StorageAccessKey',
        # 'storage_key': '<your storage key>',
        #'storage_uri': 'https://<your storage account>.blob.core.windows.net/<your blob container>/<name of output file>.bacpac',
        'administrator_login': '',
        'administrator_login_password': '',
        'authentication_type': 'SQL'
    }
)

print(poller.result())