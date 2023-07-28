
# from dotenv import load_dotenv
import pandas as pd
from azure.cosmos import CosmosClient
import os
from typing import Dict, List
from azure.storage.blob import ContainerClient
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient

######################################################################
# Constants required for connecting to storage
######################################################################
COSMOSDB_DATABASE_NAME = 'hfnDBProd'
COSMOSDB_ACCOUNT_URI = "https://hfnptdbprod.documents.azure.com:443/"


TARGET_CONTAINER_NAME = 'raw-data-zone'
TARGET_STORAGE_ACCOUNT = 'satsprdmdpdl'
TARGET_SA_SECRET_NAME = 'storage-account-access-key1-satsprdmdpdl'
TARGET_FOLDER = '/ServicePlatform/CosmosDB/{container_name}/'
TARGET_FILE_NAME = 'data.json.gz'

VAULT_NAME = 'satsprdmdpkeys'
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net/"
######################################################################

######################################################################
# Functions for interacting with blob storage
######################################################################


def get_secret(secret_name):
    credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
    secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)
    return secret_client.get_secret(secret_name).value


def get_container_client() -> ContainerClient:
    return ContainerClient(
        account_url=f"https://{TARGET_STORAGE_ACCOUNT}.blob.core.windows.net", container_name=TARGET_CONTAINER_NAME, credential=get_secret(TARGET_SA_SECRET_NAME))


def upload_local_file_to_blob(local_file, target_file):
    print(f"Uploading {local_file} to {target_file}")
    cc = get_container_client()

    with open(local_file, 'rb') as data:
        cc.upload_blob(name=target_file, data=data, overwrite=True)


def get_cosmosdb_container_client(container_name: str):
    key = get_secret('serviceplatform-cosmosdb-read-key')
    
    cosmos_client = CosmosClient(COSMOSDB_ACCOUNT_URI, credential=key)
    database = cosmos_client.get_database_client(COSMOSDB_DATABASE_NAME)
    container = database.get_container_client(container_name)

    return container

######################################################################
# Functions for interacting with cosmosdb
######################################################################

def query_container(container_name: str, previous_max_ts: int=None) -> List[Dict]:
    container = get_cosmosdb_container_client(container_name)

    query = f'SELECT * FROM {container_name} r'
    if previous_max_ts is not None:
        query += f' where r._ts > {previous_max_ts}'
    
    print(query)

    items = container.query_items(
        query=query,
        enable_cross_partition_query=True
    )

    items_paged = items.by_page()

    all_results = list()
    
    while True:
        try:
            paged_data = list(items_paged.next())
            all_results.extend(paged_data)
        except StopIteration:
            break

 
    return all_results

def results_to_blob(container_name: str, data: List[Dict]) -> None:
    df = pd.DataFrame(data)

    local_folder = f".{TARGET_FOLDER}".format(container_name=container_name)

    if not os.path.exists(local_folder):
        os.makedirs(local_folder)

    local_file = f'{local_folder}/{TARGET_FILE_NAME}'

    df.to_json(local_file, lines=True, orient='records')

    """ 
    local_file has a "." in front. it's a relative path, pointing to the local filesystem on the matillion server, where we save the file temporarily. 
    target_file describes the path where we upload the file on the storage account container.
    when it's uploaded, we can delete it from matillion server. psa reads from storage account
    """
    
    upload_local_file_to_blob(
        local_file=local_file, target_file=f"{TARGET_FOLDER}/{TARGET_FILE_NAME}".format(container_name=container_name)
    )
    
    os.remove(local_file)

    
print(vj_PrevMaxTS)
data = query_container(vj_ContainerName, vj_PrevMaxTS)
results_to_blob(vj_ContainerName, data)
