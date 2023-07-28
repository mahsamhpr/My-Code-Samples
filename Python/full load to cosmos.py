

from genericpath import exists
from urllib import response
import pandas as pd
import os
import json
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import snowflake.connector


SNOWFLAKE_USER = 'sys_matillion'
SYS_MATILLION_PASSWORD_SECRET_NAME = 'Snowflake-ETL-Admin'
SNOWFLAKE_ACCOUNT='sats.west-europe.azure'
SNOWFLAKE_ROLE='ETL'

VAULT_NAME = 'satsprdmdpkeys'
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net/"
#vj_tmp_jsfile = "C:\MyAzureRepo\BulkImport.js"
#vj_cosmosdbEnv = 'test'



def get_secret(secret_name):
  credential = DefaultAzureCredential(exclude_shared_token_cache_credential=True)
  secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)
  return secret_client.get_secret(secret_name).value



fnuggis = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=get_secret(SYS_MATILLION_PASSWORD_SECRET_NAME),
    account=SNOWFLAKE_ACCOUNT,
    role=SNOWFLAKE_ROLE
)


############################################################################################################################
# We can import to either test or prod cosmos container, by setting the vj_cosmosdbEnv variable to test or prod
############################################################################################################################

connection_info = {
    'test': {
        'url': 'https://hfnptdb.documents.azure.com:443/',
        'key': get_secret('YEARINREVIEW-COSMOSDB-KEY-TEST'),
        'database_name': 'hfnDBProto',
        'container_name': 'dwhmembersegmentlifecycle' 
    },
    'prod': {
        'url': 'https://hfnptdbprod.documents.azure.com:443/',
        'key': get_secret('YEARINREVIEW-COSMOSDB-KEY-PROD'),
        'database_name': 'hfnDBProd',
        'container_name': 'dwhmembersegmentlifecycle' 
    }
}

ENV = vj_cosmosdbEnv


url = connection_info[ENV]['url']
key = connection_info[ENV]['key']
client = CosmosClient(url, credential=key)
database_name = connection_info[ENV]['database_name']
database = client.get_database_client(database_name)
container_name = connection_info[ENV]['container_name']
container = database.get_container_client(container_name)


############################################################################################################################
# Create stored procedure for bulk import
############################################################################################################################
# Snatched from [here](https://github.com/Azure/azure-cosmosdb-js-server/blob/master/samples/stored-procedures/BulkImport.js).
# Modified  [Line 32](https://github.com/Azure/azure-cosmosdb-js-server/blob/master/samples/stored-procedures/BulkImport.js#L32) to `UpsertDocument`
############################################################################################################################
def create_stored_procedure():
  stored_procedure_id = 'BulkImport'

  with open(vj_tmp_jsfile) as file:
      file_contents = file.read()

  sproc = {
      'id': stored_procedure_id,
      'serverScript': file_contents,
  }

  try:
      created_sproc = container.scripts.create_stored_procedure(body=sproc)

  except CosmosResourceExistsError:
      print("Stored Procedure already exists, deleting and creating..")
      container.scripts.delete_stored_procedure(sproc=stored_procedure_id)      
      created_sproc = container.scripts.create_stored_procedure(body=sproc)
    

############################################################################################################################    
# Use the bulk import
############################################################################################################################
# Each bulk needs to have the same partition key (centerId), this is a requirement from cosmosDB. 
# So we itereate through centers one by one, to ensure that all bulks have the same center id.
# Within the center iterations we can upsert `BULK_SIZE` number of items per call to the stored procedure.
############################################################################################################################


BULK_SIZE = 100
LIMIT_PER_CENTER = 100000


# iterator that gives us a part (list of size "chunk_size") of the list each iteration
def chunk_list(list_to_chunk, chunk_size):
    """Yield successive chunk_size-sized chunks from list_to_chunk."""
    for i in range(0, len(list_to_chunk), chunk_size):
        yield list_to_chunk[i:i + chunk_size]

        
def get_center_list():
    all_centers_sql = """
   select distinct  member_segment_summary:"centerId"::string as CENTER_ID from dm.rpt.V_MEMBER_SEGMENT_LIFECYCLE_COSMOSDB
    """
    df = pd.read_sql(all_centers_sql, fnuggis)
    
    return list(df['CENTER_ID'])


# takes a given center id, fetches data from Segment LifeCycle dataset and does bulk upsert to cosmos
# bulk is done by utilizing the stored procedure
def bulk_import_center(center_id):
    bulkimport_sp = container.scripts.get_stored_procedure('BulkImport')


    if center_id is None:
        raise ValueError()


    member_data_sql = f"""
        select * from dm.rpt.V_MEMBER_SEGMENT_LIFECYCLE_COSMOSDB
        where  member_segment_summary:"centerId"::string = '{center_id}'
        limit {LIMIT_PER_CENTER}
    """
    center_member_data_df = pd.read_sql(member_data_sql, fnuggis)['MEMBER_SEGMENT_SUMMARY']       
    
    # Parse json strings to dict
    center_member_data_parsed = [json.loads(x) for x in list(center_member_data_df)]
    
    nof_rows = len(center_member_data_parsed)
    
    # iterate over the entire member dataset, in chunks.   
    for chunk in chunk_list(center_member_data_parsed, BULK_SIZE):
      result = container.scripts.execute_stored_procedure(sproc=bulkimport_sp, params=[chunk], partition_key=center_id)
      if result != len(chunk):
        raise ValueError("Something went wrong! Number of items in result does not equal with the number of items in the chunk")       


def process_all_centers():
  
    create_stored_procedure()
       
    centers = get_center_list()
    
    print(f"🚀  {len(centers)} centers found. Processing..")
    nof_centers = len(centers)

    # Go through centers 1 by 1 and bulk import to cosmos
    for center in centers:
        bulk_import_center(center)
      

    print(f"✅  All done!")

process_all_centers()


