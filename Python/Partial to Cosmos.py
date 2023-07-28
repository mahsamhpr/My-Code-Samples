###
# Variables are directly accessible: 
#   print (myvar)
# Updating a variable:
#   context.updateVariable('myvar', 'new-value')
# Grid Variables are accessible via the context:
#   print (context.getGridVariable('mygridvar'))
# Updating a grid variable:
#   context.updateGridVariable('mygridvar', [['list','of'],['lists','!']])
# A database cursor can be accessed from the context (Jython only):
#   cursor = context.cursor()
#   cursor.execute('select count(*) from mytable')
#   rowcount = cursor.fetchone()[0]
###

from genericpath import exists
from urllib import response
import pandas as pd
import os
import json
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import snowflake.connector


SNOWFLAKE_USER = 'sys_matillion'
SYS_MATILLION_PASSWORD_SECRET_NAME = 'Snowflake-ETL-Admin'
SNOWFLAKE_ACCOUNT='sats.west-europe.azure'
SNOWFLAKE_ROLE='ETL'

VAULT_NAME = 'satsprdmdpkeys'
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net/"
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


def upsert_Items():
    member_changed_data_sql = f"""
        select * from DM.RPT.V_MEMBER_SEGMENT_LIFECYCLE_COSMOSDB
        where To_be_Updated=1 
    """
    changed_member_data_df = pd.read_sql(member_changed_data_sql, fnuggis)['MEMBER_SEGMENT_SUMMARY']
     # Parse json strings to dict
    changed_member_data_parsed = [json.loads(x) for x in list(changed_member_data_df)]
    nof_rows = len(changed_member_data_parsed)

    for i in range(nof_rows):
        externalId = changed_member_data_parsed[i]['id']
        centerId = changed_member_data_parsed[i]['centerId']
        try:
            read_Item=container.read_item(item=externalId,partition_key=centerId)
            read_Item['lifecycle_tag'] = changed_member_data_parsed[i]['lifecycle_tag']
            read_Item['past_segment'] = changed_member_data_parsed[i]['past_segment']
            read_Item['segment'] = changed_member_data_parsed[i]['segment']
           # if( (changed_member_data_parsed[i]['segment'] != 'UNSEGMENTED') or (changed_member_data_parsed[i]['past_segment'] != 'UNSEGMENTED') ):
            read_Item['trend'] = changed_member_data_parsed[i]['trend']
            read_Item['lifecycle_start_date'] = changed_member_data_parsed[i]['lifecycle_start_date']
            read_Item['segment_start_date'] = changed_member_data_parsed[i]['segment_start_date']           
            read_Item['changeddate'] = changed_member_data_parsed[i]['changeddate']
            response = container.upsert_item(body=read_Item)
            #print('Upserted Item\'s Id is {0}'.format(response['id']))
        except CosmosResourceNotFoundError:
            response = container.upsert_item(body=changed_member_data_parsed[i])
            #print('Upserted Item\'s Id is {0}'.format(response['id']))            
            
 
upsert_Items()

