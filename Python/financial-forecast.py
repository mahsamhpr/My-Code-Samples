import datetime
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.identity import DefaultAzureCredential, AzureCliCredential
from azure.keyvault.secrets import SecretClient
import subprocess as sb
import tempfile
import os
import pandas as pd
import xlrd
import tempfile
from io import BytesIO
import re
from pathlib import Path
from typing import List

######################################################################
## Constants required for connecting to storage
######################################################################
SOURCE_STORAGE_ACCOUNT	= 'satsrdpmdpfilestore01'  # ${vj_sourceStorageAccountName}
SOURCE_CONTAINER_NAME	= 'sharepoint-landing-zone'  # ${vj_sourceContainerName}
SOURCE_SA_SECRET_NAME	= 'storage-account-access-key1-satsrdpmdpfilestore01'
SOURCE_FILE_PATTERN		= r'\d{6}.*forecast\.xlsx'

TARGET_CONTAINER_NAME   = 'raw-data-zone'  # ${vj_targetContainerName}
TARGET_STORAGE_ACCOUNT  = 'satsprdmdpdl'  # ${vj_targetStorageAccountName}
TARGET_SA_SECRET_NAME 	= 'storage-account-access-key1-satsprdmdpdl'
TARGET_FILE_NAME		= '/SharePoint/Forecasts/{forecast_file_name}/{sheet}/data.json'

VAULT_NAME = 'satsprdmdpkeys'
VAULT_URL = f"https://{VAULT_NAME}.vault.azure.net/"
######################################################################



######################################################################
## Constants required for transforming source data
######################################################################
# only keep these countries
RECOGNIZED_COUNTRIES = ['NO','SE','FI','DK']

# Metric sheets: rename columns according to this mapping
METRIC_DIM_COLUMN_NAME_MAPPING = {
        'CC': 'center_id',
        'Name': 'center_name',
        'Country': 'country_code', 
        'Business unit': 'business_unit',
        'UniqeID': 'unique_id',
        'Currency': 'currency',
        'Currency rate': 'currency_rate',
        'Account': 'account',
        'Club/OH': 'club_oh',
        'Current FC': 'current_fc'
    }

# These are the column names of the static dimensional columns that are always present, per sheet type
METRICS_RECOGNIZED_STATIC_COLUMNS = list(METRIC_DIM_COLUMN_NAME_MAPPING.values())

# create a list containing all distinct dimensional columns across all sheets
ALL_RECOGNIZED_STATIC_COLUMNS = METRICS_RECOGNIZED_STATIC_COLUMNS

# sheets to be extracted are defined here
METRIC_SHEETS = [
    'NMU',
    'Drop',
    'Freeze',
    'MemberBase_Avg.price',
    'Visits',
    'PT_compl._hours',
    'PT_price',
    'NMU_price',
    'Members_Ingoing_Balance',
    'MEMREV',
    'JoiningFee_Rev',
    'PTRev',
    'PTDirCost',
    'RetRev',
    'RetDirCost',
    'AncMemRev',
    'AncRev',
    'Revenue_Loss',
    'OtherDirCost',
    'ClubSalaries',
    'ClubJanitorial',
    'ClubRepairMaint',
    'ClubUtility',
    'ClubRent',
    'ClubIT',
    'ClubMarketing',
    'ClubOfficeSupplies',
    'ClubProffFees',
    'ClubOtherOpex',
    'Depreciations',
    'Allocations'
]
######################################################################





######################################################################
# Functions for interacting with blob storage
######################################################################
def get_access_key(secret_name):
    credential = DefaultAzureCredential()
    secret_client = SecretClient(vault_url=VAULT_URL, credential=credential)
    return secret_client.get_secret(secret_name).value

def get_container_client(storage_account, container, secret) -> ContainerClient:
    serviceClient = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=get_access_key(secret))
    containerClient: ContainerClient = serviceClient.get_container_client(container)
    return containerClient  

def get_blob_to_temp_local_file(blob, directory):
    sourceContainerClient = get_container_client(storage_account=SOURCE_STORAGE_ACCOUNT, container=SOURCE_CONTAINER_NAME, secret=SOURCE_SA_SECRET_NAME)
    tf = tempfile.NamedTemporaryFile(suffix=blob, delete=False, dir=directory)
    importstream = BytesIO()
    sourceContainerClient.download_blob(blob).download_to_stream(importstream)

    tf.write(importstream.getvalue())
    return tf.name


######################################################################
# Functions for parsing source excel sheets
######################################################################

##### Functions for parsing metric sheets

# remove rows that are irrelevant
def remove_unwanted_rows(df):
    return df.loc[df['country_code'].isin(RECOGNIZED_COUNTRIES)]
  
# remove columns that are irrelevant
def remove_unwanted_columns(df):   
    return df[df.columns & get_columns_to_keep(df)]

# list of dynamic columns (i.e. 1 column per month)
def get_dynamic_columns_to_keep(df):
    date_col_regex = r'[a-zA-Z]{3}-\d{2}'  # period cols are to be named as mmm-yy strings, not dates
    raw_cols = list(df.columns)
    filtered_cols = list()
    for col in raw_cols:
        if re.match(date_col_regex, col):
            filtered_cols.append(col)

    return filtered_cols

# ALL columns to keep
def get_columns_to_keep(df):
    all_columns = list()
    
    all_columns.extend(METRICS_RECOGNIZED_STATIC_COLUMNS)
    all_columns.extend(get_dynamic_columns_to_keep(df))
    return all_columns 

  
# metrics sheets: Pivot from 1 column per month(period) to 1 row per month
def pivot_dataframe(df):
    pivoted = df.melt(id_vars=METRICS_RECOGNIZED_STATIC_COLUMNS, var_name = "period")
    
    # make period as a string to simplify parsing
    # pivoted['period'] = pivoted['period'].dt.strftime('%Y-%m')
    
    return pivoted

# rename source columns according to the mapping
def rename_dim_columns(df: pd.DataFrame):
    return df.rename(columns=METRIC_DIM_COLUMN_NAME_MAPPING)


# any forceful transformation of columns types are defined here
def transform_columns(df: pd.DataFrame):
    # Straight casting is done via a dict
    casting = {
        'center_id': 'str'
    }
    df = df.astype(casting)

    # parse current-fc as a string to improve readability
    df['current_fc'] = df['current_fc'].dt.strftime('%Y-%m')
    
    # period columns are provided as norwegian MMM-YY.
    # so we transform to proper YYYY-MM strings
    replacements = {'mai': 'may', 'okt': 'oct', 'des': 'dec'}

    for i, j in replacements.items():
        df['period'] = df['period'].str.replace(i, j)

    df['period'] = pd.to_datetime(df['period'], format='%b-%y').dt.strftime('%Y-%m')
    

    return df


# validate the excel file with any assumptions we might have.
def validate_excel_file(dict_of_excel_dfs):
    errors=False
    actual_sheets = dict_of_excel_dfs.keys()
    for sheet in METRIC_SHEETS:
        if not sheet in actual_sheets:
            errors=True       
            print(f"ERROR: Sheet '{sheet}' is not present in excel file!")
        
    if errors:
        raise ValueError("Excel sheet has issues, aborting!")    
    else:
        print(f"Excel file at looks ok!\n\n ")


# generic function that takes a preformatted dataframe, writes it to blob storage as jsonlines
def _output_sheet(df, forecast_file_name, sheet, temp_dir, targetContainerClient):
    # make temporary local output file
    sheet_name_transformed = sheet.replace(' ', '_').replace('.', '_').lower()
    local_file_path = f'{temp_dir.name}/{sheet_name_transformed}.json'

    d = datetime.date.today()

    df['extract_date'] = d.strftime('%Y-%m-%d')
    df['metric_name'] = sheet_name_transformed
    df['forecast_file'] = forecast_file_name
    
    df.to_json(local_file_path, lines=True, orient='records')
    
    # move local file to data lake
    current_target_file = TARGET_FILE_NAME.format(forecast_file_name=forecast_file_name, sheet=sheet_name_transformed, year=d.year, month=d.month, day=d.day)
    with open(local_file_path, 'rb') as uploadfile:
    	targetContainerClient.upload_blob(name=current_target_file, data=uploadfile, overwrite=True)


# function for processing and storing sheets with financial metrics
def process_metric_sheet(input_df, forecast_file_name, sheet_name, temp_dir):
    targetContainerClient = get_container_client(storage_account=TARGET_STORAGE_ACCOUNT, container=TARGET_CONTAINER_NAME, secret=TARGET_SA_SECRET_NAME)
    print(f"Extracting and transforming sheet: {sheet_name}.. ", end="", flush=True)
    
    df = rename_dim_columns(input_df)
    df = remove_unwanted_rows(df)
    df = remove_unwanted_columns(df)
    df = pivot_dataframe(df)
    df = transform_columns(df)
    
    # write to storage
    _output_sheet(df, forecast_file_name, sheet_name, temp_dir, targetContainerClient)
    
    print("Done!")

def process_forecast_file(forecast_file_name: str):
    print(f"Processing forecast file: {forecast_file_name}.. ")

    # temp dir for json output
    temp_dir = tempfile.TemporaryDirectory()

    # copy from filestore to local file and validate
    local_file = get_blob_to_temp_local_file(forecast_file_name, temp_dir.name)

    # read exce file as dict of dfs, one df per sheet
    excel_df_dict = pd.read_excel(local_file, sheet_name = None)

    
    #local_file = f"{Path.home()}" + r"\SATS Group AB\SATS Business Analytics - DWH Inputs\Financial Forecasts\202108 SATS ASA forecast – Kopi.xlsx"
    validate_excel_file(excel_df_dict)

    # iterate through all metric sheets
    for sheet in METRIC_SHEETS:
        process_metric_sheet(excel_df_dict[sheet], forecast_file_name, sheet, temp_dir) 


    # Delete the temporary directory
    temp_dir.cleanup()

    print(f"Completed processing forecast file: {forecast_file_name}!\n")

def get_list_of_forecast_files() -> List[str]:
    sourceContainerClient = get_container_client(storage_account=SOURCE_STORAGE_ACCOUNT, container=SOURCE_CONTAINER_NAME, secret=SOURCE_SA_SECRET_NAME)
    forecast_files = [blob['name'] for blob in sourceContainerClient.list_blobs() if re.match(SOURCE_FILE_PATTERN, blob['name']) is not None]
    return forecast_files



######################################################################

# MAIN FLOW
def main(): 
    print("Processing forecast files..\n")

    for forecast_file in get_list_of_forecast_files():
        process_forecast_file(forecast_file)
    

    print("Done with all forecast files!\n")
  
main()


