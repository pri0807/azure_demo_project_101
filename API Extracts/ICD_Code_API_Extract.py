# Databricks notebook source
from pyspark.sql import SparkSession
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import current_date,lit
from pyspark.sql.types import StructType,StructField,StringType,DateType,BooleanType
from datetime import datetime

client_id="32d3e45d-e2ff-472b-bac7-5ca4402cea1c_5abe3b7b-b9f2-4080-a2a2-5062fb8299ff"
client_secret=dbutils.secrets.get("rcm-ind-secret-scope-dev","ICD-Client-Secret")
base_url = 'https://id.who.int/icd/'
auth_url='https://icdaccessmanagement.who.int/connect/token'
scope = 'icdapi_access'
grant_type = 'client_credentials'
payload = {'client_id': client_id, 
	   	   'client_secret': client_secret, 
           'scope': scope, 
           'grant_type': grant_type}
auth_response=requests.post(auth_url, data=payload,verify=False)
if auth_response.status_code != 200:
    raise Exception(f'Authentication failed with status code {auth_response.status_code}')
else:
    access_token=auth_response.json()['access_token']
headers = {
    'Authorization': f'Bearer {access_token}',
    'API-Version': 'v2',  # Add the API-Version header
    'Accept-Language': 'en',
}
def fetch_icd_code(url):
    response=requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f'Request failed with status code {response.status_code}-{response.text}')
    else:
        return response.json()
current_date=datetime.now().date()
def extract_codes(url):
    data=fetch_icd_code(url)
    codes=[]
    if 'child' in data:
        for child_url in data['child']:
            codes.extend(extract_codes(child_url))
    else:
        if 'code' in data and 'title' in data:
            codes.append({
                'icd_code':data['code'],
                'icd_code_type':'ICD-10',
                'code_descrption':data['title']['@value'],
                'inserted_date':current_date,
                'updated_date':current_date,
                'is_current_flag':True
            })
    return codes
root_url='https://id.who.int/icd/release/10/2019/A00-A09'
icd_codes=extract_codes(root_url)
schema=StructType([StructField('icd_code',StringType()),
                   StructField('icd_code_type',StringType()),
                   StructField('code_descrption',StringType()),
                   StructField('inserted_date',DateType()),
                   StructField('updated_date',DateType()),
                   StructField('is_current_flag',BooleanType())])
df=spark.createDataFrame(icd_codes,schema=schema)
df.write.format('parquet').mode('append').save('/mnt/bronze/icd_codes')                   
