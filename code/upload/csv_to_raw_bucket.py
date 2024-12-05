#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: danielaldehneh
"""

import os

# Change the working directory to the one you want
os.chdir('/Users/danielaldehneh/professionai/data_engineering/files/')

import boto3
from botocore.exceptions import NoCredentialsError

#Setting up the needed variables, namely the name of the bucket and 
#the name of the files that I'm trying to upload on AWS S3

bucket_name="raw-bucket-dani"

files=["BTC_EUR_Historical_Data.csv","google_trend_bitcoin.csv",
       "XMR_EUR Kraken Historical Data.csv","google_trend_monero.csv"]

#Setting up the "client" class item using boto3 
client = boto3.client("s3",region_name="eu-north-1",aws_access_key_id="xxx",
                      aws_secret_access_key="xxx")


#Very simple loop I use to rotate through the files uploading them one by one
#with exceptions to help me understand what kind of error it is

for file in files:
    try:
        client.upload_file(file,bucket_name,file)
        print(f"{file} was uploaded with success")
    except FileNotFoundError:
        print(f"{file} not found.")
    except NoCredentialsError:
        print("Credentials not available.")
        