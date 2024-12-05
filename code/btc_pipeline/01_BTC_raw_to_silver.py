#Python Script that processes input datasets "BTC_EUR_Historical_Data" and "google_trend_bitcoin" by 
# 1) Standardizing Date formats and column names
# 2) Handling missing prices in BTC data where `Price = -1` filling them with previous valid observation
# 3) Saves processed data into saved on silver-bucket-dani

import sys
import boto3
import pandas as pd
import os
from io import StringIO
from datetime import datetime

# Creates bucket and file names definition for further use in functions
input_bucket = "raw-bucket-dani"
output_bucket = "silver-bucket-dani"
input_file_btc = "BTC_EUR_Historical_Data.csv"
input_file_google = "google_trend_bitcoin.csv"

# Initializing the boto3 S3 client
s3 = boto3.client('s3')

# Creating a custom loader function to read CSV data from S3 bucket and load it into a DataFrame
def load_csv_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    csv_data = response['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(csv_data))

# Loading the BTC and Google Trends data
df_btc = load_csv_from_s3(input_bucket, input_file_btc)
df_google = load_csv_from_s3(input_bucket, input_file_google)

# Standardizing the date formats in both dataframes to ensure consistency
df_btc['Date'] = pd.to_datetime(df_btc['Date'], format='%m/%d/%Y', errors='coerce').dt.strftime('%d/%m/%Y')
df_google['Date'] = pd.to_datetime(df_google['Settimana'], format='%Y-%m-%d', errors='coerce').dt.strftime('%d/%m/%Y')

# Dropping the original `Settimana` column in Google data as we now have `Date`
df_google = df_google.drop(columns=['Settimana'])

# Handle missing prices in BTC data where `Price = -1`, filling with previous valid observation
# This function uses `ffill` (Pandas: "Fill NA/NaN values by propagating the last valid observation")
# Also handles cases where the first value may be -1 by backfilling
def replace_invalid_prices(df, price_column):
    df[price_column] = df[price_column].mask(df[price_column] == -1).ffill()
    df[price_column] = df[price_column].fillna(method='bfill')
    return df

# Applying the invalid price handling to BTC data
df_btc = replace_invalid_prices(df_btc, 'Price')

# Creation of a custom function to save a DataFrame in Parquet format to a temporary local file and upload to S3...
# This avoids using BytesIO, saving directly to disk
def save_df_to_parquet_and_upload(df, bucket, key, temp_filename):
    # First we save DataFrame as a Parquet file locally
    df.to_parquet(temp_filename, index=False)
    # Then we upload the file to S3
    s3.upload_file(Filename=temp_filename, Bucket=bucket, Key=key)
    # Finally we remove the temporary file after upload
    os.remove(temp_filename)

# Defining temporary file paths for BTC and Google Trends Parquet files
temp_file_btc = "/tmp/BTC_EUR_Historical_Data.parquet"
temp_file_google = "/tmp/google_trend_bitcoin.parquet"

# Saving BTC and Google data to S3 in Parquet format
output_file_btc = "BTC_EUR_Historical_Data.parquet"
output_file_google = "google_trend_bitcoin.parquet"
save_df_to_parquet_and_upload(df_btc, output_bucket, output_file_btc, temp_file_btc)
save_df_to_parquet_and_upload(df_google, output_bucket, output_file_google, temp_file_google)

print("ETL job BTC raw to silver completed successfully and data saved on silver-bucket-dani.")
